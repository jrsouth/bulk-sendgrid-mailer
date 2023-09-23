const fs = require('fs');
const readline = require('readline');

const { parse } = require('csv-parse/sync');
const sendgridClient = require('@sendgrid/client');
const sendgridMail = require('@sendgrid/mail');
const { htmlToText } = require('html-to-text');
const { default: confirm } = require('@inquirer/confirm');
const dotenv = require('dotenv');

const defaults = {
  throttle: 0, // ms to pause between batches
  concurrency: 1, // messages to send per batch
};

// Collect env vars
dotenv.config();

const init = async () => {

  // ----- Set-up and basic sanity checks -----

  // Check we're in a node context
  if (!process) {
    throw new Error('This script must be run via the node CLI');
  }

  // Check we have an API key
  if (!process.env.SENDGRID_API_KEY) {
    throw new Error('No SENDGRID_API_KEY env variable found');
  }

  // Test the key
  sendgridClient.setApiKey(process.env.SENDGRID_API_KEY);
  try {
    const [sendgridTestResponse, sendgridTestBody] = await sendgridClient.request({ method: 'GET', url: '/v3/api_keys' });
    if (sendgridTestResponse.statusCode !== 200) {
      throw new Error(`SendGrid API key test failed`);
    }
  } catch (e) {
    throw new Error(`SendGrid API key test failed`);
  }

  // Configure the SendGrid mailer with the tested and working key
  sendgridMail.setApiKey(process.env.SENDGRID_API_KEY);

  // Collect the filenames
  // ...Either supplied as a single name parameter, which will then look for the defaults:
  //  - config/name.env
  //  - data/name.csv
  //  - templates/name.html
  //
  // ...OR all three files individually specified in order
  let configFileName = process.argv[2];
  let csvFileName = process.argv[3];
  let templateFileName = process.argv[4];
  if (configFileName && !csvFileName && !templateFileName) {
    const filenameBase = configFileName;
    configFileName = `config/${filenameBase}.env`;
    csvFileName = `data/${filenameBase}.csv`;
    templateFileName = `templates/${filenameBase}.html`;
  }
  else if (!configFileName || !csvFileName || !templateFileName) {
    throw new Error('Must provide config file, CSV file, and template file as first three arguments');
  }

  // Check that our files look okay
  if (!fs.existsSync(configFileName)) {
    throw new Error('config file could not be found');
  }
  if (!fs.existsSync(csvFileName)) {
    throw new Error('csv file could not be found');
  }
  if (!fs.existsSync(templateFileName)) {
    throw new Error('template file could not be found');
  }

  // Check configuration
  const configFileContents = fs.readFileSync(configFileName);
  const config = dotenv.parse(configFileContents);

  if (!config.SUBJECT_LINE || !config.FROM_NAME || !config.FROM_ADDRESS) {
    throw new Error('Config file must contain: SUBJECT_LINE, FROM_NAME, FROM_ADDRESS');
  }

  const throttle = config.THROTTLE || defaults.throttle;
  const concurrency = config.CONCURRENCY || defaults.concurrency;

  // Check CSV
  const { columnNames, recordCount } = await getCsvInfo(csvFileName);
  const emailColumnIndex = columnNames.indexOf('Email');

  if (emailColumnIndex === -1) {
    throw new Error('CSV file does not have a column matching the expected name of "Email"');
  }

  // Check Template
  const templateFileContents = fs.readFileSync(templateFileName).toString();
  const tokensFoundInTemplate = (config.SUBJECT_LINE + templateFileContents).match(/@[a-zA-Z0-9_]*@/g);

  const usedTokens = [];
  const unavailableTokens = [];
  tokensFoundInTemplate.forEach((tokenName) => {
    if (columnNames.indexOf(tokenName.slice(1, -1)) === -1) {
      unavailableTokens.push(tokenName);
    } else {
      usedTokens.push(tokenName);
    }
  });

  if (unavailableTokens.length) {
    throw new Error(`Some tokens used in the template file or subject line are not available in the CSV: ${unavailableTokens.join(', ')}`);
  }

  // Show what we're working with
  // @TODO: Make more nicer
  console.log(config);
  console.log({ columnNames });
  console.log({ recordCount });
  console.log({ usedTokens });
  console.log({ throttle });
  console.log({ concurrency });

  if (await confirm({ message: `Send ${recordCount} email(s)?`, default: false })) {
    console.log('Running...');
  } else {
    console.log('Exiting without action');
    return;
  }

  // Loop through and send the emails
  const csvFileContents = parse(fs.readFileSync(csvFileName).toString());
  for (let i = 1; i <= recordCount; i += concurrency) { // i = 1 to skip header row

    console.log(`Starting batch`);

    // Throttle between batches
    if (throttle) {
      console.log(`Throttling for ${throttle}ms`);
      await new Promise((resolve) =>setTimeout(resolve, throttle));
    }

    const addressArray = [];
    const responseArray = [];

    for (let j = 0; j < concurrency; j++) {
      const currentRecord = csvFileContents[i + j];
      if (currentRecord) {
        addressArray.push(currentRecord[emailColumnIndex]);
        responseArray.push(sendOneEmail({
          from: config.FROM_ADDRESS,
          fromName: config.FROM_NAME,
          to: currentRecord[emailColumnIndex],
          subject: replaceTokens(config.SUBJECT_LINE, columnNames, currentRecord),
          htmlMessage: replaceTokens(templateFileContents, columnNames, currentRecord),
        }));
      } else {

      }
    }

    // Wait until this batch is done
    await Promise.all(responseArray);

    responseArray.forEach((value, index) => {
      if (value) {
        console.log(`✅ (${i + index}/${recordCount}) Message successfully sent to ${addressArray[index]} (ID: ${value})`);
      } else {
        console.error(`⛔ (${i + index}/${recordCount}) Message failed to ${addressArray[index]}`);
      }
    });
  }
};

const replaceTokens = (input, keys, values) => {
  return input.replace(/@[a-zA-Z0-9_]*@/g, (match) => values[keys.indexOf(match.slice(1, -1))] || match);
};

// Send one email
const sendOneEmail = async (config) => {

  const { from, fromName, to, subject, htmlMessage, plaintextMessage } = config;

  const recipients = Array.isArray(to) ? to : [to];

  const messageObject = {
    to: recipients,
    from: {
      email: from,
      name: fromName,
    },
    subject,
    text: plaintextMessage || htmlToText(htmlMessage),
    html: htmlMessage,
  };

  try {
    const [response, reject] = await sendgridMail.send(messageObject);

    if (reject || response.statusCode !== 202) {
      return false;
    }

    return response.headers['x-message-id'];
  } catch {
    return false;
  }

};

// Count lines in a file and get column names from first line
// From https://stackoverflow.com/a/41439945
// and https://stackoverflow.com/a/28749643
function getCsvInfo(filePath) {
  return new Promise((resolve, reject) => {
    let recordCount = 0;
    const columnNames = [];
    fs.createReadStream(filePath)
      .on("data", (buffer) => {
        if (!columnNames.length) {
          // We assume the header row fits in the first chunk
          // Otherwise we'll have to get clever with an accumulator
          const headerRow = buffer.slice(0, buffer.indexOf(10)).toString();
          columnNames.push(...parse(headerRow, {})[0])
        }
        let idx = -1;
        recordCount--; // Because the loop will run once for idx=-1
        do {
          idx = buffer.indexOf(10, idx + 1);
          recordCount++;
        } while (idx !== -1);
      }).on("end", () => {
        resolve({ columnNames, recordCount: recordCount - 1 });
      }).on("error", reject);
  });
};

// ----- Run it all -----
init().then(() => { console.log('DONE'); });
