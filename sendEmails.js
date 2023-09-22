const fs = require('fs');
const readline = require('readline');

const { parse } = require('csv-parse/sync');
const sendgridClient = require('@sendgrid/client');
const sendgridMail = require('@sendgrid/mail');
const { htmlToText } = require('html-to-text');
const { default: confirm } = require('@inquirer/confirm');
const dotenv = require('dotenv');

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

  // Collect the filenames
  const configFileName = process.argv[2];
  const csvFileName = process.argv[3];
  const templateFileName = process.argv[4];
  if (!configFileName || !csvFileName || !templateFileName) {
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

  if (await confirm({ message: `Send ${recordCount} email(s)?`, default: false })) {
    console.log('Running.');
  } else {
    console.log('Exiting without action');
    return;
  }

  // Configure SendGrid
  sendgridMail.setApiKey(process.env.SENDGRID_API_KEY);

  // Loop through and send the emails
  const csvFileContents = parse(fs.readFileSync(csvFileName).toString());
  for (let i = 1; i <= recordCount; i++) {

    // Throttle
    // await new Promise((resolve) =>setTimeout(resolve, 500));

    const currentRecord = csvFileContents[i];
    const emailId = await sendOneEmail({
      from: config.FROM_ADDRESS,
      fromName: config.FROM_NAME,
      to: currentRecord[emailColumnIndex],
      subject: replaceTokens(config.SUBJECT_LINE, columnNames, currentRecord),
      htmlMessage: replaceTokens(templateFileContents, columnNames, currentRecord),
    });
    if (emailId) {
      console.log(`✅ (${i}/${recordCount}) Message successfully sent to ${currentRecord[emailColumnIndex]} (ID: ${emailId})`);
    } else {
      console.error(`⛔ (${i}/${recordCount}) Message failed to ${currentRecord[emailColumnIndex]}`);
    }
  }
};

const replaceTokens = (input, keys, values) => {
  return input.replace(/@[a-zA-Z0-9_]*@/g, (match) => values[keys.indexOf(match.slice(1, -1))]);
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
