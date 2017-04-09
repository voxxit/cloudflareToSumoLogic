const sumoURL = process.env.SUMO_ENDPOINT;
const zoneID = process.env.CLOUDFLARE_ZONE_ID;
const cloudflareAuthEmail = process.env.CLOUDFLARE_AUTH_EMAIL;
const cloudflareAuthKey = process.env.CLOUDFLARE_AUTH_KEY;
const sourceCategoryOverride = process.env.SOURCE_CATEGORY_OVERRIDE || 'none';
const sourceHostOverride = process.env.SOURCE_HOST_OVERRIDE || 'api.cloudflare.com';
const sourceNameOverride = process.env.SOURCE_NAME_OVERRIDE || zoneID;

const https = require('https');
const url = require('url');

function sumoMetaKey() {
  let sourceCategory = '';
  let sourceName = '';
  let sourceHost = '';

  if (sourceCategoryOverride !== null && sourceCategoryOverride !== '' && sourceCategoryOverride !== 'none') {
    sourceCategory = sourceCategoryOverride;
  }

  if (sourceHostOverride !== null && sourceHostOverride !== '' && sourceHostOverride !== 'none') {
    sourceHost = sourceHostOverride;
  }

  if (sourceNameOverride !== null && sourceNameOverride !== '' && sourceNameOverride !== 'none') {
    sourceName = sourceNameOverride;
  }

  return `${sourceName}:${sourceCategory}:${sourceHost}`;
}

function postToSumo(context, messages) {
  const messagesTotal = Object.keys(messages).length;
  const messageErrors = [];

  let messagesSent = 0;

  const urlObject = url.parse(sumoURL);
  const options = {
    hostname: urlObject.hostname,
    path: urlObject.pathname,
    method: 'POST',
  };

  function finalizeContext() {
    const total = messagesSent + messageErrors.length;

    if (total === messagesTotal) {
      // console.log('messagesSent: ' + messagesSent + ' messagesErrors: ' + messageErrors.length);

      if (messageErrors.length > 0) {
        context.fail(`errors: ${messageErrors}`);
      } else {
        context.succeed();
      }
    }
  }

  Object.keys(messages).forEach((key) => {
    const headerArray = key.split(':');

    options.headers = {
      'X-Sumo-Name': headerArray[0],
      'X-Sumo-Category': headerArray[1],
      'X-Sumo-Host': headerArray[2],
    };

    const req = https.request(options, (res) => {
      res.setEncoding('utf8');
      res.on('data', () => {});
      res.on('end', () => {
        if (res.statusCode === 200) {
          messagesSent += 1;
        } else {
          messageErrors.push(`HTTP Return code ${res.statusCode}`);
        }

        finalizeContext();
      });
    });

    req.on('error', (e) => {
      messageErrors.push(e.message);

      finalizeContext();
    });

    messages[key].forEach(msg => req.write(`${JSON.stringify(msg)}\n`));

    req.end();
  });
}

exports.handler = (event, context) => {
  // Used to hold arrays of logs per zone I to post to SumoLogic
  const messageList = {};

  // Validate Sumo Logic URL has been set
  const urlObject = url.parse(sumoURL);

  if (urlObject.protocol !== 'https:' || urlObject.host === null || urlObject.path === null) {
    context.fail(`Invalid SUMO_ENDPOINT environment variable: ${sumoURL}`);
  }

  // Logs are delayed by 30 minutes...
  const endTime = new Date(new Date() - (30 * 60 * 1000));

  // Set exact time
  endTime.setSeconds(0);
  endTime.setMilliseconds(0);

  const startTime = new Date(endTime - (1 * 60 * 1000));

  // console.log("startTime:", startTime);
  // console.log("endTime:",   endTime);

  const cloudflareOpts = {
    method: 'GET',
    hostname: 'api.cloudflare.com',
    path: `/client/v4/zones/${zoneID}/logs/requests?start=${Math.floor(startTime) / 1000}&end=${Math.floor(endTime) / 1000}`,
    headers: {
      'X-Auth-Email': cloudflareAuthEmail,
      'X-Auth-Key': cloudflareAuthKey,
    },
  };

  // console.log("cloudflareOpts:", cloudflareOpts);

  const req = https.request(cloudflareOpts, (res) => {
    // console.log("res.statusCode: ", res.statusCode);
    // console.log("res.headers: ", res.headers);

    if (res.statusCode === 204) {
      return context.succeed();
    }

    let str = '';

    res.on('data', (chunk) => {
      str += chunk;
    });

    res.on('end', () => {
      const logs = str.split('\n');

      // console.log('Log events: ' + logs.length);

      logs.forEach((log) => {
        if (log === '') { return; }

        const parsedLog = JSON.parse(log);

        // Sumo Logic only supports 13 digit epoch time; convert original timestamp
        // to a JSON Date object
        parsedLog.timestamp /= 1000000;

        if (parsedLog.cache) {
          if (parsedLog.cache.startTimestamp && parsedLog.cache.startTimestamp !== null) {
            parsedLog.cache.startTimestamp /= 1000000;
          }

          if (parsedLog.cache.endTimestamp && parsedLog.cache.endTimestamp !== null) {
            parsedLog.cache.endTimestamp /= 1000000;
          }
        }

        if (parsedLog.edge) {
          if (parsedLog.edge.startTimestamp && parsedLog.edge.startTimestamp !== null) {
            parsedLog.edge.startTimestamp /= 1000000;
          }

          if (parsedLog.edge.endTimestamp && parsedLog.edge.endTimestamp !== null) {
            parsedLog.edge.endTimestamp /= 1000000;
          }
        }

        const metadataKey = sumoMetaKey();

        if (metadataKey in messageList) {
          messageList[metadataKey].push(parsedLog);
        } else {
          messageList[metadataKey] = [parsedLog];
        }
      });

      // Push messages to Sumo
      postToSumo(context, messageList);
    });

    return true;
  });

  req.on('error', e => context.fail(e));
  req.end();

  return true;
};
