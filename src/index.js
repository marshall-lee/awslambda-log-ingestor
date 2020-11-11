const fs = require('fs-extra');
const http = require('http');
const path = require('path');
const performance = require('perf_hooks').performance;
const process = require('process');
const CloudWatchLogs = require('aws-sdk/clients/cloudwatchlogs');

const extensionName = path.basename(__filename);
const extensionsAPI = `http://${process.env.AWS_LAMBDA_RUNTIME_API}/2020-01-01/extension`;

function sleep(timeout) {
  return new Promise(resolve => setTimeout(resolve, timeout));
}

function extensionApiRequest(path, options, body) {
  return new Promise((resolve, reject) => {
    const req = http.request(`${extensionsAPI}/${path}`, options, res => {
      if (res.statusCode != 200) {
        console.log(res);
        res.resume();
        reject(new Error(`Got ${res.statusCode} response`));
        return;
      }
      let rawData = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { rawData += chunk; });
      res.on('end', () => {
        resolve({ headers: res.headers, body: JSON.parse(rawData) });
      });
    });
    req.on('error', reject);
    if (body !== undefined) {
      req.write(JSON.stringify(body));
    }
    req.end();
  });
}

async function register() {
  const { headers } = await extensionApiRequest(
    'register',
    {
      method: 'post',
      headers: {
        'Content-Type': 'application/json',
        'Lambda-Extension-Name': extensionName,
      },
    },
    {
      'events': ['INVOKE', 'SHUTDOWN'],
    },
  );
  return headers['lambda-extension-identifier'];
}

async function next(extensionId) {
  const { body } = await extensionApiRequest(
    'event/next',
    {
      method: 'get',
      headers: {
        'Content-Type': 'application/json',
        'Lambda-Extension-Identifier': extensionId,
      },
    }
  );

  return body;
}

async function main() {
  console.log('Registring log ingestor extension');
  const extensionId = await register();
  console.log(`Got extension id ${extensionId}`);

  let running = true;
  let prevEvent;
  while (running) {
    const event = await next(extensionId);
    switch (event.eventType) {
      case 'SHUTDOWN':
        running = false;
        break;
      case 'INVOKE':
        try {
          await onInvoke(event);
        } catch(e) {
          console.log("Error processing invocation event", e);
        }
        break;
      default:
        throw new Error(`unknown event: ${event.eventType}`);
    }
  }
  await stopGraceful();
}

function getFieldValue(fields, name) {
  const prefix = `${name}: `;
  const field = fields.find(field => field.startsWith(prefix));
  if (!field) {
    return null;
  }
  return field.substring(prefix.length);
}

// AWS_LAMBDA_LOG_GROUP_NAME and AWS_LAMBDA_LOG_STREAM_NAME are not available to extensions
// so we have to read them from file. It's the user's responsibility to write config to this file.
async function readLogConfig(timeout) {
  const logConfigFilename = '/tmp/log-ingestor-config.json';
  let logConfig;
  const timeStart = performance.now();
  while(logConfig === undefined) {
    try {
      const data = await fs.readFile(logConfigFilename);
      logConfig = JSON.parse(data)
      await fs.remove(logConfigFilename);
    } catch(e) {
      if (e.code === 'ENOENT') {
        await sleep(10);
        if (performance.now() - timeStart > timeout) {
          throw e;
        }
        continue;
      }
      throw e;
    }
  }
  return logConfig;
}


const events = {}
const processing = {}
async function onInvoke(event) {
  const logConfig = await readLogConfig(2000);
  const processingIndex = [logConfig.logGroupName, logConfig.logStreamName];
  let events;
  if (processing[processingIndex] === undefined) {
    job = { events: {}, stopped: false };
    job.events[event.requestId] = event;
    processing[processingIndex] = {
      job,
      promise: startProcessing(job, logConfig.logGroupName, logConfig.logStreamName)
    };
  } else {
    processing[processingIndex].job.events[event.requestId] = event;
  }
}

console.log("aws region!!", process.env.AWS_REGION);
const cloudWatch = new CloudWatchLogs({ region: process.env.AWS_REGION });
async function getLogEvents(logGroupName, logStreamName, nextToken, timeout) {
  console.log("getting log events", logGroupName, logStreamName);
  const timeStart = performance.now();
  let response;
  while(response === undefined) {
    try {
      response = await cloudWatch.getLogEvents({
        startFromHead: true, // TODO: it seemse that this flag doesn't do anything :(
        nextToken: nextToken,
        limit: 100,
        logGroupName: logGroupName, //process.env.AWS_LAMBDA_LOG_GROUP_NAME
        logStreamName: logStreamName, // process.env.AWS_LAMBDA_LOG_STREAM_NAME
      }).promise()
    } catch(e) {
      if (e.code === 'ResourceNotFoundException') {
        await sleep(10);
        if (performance.now() - timeStart > timeout) {
          console.log("Bad luck", e);
          throw e;
        }
        continue;
      }
      throw e;
    }
  }
  return response;
}

async function startProcessing(job, logGroupName, logStreamName) {
  let nextToken = undefined;
  const events = job.events;
  while (Object.keys(events).length > 0 || !job.stopped) {
    const response = await getLogEvents(logGroupName, logStreamName, nextToken, 6000);
    const reportEvents = response.events.filter(event => event.message.startsWith("REPORT "));
    console.log("Report events", reportEvents);
    reportEvents.forEach(event => {
      const fields = event.message.substring("REPORT ".length).trim().split("\t");
      const requestId = getFieldValue(fields, "RequestId");
      if (!events.hasOwnProperty(requestId)) {
        return;
      }

      const duration = getFieldValue(fields, "Duration");
      const billedDuration = getFieldValue(fields, "Billed Duration");
      const memorySize = getFieldValue(fields, "Memory Size");
      const maxMemoryUsed = getFieldValue(fields, "Max Memory Used");
      console.log(`Request ${requestId} reported finish: ${JSON.stringify({ duration, billedDuration, memorySize, maxMemoryUsed })}`);
      delete events[requestId];
    });
    nextToken = response.nextForwardToken;
  }
}

async function stopGraceful() {
  const ps = Object.values(processing);
  ps.forEach(p => { p.job.stopped = true });
  return Promise.all(ps.map(p => p.promise));
}

async function test() {
  await onInvoke({ requestId: '6dbbebcc-6864-46ca-acfc-1361b5bb0948' });
  return stopGraceful()
}

// Local testing:
// test().then(() => process.exit(0), err => { console.error(err); process.exit(-1); });

// Production:
main().then(() => process.exit(0), err => { console.error(err); process.exit(-1); });
