const redis = require('amenov.redis');
const moment = require('moment');

const { redisSet, redisGet, redisKeys, redisDel } = redis();

const prefix = 'queue:';

/*
  @ADD
  
  filename: String
  options: Object
    - args?: Array
    - label?: String
    - deadLine?: String:DateTime
    - deletePrev?: Boolean
*/
const add = async (filename, options = {}) => {
  const keyStart = prefix + filename;

  // DELETE-PREV
  if (options.label && options.deletePrev) {
    try {
      const keys = await redisKeys(`${keyStart}*${options.label}*`);

      if (keys.length) redisDel(keys, false);
    } catch (err) {
      console.log(err);
    }
  }

  const label = options.label ? '_' + options.label : '';
  const key = keyStart + `_${moment().valueOf()}` + label;

  redisSet(key, JSON.stringify({ filename, options }));
};

/*
  @EXECUTOR
  
  options: Object
    - jobs: String:Path
    - logging?: Boolean
    - interval?: Number
  key: String
  value: Object
    - filename: String
    - options: Object
      - args?: Array
      - label?: String
      - deadLine?: String:DateTime
      - deletePrev?: Boolean
*/
const executor = async (options, key, value) => {
  const logging = (msg) => {
    if (options.logging) console.log(msg);
  };

  const job = require(options.jobs + value.filename);

  try {
    await job(...(value.options.args ?? []));

    logging('Job finished: ' + key);
  } catch (err) {
    logging('Job failed: ' + key);

    console.log(err);
  }

  redisDel(key, false);
};

/*
  @START

  options: Object
    - jobs: String:Path
    - logging?: Boolean
    - interval?: Number
*/
const start = (options) => {
  setInterval(async () => {
    try {
      const keys = await redisKeys(prefix + '*');

      if (keys.length) {
        const high = [];
        const middle = [];
        const low = [];

        for (const key of keys) {
          try {
            const value = JSON.parse(await redisGet(key, false));

            if (
              !value.options.deadLine ||
              moment().isAfter(value.options.deadLine)
            ) {
              if (value.options.priority) {
                switch (value.options.priority) {
                  case 'high':
                    high.push([key, value]);
                    break;

                  case 'middle':
                    middle.push([key, value]);
                    break;

                  case 'low':
                    low.push([key, value]);
                    break;

                  default:
                    break;
                }
              } else {
                executor(options, key, value);
              }
            }
          } catch (err) {
            console.log(err);
          }
        }

        const queue = [...high, ...middle, ...low];

        if (queue.length) {
          for (const [key, value] of queue) {
            executor(options, key, value);
          }
        }
      }
    } catch (err) {
      console.log(err);
    }
  }, options.interval ?? 1000);
};

module.exports = { add, start };
