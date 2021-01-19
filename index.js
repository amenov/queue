const redis = require('redis');
const moment = require('moment');

const redisClient = redis.createClient({
  host: process.env.REDIS_HOST ?? '127.0.0.1',
});

const prefix = 'queue:';

const redisKeys = async (pattern) => {
  return await new Promise((resolve, reject) => {
    redisClient.keys(pattern, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

const redisGet = async (key) => {
  return await new Promise((resolve, reject) => {
    redisClient.get(key, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

const add = async (filename, options) => {
  if (!options.args) {
    options.args = [];
  }

  if (moment().isBefore(options.deadLine)) {
    const keyStart = prefix + filename;

    if (options.label && options.deletePrev) {
      try {
        const keys = await redisKeys(`${keyStart}*${options.label}*`);

        if (keys.length) {
          redisClient.del(keys);
        }
      } catch (err) {
        console.log(err);
      }
    }

    const label = options.label ? '_' + options.label : '';

    redisClient.set(
      keyStart + `_${moment().valueOf()}` + label,
      JSON.stringify({ filename, options })
    );
  }
};

const start = (options) => {
  setInterval(async () => {
    try {
      const keys = await redisKeys(prefix + '*');

      if (keys.length) {
        for (const key of keys) {
          try {
            const value = JSON.parse(await redisGet(key));

            const executor = async () => {
              const handler = require(options.jobs + value.filename);

              try {
                await handler(...value.options.args);

                redisClient.del(key);

                if (options.logging) {
                  console.log('Job finished: ' + key);
                }
              } catch (err) {
                redisClient.del(key);

                if (options.logging) {
                  console.log('Job failed: ' + key);
                }

                console.log(err);
              }
            };

            if (!value.options.deadLine) {
              await executor();
            } else if (moment().isAfter(value.options.deadLine)) {
              await executor();
            }
          } catch (err) {
            console.log(err);
          }
        }
      }
    } catch (err) {
      console.log(err);
    }
  }, options.interval ?? 1000);
};

module.exports = { add, start };
