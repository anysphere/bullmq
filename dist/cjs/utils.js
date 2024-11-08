"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QUEUE_EVENT_SUFFIX = exports.WORKER_SUFFIX = exports.errorToJSON = exports.parseObjectValues = exports.isRedisVersionLowerThan = exports.childSend = exports.asyncSend = exports.isNotConnectionError = exports.DELAY_TIME_1 = exports.DELAY_TIME_5 = exports.clientCommandMessageReg = exports.getParentKey = exports.removeAllQueueData = exports.decreaseMaxListeners = exports.increaseMaxListeners = exports.isRedisCluster = exports.isRedisInstance = exports.delay = exports.array2obj = exports.isEmpty = exports.lengthInUtf8Bytes = exports.tryCatch = exports.errorObject = void 0;
const ioredis_1 = require("ioredis");
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const utils_1 = require("ioredis/built/utils");
const semver = require("semver");
exports.errorObject = { value: null };
function tryCatch(fn, ctx, args) {
    try {
        return fn.apply(ctx, args);
    }
    catch (e) {
        exports.errorObject.value = e;
        return exports.errorObject;
    }
}
exports.tryCatch = tryCatch;
/**
 * Checks the size of string for ascii/non-ascii characters
 * @see https://stackoverflow.com/a/23318053/1347170
 * @param str -
 */
function lengthInUtf8Bytes(str) {
    return Buffer.byteLength(str, 'utf8');
}
exports.lengthInUtf8Bytes = lengthInUtf8Bytes;
function isEmpty(obj) {
    for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
            return false;
        }
    }
    return true;
}
exports.isEmpty = isEmpty;
function array2obj(arr) {
    const obj = {};
    for (let i = 0; i < arr.length; i += 2) {
        obj[arr[i]] = arr[i + 1];
    }
    return obj;
}
exports.array2obj = array2obj;
function delay(ms) {
    return new Promise(resolve => {
        setTimeout(() => resolve(), ms);
    });
}
exports.delay = delay;
function isRedisInstance(obj) {
    if (!obj) {
        return false;
    }
    const redisApi = ['connect', 'disconnect', 'duplicate'];
    return redisApi.every(name => typeof obj[name] === 'function');
}
exports.isRedisInstance = isRedisInstance;
function isRedisCluster(obj) {
    return isRedisInstance(obj) && obj.isCluster;
}
exports.isRedisCluster = isRedisCluster;
function increaseMaxListeners(emitter, count) {
    const maxListeners = emitter.getMaxListeners();
    emitter.setMaxListeners(maxListeners + count);
}
exports.increaseMaxListeners = increaseMaxListeners;
function decreaseMaxListeners(emitter, count) {
    increaseMaxListeners(emitter, -count);
}
exports.decreaseMaxListeners = decreaseMaxListeners;
async function removeAllQueueData(client, queueName, prefix = 'bull') {
    if (client instanceof ioredis_1.Cluster) {
        // todo compat with cluster ?
        // @see https://github.com/luin/ioredis/issues/175
        return Promise.resolve(false);
    }
    const pattern = `${prefix}:${queueName}:*`;
    const removing = await new Promise((resolve, reject) => {
        const stream = client.scanStream({
            match: pattern,
        });
        stream.on('data', (keys) => {
            if (keys.length) {
                const pipeline = client.pipeline();
                keys.forEach(key => {
                    pipeline.del(key);
                });
                pipeline.exec().catch(error => {
                    reject(error);
                });
            }
        });
        stream.on('end', () => resolve());
        stream.on('error', error => reject(error));
    });
    await removing;
    await client.quit();
}
exports.removeAllQueueData = removeAllQueueData;
function getParentKey(opts) {
    if (opts) {
        return `${opts.queue}:${opts.id}`;
    }
}
exports.getParentKey = getParentKey;
exports.clientCommandMessageReg = /ERR unknown command ['`]\s*client\s*['`]/;
exports.DELAY_TIME_5 = 5000;
exports.DELAY_TIME_1 = 100;
function isNotConnectionError(error) {
    const errorMessage = `${error.message}`;
    return (errorMessage !== utils_1.CONNECTION_CLOSED_ERROR_MSG &&
        !errorMessage.includes('ECONNREFUSED'));
}
exports.isNotConnectionError = isNotConnectionError;
const asyncSend = (proc, msg) => {
    return new Promise((resolve, reject) => {
        if (typeof proc.send === 'function') {
            proc.send(msg, (err) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve();
                }
            });
        }
        else if (typeof proc.postMessage === 'function') {
            resolve(proc.postMessage(msg));
        }
        else {
            resolve();
        }
    });
};
exports.asyncSend = asyncSend;
const childSend = (proc, msg) => (0, exports.asyncSend)(proc, msg);
exports.childSend = childSend;
const isRedisVersionLowerThan = (currentVersion, minimumVersion) => {
    const version = semver.valid(semver.coerce(currentVersion));
    return semver.lt(version, minimumVersion);
};
exports.isRedisVersionLowerThan = isRedisVersionLowerThan;
const parseObjectValues = (obj) => {
    const accumulator = {};
    for (const value of Object.entries(obj)) {
        accumulator[value[0]] = JSON.parse(value[1]);
    }
    return accumulator;
};
exports.parseObjectValues = parseObjectValues;
const errorToJSON = (value) => {
    const error = {};
    Object.getOwnPropertyNames(value).forEach(function (propName) {
        error[propName] = value[propName];
    });
    return error;
};
exports.errorToJSON = errorToJSON;
exports.WORKER_SUFFIX = '';
exports.QUEUE_EVENT_SUFFIX = ':qe';
//# sourceMappingURL=utils.js.map