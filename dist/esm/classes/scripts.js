/**
 * Includes all the scripts needed by the queue and jobs.
 */
/*eslint-env node */
'use strict';
import { Packr } from 'msgpackr';
const packer = new Packr({
    useRecords: false,
    encodeUndefinedAsNil: true,
});
const pack = packer.pack;
import { ErrorCode } from '../enums';
import { array2obj, getParentKey, isRedisVersionLowerThan } from '../utils';
export class Scripts {
    constructor(queue) {
        this.queue = queue;
        const queueKeys = this.queue.keys;
        this.moveToFinishedKeys = [
            queueKeys.wait,
            queueKeys.active,
            queueKeys.priority,
            queueKeys.events,
            queueKeys.stalled,
            queueKeys.limiter,
            queueKeys.delayed,
            queueKeys.paused,
            undefined,
            undefined,
            queueKeys.meta,
            undefined,
        ];
    }
    async isJobInList(listKey, jobId) {
        const client = await this.queue.client;
        let result;
        if (isRedisVersionLowerThan(this.queue.redisVersion, '6.0.6')) {
            result = await client.isJobInList([listKey, jobId]);
        }
        else {
            result = await client.lpos(listKey, jobId);
        }
        return Number.isInteger(result);
    }
    async addJob(client, job, opts, jobId, parentOpts = {}) {
        const queueKeys = this.queue.keys;
        const keys = [
            queueKeys.wait,
            queueKeys.paused,
            queueKeys.meta,
            queueKeys.id,
            queueKeys.delayed,
            queueKeys.priority,
            queueKeys.completed,
            queueKeys.events,
        ];
        const fpof = opts.fpof ? { fpof: true } : {};
        const parent = job.parent ? Object.assign(Object.assign({}, job.parent), fpof) : null;
        const args = [
            queueKeys[''],
            typeof jobId !== 'undefined' ? jobId : '',
            job.name,
            job.timestamp,
            job.parentKey || null,
            parentOpts.waitChildrenKey || null,
            parentOpts.parentDependenciesKey || null,
            parent,
            job.repeatJobKey,
        ];
        let encodedOpts;
        if (opts.repeat) {
            const repeat = Object.assign({}, opts.repeat);
            if (repeat.startDate) {
                repeat.startDate = +new Date(repeat.startDate);
            }
            if (repeat.endDate) {
                repeat.endDate = +new Date(repeat.endDate);
            }
            encodedOpts = pack(Object.assign(Object.assign({}, opts), { repeat }));
        }
        else {
            encodedOpts = pack(opts);
        }
        keys.push(pack(args), job.data, encodedOpts);
        const result = await client.addJob(keys);
        if (result < 0) {
            throw this.finishedErrors(result, parentOpts.parentKey, 'addJob');
        }
        return result;
    }
    async pause(pause) {
        const client = await this.queue.client;
        let src = 'wait', dst = 'paused';
        if (!pause) {
            src = 'paused';
            dst = 'wait';
        }
        const keys = [src, dst, 'meta'].map((name) => this.queue.toKey(name));
        keys.push(this.queue.keys.events);
        return client.pause(keys.concat([pause ? 'paused' : 'resumed']));
    }
    removeRepeatableArgs(repeatJobId, repeatJobKey) {
        const queueKeys = this.queue.keys;
        const keys = [queueKeys.repeat, queueKeys.delayed];
        const args = [repeatJobId, repeatJobKey, queueKeys['']];
        return keys.concat(args);
    }
    async removeRepeatable(repeatJobId, repeatJobKey) {
        const client = await this.queue.client;
        const args = this.removeRepeatableArgs(repeatJobId, repeatJobKey);
        return client.removeRepeatable(args);
    }
    async remove(jobId) {
        const client = await this.queue.client;
        const keys = [''].map(name => this.queue.toKey(name));
        return client.removeJob(keys.concat([jobId]));
    }
    async extendLock(jobId, token, duration, client) {
        client = client || (await this.queue.client);
        const args = [
            this.queue.toKey(jobId) + ':lock',
            this.queue.keys.stalled,
            token,
            duration,
            jobId,
        ];
        return client.extendLock(args);
    }
    async updateData(job, data) {
        const client = await this.queue.client;
        const keys = [this.queue.toKey(job.id)];
        const dataJson = JSON.stringify(data);
        const result = await client.updateData(keys.concat([dataJson]));
        if (result < 0) {
            throw this.finishedErrors(result, job.id, 'updateData');
        }
    }
    async updateProgress(job, progress) {
        const client = await this.queue.client;
        const keys = [this.queue.toKey(job.id), this.queue.keys.events];
        const progressJson = JSON.stringify(progress);
        const result = await client.updateProgress(keys.concat([job.id, progressJson]));
        if (result < 0) {
            throw this.finishedErrors(result, job.id, 'updateProgress');
        }
        this.queue.emit('progress', job, progress);
    }
    moveToFinishedArgs(job, val, propVal, shouldRemove, target, token, timestamp, fetchNext = true) {
        var _a, _b, _c, _d;
        const queueKeys = this.queue.keys;
        const opts = this.queue.opts;
        const workerKeepJobs = target === 'completed' ? opts.removeOnComplete : opts.removeOnFail;
        const metricsKey = this.queue.toKey(`metrics:${target}`);
        const keys = this.moveToFinishedKeys;
        keys[8] = queueKeys[target];
        keys[9] = this.queue.toKey((_a = job.id) !== null && _a !== void 0 ? _a : '');
        keys[11] = metricsKey;
        const keepJobs = this.getKeepJobs(shouldRemove, workerKeepJobs);
        const args = [
            job.id,
            timestamp,
            propVal,
            typeof val === 'undefined' ? 'null' : val,
            target,
            JSON.stringify({ jobId: job.id, val: val }),
            !fetchNext || this.queue.closing ? 0 : 1,
            queueKeys[''],
            pack({
                token,
                keepJobs,
                limiter: opts.limiter,
                lockDuration: opts.lockDuration,
                attempts: job.opts.attempts,
                attemptsMade: job.attemptsMade,
                maxMetricsSize: ((_b = opts.metrics) === null || _b === void 0 ? void 0 : _b.maxDataPoints)
                    ? (_c = opts.metrics) === null || _c === void 0 ? void 0 : _c.maxDataPoints
                    : '',
                fpof: !!((_d = job.opts) === null || _d === void 0 ? void 0 : _d.failParentOnFailure),
            }),
        ];
        return keys.concat(args);
    }
    getKeepJobs(shouldRemove, workerKeepJobs) {
        if (typeof shouldRemove === 'undefined') {
            return workerKeepJobs || { count: shouldRemove ? 0 : -1 };
        }
        return typeof shouldRemove === 'object'
            ? shouldRemove
            : typeof shouldRemove === 'number'
                ? { count: shouldRemove }
                : { count: shouldRemove ? 0 : -1 };
    }
    async moveToFinished(jobId, args) {
        const client = await this.queue.client;
        const result = await client.moveToFinished(args);
        if (result < 0) {
            throw this.finishedErrors(result, jobId, 'moveToFinished', 'active');
        }
        else {
            if (typeof result !== 'undefined') {
                return raw2NextJobData(result);
            }
        }
    }
    finishedErrors(code, jobId, command, state) {
        switch (code) {
            case ErrorCode.JobNotExist:
                return new Error(`Missing key for job ${jobId}. ${command}`);
            case ErrorCode.JobLockNotExist:
                return new Error(`Missing lock for job ${jobId}. ${command}`);
            case ErrorCode.JobNotInState:
                return new Error(`Job ${jobId} is not in the ${state} state. ${command}`);
            case ErrorCode.JobPendingDependencies:
                return new Error(`Job ${jobId} has pending dependencies. ${command}`);
            case ErrorCode.ParentJobNotExist:
                return new Error(`Missing key for parent job ${jobId}. ${command}`);
            case ErrorCode.JobLockMismatch:
                return new Error(`Lock mismatch for job ${jobId}. Cmd ${command} from ${state}`);
            default:
                return new Error(`Unknown code ${code} error for ${jobId}. ${command}`);
        }
    }
    drainArgs(delayed) {
        const queueKeys = this.queue.keys;
        const keys = [
            queueKeys.wait,
            queueKeys.paused,
            delayed ? queueKeys.delayed : '',
            queueKeys.priority,
        ];
        const args = [queueKeys['']];
        return keys.concat(args);
    }
    async drain(delayed) {
        const client = await this.queue.client;
        const args = this.drainArgs(delayed);
        return client.drain(args);
    }
    getRangesArgs(types, start, end, asc) {
        const queueKeys = this.queue.keys;
        const transformedTypes = types.map(type => {
            return type === 'waiting' ? 'wait' : type;
        });
        const keys = [queueKeys['']];
        const args = [start, end, asc ? '1' : '0', ...transformedTypes];
        return keys.concat(args);
    }
    async getRanges(types, start = 0, end = 1, asc = false) {
        const client = await this.queue.client;
        const args = this.getRangesArgs(types, start, end, asc);
        return client.getRanges(args);
    }
    getCountsArgs(types) {
        const queueKeys = this.queue.keys;
        const transformedTypes = types.map(type => {
            return type === 'waiting' ? 'wait' : type;
        });
        const keys = [queueKeys['']];
        const args = [...transformedTypes];
        return keys.concat(args);
    }
    async getCounts(types) {
        const client = await this.queue.client;
        const args = this.getCountsArgs(types);
        return client.getCounts(args);
    }
    moveToCompletedArgs(job, returnvalue, removeOnComplete, token, fetchNext = false) {
        const timestamp = Date.now();
        return this.moveToFinishedArgs(job, returnvalue, 'returnvalue', removeOnComplete, 'completed', token, timestamp, fetchNext);
    }
    moveToFailedArgs(job, failedReason, removeOnFailed, token, fetchNext = false) {
        const timestamp = Date.now();
        return this.moveToFinishedArgs(job, failedReason, 'failedReason', removeOnFailed, 'failed', token, timestamp, fetchNext);
    }
    async isFinished(jobId, returnValue = false) {
        const client = await this.queue.client;
        const keys = ['completed', 'failed', jobId].map((key) => {
            return this.queue.toKey(key);
        });
        return client.isFinished(keys.concat([jobId, returnValue ? '1' : '']));
    }
    async getState(jobId) {
        const client = await this.queue.client;
        const keys = [
            'completed',
            'failed',
            'delayed',
            'active',
            'wait',
            'paused',
            'waiting-children',
        ].map((key) => {
            return this.queue.toKey(key);
        });
        if (isRedisVersionLowerThan(this.queue.redisVersion, '6.0.6')) {
            return client.getState(keys.concat([jobId]));
        }
        return client.getStateV2(keys.concat([jobId]));
    }
    async changeDelay(jobId, delay) {
        const client = await this.queue.client;
        const args = this.changeDelayArgs(jobId, delay);
        const result = await client.changeDelay(args);
        if (result < 0) {
            throw this.finishedErrors(result, jobId, 'changeDelay', 'delayed');
        }
    }
    changeDelayArgs(jobId, delay) {
        //
        // Bake in the job id first 12 bits into the timestamp
        // to guarantee correct execution order of delayed jobs
        // (up to 4096 jobs per given timestamp or 4096 jobs apart per timestamp)
        //
        // WARNING: Jobs that are so far apart that they wrap around will cause FIFO to fail
        //
        let timestamp = Date.now() + delay;
        if (timestamp > 0) {
            timestamp = timestamp * 0x1000 + (+jobId & 0xfff);
        }
        const keys = ['delayed', jobId].map(name => {
            return this.queue.toKey(name);
        });
        keys.push.apply(keys, [this.queue.keys.events]);
        return keys.concat([delay, JSON.stringify(timestamp), jobId]);
    }
    async changePriority(jobId, priority = 0, lifo = false) {
        const client = await this.queue.client;
        const args = this.changePriorityArgs(jobId, priority, lifo);
        const result = await client.changePriority(args);
        if (result < 0) {
            throw this.finishedErrors(result, jobId, 'changePriority');
        }
    }
    changePriorityArgs(jobId, priority = 0, lifo = false) {
        const keys = [
            this.queue.keys.wait,
            this.queue.keys.paused,
            this.queue.keys.meta,
            this.queue.keys.priority,
        ];
        return keys.concat([
            priority,
            this.queue.toKey(jobId),
            jobId,
            lifo ? 1 : 0,
        ]);
    }
    // Note: We have an issue here with jobs using custom job ids
    moveToDelayedArgs(jobId, timestamp, token) {
        //
        // Bake in the job id first 12 bits into the timestamp
        // to guarantee correct execution order of delayed jobs
        // (up to 4096 jobs per given timestamp or 4096 jobs apart per timestamp)
        //
        // WARNING: Jobs that are so far apart that they wrap around will cause FIFO to fail
        //
        timestamp = Math.max(0, timestamp !== null && timestamp !== void 0 ? timestamp : 0);
        if (timestamp > 0) {
            timestamp = timestamp * 0x1000 + (+jobId & 0xfff);
        }
        const keys = [
            'wait',
            'active',
            'priority',
            'delayed',
            jobId,
        ].map(name => {
            return this.queue.toKey(name);
        });
        keys.push.apply(keys, [
            this.queue.keys.events,
            this.queue.keys.paused,
            this.queue.keys.meta,
        ]);
        return keys.concat([
            this.queue.keys[''],
            Date.now(),
            JSON.stringify(timestamp),
            jobId,
            token,
        ]);
    }
    saveStacktraceArgs(jobId, stacktrace, failedReason) {
        const keys = [this.queue.toKey(jobId)];
        return keys.concat([stacktrace, failedReason]);
    }
    moveToWaitingChildrenArgs(jobId, token, opts) {
        const timestamp = Date.now();
        const childKey = getParentKey(opts.child);
        const keys = [`${jobId}:lock`, 'active', 'waiting-children', jobId].map(name => {
            return this.queue.toKey(name);
        });
        return keys.concat([
            token,
            childKey !== null && childKey !== void 0 ? childKey : '',
            JSON.stringify(timestamp),
            jobId,
        ]);
    }
    async moveToDelayed(jobId, timestamp, token = '0') {
        const client = await this.queue.client;
        const args = this.moveToDelayedArgs(jobId, timestamp, token);
        const result = await client.moveToDelayed(args);
        if (result < 0) {
            throw this.finishedErrors(result, jobId, 'moveToDelayed', 'active');
        }
    }
    /**
     * Move parent job to waiting-children state.
     *
     * @returns true if job is successfully moved, false if there are pending dependencies.
     * @throws JobNotExist
     * This exception is thrown if jobId is missing.
     * @throws JobLockNotExist
     * This exception is thrown if job lock is missing.
     * @throws JobNotInState
     * This exception is thrown if job is not in active state.
     */
    async moveToWaitingChildren(jobId, token, opts = {}) {
        const client = await this.queue.client;
        const args = this.moveToWaitingChildrenArgs(jobId, token, opts);
        const result = await client.moveToWaitingChildren(args);
        switch (result) {
            case 0:
                return true;
            case 1:
                return false;
            default:
                throw this.finishedErrors(result, jobId, 'moveToWaitingChildren', 'active');
        }
    }
    /**
     * Remove jobs in a specific state.
     *
     * @returns Id jobs from the deleted records.
     */
    async cleanJobsInSet(set, timestamp, limit = 0) {
        const client = await this.queue.client;
        return client.cleanJobsInSet([
            this.queue.toKey(set),
            this.queue.toKey('events'),
            this.queue.toKey(''),
            timestamp,
            limit,
            set,
        ]);
    }
    retryJobArgs(jobId, lifo, token) {
        const keys = [
            'active',
            'wait',
            'paused',
            jobId,
            'meta',
        ].map(name => {
            return this.queue.toKey(name);
        });
        keys.push(this.queue.keys.events, this.queue.keys.delayed, this.queue.keys.priority);
        const pushCmd = (lifo ? 'R' : 'L') + 'PUSH';
        return keys.concat([
            this.queue.toKey(''),
            Date.now(),
            pushCmd,
            jobId,
            token,
        ]);
    }
    retryJobsArgs(state, count, timestamp) {
        const keys = [
            this.queue.toKey(''),
            this.queue.keys.events,
            this.queue.toKey(state),
            this.queue.toKey('wait'),
            this.queue.toKey('paused'),
            this.queue.toKey('meta'),
        ];
        const args = [count, timestamp, state];
        return keys.concat(args);
    }
    async retryJobs(state = 'failed', count = 1000, timestamp = new Date().getTime()) {
        const client = await this.queue.client;
        const args = this.retryJobsArgs(state, count, timestamp);
        return client.retryJobs(args);
    }
    /**
     * Attempts to reprocess a job
     *
     * @param job -
     * @param state - The expected job state. If the job is not found
     * on the provided state, then it's not reprocessed. Supported states: 'failed', 'completed'
     *
     * @returns Returns a promise that evaluates to a return code:
     * 1 means the operation was a success
     * 0 means the job does not exist
     * -1 means the job is currently locked and can't be retried.
     * -2 means the job was not found in the expected set
     */
    async reprocessJob(job, state) {
        const client = await this.queue.client;
        const keys = [
            this.queue.toKey(job.id),
            this.queue.keys.events,
            this.queue.toKey(state),
            this.queue.keys.wait,
            this.queue.keys.meta,
            this.queue.keys.paused,
        ];
        const args = [
            job.id,
            (job.opts.lifo ? 'R' : 'L') + 'PUSH',
            state === 'failed' ? 'failedReason' : 'returnvalue',
            state,
        ];
        const result = await client.reprocessJob(keys.concat(args));
        switch (result) {
            case 1:
                return;
            default:
                throw this.finishedErrors(result, job.id, 'reprocessJob', state);
        }
    }
    async moveToActive(token, jobId) {
        const client = await this.queue.client;
        const opts = this.queue.opts;
        const queueKeys = this.queue.keys;
        const keys = [
            queueKeys.wait,
            queueKeys.active,
            queueKeys.priority,
            queueKeys.events,
            queueKeys.stalled,
            queueKeys.limiter,
            queueKeys.delayed,
            queueKeys.paused,
            queueKeys.meta,
        ];
        const args = [
            queueKeys[''],
            Date.now(),
            jobId,
            pack({
                token,
                lockDuration: opts.lockDuration,
                limiter: opts.limiter,
            }),
        ];
        const result = await client.moveToActive(keys.concat(args));
        return raw2NextJobData(result);
    }
    async promote(jobId) {
        const client = await this.queue.client;
        const keys = [
            this.queue.keys.delayed,
            this.queue.keys.wait,
            this.queue.keys.paused,
            this.queue.keys.meta,
            this.queue.keys.priority,
            this.queue.keys.events,
        ];
        const args = [this.queue.toKey(''), jobId];
        return client.promote(keys.concat(args));
    }
    /**
     * Looks for unlocked jobs in the active queue.
     *
     * The job was being worked on, but the worker process died and it failed to renew the lock.
     * We call these jobs 'stalled'. This is the most common case. We resolve these by moving them
     * back to wait to be re-processed. To prevent jobs from cycling endlessly between active and wait,
     * (e.g. if the job handler keeps crashing),
     * we limit the number stalled job recoveries to settings.maxStalledCount.
     */
    async moveStalledJobsToWait() {
        const client = await this.queue.client;
        const opts = this.queue.opts;
        const keys = [
            this.queue.keys.stalled,
            this.queue.keys.wait,
            this.queue.keys.active,
            this.queue.keys.failed,
            this.queue.keys['stalled-check'],
            this.queue.keys.meta,
            this.queue.keys.paused,
            this.queue.keys.events,
        ];
        const args = [
            opts.maxStalledCount,
            this.queue.toKey(''),
            Date.now(),
            opts.stalledInterval,
        ];
        return client.moveStalledJobsToWait(keys.concat(args));
    }
    /**
     * Moves a job back from Active to Wait.
     * This script is used when a job has been manually rate limited and needs
     * to be moved back to wait from active status.
     *
     * @param client - Redis client
     * @param jobId - Job id
     * @returns
     */
    async moveJobFromActiveToWait(jobId, token) {
        const client = await this.queue.client;
        const lockKey = `${this.queue.toKey(jobId)}:lock`;
        const keys = [
            this.queue.keys.active,
            this.queue.keys.wait,
            this.queue.keys.stalled,
            lockKey,
            this.queue.keys.paused,
            this.queue.keys.meta,
            this.queue.keys.limiter,
            this.queue.keys.priority,
            this.queue.keys.events,
        ];
        const args = [jobId, token, this.queue.toKey(jobId)];
        const pttl = await client.moveJobFromActiveToWait(keys.concat(args));
        return pttl < 0 ? 0 : pttl;
    }
    async obliterate(opts) {
        const client = await this.queue.client;
        const keys = [
            this.queue.keys.meta,
            this.queue.toKey(''),
        ];
        const args = [opts.count, opts.force ? 'force' : null];
        const result = await client.obliterate(keys.concat(args));
        if (result < 0) {
            switch (result) {
                case -1:
                    throw new Error('Cannot obliterate non-paused queue');
                case -2:
                    throw new Error('Cannot obliterate queue with active jobs');
            }
        }
        return result;
    }
}
export function raw2NextJobData(raw) {
    if (raw) {
        const result = [null, raw[1], raw[2], raw[3]];
        if (raw[0]) {
            result[0] = array2obj(raw[0]);
        }
        return result;
    }
    return [];
}
//# sourceMappingURL=scripts.js.map