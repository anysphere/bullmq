import { __rest } from "tslib";
import { invert } from 'lodash';
import { debuglog } from 'util';
import { errorObject, isEmpty, getParentKey, lengthInUtf8Bytes, parseObjectValues, tryCatch, } from '../utils';
import { Backoffs } from './backoffs';
import { Scripts } from './scripts';
import { UnrecoverableError } from './unrecoverable-error';
const logger = debuglog('bull');
const optsDecodeMap = {
    fpof: 'failParentOnFailure',
    kl: 'keepLogs',
};
const optsEncodeMap = invert(optsDecodeMap);
/**
 * Job
 *
 * This class represents a Job in the queue. Normally job are implicitly created when
 * you add a job to the queue with methods such as Queue.addJob( ... )
 *
 * A Job instance is also passed to the Worker's process function.
 *
 * @class Job
 */
export class Job {
    constructor(queue, 
    /**
     * The name of the Job
     */
    name, 
    /**
     * The payload for this job.
     */
    data, 
    /**
     * The options object for this job.
     */
    opts = {}, id) {
        this.queue = queue;
        this.name = name;
        this.data = data;
        this.opts = opts;
        this.id = id;
        /**
         * The progress a job has performed so far.
         * @defaultValue 0
         */
        this.progress = 0;
        /**
         * The value returned by the processor when processing this job.
         * @defaultValue null
         */
        this.returnvalue = null;
        /**
         * Stacktrace for the error (for failed jobs).
         * @defaultValue null
         */
        this.stacktrace = null;
        /**
         * Number of attempts after the job has failed.
         * @defaultValue 0
         */
        this.attemptsMade = 0;
        const _a = this.opts, { repeatJobKey } = _a, restOpts = __rest(_a, ["repeatJobKey"]);
        this.opts = Object.assign({
            attempts: 0,
            delay: 0,
        }, restOpts);
        this.delay = this.opts.delay;
        this.repeatJobKey = repeatJobKey;
        this.timestamp = opts.timestamp ? opts.timestamp : Date.now();
        this.opts.backoff = Backoffs.normalize(opts.backoff);
        this.parentKey = getParentKey(opts.parent);
        this.parent = opts.parent
            ? { id: opts.parent.id, queueKey: opts.parent.queue }
            : undefined;
        this.toKey = queue.toKey.bind(queue);
        this.scripts = new Scripts(queue);
    }
    /**
     * Creates a new job and adds it to the queue.
     *
     * @param queue - the queue where to add the job.
     * @param name - the name of the job.
     * @param data - the payload of the job.
     * @param opts - the options bag for this job.
     * @returns
     */
    static async create(queue, name, data, opts) {
        const client = await queue.client;
        const job = new this(queue, name, data, opts, opts && opts.jobId);
        job.id = await job.addJob(client, {
            parentKey: job.parentKey,
            parentDependenciesKey: job.parentKey
                ? `${job.parentKey}:dependencies`
                : '',
        });
        return job;
    }
    /**
     * Creates a bulk of jobs and adds them atomically to the given queue.
     *
     * @param queue -the queue were to add the jobs.
     * @param jobs - an array of jobs to be added to the queue.
     * @returns
     */
    static async createBulk(queue, jobs) {
        const client = await queue.client;
        const jobInstances = jobs.map(job => { var _a; return new this(queue, job.name, job.data, job.opts, (_a = job.opts) === null || _a === void 0 ? void 0 : _a.jobId); });
        const multi = client.multi();
        for (const job of jobInstances) {
            job.addJob(multi, {
                parentKey: job.parentKey,
                parentDependenciesKey: job.parentKey
                    ? `${job.parentKey}:dependencies`
                    : '',
            });
        }
        const results = (await multi.exec());
        for (let index = 0; index < results.length; ++index) {
            const [err, id] = results[index];
            if (err) {
                throw err;
            }
            jobInstances[index].id = id;
        }
        return jobInstances;
    }
    /**
     * Instantiates a Job from a JobJsonRaw object (coming from a deserialized JSON object)
     *
     * @param queue - the queue where the job belongs to.
     * @param json - the plain object containing the job.
     * @param jobId - an optional job id (overrides the id coming from the JSON object)
     * @returns
     */
    static fromJSON(queue, json, jobId) {
        const data = JSON.parse(json.data || '{}');
        const opts = Job.optsFromJSON(json.opts);
        const job = new this(queue, json.name, data, opts, json.id || jobId);
        job.progress = JSON.parse(json.progress || '0');
        job.delay = parseInt(json.delay);
        job.timestamp = parseInt(json.timestamp);
        if (json.finishedOn) {
            job.finishedOn = parseInt(json.finishedOn);
        }
        if (json.processedOn) {
            job.processedOn = parseInt(json.processedOn);
        }
        if (json.rjk) {
            job.repeatJobKey = json.rjk;
        }
        job.failedReason = json.failedReason;
        job.attemptsMade = parseInt(json.attemptsMade || '0');
        job.stacktrace = getTraces(json.stacktrace);
        if (typeof json.returnvalue === 'string') {
            job.returnvalue = getReturnValue(json.returnvalue);
        }
        if (json.parentKey) {
            job.parentKey = json.parentKey;
        }
        if (json.parent) {
            job.parent = JSON.parse(json.parent);
        }
        return job;
    }
    static optsFromJSON(rawOpts) {
        const opts = JSON.parse(rawOpts || '{}');
        const optionEntries = Object.entries(opts);
        const options = {};
        for (const item of optionEntries) {
            const [attributeName, value] = item;
            if (optsDecodeMap[attributeName]) {
                options[optsDecodeMap[attributeName]] =
                    value;
            }
            else {
                options[attributeName] = value;
            }
        }
        return options;
    }
    /**
     * Fetches a Job from the queue given the passed job id.
     *
     * @param queue - the queue where the job belongs to.
     * @param jobId - the job id.
     * @returns
     */
    static async fromId(queue, jobId) {
        // jobId can be undefined if moveJob returns undefined
        if (jobId) {
            const client = await queue.client;
            const jobData = await client.hgetall(queue.toKey(jobId));
            return isEmpty(jobData)
                ? undefined
                : this.fromJSON(queue, jobData, jobId);
        }
    }
    toJSON() {
        const _a = this, { queue, scripts } = _a, withoutQueueAndScripts = __rest(_a, ["queue", "scripts"]);
        return withoutQueueAndScripts;
    }
    /**
     * Prepares a job to be serialized for storage in Redis.
     * @returns
     */
    asJSON() {
        return {
            id: this.id,
            name: this.name,
            data: JSON.stringify(typeof this.data === 'undefined' ? {} : this.data),
            opts: this.optsAsJSON(this.opts),
            parent: this.parent ? Object.assign({}, this.parent) : undefined,
            parentKey: this.parentKey,
            progress: this.progress,
            attemptsMade: this.attemptsMade,
            finishedOn: this.finishedOn,
            processedOn: this.processedOn,
            timestamp: this.timestamp,
            failedReason: JSON.stringify(this.failedReason),
            stacktrace: JSON.stringify(this.stacktrace),
            repeatJobKey: this.repeatJobKey,
            returnvalue: JSON.stringify(this.returnvalue),
        };
    }
    optsAsJSON(opts = {}) {
        const optionEntries = Object.entries(opts);
        const options = {};
        for (const item of optionEntries) {
            const [attributeName, value] = item;
            if (optsEncodeMap[attributeName]) {
                options[optsEncodeMap[attributeName]] =
                    value;
            }
            else {
                options[attributeName] = value;
            }
        }
        return options;
    }
    /**
     * Prepares a job to be passed to Sandbox.
     * @returns
     */
    asJSONSandbox() {
        return Object.assign(Object.assign({}, this.asJSON()), { queueName: this.queueName, prefix: this.prefix });
    }
    /**
     * Updates a job's data
     *
     * @param data - the data that will replace the current jobs data.
     */
    update(data) {
        this.data = data;
        return this.scripts.updateData(this, data);
    }
    /**
     * Updates a job's progress
     *
     * @param progress - number or object to be saved as progress.
     */
    updateProgress(progress) {
        this.progress = progress;
        return this.scripts.updateProgress(this, progress);
    }
    /**
     * Logs one row of log data.
     *
     * @param logRow - string with log data to be logged.
     */
    async log(logRow) {
        const client = await this.queue.client;
        const logsKey = this.toKey(this.id) + ':logs';
        const multi = client.multi();
        multi.rpush(logsKey, logRow);
        if (this.opts.keepLogs) {
            multi.ltrim(logsKey, -this.opts.keepLogs, -1);
        }
        const result = (await multi.exec());
        return this.opts.keepLogs
            ? Math.min(this.opts.keepLogs, result[0][1])
            : result[0][1];
    }
    /**
     * Clears job's logs
     *
     * @param keepLogs - the amount of log entries to preserve
     */
    async clearLogs(keepLogs) {
        const client = await this.queue.client;
        const logsKey = this.toKey(this.id) + ':logs';
        if (keepLogs) {
            await client.ltrim(logsKey, -keepLogs, -1);
        }
        else {
            await client.del(logsKey);
        }
    }
    /**
     * Completely remove the job from the queue.
     * Note, this call will throw an exception if the job
     * is being processed when the call is performed.
     */
    async remove() {
        await this.queue.waitUntilReady();
        const queue = this.queue;
        const job = this;
        const removed = await this.scripts.remove(job.id);
        if (removed) {
            queue.emit('removed', job);
        }
        else {
            throw new Error('Could not remove job ' + job.id);
        }
    }
    /**
     * Extend the lock for this job.
     *
     * @param token - unique token for the lock
     * @param duration - lock duration in milliseconds
     */
    extendLock(token, duration) {
        return this.scripts.extendLock(this.id, token, duration);
    }
    /**
     * Moves a job to the completed queue.
     * Returned job to be used with Queue.prototype.nextJobFromJobData.
     *
     * @param returnValue - The jobs success message.
     * @param token - Worker token used to acquire completed job.
     * @param fetchNext - True when wanting to fetch the next job.
     * @returns Returns the jobData of the next job in the waiting queue.
     */
    async moveToCompleted(returnValue, token, fetchNext = true) {
        await this.queue.waitUntilReady();
        this.returnvalue = returnValue || void 0;
        const stringifiedReturnValue = tryCatch(JSON.stringify, JSON, [
            returnValue,
        ]);
        if (stringifiedReturnValue === errorObject) {
            throw errorObject.value;
        }
        const args = this.scripts.moveToCompletedArgs(this, stringifiedReturnValue, this.opts.removeOnComplete, token, fetchNext);
        const result = await this.scripts.moveToFinished(this.id, args);
        this.finishedOn = args[13];
        return result;
    }
    /**
     * Moves a job to the failed queue.
     *
     * @param err - the jobs error message.
     * @param token - token to check job is locked by current worker
     * @param fetchNext - true when wanting to fetch the next job
     * @returns void
     */
    async moveToFailed(err, token, fetchNext = false) {
        const client = await this.queue.client;
        const message = err === null || err === void 0 ? void 0 : err.message;
        const queue = this.queue;
        this.failedReason = message;
        let command;
        const multi = client.multi();
        this.saveStacktrace(multi, err);
        //
        // Check if an automatic retry should be performed
        //
        let moveToFailed = false;
        let finishedOn;
        if (this.attemptsMade < this.opts.attempts &&
            !this.discarded &&
            !(err instanceof UnrecoverableError || err.name == 'UnrecoverableError')) {
            const opts = queue.opts;
            // Check if backoff is needed
            const delay = await Backoffs.calculate(this.opts.backoff, this.attemptsMade, err, this, opts.settings && opts.settings.backoffStrategy);
            if (delay === -1) {
                moveToFailed = true;
            }
            else if (delay) {
                const args = this.scripts.moveToDelayedArgs(this.id, Date.now() + delay, token);
                multi.moveToDelayed(args);
                command = 'delayed';
            }
            else {
                // Retry immediately
                multi.retryJob(this.scripts.retryJobArgs(this.id, this.opts.lifo, token));
                command = 'retryJob';
            }
        }
        else {
            // If not, move to failed
            moveToFailed = true;
        }
        if (moveToFailed) {
            const args = this.scripts.moveToFailedArgs(this, message, this.opts.removeOnFail, token, fetchNext);
            multi.moveToFinished(args);
            finishedOn = args[13];
            command = 'failed';
        }
        const results = await multi.exec();
        const anyError = results.find(result => result[0]);
        if (anyError) {
            throw new Error(`Error "moveToFailed" with command ${command}: ${anyError}`);
        }
        const code = results[results.length - 1][1];
        if (code < 0) {
            throw this.scripts.finishedErrors(code, this.id, command, 'active');
        }
        if (finishedOn && typeof finishedOn === 'number') {
            this.finishedOn = finishedOn;
        }
    }
    /**
     * @returns true if the job has completed.
     */
    isCompleted() {
        return this.isInZSet('completed');
    }
    /**
     * @returns true if the job has failed.
     */
    isFailed() {
        return this.isInZSet('failed');
    }
    /**
     * @returns true if the job is delayed.
     */
    isDelayed() {
        return this.isInZSet('delayed');
    }
    /**
     * @returns true if the job is waiting for children.
     */
    isWaitingChildren() {
        return this.isInZSet('waiting-children');
    }
    /**
     * @returns true of the job is active.
     */
    isActive() {
        return this.isInList('active');
    }
    /**
     * @returns true if the job is waiting.
     */
    async isWaiting() {
        return (await this.isInList('wait')) || (await this.isInList('paused'));
    }
    /**
     * @returns the queue name this job belongs to.
     */
    get queueName() {
        return this.queue.name;
    }
    /**
     * @returns the prefix that is used.
     */
    get prefix() {
        return this.queue.opts.prefix;
    }
    /**
     * @returns it includes the prefix, the namespace separator :, and queue name.
     * @see https://www.gnu.org/software/gawk/manual/html_node/Qualified-Names.html
     */
    get queueQualifiedName() {
        return `${this.prefix}:${this.queueName}`;
    }
    /**
     * Get current state.
     *
     * @returns Returns one of these values:
     * 'completed', 'failed', 'delayed', 'active', 'waiting', 'waiting-children', 'unknown'.
     */
    getState() {
        return this.scripts.getState(this.id);
    }
    /**
     * Change delay of a delayed job.
     *
     * @param delay - milliseconds to be added to current time.
     * @returns void
     */
    async changeDelay(delay) {
        await this.scripts.changeDelay(this.id, delay);
        this.delay = delay;
    }
    /**
     * Change job priority.
     *
     * @returns void
     */
    async changePriority(opts) {
        await this.scripts.changePriority(this.id, opts.priority, opts.lifo);
    }
    /**
     * Get this jobs children result values if any.
     *
     * @returns Object mapping children job keys with their values.
     */
    async getChildrenValues() {
        const client = await this.queue.client;
        const result = (await client.hgetall(this.toKey(`${this.id}:processed`)));
        if (result) {
            return parseObjectValues(result);
        }
    }
    /**
     * Get children job keys if this job is a parent and has children.
     *
     * @returns dependencies separated by processed and unprocessed.
     */
    async getDependencies(opts = {}) {
        const client = await this.queue.client;
        const multi = client.multi();
        if (!opts.processed && !opts.unprocessed) {
            multi.hgetall(this.toKey(`${this.id}:processed`));
            multi.smembers(this.toKey(`${this.id}:dependencies`));
            const [[err1, processed], [err2, unprocessed]] = (await multi.exec());
            const transformedProcessed = parseObjectValues(processed);
            return { processed: transformedProcessed, unprocessed };
        }
        else {
            const defaultOpts = {
                cursor: 0,
                count: 20,
            };
            if (opts.processed) {
                const processedOpts = Object.assign(Object.assign({}, defaultOpts), opts.processed);
                multi.hscan(this.toKey(`${this.id}:processed`), processedOpts.cursor, 'COUNT', processedOpts.count);
            }
            if (opts.unprocessed) {
                const unprocessedOpts = Object.assign(Object.assign({}, defaultOpts), opts.unprocessed);
                multi.sscan(this.toKey(`${this.id}:dependencies`), unprocessedOpts.cursor, 'COUNT', unprocessedOpts.count);
            }
            const [result1, result2] = (await multi.exec());
            const [processedCursor, processed = []] = opts.processed
                ? result1[1]
                : [];
            const [unprocessedCursor, unprocessed = []] = opts.unprocessed
                ? opts.processed
                    ? result2[1]
                    : result1[1]
                : [];
            const transformedProcessed = {};
            for (let index = 0; index < processed.length; ++index) {
                if (index % 2) {
                    transformedProcessed[processed[index - 1]] = JSON.parse(processed[index]);
                }
            }
            return Object.assign(Object.assign({}, (processedCursor
                ? {
                    processed: transformedProcessed,
                    nextProcessedCursor: Number(processedCursor),
                }
                : {})), (unprocessedCursor
                ? { unprocessed, nextUnprocessedCursor: Number(unprocessedCursor) }
                : {}));
        }
    }
    /**
     * Get children job counts if this job is a parent and has children.
     *
     * @returns dependencies count separated by processed and unprocessed.
     */
    async getDependenciesCount(opts = {}) {
        const client = await this.queue.client;
        const multi = client.multi();
        const updatedOpts = !opts.processed && !opts.unprocessed
            ? { processed: true, unprocessed: true }
            : opts;
        if (updatedOpts.processed) {
            multi.hlen(this.toKey(`${this.id}:processed`));
        }
        if (updatedOpts.unprocessed) {
            multi.scard(this.toKey(`${this.id}:dependencies`));
        }
        const [[err1, result1] = [], [err2, result2] = []] = (await multi.exec());
        const processed = updatedOpts.processed ? result1 : undefined;
        const unprocessed = updatedOpts.unprocessed
            ? updatedOpts.processed
                ? result2
                : result1
            : undefined;
        return Object.assign(Object.assign({}, (updatedOpts.processed
            ? {
                processed,
            }
            : {})), (updatedOpts.unprocessed ? { unprocessed } : {}));
    }
    /**
     * Returns a promise the resolves when the job has completed (containing the return value of the job),
     * or rejects when the job has failed (containing the failedReason).
     *
     * @param queueEvents - Instance of QueueEvents.
     * @param ttl - Time in milliseconds to wait for job to finish before timing out.
     */
    async waitUntilFinished(queueEvents, ttl) {
        await this.queue.waitUntilReady();
        const jobId = this.id;
        return new Promise(async (resolve, reject) => {
            let timeout;
            if (ttl) {
                timeout = setTimeout(() => onFailed(
                /* eslint-disable max-len */
                `Job wait ${this.name} timed out before finishing, no finish notification arrived after ${ttl}ms (id=${jobId})`), ttl);
            }
            function onCompleted(args) {
                removeListeners();
                resolve(args.returnvalue);
            }
            function onFailed(args) {
                removeListeners();
                reject(new Error(args.failedReason || args));
            }
            const completedEvent = `completed:${jobId}`;
            const failedEvent = `failed:${jobId}`;
            queueEvents.on(completedEvent, onCompleted);
            queueEvents.on(failedEvent, onFailed);
            this.queue.on('closing', onFailed);
            const removeListeners = () => {
                clearInterval(timeout);
                queueEvents.removeListener(completedEvent, onCompleted);
                queueEvents.removeListener(failedEvent, onFailed);
                this.queue.removeListener('closing', onFailed);
            };
            // Poll once right now to see if the job has already finished. The job may have been completed before we were able
            // to register the event handlers on the QueueEvents, so we check here to make sure we're not waiting for an event
            // that has already happened. We block checking the job until the queue events object is actually listening to
            // Redis so there's no chance that it will miss events.
            await queueEvents.waitUntilReady();
            const [status, result] = (await this.scripts.isFinished(jobId, true));
            const finished = status != 0;
            if (finished) {
                if (status == -1 || status == 2) {
                    onFailed({ failedReason: result });
                }
                else {
                    onCompleted({ returnvalue: getReturnValue(result) });
                }
            }
        });
    }
    /**
     * Moves the job to the delay set.
     *
     * @param timestamp - timestamp where the job should be moved back to "wait"
     * @param token - token to check job is locked by current worker
     * @returns
     */
    moveToDelayed(timestamp, token) {
        return this.scripts.moveToDelayed(this.id, timestamp, token);
    }
    /**
     * Moves the job to the waiting-children set.
     *
     * @param token - Token to check job is locked by current worker
     * @param opts - The options bag for moving a job to waiting-children.
     * @returns true if the job was moved
     */
    moveToWaitingChildren(token, opts = {}) {
        return this.scripts.moveToWaitingChildren(this.id, token, opts);
    }
    /**
     * Promotes a delayed job so that it starts to be processed as soon as possible.
     */
    async promote() {
        const jobId = this.id;
        const code = await this.scripts.promote(jobId);
        if (code < 0) {
            throw this.scripts.finishedErrors(code, this.id, 'promote', 'delayed');
        }
    }
    /**
     * Attempts to retry the job. Only a job that has failed or completed can be retried.
     *
     * @param state - completed / failed
     * @returns If resolved and return code is 1, then the queue emits a waiting event
     * otherwise the operation was not a success and throw the corresponding error. If the promise
     * rejects, it indicates that the script failed to execute
     */
    retry(state = 'failed') {
        this.failedReason = null;
        this.finishedOn = null;
        this.processedOn = null;
        this.returnvalue = null;
        return this.scripts.reprocessJob(this, state);
    }
    /**
     * Marks a job to not be retried if it fails (even if attempts has been configured)
     */
    discard() {
        this.discarded = true;
    }
    async isInZSet(set) {
        const client = await this.queue.client;
        const score = await client.zscore(this.queue.toKey(set), this.id);
        return score !== null;
    }
    async isInList(list) {
        return this.scripts.isJobInList(this.queue.toKey(list), this.id);
    }
    /**
     * Adds the job to Redis.
     *
     * @param client -
     * @param parentOpts -
     * @returns
     */
    addJob(client, parentOpts) {
        var _a;
        const jobData = this.asJSON();
        const exceedLimit = this.opts.sizeLimit &&
            lengthInUtf8Bytes(jobData.data) > this.opts.sizeLimit;
        if (exceedLimit) {
            throw new Error(`The size of job ${this.name} exceeds the limit ${this.opts.sizeLimit} bytes`);
        }
        if (this.opts.delay && this.opts.repeat && !((_a = this.opts.repeat) === null || _a === void 0 ? void 0 : _a.count)) {
            throw new Error(`Delay and repeat options could not be used together`);
        }
        if (`${parseInt(this.id, 10)}` === this.id) {
            //TODO: throw an error in next breaking change
            console.warn('Custom Ids should not be integers: https://github.com/taskforcesh/bullmq/pull/1569');
        }
        return this.scripts.addJob(client, jobData, jobData.opts, this.id, parentOpts);
    }
    saveStacktrace(multi, err) {
        this.stacktrace = this.stacktrace || [];
        if (err === null || err === void 0 ? void 0 : err.stack) {
            this.stacktrace.push(err.stack);
            if (this.opts.stackTraceLimit) {
                this.stacktrace = this.stacktrace.slice(0, this.opts.stackTraceLimit);
            }
        }
        const args = this.scripts.saveStacktraceArgs(this.id, JSON.stringify(this.stacktrace), err === null || err === void 0 ? void 0 : err.message);
        multi.saveStacktrace(args);
    }
}
function getTraces(stacktrace) {
    const traces = tryCatch(JSON.parse, JSON, [stacktrace]);
    if (traces === errorObject || !(traces instanceof Array)) {
        return [];
    }
    else {
        return traces;
    }
}
function getReturnValue(_value) {
    const value = tryCatch(JSON.parse, JSON, [_value]);
    if (value !== errorObject) {
        return value;
    }
    else {
        logger('corrupted returnvalue: ' + _value, value);
    }
}
//# sourceMappingURL=job.js.map