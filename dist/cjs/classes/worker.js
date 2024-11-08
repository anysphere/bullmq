"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Worker = void 0;
const fs = require("fs");
const path = require("path");
const uuid_1 = require("uuid");
const utils_1 = require("../utils");
const queue_base_1 = require("./queue-base");
const repeat_1 = require("./repeat");
const child_pool_1 = require("./child-pool");
const job_1 = require("./job");
const redis_connection_1 = require("./redis-connection");
const sandbox_1 = require("./sandbox");
const async_fifo_queue_1 = require("./async-fifo-queue");
const delayed_error_1 = require("./delayed-error");
const waiting_children_error_1 = require("./waiting-children-error");
// 10 seconds is the maximum time a BRPOPLPUSH can block.
const maximumBlockTimeout = 10;
const RATE_LIMIT_ERROR = 'bullmq:rateLimitExceeded';
/**
 *
 * This class represents a worker that is able to process jobs from the queue.
 * As soon as the class is instantiated and a connection to Redis is established
 * it will start processing jobs.
 *
 */
class Worker extends queue_base_1.QueueBase {
    static RateLimitError() {
        return new Error(RATE_LIMIT_ERROR);
    }
    constructor(name, processor, opts = {}, Connection) {
        super(name, Object.assign(Object.assign({}, opts), { sharedConnection: (0, utils_1.isRedisInstance)(opts.connection), blockingConnection: true }), Connection);
        this.drained = false;
        this.waiting = null;
        this.running = false;
        this.blockUntil = 0;
        this.limitUntil = 0;
        this.extendLocksTimer = null;
        if (this.opts.stalledInterval <= 0) {
            throw new Error('stalledInterval must be greater than 0');
        }
        this.opts = Object.assign({ drainDelay: 5, concurrency: 1, lockDuration: 30000, maxStalledCount: 1, stalledInterval: 30000, autorun: true, runRetryDelay: 15000 }, this.opts);
        this.concurrency = this.opts.concurrency;
        this.opts.lockRenewTime =
            this.opts.lockRenewTime || this.opts.lockDuration / 2;
        this.id = (0, uuid_1.v4)();
        if (processor) {
            if (typeof processor === 'function') {
                this.processFn = processor;
            }
            else {
                // SANDBOXED
                const supportedFileTypes = ['.js', '.ts', '.flow', '.cjs'];
                const processorFile = processor +
                    (supportedFileTypes.includes(path.extname(processor)) ? '' : '.js');
                if (!fs.existsSync(processorFile)) {
                    throw new Error(`File ${processorFile} does not exist`);
                }
                const mainFile = this.opts.useWorkerThreads
                    ? 'main-worker.js'
                    : 'main.js';
                let mainFilePath = path.join(__dirname, `${mainFile}`);
                try {
                    fs.statSync(mainFilePath); // would throw if file not exists
                }
                catch (_) {
                    mainFilePath = path.join(process.cwd(), `dist/cjs/classes/${mainFile}`);
                    fs.statSync(mainFilePath);
                }
                this.childPool = new child_pool_1.ChildPool({
                    mainFile: mainFilePath,
                    useWorkerThreads: this.opts.useWorkerThreads,
                });
                this.processFn = (0, sandbox_1.default)(processor, this.childPool).bind(this);
            }
            if (this.opts.autorun) {
                this.run().catch(error => this.emit('error', error));
            }
        }
        const connectionName = this.clientName(utils_1.WORKER_SUFFIX);
        this.blockingConnection = new redis_connection_1.RedisConnection((0, utils_1.isRedisInstance)(opts.connection)
            ? opts.connection.duplicate({ connectionName })
            : Object.assign(Object.assign({}, opts.connection), { connectionName }));
        this.blockingConnection.on('error', error => this.emit('error', error));
        this.blockingConnection.on('ready', () => setTimeout(() => this.emit('ready'), 0));
    }
    emit(event, ...args) {
        return super.emit(event, ...args);
    }
    off(eventName, listener) {
        super.off(eventName, listener);
        return this;
    }
    on(event, listener) {
        super.on(event, listener);
        return this;
    }
    once(event, listener) {
        super.once(event, listener);
        return this;
    }
    callProcessJob(job, token) {
        return this.processFn(job, token);
    }
    createJob(data, jobId) {
        return this.Job.fromJSON(this, data, jobId);
    }
    /**
     *
     * Waits until the worker is ready to start processing jobs.
     * In general only useful when writing tests.
     *
     */
    async waitUntilReady() {
        await super.waitUntilReady();
        return this.blockingConnection.client;
    }
    set concurrency(concurrency) {
        if (typeof concurrency !== 'number' || concurrency < 1) {
            throw new Error('concurrency must be a number greater than 0');
        }
        this.opts.concurrency = concurrency;
    }
    get repeat() {
        return new Promise(async (resolve) => {
            if (!this._repeat) {
                const connection = await this.client;
                this._repeat = new repeat_1.Repeat(this.name, Object.assign(Object.assign({}, this.opts), { connection }));
                this._repeat.on('error', e => this.emit.bind(this, e));
            }
            resolve(this._repeat);
        });
    }
    async run() {
        if (!this.processFn) {
            throw new Error('No process function is defined.');
        }
        if (this.running) {
            throw new Error('Worker is already running.');
        }
        try {
            this.running = true;
            if (this.closing) {
                return;
            }
            await this.startStalledCheckTimer();
            const jobsInProgress = new Set();
            this.startLockExtenderTimer(jobsInProgress);
            const asyncFifoQueue = (this.asyncFifoQueue =
                new async_fifo_queue_1.AsyncFifoQueue());
            let tokenPostfix = 0;
            while (!this.closing) {
                while (!this.waiting &&
                    asyncFifoQueue.numTotal() < this.opts.concurrency &&
                    (!this.limitUntil || asyncFifoQueue.numTotal() == 0)) {
                    const token = `${this.id}:${tokenPostfix++}`;
                    asyncFifoQueue.add(this.retryIfFailed(() => this.getNextJob(token), this.opts.runRetryDelay));
                }
                const job = await asyncFifoQueue.fetch();
                if (job) {
                    const token = job.token;
                    asyncFifoQueue.add(this.retryIfFailed(() => this.processJob(job, token, () => asyncFifoQueue.numTotal() <= this.opts.concurrency, jobsInProgress), this.opts.runRetryDelay));
                }
            }
            this.running = false;
            return asyncFifoQueue.waitAll();
        }
        catch (error) {
            this.running = false;
            throw error;
        }
    }
    /**
     * Returns a promise that resolves to the next job in queue.
     * @param token - worker token to be assigned to retrieved job
     * @returns a Job or undefined if no job was available in the queue.
     */
    async getNextJob(token, { block = true } = {}) {
        if (this.paused) {
            if (block) {
                await this.paused;
            }
            else {
                return;
            }
        }
        if (this.closing) {
            return;
        }
        if (this.drained && block && !this.limitUntil && !this.waiting) {
            try {
                this.waiting = this.waitForJob();
                try {
                    const jobId = await this.waiting;
                    return this.moveToActive(token, jobId);
                }
                finally {
                    this.waiting = null;
                }
            }
            catch (err) {
                // Swallow error if locally paused or closing since we did force a disconnection
                if (!(this.paused || this.closing) &&
                    (0, utils_1.isNotConnectionError)(err)) {
                    throw err;
                }
            }
        }
        else {
            if (this.limitUntil) {
                // TODO: We need to be able to break this delay when we are closing the worker.
                await this.delay(this.limitUntil);
            }
            return this.moveToActive(token);
        }
    }
    /**
     * Overrides the rate limit to be active for the next jobs.
     *
     * @param expireTimeMs - expire time in ms of this rate limit.
     */
    async rateLimit(expireTimeMs) {
        await this.client.then(client => client.set(this.keys.limiter, Number.MAX_SAFE_INTEGER, 'PX', expireTimeMs));
    }
    async moveToActive(token, jobId) {
        // If we get the special delayed job ID, we pick the delay as the next
        // block timeout.
        if (jobId && jobId.startsWith('0:')) {
            this.blockUntil = parseInt(jobId.split(':')[1]) || 0;
        }
        const [jobData, id, limitUntil, delayUntil] = await this.scripts.moveToActive(token, jobId);
        return this.nextJobFromJobData(jobData, id, limitUntil, delayUntil, token);
    }
    async waitForJob() {
        // I am not sure returning here this quick is a good idea, the main
        // loop could stay looping at a very high speed and consume all CPU time.
        if (this.paused) {
            return;
        }
        try {
            const opts = this.opts;
            if (!this.closing) {
                const client = await this.blockingConnection.client;
                let blockTimeout = Math.max(this.blockUntil
                    ? (this.blockUntil - Date.now()) / 1000
                    : opts.drainDelay, 0.01);
                // Only Redis v6.0.0 and above supports doubles as block time
                blockTimeout = (0, utils_1.isRedisVersionLowerThan)(this.blockingConnection.redisVersion, '6.0.0')
                    ? Math.ceil(blockTimeout)
                    : blockTimeout;
                // We restrict the maximum block timeout to 10 second to avoid
                // blocking the connection for too long in the case of reconnections
                // reference: https://github.com/taskforcesh/bullmq/issues/1658
                blockTimeout = Math.min(blockTimeout, maximumBlockTimeout);
                const jobId = await client.brpoplpush(this.keys.wait, this.keys.active, blockTimeout);
                return jobId;
            }
        }
        catch (error) {
            if ((0, utils_1.isNotConnectionError)(error)) {
                this.emit('error', error);
            }
            if (!this.closing) {
                await this.delay();
            }
        }
        finally {
            this.waiting = null;
        }
    }
    /**
     *
     * This function is exposed only for testing purposes.
     */
    async delay(milliseconds) {
        await (0, utils_1.delay)(milliseconds || utils_1.DELAY_TIME_1);
    }
    async nextJobFromJobData(jobData, jobId, limitUntil, delayUntil, token) {
        if (!jobData) {
            if (!this.drained) {
                this.emit('drained');
                this.drained = true;
                this.blockUntil = 0;
            }
        }
        this.limitUntil = Math.max(limitUntil, 0) || 0;
        if (delayUntil) {
            this.blockUntil = Math.max(delayUntil, 0) || 0;
        }
        if (jobData) {
            this.drained = false;
            const job = this.createJob(jobData, jobId);
            job.token = token;
            if (job.opts.repeat) {
                const repeat = await this.repeat;
                await repeat.addNextRepeatableJob(job.name, job.data, job.opts);
            }
            return job;
        }
    }
    async processJob(job, token, fetchNextCallback = () => true, jobsInProgress) {
        if (!job || this.closing || this.paused) {
            return;
        }
        const handleCompleted = async (result) => {
            if (!this.connection.closing) {
                const completed = await job.moveToCompleted(result, token, fetchNextCallback() && !(this.closing || this.paused));
                this.emit('completed', job, result, 'active');
                const [jobData, jobId, limitUntil, delayUntil] = completed || [];
                return this.nextJobFromJobData(jobData, jobId, limitUntil, delayUntil, token);
            }
        };
        const handleFailed = async (err) => {
            if (!this.connection.closing) {
                try {
                    if (err.message == RATE_LIMIT_ERROR) {
                        this.limitUntil = await this.moveLimitedBackToWait(job, token);
                        return;
                    }
                    if (err instanceof delayed_error_1.DelayedError ||
                        err.name == 'DelayedError' ||
                        err instanceof waiting_children_error_1.WaitingChildrenError ||
                        err.name == 'WaitingChildrenError') {
                        return;
                    }
                    await job.moveToFailed(err, token);
                    this.emit('failed', job, err, 'active');
                }
                catch (err) {
                    this.emit('error', err);
                    // It probably means that the job has lost the lock before completion
                    // A worker will (or already has) moved the job back
                    // to the waiting list (as stalled)
                }
            }
        };
        this.emit('active', job, 'waiting');
        const inProgressItem = { job, ts: Date.now() };
        try {
            jobsInProgress.add(inProgressItem);
            const result = await this.callProcessJob(job, token);
            return await handleCompleted(result);
        }
        catch (err) {
            return handleFailed(err);
        }
        finally {
            jobsInProgress.delete(inProgressItem);
        }
    }
    /**
     *
     * Pauses the processing of this queue only for this worker.
     */
    async pause(doNotWaitActive) {
        if (!this.paused) {
            this.paused = new Promise(resolve => {
                this.resumeWorker = function () {
                    resolve();
                    this.paused = null; // Allow pause to be checked externally for paused state.
                    this.resumeWorker = null;
                };
            });
            await (!doNotWaitActive && this.whenCurrentJobsFinished());
            this.emit('paused');
        }
    }
    /**
     *
     * Resumes processing of this worker (if paused).
     */
    resume() {
        if (this.resumeWorker) {
            this.resumeWorker();
            this.emit('resumed');
        }
    }
    /**
     *
     * Checks if worker is paused.
     *
     * @returns true if worker is paused, false otherwise.
     */
    isPaused() {
        return !!this.paused;
    }
    /**
     *
     * Checks if worker is currently running.
     *
     * @returns true if worker is running, false otherwise.
     */
    isRunning() {
        return this.running;
    }
    /**
     *
     * Closes the worker and related redis connections.
     *
     * This method waits for current jobs to finalize before returning.
     *
     * @param force - Use force boolean parameter if you do not want to wait for
     * current jobs to be processed.
     *
     * @returns Promise that resolves when the worker has been closed.
     */
    close(force = false) {
        if (this.closing) {
            return this.closing;
        }
        this.closing = (async () => {
            this.emit('closing', 'closing queue');
            const client = await this.blockingConnection.client;
            this.resume();
            await Promise.resolve()
                .finally(() => {
                return force || this.whenCurrentJobsFinished(false);
            })
                .finally(() => {
                var _a;
                const closePoolPromise = (_a = this.childPool) === null || _a === void 0 ? void 0 : _a.clean();
                if (force) {
                    // since we're not waiting for the job to end attach
                    // an error handler to avoid crashing the whole process
                    closePoolPromise === null || closePoolPromise === void 0 ? void 0 : closePoolPromise.catch(err => {
                        console.error(err); // TODO: emit error in next breaking change version
                    });
                    return;
                }
                return closePoolPromise;
            })
                .finally(() => clearTimeout(this.extendLocksTimer))
                .finally(() => clearTimeout(this.stalledCheckTimer))
                .finally(() => client.disconnect())
                .finally(() => this.connection.close())
                .finally(() => this.emit('closed'));
        })();
        return this.closing;
    }
    /**
     *
     * Manually starts the stalled checker.
     * The check will run once as soon as this method is called, and
     * then every opts.stalledInterval milliseconds until the worker is closed.
     * Note: Normally you do not need to call this method, since the stalled checker
     * is automatically started when the worker starts processing jobs after
     * calling run. However if you want to process the jobs manually you need
     * to call this method to start the stalled checker.
     *
     * @see {@link https://docs.bullmq.io/patterns/manually-fetching-jobs}
     */
    async startStalledCheckTimer() {
        if (!this.opts.skipStalledCheck) {
            clearTimeout(this.stalledCheckTimer);
            if (!this.closing) {
                try {
                    await this.checkConnectionError(() => this.moveStalledJobsToWait());
                    this.stalledCheckTimer = setTimeout(async () => {
                        await this.startStalledCheckTimer();
                    }, this.opts.stalledInterval);
                }
                catch (err) {
                    this.emit('error', err);
                }
            }
        }
    }
    startLockExtenderTimer(jobsInProgress) {
        if (!this.opts.skipLockRenewal) {
            clearTimeout(this.extendLocksTimer);
            if (!this.closing) {
                this.extendLocksTimer = setTimeout(async () => {
                    // Get all the jobs whose locks expire in less than 1/2 of the lockRenewTime
                    const now = Date.now();
                    const jobsToExtend = [];
                    for (const item of jobsInProgress) {
                        const { job, ts } = item;
                        if (!ts) {
                            item.ts = now;
                            continue;
                        }
                        if (ts + this.opts.lockRenewTime / 2 < now) {
                            item.ts = now;
                            jobsToExtend.push(job);
                        }
                    }
                    try {
                        if (jobsToExtend.length) {
                            await this.extendLocks(jobsToExtend);
                        }
                    }
                    catch (err) {
                        this.emit('error', err);
                    }
                    this.startLockExtenderTimer(jobsInProgress);
                }, this.opts.lockRenewTime / 2);
            }
        }
    }
    /**
     * Returns a promise that resolves when active jobs are cleared
     *
     * @returns
     */
    async whenCurrentJobsFinished(reconnect = true) {
        //
        // Force reconnection of blocking connection to abort blocking redis call immediately.
        //
        if (this.waiting) {
            await this.blockingConnection.disconnect();
        }
        else {
            reconnect = false;
        }
        if (this.asyncFifoQueue) {
            await this.asyncFifoQueue.waitAll();
        }
        reconnect && (await this.blockingConnection.reconnect());
    }
    async retryIfFailed(fn, delayInMs) {
        const retry = 1;
        do {
            try {
                return await fn();
            }
            catch (err) {
                this.emit('error', err);
                if (delayInMs) {
                    await this.delay(delayInMs);
                }
                else {
                    return;
                }
            }
        } while (retry);
    }
    async extendLocks(jobs) {
        try {
            const multi = (await this.client).multi();
            for (const job of jobs) {
                await this.scripts.extendLock(job.id, job.token, this.opts.lockDuration, multi);
            }
            const result = (await multi.exec());
            for (const [err, jobId] of result) {
                if (err) {
                    // TODO: signal process function that the job has been lost.
                    this.emit('error', new Error(`could not renew lock for job ${jobId}`));
                }
            }
        }
        catch (err) {
            this.emit('error', err);
        }
    }
    async moveStalledJobsToWait() {
        const chunkSize = 50;
        const [failed, stalled] = await this.scripts.moveStalledJobsToWait();
        stalled.forEach((jobId) => this.emit('stalled', jobId, 'active'));
        const jobPromises = [];
        for (let i = 0; i < failed.length; i++) {
            jobPromises.push(job_1.Job.fromId(this, failed[i]));
            if ((i + 1) % chunkSize === 0) {
                this.notifyFailedJobs(await Promise.all(jobPromises));
                jobPromises.length = 0;
            }
        }
        this.notifyFailedJobs(await Promise.all(jobPromises));
    }
    notifyFailedJobs(failedJobs) {
        failedJobs.forEach((job) => this.emit('failed', job, new Error('job stalled more than allowable limit'), 'active'));
    }
    moveLimitedBackToWait(job, token) {
        return this.scripts.moveJobFromActiveToWait(job.id, token);
    }
}
exports.Worker = Worker;
//# sourceMappingURL=worker.js.map