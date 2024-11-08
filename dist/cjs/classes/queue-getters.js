/*eslint-env node */
'use strict';
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueueGetters = void 0;
const queue_base_1 = require("./queue-base");
const job_1 = require("./job");
const utils_1 = require("../utils");
/**
 *
 * @class QueueGetters
 * @extends QueueBase
 *
 * @description Provides different getters for different aspects of a queue.
 */
class QueueGetters extends queue_base_1.QueueBase {
    getJob(jobId) {
        return this.Job.fromId(this, jobId);
    }
    commandByType(types, count, callback) {
        return types.map((type) => {
            type = type === 'waiting' ? 'wait' : type; // alias
            const key = this.toKey(type);
            switch (type) {
                case 'completed':
                case 'failed':
                case 'delayed':
                case 'repeat':
                case 'waiting-children':
                    return callback(key, count ? 'zcard' : 'zrange');
                case 'active':
                case 'wait':
                case 'paused':
                    return callback(key, count ? 'llen' : 'lrange');
            }
        });
    }
    /**
     * Helper to easily extend Job class calls.
     */
    get Job() {
        return job_1.Job;
    }
    sanitizeJobTypes(types) {
        const currentTypes = typeof types === 'string' ? [types] : types;
        if (Array.isArray(currentTypes) && currentTypes.length > 0) {
            const sanitizedTypes = [...currentTypes];
            if (sanitizedTypes.indexOf('waiting') !== -1) {
                sanitizedTypes.push('paused');
            }
            return [...new Set(sanitizedTypes)];
        }
        return [
            'active',
            'completed',
            'delayed',
            'failed',
            'paused',
            'waiting',
            'waiting-children',
        ];
    }
    /**
      Returns the number of jobs waiting to be processed. This includes jobs that are "waiting" or "delayed".
    */
    async count() {
        const count = await this.getJobCountByTypes('waiting', 'paused', 'delayed', 'waiting-children');
        return count;
    }
    /**
     * Job counts by type
     *
     * Queue#getJobCountByTypes('completed') => completed count
     * Queue#getJobCountByTypes('completed,failed') => completed + failed count
     * Queue#getJobCountByTypes('completed', 'failed') => completed + failed count
     * Queue#getJobCountByTypes('completed', 'waiting', 'failed') => completed + waiting + failed count
     */
    async getJobCountByTypes(...types) {
        const result = await this.getJobCounts(...types);
        return Object.values(result).reduce((sum, count) => sum + count, 0);
    }
    /**
     * Returns the job counts for each type specified or every list/set in the queue by default.
     *
     * @returns An object, key (type) and value (count)
     */
    async getJobCounts(...types) {
        const currentTypes = this.sanitizeJobTypes(types);
        const responses = await this.scripts.getCounts(currentTypes);
        const counts = {};
        responses.forEach((res, index) => {
            counts[currentTypes[index]] = res || 0;
        });
        return counts;
    }
    /**
     * Get current job state.
     *
     * @returns Returns one of these values:
     * 'completed', 'failed', 'delayed', 'active', 'waiting', 'waiting-children', 'unknown'.
     */
    getJobState(jobId) {
        return this.scripts.getState(jobId);
    }
    /**
     * Returns the number of jobs in completed status.
     */
    getCompletedCount() {
        return this.getJobCountByTypes('completed');
    }
    /**
     * Returns the number of jobs in failed status.
     */
    getFailedCount() {
        return this.getJobCountByTypes('failed');
    }
    /**
     * Returns the number of jobs in delayed status.
     */
    getDelayedCount() {
        return this.getJobCountByTypes('delayed');
    }
    /**
     * Returns the number of jobs in active status.
     */
    getActiveCount() {
        return this.getJobCountByTypes('active');
    }
    /**
     * Returns the number of jobs in waiting or paused statuses.
     */
    getWaitingCount() {
        return this.getJobCountByTypes('waiting');
    }
    /**
     * Returns the number of jobs in waiting-children status.
     */
    getWaitingChildrenCount() {
        return this.getJobCountByTypes('waiting-children');
    }
    /**
     * Returns the jobs that are in the "waiting" status.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     */
    getWaiting(start = 0, end = -1) {
        return this.getJobs(['waiting'], start, end, true);
    }
    /**
     * Returns the jobs that are in the "waiting" status.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     */
    getWaitingChildren(start = 0, end = -1) {
        return this.getJobs(['waiting-children'], start, end, true);
    }
    /**
     * Returns the jobs that are in the "active" status.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     */
    getActive(start = 0, end = -1) {
        return this.getJobs(['active'], start, end, true);
    }
    /**
     * Returns the jobs that are in the "delayed" status.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     */
    getDelayed(start = 0, end = -1) {
        return this.getJobs(['delayed'], start, end, true);
    }
    /**
     * Returns the jobs that are in the "completed" status.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     */
    getCompleted(start = 0, end = -1) {
        return this.getJobs(['completed'], start, end, false);
    }
    /**
     * Returns the jobs that are in the "failed" status.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     */
    getFailed(start = 0, end = -1) {
        return this.getJobs(['failed'], start, end, false);
    }
    async getRanges(types, start = 0, end = 1, asc = false) {
        const multiCommands = [];
        this.commandByType(types, false, (key, command) => {
            switch (command) {
                case 'lrange':
                    multiCommands.push('lrange');
                    break;
                case 'zrange':
                    multiCommands.push('zrange');
                    break;
            }
        });
        const responses = await this.scripts.getRanges(types, start, end, asc);
        let results = [];
        responses.forEach((response, index) => {
            const result = response || [];
            if (asc && multiCommands[index] === 'lrange') {
                results = results.concat(result.reverse());
            }
            else {
                results = results.concat(result);
            }
        });
        return [...new Set(results)];
    }
    /**
     * Returns the jobs that are on the given statuses (note that JobType is synonym for job status)
     * @param types - the statuses of the jobs to return.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     * @param asc - if true, the jobs will be returned in ascending order.
     */
    async getJobs(types, start = 0, end = -1, asc = false) {
        types = this.sanitizeJobTypes(types);
        const jobIds = await this.getRanges(types, start, end, asc);
        return Promise.all(jobIds.map(jobId => this.Job.fromId(this, jobId)));
    }
    /**
     * Returns the logs for a given Job.
     * @param jobId - the id of the job to get the logs for.
     * @param start - zero based index from where to start returning jobs.
     * @param end - zero based index where to stop returning jobs.
     * @param asc - if true, the jobs will be returned in ascending order.
     */
    async getJobLogs(jobId, start = 0, end = -1, asc = true) {
        const client = await this.client;
        const multi = client.multi();
        const logsKey = this.toKey(jobId + ':logs');
        if (asc) {
            multi.lrange(logsKey, start, end);
        }
        else {
            multi.lrange(logsKey, -(end + 1), -(start + 1));
        }
        multi.llen(logsKey);
        const result = (await multi.exec());
        if (!asc) {
            result[0][1].reverse();
        }
        return {
            logs: result[0][1],
            count: result[1][1],
        };
    }
    async baseGetClients(suffix) {
        const client = await this.client;
        const clients = (await client.client('LIST'));
        try {
            const list = this.parseClientList(clients, suffix);
            return list;
        }
        catch (err) {
            if (!utils_1.clientCommandMessageReg.test(err.message)) {
                throw err;
            }
            return [];
        }
    }
    /**
     * Get the worker list related to the queue. i.e. all the known
     * workers that are available to process jobs for this queue.
     * Note: GCP does not support SETNAME, so this call will not work
     *
     * @returns - Returns an array with workers info.
     */
    getWorkers() {
        return this.baseGetClients(utils_1.WORKER_SUFFIX);
    }
    /**
     * Get queue events list related to the queue.
     * Note: GCP does not support SETNAME, so this call will not work
     *
     * @returns - Returns an array with queue events info.
     */
    async getQueueEvents() {
        return this.baseGetClients(utils_1.QUEUE_EVENT_SUFFIX);
    }
    /**
     * Get queue metrics related to the queue.
     *
     * This method returns the gathered metrics for the queue.
     * The metrics are represented as an array of job counts
     * per unit of time (1 minute).
     *
     * @param start - Start point of the metrics, where 0
     * is the newest point to be returned.
     * @param end - End point of the metrics, where -1 is the
     * oldest point to be returned.
     *
     * @returns - Returns an object with queue metrics.
     */
    async getMetrics(type, start = 0, end = -1) {
        const client = await this.client;
        const metricsKey = this.toKey(`metrics:${type}`);
        const dataKey = `${metricsKey}:data`;
        const multi = client.multi();
        multi.hmget(metricsKey, 'count', 'prevTS', 'prevCount');
        multi.lrange(dataKey, start, end);
        multi.llen(dataKey);
        const [hmget, range, len] = (await multi.exec());
        const [err, [count, prevTS, prevCount]] = hmget;
        const [err2, data] = range;
        const [err3, numPoints] = len;
        if (err || err2) {
            throw err || err2 || err3;
        }
        return {
            meta: {
                count: parseInt(count || '0', 10),
                prevTS: parseInt(prevTS || '0', 10),
                prevCount: parseInt(prevCount || '0', 10),
            },
            data,
            count: numPoints,
        };
    }
    parseClientList(list, suffix = '') {
        const lines = list.split('\n');
        const clients = [];
        lines.forEach((line) => {
            const client = {};
            const keyValues = line.split(' ');
            keyValues.forEach(function (keyValue) {
                const index = keyValue.indexOf('=');
                const key = keyValue.substring(0, index);
                const value = keyValue.substring(index + 1);
                client[key] = value;
            });
            const name = client['name'];
            if (name && name === `${this.clientName()}${suffix ? `${suffix}` : ''}`) {
                client['name'] = this.name;
                clients.push(client);
            }
        });
        return clients;
    }
}
exports.QueueGetters = QueueGetters;
//# sourceMappingURL=queue-getters.js.map