import { randomUUID } from "node:crypto";
import CronParser from "cron-parser";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

//#region rolldown:runtime
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function() {
	return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
	if (from && typeof from === "object" || typeof from === "function") for (var keys = __getOwnPropNames(from), i = 0, n = keys.length, key; i < n; i++) {
		key = keys[i];
		if (!__hasOwnProp.call(to, key) && key !== except) __defProp(to, key, {
			get: ((k) => from[k]).bind(null, key),
			enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable
		});
	}
	return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", {
	value: mod,
	enumerable: true
}) : target, mod));

//#endregion
//#region node_modules/@bull-board/api/dist/queueAdapters/base.js
var require_base = /* @__PURE__ */ __commonJS({ "node_modules/@bull-board/api/dist/queueAdapters/base.js": ((exports) => {
	Object.defineProperty(exports, "__esModule", { value: true });
	exports.BaseAdapter = void 0;
	var BaseAdapter$1 = class {
		constructor(type, options = {}) {
			this.formatters = /* @__PURE__ */ new Map();
			this._visibilityGuard = () => true;
			this.readOnlyMode = options.readOnlyMode === true;
			this.allowRetries = this.readOnlyMode ? false : options.allowRetries !== false;
			this.allowCompletedRetries = this.allowRetries && options.allowCompletedRetries !== false;
			this.prefix = options.prefix || "";
			this.delimiter = options.delimiter || "";
			this.description = options.description || "";
			this.displayName = options.displayName || "";
			this.type = type;
			this.externalJobUrl = options.externalJobUrl;
		}
		getDescription() {
			return this.description;
		}
		getDisplayName() {
			return this.displayName;
		}
		setFormatter(field, formatter) {
			this.formatters.set(field, formatter);
		}
		format(field, data, defaultValue = data) {
			const fieldFormatter = this.formatters.get(field);
			return typeof fieldFormatter === "function" ? fieldFormatter(data) : defaultValue;
		}
		setVisibilityGuard(guard) {
			this._visibilityGuard = guard;
		}
		isVisible(request) {
			return this._visibilityGuard(request);
		}
	};
	exports.BaseAdapter = BaseAdapter$1;
}) });

//#endregion
//#region src/adapters/groupmq-bullboard-adapter.ts
var import_base = /* @__PURE__ */ __toESM(require_base(), 1);
var BullBoardGroupMQAdapter = class extends import_base.BaseAdapter {
	constructor(queue, options = {}) {
		const libName = queue.namespace;
		super(libName, options);
		this.queue = queue;
		this.options = options;
	}
	getDescription() {
		return this.options.description || "";
	}
	getDisplayName() {
		return this.options.displayName || "";
	}
	getName() {
		return `${this.options.prefix || ""}${this.options.delimiter || ""}${this.queue.rawNamespace}`.replace(/(^[\s:]+)|([\s:]+$)/g, "");
	}
	async getRedisInfo() {
		return this.queue.redis.info();
	}
	async getJob(id) {
		return await this.queue.getJob(id);
	}
	async getJobs(jobStatuses, start, end) {
		return await this.queue.getJobsByStatus(jobStatuses, start, end);
	}
	async getJobCounts() {
		const base = await this.queue.getJobCounts();
		return {
			latest: 0,
			active: base.active,
			waiting: base.waiting,
			"waiting-children": base["waiting-children"],
			prioritized: base.prioritized,
			completed: base.completed,
			failed: base.failed,
			delayed: base.delayed,
			paused: base.paused
		};
	}
	async getJobLogs(_id) {
		return [];
	}
	getStatuses() {
		return [
			"latest",
			"active",
			"waiting",
			"waiting-children",
			"prioritized",
			"completed",
			"failed",
			"delayed",
			"paused"
		];
	}
	getJobStatuses() {
		return [
			"active",
			"waiting",
			"waiting-children",
			"prioritized",
			"completed",
			"failed",
			"delayed",
			"paused"
		];
	}
	assertWritable() {
		if (this.options.readOnlyMode) throw new Error("This adapter is in read-only mode. Mutations are disabled.");
	}
	async clean(jobStatus, graceTimeMs) {
		this.assertWritable();
		if (jobStatus !== "completed" && jobStatus !== "failed" && jobStatus !== "delayed") return;
		await this.queue.clean(graceTimeMs, Number.MAX_SAFE_INTEGER, jobStatus);
	}
	async addJob(_name, data, options) {
		this.assertWritable();
		return await this.queue.add({
			groupId: options.groupId ?? Math.random().toString(36).substring(2, 15),
			data,
			...options
		});
	}
	async isPaused() {
		return this.queue.isPaused();
	}
	async pause() {
		this.assertWritable();
		await this.queue.pause();
	}
	async resume() {
		this.assertWritable();
		await this.queue.resume();
	}
	async empty() {
		this.assertWritable();
		throw new Error("Not implemented");
	}
	async promoteAll() {
		this.assertWritable();
		throw new Error("Not implemented");
	}
};

//#endregion
//#region src/errors.ts
/**
* Error thrown when a job is cancelled via AbortSignal.
* Handlers can throw this error when they detect cancellation.
*
* @example
* ```ts
* const worker = new Worker(queue, {
*   handler: async (job, signal) => {
*     for (const item of largeDataset) {
*       if (signal.aborted) {
*         throw new JobCancelledError(job.id);
*       }
*       await processItem(item);
*     }
*   },
* });
* ```
*/
var JobCancelledError = class JobCancelledError extends Error {
	constructor(jobId, message) {
		super(message ?? `Job ${jobId} was cancelled`);
		this.name = "JobCancelledError";
		this.jobId = jobId;
		if (Error.captureStackTrace) Error.captureStackTrace(this, JobCancelledError);
	}
};

//#endregion
//#region src/helpers.ts
/**
* Wait for a queue to become empty
* @param queue The queue to monitor
* @param timeoutMs Maximum time to wait (default: 60 seconds)
* @returns Promise that resolves when queue is empty or timeout is reached
*/
async function waitForQueueToEmpty(queue, timeoutMs = 6e4) {
	return queue.waitForEmpty(timeoutMs);
}
/**
* Get status of all workers
*/
function getWorkersStatus(workers) {
	const workersStatus = workers.map((worker, index) => {
		const currentJob = worker.getCurrentJob();
		return {
			index,
			isProcessing: worker.isProcessing(),
			currentJob: currentJob ? {
				jobId: currentJob.job.id,
				groupId: currentJob.job.groupId,
				processingTimeMs: currentJob.processingTimeMs
			} : void 0
		};
	});
	const processing = workersStatus.filter((w) => w.isProcessing).length;
	const idle = workersStatus.length - processing;
	return {
		total: workers.length,
		processing,
		idle,
		workers: workersStatus
	};
}

//#endregion
//#region src/job.ts
var Job = class Job {
	constructor(args) {
		this.queue = args.queue;
		this.id = args.id;
		this.name = args.name ?? "groupmq";
		this.data = args.data;
		this.groupId = args.groupId;
		this.attemptsMade = args.attemptsMade;
		this.opts = args.opts;
		this.processedOn = args.processedOn;
		this.finishedOn = args.finishedOn;
		this.failedReason = args.failedReason;
		this.stacktrace = args.stacktrace;
		this.returnvalue = args.returnvalue;
		this.timestamp = args.timestamp;
		this.orderMs = args.orderMs;
		this.status = args.status ?? "unknown";
	}
	async getState() {
		return this.status ?? "unknown";
	}
	toJSON() {
		return {
			id: this.id,
			name: this.name,
			data: this.data,
			groupId: this.groupId,
			attemptsMade: this.attemptsMade,
			opts: this.opts,
			processedOn: this.processedOn,
			finishedOn: this.finishedOn,
			failedReason: this.failedReason,
			stacktrace: this.stacktrace ? [this.stacktrace] : null,
			returnvalue: this.returnvalue,
			timestamp: this.timestamp,
			orderMs: this.orderMs,
			status: this.status,
			progress: 0
		};
	}
	changeDelay(newDelay) {
		return this.queue.changeDelay(this.id, newDelay);
	}
	async promote() {
		await this.queue.promote(this.id);
	}
	async remove() {
		await this.queue.remove(this.id);
	}
	async retry(_state) {
		await this.queue.retry(this.id);
	}
	async updateData(jobData) {
		await this.queue.updateData(this.id, jobData);
	}
	async update(jobData) {
		await this.updateData(jobData);
	}
	/**
	* Cancel this job if it is currently being processed.
	* Sends an abort signal to the worker processing this job.
	*
	* @returns true if the job was active and cancel signal was sent, false otherwise
	*/
	async cancel() {
		return this.queue.cancel(this.id);
	}
	static fromReserved(queue, reserved, meta) {
		return new Job({
			queue,
			id: reserved.id,
			name: "groupmq",
			data: reserved.data,
			groupId: reserved.groupId,
			attemptsMade: reserved.attempts,
			opts: {
				attempts: reserved.maxAttempts,
				delay: meta?.delayMs
			},
			processedOn: meta?.processedOn,
			finishedOn: meta?.finishedOn,
			failedReason: meta?.failedReason,
			stacktrace: meta?.stacktrace,
			returnvalue: meta?.returnvalue,
			timestamp: reserved.timestamp ? reserved.timestamp : Date.now(),
			orderMs: reserved.orderMs,
			status: coerceStatus(meta?.status)
		});
	}
	/**
	* Create a Job from raw Redis hash data with optional known status
	* This avoids extra Redis lookups when status is already known
	*/
	static fromRawHash(queue, id, raw, knownStatus) {
		const groupId = raw.groupId ?? "";
		const payload = raw.data ? safeJsonParse$1(raw.data) : null;
		const attempts = raw.attempts ? parseInt(raw.attempts, 10) : 0;
		const maxAttempts = raw.maxAttempts ? parseInt(raw.maxAttempts, 10) : queue.maxAttemptsDefault;
		const timestampMs = raw.timestamp ? parseInt(raw.timestamp, 10) : 0;
		const orderMs = raw.orderMs ? parseInt(raw.orderMs, 10) : void 0;
		const delayUntil = raw.delayUntil ? parseInt(raw.delayUntil, 10) : 0;
		const processedOn = raw.processedOn ? parseInt(raw.processedOn, 10) : void 0;
		const finishedOn = raw.finishedOn ? parseInt(raw.finishedOn, 10) : void 0;
		const failedReason = (raw.failedReason ?? raw.lastErrorMessage) || void 0;
		const stacktrace = (raw.stacktrace ?? raw.lastErrorStack) || void 0;
		const returnvalue = raw.returnvalue ? safeJsonParse$1(raw.returnvalue) : void 0;
		return new Job({
			queue,
			id,
			name: "groupmq",
			data: payload,
			groupId,
			attemptsMade: attempts,
			opts: {
				attempts: maxAttempts,
				delay: delayUntil && delayUntil > Date.now() ? delayUntil - Date.now() : void 0
			},
			processedOn,
			finishedOn,
			failedReason,
			stacktrace,
			returnvalue,
			timestamp: timestampMs || Date.now(),
			orderMs,
			status: knownStatus ?? coerceStatus(raw.status)
		});
	}
	static async fromStore(queue, id) {
		const jobKey = `${queue.namespace}:job:${id}`;
		const raw = await queue.redis.hgetall(jobKey);
		if (!raw || Object.keys(raw).length === 0) throw new Error(`Job ${id} not found`);
		const groupId = raw.groupId ?? "";
		const payload = raw.data ? safeJsonParse$1(raw.data) : null;
		const attempts = raw.attempts ? parseInt(raw.attempts, 10) : 0;
		const maxAttempts = raw.maxAttempts ? parseInt(raw.maxAttempts, 10) : queue.maxAttemptsDefault;
		const timestampMs = raw.timestamp ? parseInt(raw.timestamp, 10) : 0;
		const orderMs = raw.orderMs ? parseInt(raw.orderMs, 10) : void 0;
		const delayUntil = raw.delayUntil ? parseInt(raw.delayUntil, 10) : 0;
		const processedOn = raw.processedOn ? parseInt(raw.processedOn, 10) : void 0;
		const finishedOn = raw.finishedOn ? parseInt(raw.finishedOn, 10) : void 0;
		const failedReason = (raw.failedReason ?? raw.lastErrorMessage) || void 0;
		const stacktrace = (raw.stacktrace ?? raw.lastErrorStack) || void 0;
		const returnvalue = raw.returnvalue ? safeJsonParse$1(raw.returnvalue) : void 0;
		const [inProcessing, inDelayed] = await Promise.all([queue.redis.zscore(`${queue.namespace}:processing`, id), queue.redis.zscore(`${queue.namespace}:delayed`, id)]);
		let status = raw.status;
		if (inProcessing !== null) status = "active";
		else if (inDelayed !== null) status = "delayed";
		else if (groupId) {
			if (await queue.redis.zscore(`${queue.namespace}:g:${groupId}`, id) !== null) status = "waiting";
		}
		return new Job({
			queue,
			id,
			name: "groupmq",
			data: payload,
			groupId,
			attemptsMade: attempts,
			opts: {
				attempts: maxAttempts,
				delay: delayUntil && delayUntil > Date.now() ? delayUntil - Date.now() : void 0
			},
			processedOn,
			finishedOn,
			failedReason,
			stacktrace,
			returnvalue,
			timestamp: timestampMs || Date.now(),
			orderMs,
			status: coerceStatus(status)
		});
	}
};
function safeJsonParse$1(input) {
	try {
		return JSON.parse(input);
	} catch (_e) {
		return null;
	}
}
function coerceStatus(input) {
	const valid = [
		"latest",
		"active",
		"waiting",
		"waiting-children",
		"prioritized",
		"completed",
		"failed",
		"delayed",
		"paused"
	];
	if (!input) return "unknown";
	if (valid.includes(input)) return input;
	return "unknown";
}

//#endregion
//#region src/logger.ts
var Logger = class {
	constructor(enabled, name) {
		this.enabled = enabled;
		this.name = name;
	}
	debug(...args) {
		if (this.enabled) console.debug(`[${this.name}]`, ...args);
	}
	info(...args) {
		if (this.enabled) console.log(`[${this.name}]`, ...args);
	}
	warn(...args) {
		if (this.enabled) console.warn(`⚠️ [${this.name}]`, ...args);
	}
	error(...args) {
		if (this.enabled) console.error(`💥 [${this.name}]`, ...args);
	}
};

//#endregion
//#region src/lua/loader.ts
const cacheByClient = /* @__PURE__ */ new WeakMap();
function scriptPath(name) {
	const currentDir = path.dirname(fileURLToPath(import.meta.url));
	const candidates = [path.join(currentDir, `${name}.lua`), path.join(currentDir, "lua", `${name}.lua`)];
	for (const candidate of candidates) if (fs.existsSync(candidate)) return candidate;
	return candidates[0];
}
async function loadScript(client, name) {
	let map = cacheByClient.get(client);
	if (!map) {
		map = /* @__PURE__ */ new Map();
		cacheByClient.set(client, map);
	}
	const cached = map.get(name);
	if (cached) return cached;
	const file = scriptPath(name);
	const lua = fs.readFileSync(file, "utf8");
	const sha = await client.script("load", lua);
	map.set(name, sha);
	return sha;
}
async function evalScript(client, name, argv, numKeys) {
	const sha = await loadScript(client, name);
	return client.evalsha(sha, numKeys, ...argv);
}

//#endregion
//#region src/queue.ts
function nsKey(ns, ...parts) {
	return [ns, ...parts].join(":");
}
function safeJsonParse(input) {
	try {
		return JSON.parse(input);
	} catch (_e) {
		return null;
	}
}
var Queue = class {
	constructor(opts) {
		this._consecutiveEmptyReserves = 0;
		this.promoterRunning = false;
		this.batchBuffer = [];
		this.flushing = false;
		this._groupCleanupTracking = /* @__PURE__ */ new Map();
		this.r = opts.redis;
		this.rawNs = opts.namespace;
		this.name = opts.namespace;
		this.ns = `groupmq:${this.rawNs}`;
		const rawVt = opts.jobTimeoutMs ?? 3e4;
		this.vt = Math.max(1, rawVt);
		this.defaultMaxAttempts = opts.maxAttempts ?? 3;
		this.scanLimit = opts.reserveScanLimit ?? 20;
		this.keepCompleted = Math.max(0, opts.keepCompleted ?? 0);
		this.keepFailed = Math.max(0, opts.keepFailed ?? 0);
		this.schedulerLockTtlMs = opts.schedulerLockTtlMs ?? 1500;
		this.orderingDelayMs = opts.orderingDelayMs ?? 0;
		if (opts.autoBatch) this.batchConfig = typeof opts.autoBatch === "boolean" ? {
			size: 10,
			maxWaitMs: 10
		} : {
			size: opts.autoBatch.size ?? 10,
			maxWaitMs: opts.autoBatch.maxWaitMs ?? 10
		};
		this.logger = typeof opts.logger === "object" ? opts.logger : new Logger(!!opts.logger, this.namespace);
		this.r.on("error", (err) => {
			this.logger.error("Redis error (main):", err);
		});
	}
	get redis() {
		return this.r;
	}
	get namespace() {
		return this.ns;
	}
	get rawNamespace() {
		return this.rawNs;
	}
	get jobTimeoutMs() {
		return this.vt;
	}
	get maxAttemptsDefault() {
		return this.defaultMaxAttempts;
	}
	async add(opts) {
		const maxAttempts = opts.maxAttempts ?? this.defaultMaxAttempts;
		const orderMs = opts.orderMs ?? Date.now();
		const now = Date.now();
		const jobId = opts.jobId ?? randomUUID();
		if (opts.repeat) return this.addRepeatingJob({
			...opts,
			orderMs,
			maxAttempts
		});
		let delayMs;
		if (opts.delay !== void 0 && opts.delay > 0) delayMs = opts.delay;
		else if (opts.runAt !== void 0) {
			const runAtTimestamp = opts.runAt instanceof Date ? opts.runAt.getTime() : opts.runAt;
			delayMs = Math.max(0, runAtTimestamp - now);
		}
		const data = opts.data === void 0 ? null : opts.data;
		if (this.batchConfig) return new Promise((resolve, reject) => {
			this.batchBuffer.push({
				groupId: opts.groupId,
				data,
				jobId,
				maxAttempts,
				delayMs,
				orderMs,
				resolve,
				reject
			});
			if (this.batchBuffer.length >= this.batchConfig.size) this.flushBatch();
			else if (!this.batchTimer) this.batchTimer = setTimeout(() => this.flushBatch(), this.batchConfig.maxWaitMs);
		});
		return this.addSingle({
			...opts,
			data,
			jobId,
			maxAttempts,
			orderMs,
			delayMs
		});
	}
	async addSingle(opts) {
		const now = Date.now();
		let delayUntil = 0;
		if (opts.delayMs !== void 0 && opts.delayMs > 0) delayUntil = now + opts.delayMs;
		const serializedPayload = JSON.stringify(opts.data);
		const result = await evalScript(this.r, "enqueue", [
			this.ns,
			opts.groupId,
			serializedPayload,
			String(opts.maxAttempts),
			String(opts.orderMs),
			String(delayUntil),
			String(opts.jobId),
			String(this.keepCompleted),
			String(now),
			String(this.orderingDelayMs)
		], 1);
		if (Array.isArray(result)) {
			const [returnedJobId, returnedGroupId, returnedData, attempts, returnedMaxAttempts, timestamp, returnedOrderMs, returnedDelayUntil, status] = result;
			return Job.fromRawHash(this, returnedJobId, {
				id: returnedJobId,
				groupId: returnedGroupId,
				data: returnedData,
				attempts,
				maxAttempts: returnedMaxAttempts,
				timestamp,
				orderMs: returnedOrderMs,
				delayUntil: returnedDelayUntil,
				status
			}, status);
		}
		return this.getJob(result);
	}
	async flushBatch() {
		if (this.batchTimer) {
			clearTimeout(this.batchTimer);
			this.batchTimer = void 0;
		}
		if (this.batchBuffer.length === 0 || this.flushing) return;
		this.flushing = true;
		const batch = this.batchBuffer.splice(0);
		try {
			this.logger.debug(`Flushing batch of ${batch.length} jobs`);
			const now = Date.now();
			const jobsData = batch.map((job) => ({
				jobId: job.jobId,
				groupId: job.groupId,
				data: JSON.stringify(job.data),
				maxAttempts: job.maxAttempts,
				orderMs: job.orderMs,
				delayMs: job.delayMs
			}));
			const jobDataArrays = await evalScript(this.r, "enqueue-batch", [
				this.ns,
				JSON.stringify(jobsData),
				String(this.keepCompleted),
				String(now),
				String(this.orderingDelayMs)
			], 1);
			for (let i = 0; i < batch.length; i++) {
				const job = batch[i];
				const jobDataArray = jobDataArrays[i];
				try {
					if (jobDataArray && jobDataArray.length >= 9) {
						const [returnedJobId, returnedGroupId, returnedData, attempts, returnedMaxAttempts, timestamp, returnedOrderMs, returnedDelayUntil, status] = jobDataArray;
						const jobEntity = Job.fromRawHash(this, returnedJobId, {
							id: returnedJobId,
							groupId: returnedGroupId,
							data: returnedData,
							attempts,
							maxAttempts: returnedMaxAttempts,
							timestamp,
							orderMs: returnedOrderMs,
							delayUntil: returnedDelayUntil,
							status
						}, status);
						job.resolve(jobEntity);
					} else throw new Error("Invalid job data returned from batch enqueue");
				} catch (err) {
					job.reject(err instanceof Error ? err : new Error(String(err)));
				}
			}
		} catch (err) {
			for (const job of batch) job.reject(err instanceof Error ? err : new Error(String(err)));
		} finally {
			this.flushing = false;
			if (this.batchBuffer.length > 0) setImmediate(() => this.flushBatch());
		}
	}
	async reserve() {
		const now = Date.now();
		const raw = await evalScript(this.r, "reserve", [
			this.ns,
			String(now),
			String(this.vt),
			String(this.scanLimit)
		], 1);
		if (!raw) return null;
		const parts = raw.split("|||");
		if (parts.length !== 10) return null;
		let data;
		try {
			data = JSON.parse(parts[2]);
		} catch (err) {
			this.logger.warn(`Failed to parse job data: ${err.message}, raw: ${parts[2]}`);
			data = null;
		}
		const parsedOrderMs = Number.parseInt(parts[7], 10);
		return {
			id: parts[0],
			groupId: parts[1],
			data,
			attempts: Number.parseInt(parts[3], 10),
			maxAttempts: Number.parseInt(parts[4], 10),
			seq: Number.parseInt(parts[5], 10),
			timestamp: Number.parseInt(parts[6], 10),
			orderMs: Number.isNaN(parsedOrderMs) ? Number.parseInt(parts[6], 10) : parsedOrderMs,
			score: Number(parts[8]),
			deadlineAt: Number.parseInt(parts[9], 10)
		};
	}
	/**
	* Check how many jobs are waiting in a specific group
	*/
	async getGroupJobCount(groupId) {
		const gZ = `${this.ns}:g:${groupId}`;
		return await this.r.zcard(gZ);
	}
	/**
	* Complete a job by removing from processing and unlocking the group.
	* Note: Job metadata recording is handled separately by recordCompleted().
	*
	* @deprecated Use completeWithMetadata() for internal operations. This method
	* is kept for backward compatibility and testing only.
	*/
	async complete(job) {
		await evalScript(this.r, "complete", [
			this.ns,
			job.id,
			job.groupId
		], 1);
	}
	/**
	* Complete a job AND record metadata in a single atomic operation.
	* This is the efficient internal method used by workers.
	*/
	async completeWithMetadata(job, result, meta) {
		await evalScript(this.r, "complete-with-metadata", [
			this.ns,
			job.id,
			job.groupId,
			"completed",
			String(meta.finishedOn),
			JSON.stringify(result ?? null),
			String(this.keepCompleted),
			String(this.keepFailed),
			String(meta.processedOn),
			String(meta.finishedOn),
			String(meta.attempts),
			String(meta.maxAttempts)
		], 1);
	}
	/**
	* Atomically complete a job and try to reserve the next job from the same group
	* This prevents race conditions where other workers can steal subsequent jobs from the same group
	*/
	/**
	* Atomically complete a job with metadata and reserve the next job from the same group.
	*/
	async completeAndReserveNextWithMetadata(completedJobId, groupId, handlerResult, meta) {
		const now = Date.now();
		try {
			const result = await evalScript(this.r, "complete-and-reserve-next-with-metadata", [
				this.ns,
				completedJobId,
				groupId,
				"completed",
				String(meta.finishedOn),
				JSON.stringify(handlerResult ?? null),
				String(this.keepCompleted),
				String(this.keepFailed),
				String(meta.processedOn),
				String(meta.finishedOn),
				String(meta.attempts),
				String(meta.maxAttempts),
				String(now),
				String(this.jobTimeoutMs)
			], 1);
			if (!result) return null;
			const parts = result.split("|||");
			if (parts.length !== 10) {
				this.logger.error("Queue completeAndReserveNextWithMetadata: unexpected result format:", result);
				return null;
			}
			const [id, , data, attempts, maxAttempts, seq, enq, orderMs, score, deadline] = parts;
			return {
				id,
				groupId,
				data: JSON.parse(data),
				attempts: parseInt(attempts, 10),
				maxAttempts: parseInt(maxAttempts, 10),
				seq: parseInt(seq, 10),
				timestamp: parseInt(enq, 10),
				orderMs: parseInt(orderMs, 10),
				score: parseFloat(score),
				deadlineAt: parseInt(deadline, 10)
			};
		} catch (error) {
			this.logger.error("Queue completeAndReserveNextWithMetadata error:", error);
			return null;
		}
	}
	/**
	* Check if a job is currently in processing state
	*/
	async isJobProcessing(jobId) {
		return await this.r.zscore(`${this.ns}:processing`, jobId) !== null;
	}
	async retry(jobId, backoffMs = 0) {
		return evalScript(this.r, "retry", [
			this.ns,
			jobId,
			String(backoffMs)
		], 1);
	}
	/**
	* Dead letter a job (remove from group and optionally store in dead letter queue)
	*/
	async deadLetter(jobId, groupId) {
		return evalScript(this.r, "dead-letter", [
			this.ns,
			jobId,
			groupId
		], 1);
	}
	/**
	* Record a successful completion for retention and inspection
	* Uses consolidated Lua script for atomic operation with retention management
	*/
	async recordCompleted(job, result, meta) {
		const processedOn = meta.processedOn ?? Date.now();
		const finishedOn = meta.finishedOn ?? Date.now();
		const attempts = meta.attempts ?? 0;
		const maxAttempts = meta.maxAttempts ?? this.defaultMaxAttempts;
		try {
			await evalScript(this.r, "record-job-result", [
				this.ns,
				job.id,
				"completed",
				String(finishedOn),
				JSON.stringify(result ?? null),
				String(this.keepCompleted),
				String(this.keepFailed),
				String(processedOn),
				String(finishedOn),
				String(attempts),
				String(maxAttempts)
			], 1);
		} catch (error) {
			this.logger.error(`Error recording completion for job ${job.id}:`, error);
			throw error;
		}
	}
	/**
	* Record a failure attempt (non-final), storing last error for visibility
	*/
	async recordAttemptFailure(job, error, meta) {
		const jobKey = `${this.ns}:job:${job.id}`;
		const processedOn = meta.processedOn ?? Date.now();
		const finishedOn = meta.finishedOn ?? Date.now();
		const message = typeof error === "string" ? error : error.message ?? "Error";
		const name = typeof error === "string" ? "Error" : error.name ?? "Error";
		const stack = typeof error === "string" ? "" : error.stack ?? "";
		await this.r.hset(jobKey, "lastErrorMessage", message, "lastErrorName", name, "lastErrorStack", stack, "processedOn", String(processedOn), "finishedOn", String(finishedOn));
	}
	/**
	* Record a final failure (dead-lettered) for retention and inspection
	* Uses consolidated Lua script for atomic operation
	*/
	async recordFinalFailure(job, error, meta) {
		const processedOn = meta.processedOn ?? Date.now();
		const finishedOn = meta.finishedOn ?? Date.now();
		const attempts = meta.attempts ?? 0;
		const maxAttempts = meta.maxAttempts ?? this.defaultMaxAttempts;
		const message = typeof error === "string" ? error : error.message ?? "Error";
		const name = typeof error === "string" ? "Error" : error.name ?? "Error";
		const stack = typeof error === "string" ? "" : error.stack ?? "";
		const errorInfo = JSON.stringify({
			message,
			name,
			stack
		});
		try {
			await evalScript(this.r, "record-job-result", [
				this.ns,
				job.id,
				"failed",
				String(finishedOn),
				errorInfo,
				String(this.keepCompleted),
				String(this.keepFailed),
				String(processedOn),
				String(finishedOn),
				String(attempts),
				String(maxAttempts)
			], 1);
		} catch (err) {
			this.logger.error(`Error recording final failure for job ${job.id}:`, err);
			throw err;
		}
	}
	async getCompleted(limit = this.keepCompleted) {
		const completedKey = `${this.ns}:completed`;
		const ids = await this.r.zrevrange(completedKey, 0, Math.max(0, limit - 1));
		if (ids.length === 0) return [];
		const pipe = this.r.multi();
		for (const id of ids) pipe.hmget(`${this.ns}:job:${id}`, "groupId", "data", "returnvalue", "processedOn", "finishedOn", "attempts", "maxAttempts");
		const rows = await pipe.exec() ?? [];
		return ids.map((id, idx) => {
			const [groupId, dataStr, retStr, processedOn, finishedOn, attempts, maxAttempts] = rows[idx]?.[1] || [];
			return {
				id,
				groupId: groupId || "",
				data: dataStr ? safeJsonParse(dataStr) : null,
				returnvalue: retStr ? safeJsonParse(retStr) : null,
				processedOn: processedOn ? parseInt(processedOn, 10) : void 0,
				finishedOn: finishedOn ? parseInt(finishedOn, 10) : void 0,
				attempts: attempts ? parseInt(attempts, 10) : 0,
				maxAttempts: maxAttempts ? parseInt(maxAttempts, 10) : this.defaultMaxAttempts
			};
		});
	}
	async getFailed(limit = this.keepFailed) {
		const failedKey = `${this.ns}:failed`;
		const ids = await this.r.zrevrange(failedKey, 0, Math.max(0, limit - 1));
		if (ids.length === 0) return [];
		const pipe = this.r.multi();
		for (const id of ids) pipe.hmget(`${this.ns}:job:${id}`, "groupId", "data", "failedReason", "stacktrace", "processedOn", "finishedOn", "attempts", "maxAttempts");
		const rows = await pipe.exec() ?? [];
		return ids.map((id, idx) => {
			const [groupId, dataStr, failedReason, stacktrace, processedOn, finishedOn, attempts, maxAttempts] = rows[idx]?.[1] || [];
			return {
				id,
				groupId: groupId || "",
				data: dataStr ? safeJsonParse(dataStr) : null,
				failedReason: failedReason || "",
				stacktrace: stacktrace || void 0,
				processedOn: processedOn ? parseInt(processedOn, 10) : void 0,
				finishedOn: finishedOn ? parseInt(finishedOn, 10) : void 0,
				attempts: attempts ? parseInt(attempts, 10) : 0,
				maxAttempts: maxAttempts ? parseInt(maxAttempts, 10) : this.defaultMaxAttempts
			};
		});
	}
	/**
	* Convenience: return completed jobs as Job entities (non-breaking, new API)
	*/
	async getCompletedJobs(limit = this.keepCompleted) {
		const completedKey = `${this.ns}:completed`;
		const ids = await this.r.zrevrange(completedKey, 0, Math.max(0, limit - 1));
		if (ids.length === 0) return [];
		const pipe = this.r.multi();
		for (const id of ids) pipe.hgetall(`${this.ns}:job:${id}`);
		const rows = await pipe.exec();
		const jobs = [];
		for (let i = 0; i < ids.length; i++) {
			const id = ids[i];
			const raw = rows?.[i]?.[1] || {};
			if (!raw || Object.keys(raw).length === 0) {
				this.logger.warn(`Skipping completed job ${id} - not found (likely cleaned up)`);
				continue;
			}
			const job = Job.fromRawHash(this, id, raw, "completed");
			jobs.push(job);
		}
		return jobs;
	}
	/**
	* Convenience: return failed jobs as Job entities (non-breaking, new API)
	*/
	async getFailedJobs(limit = this.keepFailed) {
		const failedKey = `${this.ns}:failed`;
		const ids = await this.r.zrevrange(failedKey, 0, Math.max(0, limit - 1));
		if (ids.length === 0) return [];
		const pipe = this.r.multi();
		for (const id of ids) pipe.hgetall(`${this.ns}:job:${id}`);
		const rows = await pipe.exec();
		const jobs = [];
		for (let i = 0; i < ids.length; i++) {
			const id = ids[i];
			const raw = rows?.[i]?.[1] || {};
			if (!raw || Object.keys(raw).length === 0) {
				this.logger.warn(`Skipping failed job ${id} - not found (likely cleaned up)`);
				continue;
			}
			const job = Job.fromRawHash(this, id, raw, "failed");
			jobs.push(job);
		}
		return jobs;
	}
	async getCompletedCount() {
		return this.r.zcard(`${this.ns}:completed`);
	}
	async getFailedCount() {
		return this.r.zcard(`${this.ns}:failed`);
	}
	async heartbeat(job, extendMs = this.vt) {
		return evalScript(this.r, "heartbeat", [
			this.ns,
			job.id,
			job.groupId,
			String(extendMs)
		], 1);
	}
	/**
	* Clean up expired jobs and stale data.
	* Uses distributed lock to ensure only one worker runs cleanup at a time,
	* similar to scheduler lock pattern.
	*/
	async cleanup() {
		const cleanupLockKey = `${this.ns}:cleanup:lock`;
		const ttlMs = 6e4;
		try {
			if (await this.r.set(cleanupLockKey, "1", "PX", ttlMs, "NX") !== "OK") return 0;
			const now = Date.now();
			return evalScript(this.r, "cleanup", [this.ns, String(now)], 1);
		} catch (_e) {
			return 0;
		}
	}
	/**
	* Calculate adaptive blocking timeout like BullMQ
	* Returns timeout in seconds
	*
	* Inspiration by BullMQ ⭐️
	*/
	getBlockTimeout(maxTimeout, blockUntil) {
		const minimumBlockTimeout = .001;
		const maximumBlockTimeout = 5;
		if (blockUntil) {
			const blockDelay = blockUntil - Date.now();
			if (blockDelay <= 0) return minimumBlockTimeout;
			else if (blockDelay < minimumBlockTimeout * 1e3) return minimumBlockTimeout;
			else return Math.min(blockDelay / 1e3, maximumBlockTimeout);
		}
		return Math.max(minimumBlockTimeout, Math.min(maxTimeout, maximumBlockTimeout));
	}
	/**
	* Check if an error is a Redis connection error (should retry)
	* Conservative approach: only connection closed and ECONNREFUSED
	*/
	isConnectionError(err) {
		if (!err) return false;
		const message = `${err.message || ""}`;
		return message === "Connection is closed." || message.includes("ECONNREFUSED");
	}
	async reserveBlocking(timeoutSec = 5, blockUntil, blockingClient) {
		const startTime = Date.now();
		if (await this.isPaused()) {
			await sleep$1(50);
			return null;
		}
		if (!(this._consecutiveEmptyReserves >= 3)) {
			const immediateJob = await this.reserve();
			if (immediateJob) {
				this.logger.debug(`Immediate reserve successful (${Date.now() - startTime}ms)`);
				this._consecutiveEmptyReserves = 0;
				return immediateJob;
			}
		}
		const adaptiveTimeout = this.getBlockTimeout(timeoutSec, blockUntil);
		if (this._consecutiveEmptyReserves % 10 === 0) this.logger.debug(`Starting blocking operation (timeout: ${adaptiveTimeout}s, consecutive empty: ${this._consecutiveEmptyReserves})`);
		const readyKey = nsKey(this.ns, "ready");
		try {
			const bzpopminStart = Date.now();
			const result = await (blockingClient ?? this.r).bzpopmin(readyKey, adaptiveTimeout);
			const bzpopminDuration = Date.now() - bzpopminStart;
			if (!result || result.length < 3) {
				this.logger.debug(`Blocking timeout/empty (took ${bzpopminDuration}ms)`);
				this._consecutiveEmptyReserves = this._consecutiveEmptyReserves + 1;
				return null;
			}
			const [, groupId, score] = result;
			if (this._consecutiveEmptyReserves % 10 === 0) this.logger.debug(`Blocking result: group=${groupId}, score=${score} (took ${bzpopminDuration}ms)`);
			const reserveStart = Date.now();
			const job = await this.reserveAtomic(groupId);
			const reserveDuration = Date.now() - reserveStart;
			if (job) {
				this.logger.debug(`Successful job reserve after blocking: ${job.id} from group ${job.groupId} (reserve took ${reserveDuration}ms)`);
				this._consecutiveEmptyReserves = 0;
			} else {
				this.logger.warn(`Blocking found group but reserve failed: group=${groupId} (reserve took ${reserveDuration}ms)`);
				try {
					const groupKey = `${this.ns}:g:${groupId}`;
					const jobCount = await this.r.zcard(groupKey);
					if (jobCount > 0) {
						await this.r.zadd(readyKey, Number(score), groupId);
						this.logger.debug(`Restored group ${groupId} to ready with score ${score} after failed atomic reserve (${jobCount} jobs)`);
					} else this.logger.warn(`Not restoring empty group ${groupId} - preventing poisoned group loop`);
				} catch (_e) {
					this.logger.warn(`Failed to check group ${groupId} job count, not restoring`);
				}
				this._consecutiveEmptyReserves = this._consecutiveEmptyReserves + 1;
				return this.reserve();
			}
			return job;
		} catch (err) {
			const errorDuration = Date.now() - startTime;
			this.logger.error(`Blocking error after ${errorDuration}ms:`, err);
			if (this.isConnectionError(err)) {
				this.logger.error(`Connection error detected - rethrowing`);
				throw err;
			}
			this.logger.warn(`Falling back to regular reserve due to error`);
			return this.reserve();
		} finally {
			const totalDuration = Date.now() - startTime;
			if (totalDuration > 1e3) this.logger.debug(`ReserveBlocking completed in ${totalDuration}ms`);
		}
	}
	/**
	* Reserve a job from a specific group atomically (eliminates race conditions)
	* @param groupId - The group to reserve from
	*/
	async reserveAtomic(groupId) {
		const now = Date.now();
		const result = await evalScript(this.r, "reserve-atomic", [
			this.ns,
			String(now),
			String(this.vt),
			String(groupId)
		], 1);
		if (!result) return null;
		const parts = result.split("|||");
		if (parts.length < 10) return null;
		const [id, groupIdRaw, data, attempts, maxAttempts, seq, timestamp, orderMs, score, deadline] = parts;
		const parsedTimestamp = parseInt(timestamp, 10);
		const parsedOrderMs = parseInt(orderMs, 10);
		return {
			id,
			groupId: groupIdRaw,
			data: JSON.parse(data),
			attempts: parseInt(attempts, 10),
			maxAttempts: parseInt(maxAttempts, 10),
			seq: parseInt(seq, 10),
			timestamp: parsedTimestamp,
			orderMs: Number.isNaN(parsedOrderMs) ? parsedTimestamp : parsedOrderMs,
			score: parseFloat(score),
			deadlineAt: parseInt(deadline, 10)
		};
	}
	/**
	* Reserve up to maxBatch jobs (one per available group) atomically in Lua.
	*/
	async reserveBatch(maxBatch = 16) {
		const now = Date.now();
		const results = await evalScript(this.r, "reserve-batch", [
			this.ns,
			String(now),
			String(this.vt),
			String(Math.max(1, maxBatch))
		], 1);
		const out = [];
		for (const r of results || []) {
			if (!r) continue;
			const parts = r.split("|||");
			if (parts.length !== 10) continue;
			out.push({
				id: parts[0],
				groupId: parts[1],
				data: safeJsonParse(parts[2]),
				attempts: parseInt(parts[3], 10),
				maxAttempts: parseInt(parts[4], 10),
				seq: parseInt(parts[5], 10),
				timestamp: parseInt(parts[6], 10),
				orderMs: parseInt(parts[7], 10),
				score: parseFloat(parts[8]),
				deadlineAt: parseInt(parts[9], 10)
			});
		}
		return out;
	}
	/**
	* Get the number of jobs currently being processed (active jobs)
	*/
	async getActiveCount() {
		return evalScript(this.r, "get-active-count", [this.ns], 1);
	}
	/**
	* Get the number of jobs waiting to be processed
	*/
	async getWaitingCount() {
		return evalScript(this.r, "get-waiting-count", [this.ns], 1);
	}
	/**
	* Get the number of jobs delayed due to backoff
	*/
	async getDelayedCount() {
		return evalScript(this.r, "get-delayed-count", [this.ns], 1);
	}
	/**
	* Get list of active job IDs
	*/
	async getActiveJobs() {
		return evalScript(this.r, "get-active-jobs", [this.ns], 1);
	}
	/**
	* Get list of waiting job IDs
	*/
	async getWaitingJobs() {
		return evalScript(this.r, "get-waiting-jobs", [this.ns], 1);
	}
	/**
	* Get list of delayed job IDs
	*/
	async getDelayedJobs() {
		return evalScript(this.r, "get-delayed-jobs", [this.ns], 1);
	}
	/**
	* Get list of unique group IDs that have jobs
	*/
	async getUniqueGroups() {
		return evalScript(this.r, "get-unique-groups", [this.ns], 1);
	}
	/**
	* Get count of unique groups that have jobs
	*/
	async getUniqueGroupsCount() {
		return evalScript(this.r, "get-unique-groups-count", [this.ns], 1);
	}
	/**
	* Fetch a single job by ID with enriched fields for UI/inspection.
	* Attempts to mimic BullMQ's Job shape for fields commonly used by BullBoard.
	*/
	async getJob(id) {
		return Job.fromStore(this, id);
	}
	/**
	* Fetch jobs by statuses, emulating BullMQ's Queue.getJobs API used by BullBoard.
	* Only getter functionality; ordering is best-effort.
	*
	* Optimized with pagination to reduce Redis load - especially important for BullBoard polling.
	*/
	async getJobsByStatus(jobStatuses, start = 0, end = -1) {
		const requestedCount = end >= 0 ? end - start + 1 : 100;
		const fetchLimit = Math.min(requestedCount * 2, 500);
		const idToStatus = /* @__PURE__ */ new Map();
		const idSets = [];
		const pushZRange = async (key, status, reverse = false) => {
			try {
				const ids = reverse ? await this.r.zrevrange(key, 0, fetchLimit - 1) : await this.r.zrange(key, 0, fetchLimit - 1);
				for (const id of ids) idToStatus.set(id, status);
				idSets.push(...ids);
			} catch (_e) {}
		};
		const statuses = new Set(jobStatuses);
		if (statuses.has("active")) await pushZRange(`${this.ns}:processing`, "active");
		if (statuses.has("delayed")) await pushZRange(`${this.ns}:delayed`, "delayed");
		if (statuses.has("completed")) await pushZRange(`${this.ns}:completed`, "completed", true);
		if (statuses.has("failed")) await pushZRange(`${this.ns}:failed`, "failed", true);
		if (statuses.has("waiting")) try {
			const groupIds = await this.r.smembers(`${this.ns}:groups`);
			if (groupIds.length > 0) {
				const groupsToScan = groupIds.slice(0, Math.min(100, groupIds.length));
				const pipe$1 = this.r.multi();
				const jobsPerGroup = Math.max(1, Math.ceil(fetchLimit / groupsToScan.length));
				for (const gid of groupsToScan) pipe$1.zrange(`${this.ns}:g:${gid}`, 0, jobsPerGroup - 1);
				const rows$1 = await pipe$1.exec();
				for (const r of rows$1 || []) {
					const arr = r?.[1] || [];
					for (const id of arr) idToStatus.set(id, "waiting");
					idSets.push(...arr);
				}
			}
		} catch (_e) {}
		const seen = /* @__PURE__ */ new Set();
		const uniqueIds = [];
		for (const id of idSets) if (!seen.has(id)) {
			seen.add(id);
			uniqueIds.push(id);
		}
		const slice = end >= 0 ? uniqueIds.slice(start, end + 1) : uniqueIds.slice(start);
		if (slice.length === 0) return [];
		const pipe = this.r.multi();
		for (const id of slice) pipe.hgetall(`${this.ns}:job:${id}`);
		const rows = await pipe.exec();
		const jobs = [];
		for (let i = 0; i < slice.length; i++) {
			const id = slice[i];
			const raw = rows?.[i]?.[1] || {};
			if (!raw || Object.keys(raw).length === 0) {
				this.logger.warn(`Skipping job ${id} - not found (likely cleaned up by retention)`);
				continue;
			}
			const knownStatus = idToStatus.get(id);
			const job = Job.fromRawHash(this, id, raw, knownStatus);
			jobs.push(job);
		}
		return jobs;
	}
	/**
	* Provide counts structured like BullBoard expects.
	*/
	async getJobCounts() {
		const [active, waiting, delayed, completed, failed] = await Promise.all([
			this.getActiveCount(),
			this.getWaitingCount(),
			this.getDelayedCount(),
			this.getCompletedCount(),
			this.getFailedCount()
		]);
		return {
			active,
			waiting,
			delayed,
			completed,
			failed,
			paused: 0,
			"waiting-children": 0,
			prioritized: 0
		};
	}
	/**
	* Check for stalled jobs and recover or fail them
	* Returns array of [jobId, groupId, action] tuples
	*/
	async checkStalledJobs(now, gracePeriod, maxStalledCount) {
		try {
			return await evalScript(this.r, "check-stalled", [
				this.ns,
				String(now),
				String(gracePeriod),
				String(maxStalledCount)
			], 1) || [];
		} catch (error) {
			this.logger.error("Error checking stalled jobs:", error);
			return [];
		}
	}
	/**
	* Start the promoter service for staging system.
	* Promoter listens to Redis keyspace notifications and promotes staged jobs when ready.
	* This is idempotent - calling multiple times has no effect if already running.
	*/
	async startPromoter() {
		if (this.promoterRunning || this.orderingDelayMs <= 0) return;
		this.promoterRunning = true;
		this.promoterLockId = randomUUID();
		try {
			this.promoterRedis = this.r.duplicate();
			try {
				await this.promoterRedis.config("SET", "notify-keyspace-events", "Ex");
				this.logger.debug("Enabled Redis keyspace notifications for staging promoter");
			} catch (err) {
				this.logger.warn("Failed to enable keyspace notifications. Promoter will use polling fallback.", err);
			}
			const db = this.promoterRedis.options.db ?? 0;
			const timerKey = `${this.ns}:stage:timer`;
			const expiredChannel = `__keyevent@${db}__:expired`;
			await this.promoterRedis.subscribe(expiredChannel, (err) => {
				if (err) this.logger.error("Failed to subscribe to keyspace events:", err);
				else this.logger.debug(`Subscribed to ${expiredChannel}`);
			});
			this.promoterRedis.on("message", async (channel, message) => {
				if (channel === expiredChannel && message === timerKey) await this.runPromotion();
			});
			this.promoterInterval = setInterval(async () => {
				await this.runPromotion();
			}, 100);
			await this.runPromotion();
			this.logger.debug("Staging promoter started");
		} catch (err) {
			this.logger.error("Failed to start promoter:", err);
			this.promoterRunning = false;
			await this.stopPromoter();
		}
	}
	/**
	* Run a single promotion cycle with distributed locking
	*/
	async runPromotion() {
		if (!this.promoterRunning) return;
		const lockKey = `${this.ns}:promoter:lock`;
		const lockTtl = 3e4;
		try {
			if (await this.r.set(lockKey, this.promoterLockId, "PX", lockTtl, "NX") === "OK") try {
				const promoted = await evalScript(this.r, "promote-staged", [
					this.ns,
					String(Date.now()),
					String(100)
				], 1);
				if (promoted > 0) this.logger.debug(`Promoted ${promoted} staged jobs`);
			} finally {
				if (await this.r.get(lockKey) === this.promoterLockId) await this.r.del(lockKey);
			}
		} catch (err) {
			this.logger.error("Error during promotion:", err);
		}
	}
	/**
	* Stop the promoter service
	*/
	async stopPromoter() {
		if (!this.promoterRunning) return;
		this.promoterRunning = false;
		if (this.promoterInterval) {
			clearInterval(this.promoterInterval);
			this.promoterInterval = void 0;
		}
		if (this.promoterRedis) {
			try {
				await this.promoterRedis.unsubscribe();
				await this.promoterRedis.quit();
			} catch (_err) {
				try {
					this.promoterRedis.disconnect();
				} catch (_e) {}
			}
			this.promoterRedis = void 0;
		}
		this.logger.debug("Staging promoter stopped");
	}
	/**
	* Close underlying Redis connections
	*/
	async close() {
		if (this.batchConfig && this.batchBuffer.length > 0) {
			this.logger.debug(`Flushing ${this.batchBuffer.length} pending batched jobs before close`);
			await this.flushBatch();
		}
		await this.stopPromoter();
		try {
			await this.r.quit();
		} catch (_e) {
			try {
				this.r.disconnect();
			} catch (_e2) {}
		}
	}
	get pausedKey() {
		return `${this.ns}:paused`;
	}
	async pause() {
		await this.r.set(this.pausedKey, "1");
	}
	async resume() {
		await this.r.del(this.pausedKey);
	}
	async isPaused() {
		return await this.r.get(this.pausedKey) !== null;
	}
	/**
	* Wait for the queue to become empty (no active jobs)
	* @param timeoutMs Maximum time to wait in milliseconds (default: 60 seconds)
	* @returns true if queue became empty, false if timeout reached
	*/
	async waitForEmpty(timeoutMs = 6e4) {
		const startTime = Date.now();
		while (Date.now() - startTime < timeoutMs) try {
			if (await evalScript(this.r, "is-empty", [this.ns], 1) === 1) {
				await sleep$1(0);
				return true;
			}
			await sleep$1(200);
		} catch (err) {
			if (this.isConnectionError(err)) {
				this.logger.warn("Redis connection error in waitForEmpty, retrying...");
				await sleep$1(1e3);
				continue;
			}
			throw err;
		}
		return false;
	}
	/**
	* Remove problematic groups from ready queue to prevent infinite loops
	* Handles both poisoned groups (only failed/expired jobs) and locked groups
	*
	* Throttled to 1% sampling rate to reduce Redis overhead
	*/
	async cleanupPoisonedGroup(groupId) {
		if (Math.random() > .01) return "skipped";
		const lastCheck = this._groupCleanupTracking.get(groupId) || 0;
		const now = Date.now();
		if (now - lastCheck < 1e4) return "throttled";
		this._groupCleanupTracking.set(groupId, now);
		if (this._groupCleanupTracking.size > 1e3) {
			const cutoff = now - 6e4;
			for (const [gid, ts] of this._groupCleanupTracking.entries()) if (ts < cutoff) this._groupCleanupTracking.delete(gid);
		}
		try {
			const result = await evalScript(this.r, "cleanup-poisoned-group", [
				this.ns,
				groupId,
				String(now)
			], 1);
			if (result === "poisoned") this.logger.warn(`Removed poisoned group ${groupId} from ready queue`);
			else if (result === "empty") this.logger.warn(`Removed empty group ${groupId} from ready queue`);
			else if (result === "locked") {
				if (Math.random() < .1) this.logger.debug(`Detected group ${groupId} is locked by another worker (this is normal with high concurrency)`);
			}
			return result;
		} catch (error) {
			this.logger.error(`Error cleaning up group ${groupId}:`, error);
			return "error";
		}
	}
	/**
	* Distributed one-shot scheduler: promotes delayed jobs and processes repeating jobs.
	* Only proceeds if a short-lived scheduler lock can be acquired.
	*/
	schedulerLockKey() {
		return `${this.ns}:sched:lock`;
	}
	async acquireSchedulerLock(ttlMs = 1500) {
		try {
			return await this.r.set(this.schedulerLockKey(), "1", "PX", ttlMs, "NX") === "OK";
		} catch (_e) {
			return false;
		}
	}
	async runSchedulerOnce(now = Date.now()) {
		if (!await this.acquireSchedulerLock(this.schedulerLockTtlMs)) return;
		await this.promoteDelayedJobsBounded(32, now);
		await this.processRepeatingJobsBounded(16, now);
	}
	/**
	* Promote up to `limit` delayed jobs that are due now. Uses a small Lua to move one item per tick.
	*/
	async promoteDelayedJobsBounded(limit = 256, now = Date.now()) {
		let moved = 0;
		for (let i = 0; i < limit; i++) try {
			const n = await evalScript(this.r, "promote-delayed-one", [this.ns, String(now)], 1);
			if (!n || n <= 0) break;
			moved += n;
		} catch (_e) {
			break;
		}
		return moved;
	}
	/**
	* Process up to `limit` repeating job ticks.
	* Intentionally small per-tick work to keep Redis CPU flat.
	*/
	async processRepeatingJobsBounded(limit = 128, now = Date.now()) {
		const scheduleKey = `${this.ns}:repeat:schedule`;
		let processed = 0;
		for (let i = 0; i < limit; i++) {
			const due = await this.r.zrangebyscore(scheduleKey, 0, now, "LIMIT", 0, 1);
			if (!due || due.length === 0) break;
			const repeatKey = due[0];
			try {
				const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
				const repeatJobDataStr = await this.r.get(repeatJobKey);
				if (!repeatJobDataStr) {
					await this.r.zrem(scheduleKey, repeatKey);
					continue;
				}
				const repeatJobData = JSON.parse(repeatJobDataStr);
				if (repeatJobData.removed) {
					await this.r.zrem(scheduleKey, repeatKey);
					await this.r.del(repeatJobKey);
					continue;
				}
				await this.r.zrem(scheduleKey, repeatKey);
				let nextRunTime;
				if ("every" in repeatJobData.repeat) nextRunTime = now + repeatJobData.repeat.every;
				else nextRunTime = this.getNextCronTime(repeatJobData.repeat.pattern, now);
				repeatJobData.nextRunTime = nextRunTime;
				repeatJobData.lastRunTime = now;
				await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));
				await this.r.zadd(scheduleKey, nextRunTime, repeatKey);
				await evalScript(this.r, "enqueue", [
					this.ns,
					repeatJobData.groupId,
					JSON.stringify(repeatJobData.data),
					String(repeatJobData.maxAttempts ?? this.defaultMaxAttempts),
					String(repeatJobData.orderMs ?? now),
					String(0),
					String(randomUUID()),
					String(this.keepCompleted)
				], 1);
				processed++;
			} catch (error) {
				this.logger.error(`Error processing repeating job ${repeatKey}:`, error);
				await this.r.zrem(scheduleKey, repeatKey);
			}
		}
		return processed;
	}
	/**
	* Promote delayed jobs that are now ready to be processed
	* This should be called periodically to move jobs from delayed set to ready queue
	*/
	async promoteDelayedJobs() {
		try {
			return await evalScript(this.r, "promote-delayed-jobs", [this.ns, String(Date.now())], 1);
		} catch (error) {
			this.logger.error(`Error promoting delayed jobs:`, error);
			return 0;
		}
	}
	/**
	* Change the delay of a specific job
	*/
	async changeDelay(jobId, newDelay) {
		const newDelayUntil = newDelay > 0 ? Date.now() + newDelay : 0;
		try {
			return await evalScript(this.r, "change-delay", [
				this.ns,
				jobId,
				String(newDelayUntil),
				String(Date.now())
			], 1) === 1;
		} catch (error) {
			this.logger.error(`Error changing delay for job ${jobId}:`, error);
			return false;
		}
	}
	/**
	* Promote a delayed job to be ready immediately
	*/
	async promote(jobId) {
		return this.changeDelay(jobId, 0);
	}
	/**
	* Remove a job from the queue regardless of state (waiting, delayed, processing)
	*/
	async remove(jobId) {
		try {
			return await evalScript(this.r, "remove", [this.ns, jobId], 1) === 1;
		} catch (error) {
			this.logger.error(`Error removing job ${jobId}:`, error);
			return false;
		}
	}
	/**
	* Clean jobs of a given status older than graceTimeMs
	* @param graceTimeMs Remove jobs with finishedOn <= now - graceTimeMs (for completed/failed)
	* @param limit Max number of jobs to clean in one call
	* @param status Either 'completed' or 'failed'
	*/
	async clean(graceTimeMs, limit, status) {
		const graceAt = Date.now() - graceTimeMs;
		try {
			return await evalScript(this.r, "clean-status", [
				this.ns,
				status,
				String(graceAt),
				String(Math.max(0, Math.min(limit, 1e5)))
			], 1) ?? 0;
		} catch (error) {
			console.log("HERE?", error);
			this.logger.error(`Error cleaning ${status} jobs:`, error);
			return 0;
		}
	}
	/**
	* Update a job's data payload (BullMQ-style)
	*/
	async updateData(jobId, data) {
		const jobKey = `${this.ns}:job:${jobId}`;
		if (!await this.r.exists(jobKey)) throw new Error(`Job ${jobId} not found`);
		const serialized = JSON.stringify(data === void 0 ? null : data);
		await this.r.hset(jobKey, "data", serialized);
	}
	/**
	* Add a repeating job (cron job)
	*/
	async addRepeatingJob(opts) {
		if (!opts.repeat) throw new Error("Repeat options are required for repeating jobs");
		const now = Date.now();
		const repeatKey = `${opts.groupId}:${JSON.stringify(opts.repeat)}:${now}:${Math.random().toString(36).slice(2)}`;
		let nextRunTime;
		if ("every" in opts.repeat) nextRunTime = now + opts.repeat.every;
		else nextRunTime = this.getNextCronTime(opts.repeat.pattern, now);
		const repeatJobData = {
			groupId: opts.groupId,
			data: opts.data === void 0 ? null : opts.data,
			maxAttempts: opts.maxAttempts ?? this.defaultMaxAttempts,
			orderMs: opts.orderMs,
			repeat: opts.repeat,
			nextRunTime,
			lastRunTime: null,
			removed: false
		};
		const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
		await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));
		await this.r.zadd(`${this.ns}:repeat:schedule`, nextRunTime, repeatKey);
		const lookupKey = `${this.ns}:repeat:lookup:${opts.groupId}:${JSON.stringify(opts.repeat)}`;
		await this.r.set(lookupKey, repeatKey);
		const repeatId = `repeat:${repeatKey}`;
		const jobHashKey = `${this.ns}:job:${repeatId}`;
		try {
			await this.r.hmset(jobHashKey, "id", repeatId, "groupId", repeatJobData.groupId, "data", JSON.stringify(repeatJobData.data), "attempts", "0", "maxAttempts", String(repeatJobData.maxAttempts), "seq", "0", "timestamp", String(Date.now()), "orderMs", String(repeatJobData.orderMs ?? now), "status", "waiting");
		} catch (_e) {}
		return Job.fromStore(this, repeatId);
	}
	/**
	* Compute next execution time using cron-parser (BullMQ-style)
	*/
	getNextCronTime(pattern, fromTime) {
		try {
			return CronParser.parseExpression(pattern, { currentDate: new Date(fromTime) }).next().getTime();
		} catch (_e) {
			throw new Error(`Invalid cron pattern: ${pattern}`);
		}
	}
	/**
	* Remove a repeating job
	*/
	async removeRepeatingJob(groupId, repeat) {
		try {
			const lookupKey = `${this.ns}:repeat:lookup:${groupId}:${JSON.stringify(repeat)}`;
			const repeatKey = await this.r.get(lookupKey);
			if (!repeatKey) return false;
			const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
			const scheduleKey = `${this.ns}:repeat:schedule`;
			const repeatJobDataStr = await this.r.get(repeatJobKey);
			if (!repeatJobDataStr) {
				await this.r.del(lookupKey);
				return false;
			}
			const repeatJobData = JSON.parse(repeatJobDataStr);
			repeatJobData.removed = true;
			await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));
			await this.r.zrem(scheduleKey, repeatKey);
			await this.r.del(lookupKey);
			try {
				const repeatId = `repeat:${repeatKey}`;
				await this.r.del(`${this.ns}:job:${repeatId}`);
			} catch (_e) {}
			return true;
		} catch (error) {
			this.logger.error(`Error removing repeating job:`, error);
			return false;
		}
	}
	/**
	* Get the current state of a job
	* Returns 'active', 'delayed', 'waiting', 'completed', 'failed', or 'unknown'
	*/
	async getJobState(jobId) {
		if (await this.r.zscore(`${this.ns}:processing`, jobId) !== null) return "active";
		if (await this.r.zscore(`${this.ns}:delayed`, jobId) !== null) return "delayed";
		if (await this.r.zscore(`${this.ns}:completed`, jobId) !== null) return "completed";
		if (await this.r.zscore(`${this.ns}:failed`, jobId) !== null) return "failed";
		const jobKey = `${this.ns}:job:${jobId}`;
		const groupId = await this.r.hget(jobKey, "groupId");
		if (groupId) {
			if (await this.r.zscore(`${this.ns}:g:${groupId}`, jobId) !== null) return "waiting";
		}
		return "unknown";
	}
	/**
	* Cancel a running job by publishing a cancel event via pub/sub.
	* The worker processing the job will receive an abort signal.
	*
	* @param jobId The ID of the job to cancel
	* @returns true if the job was active and cancel event was published, false otherwise
	*
	* @example
	* ```ts
	* const cancelled = await queue.cancel(jobId);
	* if (cancelled) {
	*   console.log('Job cancellation signal sent');
	* } else {
	*   console.log('Job was not active (may have completed or not started)');
	* }
	* ```
	*/
	async cancel(jobId) {
		const state = await this.getJobState(jobId);
		if (state !== "active") {
			this.logger.debug(`Cannot cancel job ${jobId}: job is not active (state: ${state})`);
			return false;
		}
		const cancelChannel = `${this.ns}:cancel`;
		await this.r.publish(cancelChannel, jobId);
		this.logger.debug(`Published cancel event for job ${jobId}`);
		return true;
	}
};
function sleep$1(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

//#endregion
//#region src/async-fifo-queue.ts
/**
* This file contains code copied from BullMQ (https://github.com/taskforcesh/bullmq)
*
* BullMQ is a fantastic library and one of the most popular Redis-based job queue
* libraries for Node.js. We've copied the AsyncFifoQueue implementation from BullMQ
* as it's a well-designed component that fits our needs perfectly.
*
* Original copyright notice:
* Copyright (c) Taskforce.sh and contributors
*
* This code is used under the MIT License. The original license can be found at:
* https://github.com/taskforcesh/bullmq/blob/main/LICENSE
*
* Modifications may have been made to adapt this code for use in GroupMQ.
*/
var Node = class {
	constructor(value) {
		this.value = void 0;
		this.next = null;
		this.value = value;
	}
};
var LinkedList = class {
	constructor() {
		this.length = 0;
		this.head = null;
		this.tail = null;
	}
	push(value) {
		const newNode = new Node(value);
		if (!this.length) this.head = newNode;
		else this.tail.next = newNode;
		this.tail = newNode;
		this.length += 1;
		return newNode;
	}
	shift() {
		if (!this.length) return null;
		const head = this.head;
		this.head = this.head.next;
		this.length -= 1;
		return head;
	}
};
/**
* AsyncFifoQueue
*
* A minimal FIFO queue for asynchronous operations. Allows adding asynchronous operations
* and consume them in the order they are resolved.
*/
var AsyncFifoQueue = class {
	constructor(ignoreErrors = false) {
		this.ignoreErrors = ignoreErrors;
		this.queue = new LinkedList();
		this.pending = /* @__PURE__ */ new Set();
		this.newPromise();
	}
	add(promise) {
		this.pending.add(promise);
		promise.then((data) => {
			this.pending.delete(promise);
			if (this.queue.length === 0) this.resolvePromise(data);
			this.queue.push(data);
		}).catch((err) => {
			this.pending.delete(promise);
			if (this.ignoreErrors) {
				if (this.queue.length === 0) this.resolvePromise(void 0);
				this.queue.push(void 0);
			} else this.rejectPromise(err);
		});
	}
	async waitAll() {
		await Promise.all(this.pending);
	}
	numTotal() {
		return this.pending.size + this.queue.length;
	}
	numPending() {
		return this.pending.size;
	}
	numQueued() {
		return this.queue.length;
	}
	resolvePromise(data) {
		this.resolve(data);
		this.newPromise();
	}
	rejectPromise(err) {
		this.reject(err);
		this.newPromise();
	}
	newPromise() {
		this.nextPromise = new Promise((resolve, reject) => {
			this.resolve = resolve;
			this.reject = reject;
		});
	}
	async wait() {
		return this.nextPromise;
	}
	async fetch() {
		if (this.pending.size === 0 && this.queue.length === 0) return;
		while (this.queue.length === 0) try {
			await this.wait();
		} catch (err) {
			if (!this.ignoreErrors) console.error("Unexpected Error in AsyncFifoQueue", err);
		}
		return this.queue.shift()?.value;
	}
};

//#endregion
//#region src/worker.ts
var TypedEventEmitter = class {
	constructor() {
		this.listeners = /* @__PURE__ */ new Map();
	}
	on(event, listener) {
		if (!this.listeners.has(event)) this.listeners.set(event, []);
		this.listeners.get(event).push(listener);
		return this;
	}
	off(event, listener) {
		const eventListeners = this.listeners.get(event);
		if (eventListeners) {
			const index = eventListeners.indexOf(listener);
			if (index !== -1) eventListeners.splice(index, 1);
		}
		return this;
	}
	emit(event, ...args) {
		const eventListeners = this.listeners.get(event);
		if (eventListeners && eventListeners.length > 0) {
			for (const listener of eventListeners) try {
				listener(...args);
			} catch (error) {
				console.error(`Error in event listener for '${String(event)}':`, error);
			}
			return true;
		}
		return false;
	}
	removeAllListeners(event) {
		if (event) this.listeners.delete(event);
		else this.listeners.clear();
		return this;
	}
};
const defaultBackoff = (attempt) => {
	const base = Math.min(3e4, 2 ** (attempt - 1) * 500);
	return base + Math.floor(base * .25 * Math.random());
};
var _Worker = class extends TypedEventEmitter {
	constructor(opts) {
		super();
		this.stopping = false;
		this.ready = false;
		this.closed = false;
		this.blockingClient = null;
		this.jobsInProgress = /* @__PURE__ */ new Set();
		this.abortControllers = /* @__PURE__ */ new Map();
		this.cancelSubscriber = null;
		this.lastJobPickupTime = Date.now();
		this.totalJobsProcessed = 0;
		this.blockingStats = {
			totalBlockingCalls: 0,
			consecutiveEmptyReserves: 0,
			lastActivityTime: Date.now()
		};
		this.emptyReserveBackoffMs = 0;
		if (!opts.handler || typeof opts.handler !== "function") throw new Error("Worker handler must be a function");
		this.q = opts.queue;
		this.name = opts.name ?? this.q.name;
		this.logger = typeof opts.logger === "object" ? opts.logger : new Logger(!!opts.logger, this.name);
		this.handler = opts.handler;
		const jobTimeoutMs = this.q.jobTimeoutMs ?? 3e4;
		this.hbMs = opts.heartbeatMs ?? Math.max(1e3, Math.floor(jobTimeoutMs / 3));
		this.onError = opts.onError;
		this.maxAttempts = opts.maxAttempts ?? this.q.maxAttemptsDefault ?? 3;
		this.backoff = opts.backoff ?? defaultBackoff;
		this.enableCleanup = opts.enableCleanup ?? true;
		this.cleanupMs = opts.cleanupIntervalMs ?? 6e4;
		this.schedulerMs = opts.schedulerIntervalMs ?? 1e3;
		this.blockingTimeoutSec = opts.blockingTimeoutSec ?? 5;
		this.concurrency = Math.max(1, opts.concurrency ?? 1);
		this.stalledInterval = opts.stalledInterval ?? (this.concurrency > 50 ? 6e4 : 3e4);
		this.maxStalledCount = opts.maxStalledCount ?? (this.concurrency > 50 ? 2 : 1);
		this.stalledGracePeriod = opts.stalledGracePeriod ?? 5e3;
		this.setupRedisEventHandlers();
		if (this.q.orderingDelayMs > 0) this.q.startPromoter().catch((err) => {
			this.logger.error("Failed to start staging promoter:", err);
		});
		this.run();
	}
	get isClosed() {
		return this.closed;
	}
	/**
	* Add jitter to prevent thundering herd problems in high-concurrency environments
	* @param baseInterval The base interval in milliseconds
	* @param jitterPercent Percentage of jitter to add (0-1, default 0.1 for 10%)
	* @returns The interval with jitter applied
	*/
	addJitter(baseInterval, jitterPercent = .1) {
		return baseInterval + Math.random() * baseInterval * jitterPercent;
	}
	setupRedisEventHandlers() {
		const redis = this.q.redis;
		if (redis) {
			this.redisCloseHandler = () => {
				this.ready = false;
				this.emit("ioredis:close");
			};
			this.redisErrorHandler = (error) => {
				this.emit("error", error);
			};
			this.redisReadyHandler = () => {
				if (!this.ready && !this.stopping) {
					this.ready = true;
					this.emit("ready");
				}
			};
			redis.on("close", this.redisCloseHandler);
			redis.on("error", this.redisErrorHandler);
			redis.on("ready", this.redisReadyHandler);
		}
	}
	/**
	* Set up the Redis subscriber for job cancellation events
	*/
	async setupCancelSubscriber() {
		try {
			this.cancelSubscriber = this.q.redis.duplicate({ maxRetriesPerRequest: null });
			const cancelChannel = `${this.q.namespace}:cancel`;
			await this.cancelSubscriber.subscribe(cancelChannel);
			this.cancelSubscriber.on("message", (_channel, jobId) => {
				const controller = this.abortControllers.get(jobId);
				if (controller) {
					this.logger.debug(`Cancelling job ${jobId} via abort signal`);
					controller.abort();
				}
			});
			this.cancelSubscriber.on("error", (err) => {
				if (!this.stopping) this.logger.error("Cancel subscriber error:", err);
			});
			this.logger.debug(`Subscribed to cancel channel: ${cancelChannel}`);
		} catch (err) {
			this.logger.error("Failed to set up cancel subscriber:", err);
			this.cancelSubscriber = null;
		}
	}
	async run() {
		if (this.runLoopPromise) return this.runLoopPromise;
		const runPromise = this._runLoop();
		this.runLoopPromise = runPromise;
		return runPromise;
	}
	async _runLoop() {
		this.logger.info(`🚀 Worker ${this.name} starting...`);
		try {
			this.blockingClient = this.q.redis.duplicate({
				enableAutoPipelining: true,
				maxRetriesPerRequest: null,
				retryStrategy: (times) => {
					return Math.max(Math.min(Math.exp(times) * 1e3, 2e4), 1e3);
				}
			});
			this.blockingClient.on("error", (err) => {
				if (!this.q.isConnectionError(err)) this.logger.error("Blocking client error (non-connection):", err);
				else this.logger.warn("Blocking client connection error:", err.message);
				this.emit("error", err instanceof Error ? err : new Error(String(err)));
			});
			this.blockingClient.on("close", () => {
				if (!this.stopping && !this.closed) this.logger.warn("Blocking client disconnected, will reconnect on next operation");
			});
			this.blockingClient.on("reconnecting", () => {
				if (!this.stopping && !this.closed) this.logger.info("Blocking client reconnecting...");
			});
			this.blockingClient.on("ready", () => {
				if (!this.stopping && !this.closed) this.logger.info("Blocking client ready");
			});
		} catch (err) {
			this.logger.error("Failed to create blocking client:", err);
			this.blockingClient = null;
		}
		await this.setupCancelSubscriber();
		if (this.enableCleanup) {
			this.cleanupTimer = setInterval(async () => {
				try {
					await this.q.cleanup();
				} catch (err) {
					this.onError?.(err);
				}
			}, this.addJitter(this.cleanupMs));
			const schedulerInterval = Math.min(this.schedulerMs, this.cleanupMs);
			this.schedulerTimer = setInterval(async () => {
				try {
					await this.q.runSchedulerOnce();
				} catch (_err) {}
			}, this.addJitter(schedulerInterval));
		}
		this.startStalledChecker();
		let connectionRetries = 0;
		const maxConnectionRetries = 10;
		const asyncFifoQueue = new AsyncFifoQueue(true);
		while (!this.stopping || asyncFifoQueue.numTotal() > 0) try {
			while (!this.stopping) {
				if (asyncFifoQueue.numTotal() >= this.concurrency) break;
				this.blockingStats.totalBlockingCalls++;
				if (this.blockingStats.totalBlockingCalls >= 1e9) this.blockingStats.totalBlockingCalls = 0;
				this.logger.debug(`Fetching job (call #${this.blockingStats.totalBlockingCalls}, processing: ${this.jobsInProgress.size}/${this.concurrency}, queue: ${asyncFifoQueue.numTotal()} (queued: ${asyncFifoQueue.numQueued()}, pending: ${asyncFifoQueue.numPending()}), total: ${asyncFifoQueue.numTotal()}/${this.concurrency})...`);
				const availableCapacity = this.concurrency - asyncFifoQueue.numTotal();
				if (availableCapacity > 0 && asyncFifoQueue.numTotal() === 0) {
					const batchSize = Math.min(availableCapacity, 8);
					const batchJobs = await this.q.reserveBatch(batchSize);
					if (batchJobs.length > 0) {
						this.logger.debug(`Batch reserved ${batchJobs.length} jobs`);
						for (const job$2 of batchJobs) asyncFifoQueue.add(Promise.resolve(job$2));
						connectionRetries = 0;
						this.lastJobPickupTime = Date.now();
						this.blockingStats.consecutiveEmptyReserves = 0;
						this.blockingStats.lastActivityTime = Date.now();
						this.emptyReserveBackoffMs = 0;
						continue;
					}
				}
				const allowBlocking = this.blockingStats.consecutiveEmptyReserves >= 2 && asyncFifoQueue.numTotal() === 0 && this.jobsInProgress.size === 0;
				const adaptiveTimeout = this.blockingTimeoutSec;
				const fetchedJob = allowBlocking ? this.q.reserveBlocking(adaptiveTimeout, void 0, this.blockingClient ?? void 0) : this.q.reserve();
				asyncFifoQueue.add(fetchedJob);
				const job$1 = await fetchedJob;
				if (job$1) {
					connectionRetries = 0;
					this.lastJobPickupTime = Date.now();
					this.blockingStats.consecutiveEmptyReserves = 0;
					this.blockingStats.lastActivityTime = Date.now();
					this.emptyReserveBackoffMs = 0;
					this.logger.debug(`Fetched job ${job$1.id} from group ${job$1.groupId}`);
				} else {
					this.blockingStats.consecutiveEmptyReserves++;
					if (this.blockingStats.consecutiveEmptyReserves % 50 === 0) this.logger.debug(`No job available (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves})`);
					const backoffThreshold = this.concurrency >= 100 ? 5 : 3;
					if (this.blockingStats.consecutiveEmptyReserves > backoffThreshold && asyncFifoQueue.numTotal() === 0 && this.jobsInProgress.size === 0) {
						const maxBackoff = this.concurrency >= 100 ? 2e3 : 5e3;
						if (this.emptyReserveBackoffMs === 0) this.emptyReserveBackoffMs = this.concurrency >= 100 ? 100 : 50;
						else this.emptyReserveBackoffMs = Math.min(maxBackoff, Math.max(100, this.emptyReserveBackoffMs * 1.2));
						if (this.blockingStats.consecutiveEmptyReserves % 20 === 0) this.logger.debug(`Applying backoff: ${Math.round(this.emptyReserveBackoffMs)}ms (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves}, jobs in progress: ${this.jobsInProgress.size})`);
						await this.delay(this.emptyReserveBackoffMs);
					}
					if (asyncFifoQueue.numTotal() === 0 && this.jobsInProgress.size === 0) break;
					if (asyncFifoQueue.numTotal() > 0 || this.jobsInProgress.size > 0) break;
				}
			}
			let job;
			do
				job = await asyncFifoQueue.fetch() ?? void 0;
			while (!job && asyncFifoQueue.numQueued() > 0);
			if (job && typeof job === "object" && "id" in job) {
				this.totalJobsProcessed++;
				this.logger.debug(`Processing job ${job.id} from group ${job.groupId} immediately`);
				const processingPromise = this.processJob(job, () => {
					return asyncFifoQueue.numTotal() <= this.concurrency;
				}, this.jobsInProgress);
				asyncFifoQueue.add(processingPromise);
			}
		} catch (err) {
			if (this.stopping) return;
			if (this.q.isConnectionError(err)) {
				connectionRetries++;
				this.logger.error(`Connection error (retry ${connectionRetries}/${maxConnectionRetries}):`, err);
				if (connectionRetries >= maxConnectionRetries) {
					this.logger.error(`⚠️  Max connection retries (${maxConnectionRetries}) exceeded! Worker will continue but may be experiencing persistent Redis issues.`);
					this.emit("error", /* @__PURE__ */ new Error(`Max connection retries (${maxConnectionRetries}) exceeded - worker continuing with backoff`));
					await this.delay(2e4);
					connectionRetries = 0;
				} else {
					const delayMs = Math.max(Math.min(Math.exp(connectionRetries) * 1e3, 2e4), 1e3);
					this.logger.debug(`Waiting ${Math.round(delayMs)}ms before retry (exponential backoff)`);
					await this.delay(delayMs);
				}
			} else {
				this.logger.error(`Worker loop error (non-connection, continuing):`, err);
				this.emit("error", err instanceof Error ? err : new Error(String(err)));
				connectionRetries = 0;
				await this.delay(100);
			}
			this.onError?.(err);
		}
		this.logger.info(`Stopped`);
	}
	async delay(ms) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}
	/**
	* Process a job and return the next job if atomic completion succeeds
	* This matches BullMQ's processJob signature
	*/
	async processJob(job, fetchNextCallback, jobsInProgress) {
		const existingItem = Array.from(jobsInProgress).find((item) => item.job.id === job.id);
		let inProgressItem;
		if (existingItem) {
			existingItem.ts = Date.now();
			inProgressItem = existingItem;
		} else {
			inProgressItem = {
				job,
				ts: Date.now()
			};
			jobsInProgress.add(inProgressItem);
		}
		try {
			const nextJob = await this.processSingleJob(job, fetchNextCallback);
			if (nextJob && typeof nextJob === "object" && "id" in nextJob && "groupId" in nextJob) {
				const chainedItem = {
					job: nextJob,
					ts: Date.now()
				};
				jobsInProgress.add(chainedItem);
				jobsInProgress.delete(inProgressItem);
				return nextJob;
			}
			return nextJob;
		} finally {
			if (jobsInProgress.has(inProgressItem)) jobsInProgress.delete(inProgressItem);
		}
	}
	/**
	* Complete a job and try to atomically get next job from same group
	*/
	async completeJob(job, handlerResult, fetchNextCallback, processedOn, finishedOn) {
		if (fetchNextCallback?.()) {
			const nextJob = await this.q.completeAndReserveNextWithMetadata(job.id, job.groupId, handlerResult, {
				processedOn: processedOn || Date.now(),
				finishedOn: finishedOn || Date.now(),
				attempts: job.attempts,
				maxAttempts: job.maxAttempts
			});
			if (nextJob) {
				this.logger.debug(`Got next job ${nextJob.id} from same group ${nextJob.groupId} atomically`);
				return nextJob;
			}
			this.logger.debug(`Atomic chaining returned nil for job ${job.id} - job completed, but no next job chained`);
			if (Math.random() < .1) await new Promise((resolve) => setTimeout(resolve, Math.random() * 100));
		} else await this.q.completeWithMetadata(job, handlerResult, {
			processedOn: processedOn || Date.now(),
			finishedOn: finishedOn || Date.now(),
			attempts: job.attempts,
			maxAttempts: job.maxAttempts
		});
	}
	/**
	* Start the stalled job checker
	* Checks periodically for jobs that exceeded their deadline and recovers or fails them
	*/
	startStalledChecker() {
		if (this.stalledInterval <= 0) return;
		this.stalledCheckTimer = setInterval(async () => {
			try {
				await this.checkStalled();
			} catch (err) {
				this.logger.error("Error in stalled job checker:", err);
				this.emit("error", err instanceof Error ? err : new Error(String(err)));
			}
		}, this.stalledInterval);
	}
	/**
	* Check for stalled jobs and recover or fail them
	* A job is stalled when its worker crashed or lost connection
	*/
	async checkStalled() {
		if (this.stopping || this.closed) return;
		try {
			const now = Date.now();
			const results = await this.q.checkStalledJobs(now, this.stalledGracePeriod, this.maxStalledCount);
			if (results.length > 0) for (let i = 0; i < results.length; i += 3) {
				const jobId = results[i];
				const groupId = results[i + 1];
				const action = results[i + 2];
				if (action === "recovered") {
					this.logger.info(`Recovered stalled job ${jobId} from group ${groupId}`);
					this.emit("stalled", jobId, groupId);
				} else if (action === "failed") {
					this.logger.warn(`Failed stalled job ${jobId} from group ${groupId} (exceeded max stalled count)`);
					this.emit("stalled", jobId, groupId);
				}
			}
		} catch (err) {
			this.logger.error("Error checking stalled jobs:", err);
		}
	}
	/**
	* Get worker performance metrics
	*/
	getWorkerMetrics() {
		const now = Date.now();
		return {
			name: this.name,
			totalJobsProcessed: this.totalJobsProcessed,
			lastJobPickupTime: this.lastJobPickupTime,
			timeSinceLastJob: this.lastJobPickupTime > 0 ? now - this.lastJobPickupTime : null,
			blockingStats: { ...this.blockingStats },
			isProcessing: this.jobsInProgress.size > 0,
			jobsInProgressCount: this.jobsInProgress.size,
			jobsInProgress: Array.from(this.jobsInProgress).map((item) => ({
				jobId: item.job.id,
				groupId: item.job.groupId,
				processingTimeMs: now - item.ts
			}))
		};
	}
	/**
	* Stop the worker gracefully
	* @param gracefulTimeoutMs Maximum time to wait for current job to finish (default: 30 seconds)
	*/
	async close(gracefulTimeoutMs = 3e4) {
		this.stopping = true;
		await this.delay(100);
		if (this.cleanupTimer) clearInterval(this.cleanupTimer);
		if (this.schedulerTimer) clearInterval(this.schedulerTimer);
		if (this.stalledCheckTimer) clearInterval(this.stalledCheckTimer);
		const startTime = Date.now();
		while (this.jobsInProgress.size > 0 && Date.now() - startTime < gracefulTimeoutMs) await sleep(100);
		if (this.blockingClient) {
			try {
				if (this.jobsInProgress.size > 0 && gracefulTimeoutMs > 0) {
					this.logger.debug("Gracefully closing blocking client (quit)...");
					await this.blockingClient.quit();
				} else {
					this.logger.debug("Force closing blocking client (disconnect)...");
					this.blockingClient.disconnect();
				}
			} catch (err) {
				this.logger.debug("Error closing blocking client:", err);
			}
			this.blockingClient = null;
		}
		if (this.runLoopPromise) {
			const runLoopTimeout = this.jobsInProgress.size > 0 ? gracefulTimeoutMs : 2e3;
			const timeoutPromise = new Promise((resolve) => {
				setTimeout(resolve, runLoopTimeout);
			});
			try {
				await Promise.race([this.runLoopPromise, timeoutPromise]);
			} catch (err) {
				this.logger.warn("Error while waiting for run loop to exit:", err);
			}
		}
		if (this.jobsInProgress.size > 0) {
			this.logger.warn(`Worker stopped with ${this.jobsInProgress.size} jobs still processing after ${gracefulTimeoutMs}ms timeout.`);
			for (const [jobId, controller] of this.abortControllers) {
				this.logger.debug(`Aborting job ${jobId} due to worker shutdown`);
				controller.abort();
			}
			const nowWall = Date.now();
			for (const item of this.jobsInProgress) this.emit("graceful-timeout", Job.fromReserved(this.q, item.job, {
				processedOn: item.ts,
				finishedOn: nowWall,
				status: "active"
			}));
		}
		if (this.cancelSubscriber) {
			try {
				await this.cancelSubscriber.unsubscribe();
				this.cancelSubscriber.disconnect();
			} catch (err) {
				this.logger.debug("Error closing cancel subscriber:", err);
			}
			this.cancelSubscriber = null;
		}
		this.abortControllers.clear();
		this.jobsInProgress.clear();
		this.ready = false;
		this.closed = true;
		try {
			const redis = this.q.redis;
			if (redis) {
				if (this.redisCloseHandler) redis.off?.("close", this.redisCloseHandler);
				if (this.redisErrorHandler) redis.off?.("error", this.redisErrorHandler);
				if (this.redisReadyHandler) redis.off?.("ready", this.redisReadyHandler);
			}
		} catch (_e) {}
		this.emit("closed");
	}
	/**
	* Get information about the first currently processing job (if any)
	* For concurrency > 1, returns the oldest job in progress
	*/
	getCurrentJob() {
		if (this.jobsInProgress.size === 0) return null;
		const oldest = Array.from(this.jobsInProgress)[0];
		const now = Date.now();
		return {
			job: oldest.job,
			processingTimeMs: now - oldest.ts
		};
	}
	/**
	* Get information about all currently processing jobs
	*/
	getCurrentJobs() {
		const now = Date.now();
		return Array.from(this.jobsInProgress).map((item) => ({
			job: item.job,
			processingTimeMs: now - item.ts
		}));
	}
	/**
	* Check if the worker is currently processing any jobs
	*/
	isProcessing() {
		return this.jobsInProgress.size > 0;
	}
	async add(opts) {
		return this.q.add(opts);
	}
	async processSingleJob(job, fetchNextCallback) {
		const jobStartWallTime = Date.now();
		const abortController = new AbortController();
		this.abortControllers.set(job.id, abortController);
		let hbTimer;
		let heartbeatDelayTimer;
		const startHeartbeat = () => {
			const jobTimeout = this.q.jobTimeoutMs || 3e4;
			const minInterval = Math.min(this.hbMs, Math.floor(jobTimeout / 3), 1e4);
			this.logger.debug(`Starting heartbeat for job ${job.id} (interval: ${minInterval}ms, concurrency: ${this.concurrency})`);
			hbTimer = setInterval(async () => {
				try {
					if (await this.q.heartbeat(job) === 0) {
						this.logger.warn(`Heartbeat failed for job ${job.id} - job may have been removed or completed elsewhere`);
						if (hbTimer) clearInterval(hbTimer);
					}
				} catch (e) {
					const isConnErr = this.q.isConnectionError(e);
					if (!isConnErr || !this.stopping) this.logger.error(`Heartbeat error for job ${job.id}:`, e instanceof Error ? e.message : String(e));
					this.onError?.(e, job);
					if (!isConnErr || !this.stopping) this.emit("error", e instanceof Error ? e : new Error(String(e)));
				}
			}, minInterval);
		};
		try {
			const jobTimeout = this.q.jobTimeoutMs || 3e4;
			const heartbeatThreshold = Math.min(jobTimeout * .1, 2e3);
			heartbeatDelayTimer = setTimeout(() => {
				startHeartbeat();
			}, heartbeatThreshold);
			const handlerResult = await this.handler(job, abortController.signal);
			if (heartbeatDelayTimer) clearTimeout(heartbeatDelayTimer);
			if (hbTimer) clearInterval(hbTimer);
			const finishedAtWall = Date.now();
			const nextJob = await this.completeJob(job, handlerResult, fetchNextCallback, jobStartWallTime, finishedAtWall);
			this.blockingStats.consecutiveEmptyReserves = 0;
			this.emptyReserveBackoffMs = 0;
			this.emit("completed", Job.fromReserved(this.q, job, {
				processedOn: jobStartWallTime,
				finishedOn: finishedAtWall,
				returnvalue: handlerResult,
				status: "completed"
			}));
			return nextJob;
		} catch (err) {
			if (heartbeatDelayTimer) clearTimeout(heartbeatDelayTimer);
			if (hbTimer) clearInterval(hbTimer);
			await this.handleJobFailure(err, job, jobStartWallTime);
		} finally {
			this.abortControllers.delete(job.id);
		}
	}
	/**
	* Handle job failure: emit events, retry or dead-letter
	*/
	async handleJobFailure(err, job, jobStartWallTime) {
		this.onError?.(err, job);
		this.blockingStats.consecutiveEmptyReserves = 0;
		this.emptyReserveBackoffMs = 0;
		try {
			this.emit("error", err instanceof Error ? err : new Error(String(err)));
		} catch (_emitError) {}
		const failedAt = Date.now();
		this.emit("failed", Job.fromReserved(this.q, job, {
			processedOn: jobStartWallTime,
			finishedOn: failedAt,
			failedReason: err instanceof Error ? err.message : String(err),
			stacktrace: err instanceof Error ? err.stack : typeof err === "object" && err !== null ? err.stack : void 0,
			status: "failed"
		}));
		const nextAttempt = job.attempts + 1;
		const backoffMs = this.backoff(nextAttempt);
		if (nextAttempt >= this.maxAttempts) {
			await this.deadLetterJob(err, job, jobStartWallTime, failedAt, nextAttempt);
			return;
		}
		if (await this.q.retry(job.id, backoffMs) === -1) {
			await this.deadLetterJob(err, job, jobStartWallTime, failedAt, job.maxAttempts);
			return;
		}
		await this.recordFailureAttempt(err, job, jobStartWallTime, failedAt, nextAttempt);
	}
	/**
	* Dead-letter a job that exceeded max attempts
	*/
	async deadLetterJob(err, job, processedOn, finishedOn, attempts) {
		this.logger.info(`Dead lettering job ${job.id} from group ${job.groupId} (attempts: ${attempts}/${job.maxAttempts})`);
		const errObj = err instanceof Error ? err : new Error(String(err));
		try {
			await this.q.recordFinalFailure({
				id: job.id,
				groupId: job.groupId
			}, {
				name: errObj.name,
				message: errObj.message,
				stack: errObj.stack
			}, {
				processedOn,
				finishedOn,
				attempts,
				maxAttempts: job.maxAttempts,
				data: job.data
			});
		} catch (e) {
			this.logger.warn("Failed to record final failure", e);
		}
		await this.q.deadLetter(job.id, job.groupId);
	}
	/**
	* Record a failed attempt (not final)
	*/
	async recordFailureAttempt(err, job, processedOn, finishedOn, attempts) {
		const errObj = err instanceof Error ? err : new Error(String(err));
		try {
			await this.q.recordAttemptFailure({
				id: job.id,
				groupId: job.groupId
			}, {
				name: errObj.name,
				message: errObj.message,
				stack: errObj.stack
			}, {
				processedOn,
				finishedOn,
				attempts,
				maxAttempts: job.maxAttempts
			});
		} catch (e) {
			this.logger.warn("Failed to record attempt failure", e);
		}
	}
};
const Worker = _Worker;
function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

//#endregion
export { BullBoardGroupMQAdapter, Job, JobCancelledError, Queue, Worker, getWorkersStatus, waitForQueueToEmpty };
//# sourceMappingURL=index.js.map