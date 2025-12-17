import { BaseAdapter } from "@bull-board/api/dist/queueAdapters/base.js";
import { JobCounts, JobStatus, QueueJob, Status } from "@bull-board/api/typings/app";
import * as ioredis0 from "ioredis";
import Redis from "ioredis";

//#region src/status.d.ts
declare const STATUS: {
  readonly latest: 'latest';
  readonly active: 'active';
  readonly waiting: 'waiting';
  readonly waitingChildren: 'waiting-children';
  readonly prioritized: 'prioritized';
  readonly completed: 'completed';
  readonly failed: 'failed';
  readonly delayed: 'delayed';
  readonly paused: 'paused';
};
type Status$1 = (typeof STATUS)[keyof typeof STATUS];
//#endregion
//#region src/job.d.ts
declare class Job<T = any> {
  readonly queue: Queue<T>;
  readonly id: string;
  readonly name: string;
  readonly data: T;
  readonly groupId: string;
  readonly attemptsMade: number;
  readonly opts: {
    attempts: number;
    delay?: number;
  };
  readonly processedOn?: number;
  readonly finishedOn?: number;
  readonly failedReason: string;
  readonly stacktrace?: string;
  readonly returnvalue?: any;
  readonly timestamp: number;
  readonly orderMs?: number;
  readonly status: Status$1 | 'unknown';
  constructor(args: {
    queue: Queue<T>;
    id: string;
    name?: string;
    data: T;
    groupId: string;
    attemptsMade: number;
    opts: {
      attempts: number;
      delay?: number;
    };
    processedOn?: number;
    finishedOn?: number;
    failedReason?: string;
    stacktrace?: string;
    returnvalue?: any;
    timestamp: number;
    orderMs?: number;
    status?: Status$1 | 'unknown';
  });
  getState(): Promise<Status$1 | 'stuck' | 'waiting-children' | 'prioritized' | 'unknown'>;
  toJSON(): {
    id: string;
    name: string;
    data: T;
    groupId: string;
    attemptsMade: number;
    opts: {
      attempts: number;
      delay?: number;
    };
    processedOn: number | undefined;
    finishedOn: number | undefined;
    failedReason: string;
    stacktrace: string[] | null;
    returnvalue: any;
    timestamp: number;
    orderMs: number | undefined;
    status: Status$1 | "unknown";
    progress: number;
  };
  changeDelay(newDelay: number): Promise<boolean>;
  promote(): Promise<void>;
  remove(): Promise<void>;
  retry(_state?: Extract<Status$1, 'completed' | 'failed'>): Promise<void>;
  updateData(jobData: T): Promise<void>;
  update(jobData: T): Promise<void>;
  /**
   * Cancel this job if it is currently being processed.
   * Sends an abort signal to the worker processing this job.
   *
   * @returns true if the job was active and cancel signal was sent, false otherwise
   */
  cancel(): Promise<boolean>;
  static fromReserved<T = any>(queue: Queue<T>, reserved: ReservedJob<T>, meta?: {
    processedOn?: number;
    finishedOn?: number;
    failedReason?: string;
    stacktrace?: string;
    returnvalue?: any;
    status?: Status$1 | string;
    delayMs?: number;
  }): Job<T>;
  /**
   * Create a Job from raw Redis hash data with optional known status
   * This avoids extra Redis lookups when status is already known
   */
  static fromRawHash<T = any>(queue: Queue<T>, id: string, raw: Record<string, string>, knownStatus?: Status$1 | 'unknown'): Job<T>;
  static fromStore<T = any>(queue: Queue<T>, id: string): Promise<Job<T>>;
}
//#endregion
//#region src/logger.d.ts
interface LoggerInterface {
  warn(...args: any[]): void;
  info(...args: any[]): void;
  error(...args: any[]): void;
  debug(...args: any[]): void;
}
//#endregion
//#region src/queue.d.ts
/**
 * Options for configuring a GroupMQ queue
 */
type QueueOptions = {
  /**
   * Logger configuration for queue operations and debugging.
   *
   * @default false (no logging)
   * @example true // Enable basic logging
   * @example customLogger // Use custom logger instance
   *
   * **When to enable:**
   * - Development: For debugging queue operations
   * - Production monitoring: For operational insights
   * - Troubleshooting: When investigating performance issues
   */
  logger?: LoggerInterface | boolean;
  /**
   * Redis client instance for queue operations.
   * Should be a connected ioredis client.
   *
   * @example new Redis('redis://localhost:6379')
   * @example new Redis({ host: 'localhost', port: 6379, db: 0 })
   */
  redis: Redis;
  /**
   * Unique namespace for this queue. Used to separate different queues in the same Redis instance.
   * Should be unique across your application to avoid conflicts.
   *
   * @example 'email-queue'
   * @example 'user-notifications'
   * @example 'data-processing'
   */
  namespace: string;
  /**
   * Maximum time in milliseconds a job can run before being considered failed.
   * Jobs that exceed this timeout will be retried or moved to failed state.
   *
   * @default 30000 (30 seconds)
   * @example 60000 // 1 minute timeout
   * @example 300000 // 5 minute timeout for long-running jobs
   *
   * **When to adjust:**
   * - Long-running jobs: Increase (5-30 minutes)
   * - Short jobs: Decrease (5-15 seconds) for faster failure detection
   * - External API calls: Consider API timeout + buffer
   * - Database operations: Consider query timeout + buffer
   */
  jobTimeoutMs?: number;
  /**
   * Default maximum number of retry attempts for failed jobs.
   * Can be overridden per job or per worker.
   *
   * @default 3
   * @example 5 // Retry failed jobs up to 5 times
   * @example 1 // Fail fast with minimal retries
   *
   * **When to override:**
   * - Critical jobs: Increase retries
   * - Non-critical jobs: Decrease retries
   * - Idempotent operations: Can safely retry more
   * - External API calls: Consider API reliability
   */
  maxAttempts?: number;
  /**
   * Maximum number of groups to scan when looking for available jobs.
   * Higher values may find more jobs but use more Redis resources.
   *
   * @default 20
   * @example 50 // Scan more groups for better job distribution
   * @example 10 // Reduce Redis load for simple queues
   *
   * **When to adjust:**
   * - Many groups: Increase (50-100) for better job distribution
   * - Few groups: Decrease (5-10) to reduce Redis overhead
   * - High job volume: Increase for better throughput
   * - Resource constraints: Decrease to reduce Redis load
   */
  reserveScanLimit?: number;
  /**
   * Maximum number of completed jobs to retain for inspection.
   * Jobs beyond this limit are automatically cleaned up.
   *
   * @default 0 (no retention)
   * @example 100 // Keep last 100 completed jobs
   * @example 1000 // Keep last 1000 completed jobs for analysis
   *
   * **When to adjust:**
   * - Debugging: Increase to investigate issues
   * - Memory constraints: Decrease to reduce Redis memory usage
   * - Compliance: Increase for audit requirements
   */
  keepCompleted?: number;
  /**
   * Maximum number of failed jobs to retain for inspection.
   * Jobs beyond this limit are automatically cleaned up.
   *
   * @default 0 (no retention)
   * @example 1000 // Keep last 1000 failed jobs for analysis
   * @example 10000 // Keep more failed jobs for trend analysis
   *
   * **When to adjust:**
   * - Error analysis: Increase to investigate failure patterns
   * - Memory constraints: Decrease to reduce Redis memory usage
   * - Compliance: Increase for audit requirements
   */
  keepFailed?: number;
  /**
   * TTL for scheduler lock in milliseconds.
   * Prevents multiple schedulers from running simultaneously.
   *
   * @default 1500
   * @example 3000 // 3 seconds for slower environments
   * @example 1000 // 1 second for faster environments
   */
  schedulerLockTtlMs?: number;
  /**
   * Ordering delay in milliseconds. When set, jobs with orderMs will be staged
   * and promoted only after orderMs + orderingDelayMs to ensure proper ordering
   * even when producers are out of sync.
   *
   * @default 0 (no staging, jobs processed immediately)
   * @example 200 // Wait 200ms to ensure all jobs arrive in order
   * @example 1000 // Wait 1 second for strict ordering
   *
   * **When to use:**
   * - Distributed producers with clock drift
   * - Strict timestamp ordering required
   * - Network latency between producers
   *
   * **Note:** Only applies to jobs with orderMs set. Jobs without orderMs
   * are never staged.
   */
  orderingDelayMs?: number;
  /**
   * Enable automatic job batching to reduce Redis load.
   * Jobs are buffered in memory and sent in batches.
   *
   * @default undefined (disabled)
   * @example true // Enable with defaults (size: 10, maxWaitMs: 10)
   * @example { size: 20, maxWaitMs: 5 } // Custom configuration
   *
   * **Trade-offs:**
   * - ✅ 10x fewer Redis calls (huge performance win)
   * - ✅ Higher throughput (5-10x improvement)
   * - ✅ Lower latency per add() call
   * - ⚠️ Jobs buffered in memory briefly before Redis
   * - ⚠️ If process crashes during batch window, those jobs are lost
   *
   * **When to use:**
   * - High job volume (>100 jobs/s)
   * - Using orderingDelayMs (already buffering)
   * - Network latency is a bottleneck
   * - Acceptable risk of losing jobs during crash (e.g., non-critical jobs)
   *
   * **When NOT to use:**
   * - Critical jobs that must be persisted immediately
   * - Very low volume (<10 jobs/s)
   * - Zero tolerance for data loss
   *
   * **Configuration:**
   * - size: Maximum jobs per batch (default: 10)
   * - maxWaitMs: Maximum time to wait before flushing (default: 10)
   *
   * **Safety:**
   * - Keep maxWaitMs small (10ms = very low risk)
   * - Batches are flushed on queue.close()
   * - Consider graceful shutdown handling
   */
  autoBatch?: boolean | {
    size?: number;
    maxWaitMs?: number;
  };
};
/**
 * Configuration for repeating jobs
 */
type RepeatOptions = {
  /**
   * Repeat interval in milliseconds. Job will be created every N milliseconds.
   *
   * @example 60000 // Every minute
   * @example 3600000 // Every hour
   * @example 86400000 // Every day
   *
   * When to use:
   * - Simple intervals: Use for regular, predictable schedules
   * - High frequency: Good for sub-hour intervals
   * - Performance: More efficient than cron for simple intervals
   */
  every: number;
} | {
  /**
   * Cron pattern for complex scheduling. Uses standard cron syntax with seconds.
   * Format: second minute hour day month dayOfWeek
   *
   * When to use:
   * - Complex schedules: Business hours, specific days, etc.
   * - Low frequency: Good for daily, weekly, monthly schedules
   * - Business logic: Align with business requirements
   *
   * Cron format uses standard syntax with seconds precision.
   */
  pattern: string;
};
/**
 * Options for adding a job to the queue
 *
 * @template T The type of data to store in the job
 */
type AddOptions<T> = {
  /**
   * Group ID for this job. Jobs with the same groupId are processed sequentially (FIFO).
   * Only one job per group can be processed at a time.
   *
   * @example 'user-123' // All jobs for user 123
   * @example 'email-notifications' // All email jobs
   * @example 'order-processing' // All order-related jobs
   *
   * **Best practices:**
   * - Use meaningful group IDs (user ID, resource ID, etc.)
   * - Keep group IDs consistent for related jobs
   * - Avoid too many unique groups (can impact performance)
   */
  groupId: string;
  /**
   * The data payload for this job. Can be any serializable data.
   *
   * @example { userId: 123, email: 'user@example.com' }
   * @example { orderId: 'order-456', items: [...] }
   * @example 'simple string data'
   */
  data: T;
  /**
   * Custom ordering timestamp in milliseconds. Jobs are processed in orderMs order within each group.
   * If not provided, uses current timestamp (Date.now()).
   *
   * @default Date.now()
   * @example Date.now() + 5000 // Process 5 seconds from now
   * @example 1640995200000 // Specific timestamp
   *
   * **When to use:**
   * - Delayed processing: Set future timestamp
   * - Priority ordering: Use lower timestamps for higher priority
   * - Batch processing: Group related jobs with same timestamp
   */
  orderMs?: number;
  /**
   * Maximum number of retry attempts for this specific job.
   * Overrides the queue's default maxAttempts setting.
   *
   * @default queue.maxAttemptsDefault
   * @example 5 // Retry this job up to 5 times
   * @example 1 // Fail fast with no retries
   *
   * **When to override:**
   * - Critical jobs: Increase retries
   * - Non-critical jobs: Decrease retries
   * - Idempotent operations: Can safely retry more
   * - External API calls: Consider API reliability
   */
  maxAttempts?: number;
  /**
   * Delay in milliseconds before this job becomes available for processing.
   * Alternative to using orderMs for simple delays.
   *
   * @example 5000 // Process after 5 seconds
   * @example 300000 // Process after 5 minutes
   *
   * **When to use:**
   * - Simple delays: Use delay instead of orderMs
   * - Rate limiting: Delay jobs to spread load
   * - Retry backoff: Delay retry attempts
   */
  delay?: number;
  /**
   * Specific time when this job should be processed.
   * Can be a Date object or timestamp in milliseconds.
   *
   * @example new Date('2024-01-01T12:00:00Z')
   * @example Date.now() + 3600000 // 1 hour from now
   *
   * **When to use:**
   * - Scheduled processing: Process at specific time
   * - Business hours: Schedule during working hours
   * - Maintenance windows: Schedule during low-traffic periods
   */
  runAt?: Date | number;
  /**
   * Configuration for repeating jobs (cron or interval-based).
   * Creates a repeating job that generates new instances automatically.
   *
   * @example { every: 60000 } // Every minute
   *
   * When to use:
   * - Periodic tasks: Regular cleanup, reports, etc.
   * - Monitoring: Health checks, metrics collection
   * - Maintenance: Regular database cleanup, cache warming
   */
  repeat?: RepeatOptions;
  /**
   * Custom job ID for idempotence. If a job with this ID already exists,
   * the new job will be ignored (idempotent behavior).
   *
   * @example 'user-123-email-welcome'
   * @example 'order-456-payment-process'
   *
   * **When to use:**
   * - Idempotent operations: Prevent duplicate processing
   * - External system integration: Use external IDs
   * - Retry scenarios: Ensure same job isn't added multiple times
   * - Deduplication: Prevent duplicate jobs from being created
   */
  jobId?: string;
};
type ReservedJob<T = any> = {
  id: string;
  groupId: string;
  data: T;
  attempts: number;
  maxAttempts: number;
  seq: number;
  timestamp: number;
  orderMs: number;
  score: number;
  deadlineAt: number;
};
declare class Queue<T = any> {
  private logger;
  private r;
  private rawNs;
  private ns;
  private vt;
  private defaultMaxAttempts;
  private scanLimit;
  private keepCompleted;
  private keepFailed;
  private schedulerLockTtlMs;
  orderingDelayMs: number;
  name: string;
  private _consecutiveEmptyReserves;
  private promoterRedis?;
  private promoterRunning;
  private promoterLockId?;
  private promoterInterval?;
  private batchConfig?;
  private batchBuffer;
  private batchTimer?;
  private flushing;
  constructor(opts: QueueOptions);
  get redis(): Redis;
  get namespace(): string;
  get rawNamespace(): string;
  get jobTimeoutMs(): number;
  get maxAttemptsDefault(): number;
  add(opts: AddOptions<T>): Promise<Job<T>>;
  private addSingle;
  private flushBatch;
  reserve(): Promise<ReservedJob<T> | null>;
  /**
   * Check how many jobs are waiting in a specific group
   */
  getGroupJobCount(groupId: string): Promise<number>;
  /**
   * Complete a job by removing from processing and unlocking the group.
   * Note: Job metadata recording is handled separately by recordCompleted().
   *
   * @deprecated Use completeWithMetadata() for internal operations. This method
   * is kept for backward compatibility and testing only.
   */
  complete(job: {
    id: string;
    groupId: string;
  }): Promise<void>;
  /**
   * Complete a job AND record metadata in a single atomic operation.
   * This is the efficient internal method used by workers.
   */
  completeWithMetadata(job: {
    id: string;
    groupId: string;
  }, result: unknown, meta: {
    processedOn: number;
    finishedOn: number;
    attempts: number;
    maxAttempts: number;
  }): Promise<void>;
  /**
   * Atomically complete a job and try to reserve the next job from the same group
   * This prevents race conditions where other workers can steal subsequent jobs from the same group
   */
  /**
   * Atomically complete a job with metadata and reserve the next job from the same group.
   */
  completeAndReserveNextWithMetadata(completedJobId: string, groupId: string, handlerResult: unknown, meta: {
    processedOn: number;
    finishedOn: number;
    attempts: number;
    maxAttempts: number;
  }): Promise<ReservedJob<T> | null>;
  /**
   * Check if a job is currently in processing state
   */
  isJobProcessing(jobId: string): Promise<boolean>;
  retry(jobId: string, backoffMs?: number): Promise<number>;
  /**
   * Dead letter a job (remove from group and optionally store in dead letter queue)
   */
  deadLetter(jobId: string, groupId: string): Promise<number>;
  /**
   * Record a successful completion for retention and inspection
   * Uses consolidated Lua script for atomic operation with retention management
   */
  recordCompleted(job: {
    id: string;
    groupId: string;
  }, result: unknown, meta: {
    processedOn?: number;
    finishedOn?: number;
    attempts?: number;
    maxAttempts?: number;
    data?: unknown;
  }): Promise<void>;
  /**
   * Record a failure attempt (non-final), storing last error for visibility
   */
  recordAttemptFailure(job: {
    id: string;
    groupId: string;
  }, error: {
    message?: string;
    name?: string;
    stack?: string;
  } | string, meta: {
    processedOn?: number;
    finishedOn?: number;
    attempts?: number;
    maxAttempts?: number;
  }): Promise<void>;
  /**
   * Record a final failure (dead-lettered) for retention and inspection
   * Uses consolidated Lua script for atomic operation
   */
  recordFinalFailure(job: {
    id: string;
    groupId: string;
  }, error: {
    message?: string;
    name?: string;
    stack?: string;
  } | string, meta: {
    processedOn?: number;
    finishedOn?: number;
    attempts?: number;
    maxAttempts?: number;
    data?: unknown;
  }): Promise<void>;
  getCompleted(limit?: number): Promise<Array<{
    id: string;
    groupId: string;
    data: any;
    returnvalue: any;
    processedOn?: number;
    finishedOn?: number;
    attempts: number;
    maxAttempts: number;
  }>>;
  getFailed(limit?: number): Promise<Array<{
    id: string;
    groupId: string;
    data: any;
    failedReason: string;
    stacktrace?: string;
    processedOn?: number;
    finishedOn?: number;
    attempts: number;
    maxAttempts: number;
  }>>;
  /**
   * Convenience: return completed jobs as Job entities (non-breaking, new API)
   */
  getCompletedJobs(limit?: number): Promise<Array<Job<T>>>;
  /**
   * Convenience: return failed jobs as Job entities (non-breaking, new API)
   */
  getFailedJobs(limit?: number): Promise<Array<Job<T>>>;
  getCompletedCount(): Promise<number>;
  getFailedCount(): Promise<number>;
  heartbeat(job: {
    id: string;
    groupId: string;
  }, extendMs?: number): Promise<number>;
  /**
   * Clean up expired jobs and stale data.
   * Uses distributed lock to ensure only one worker runs cleanup at a time,
   * similar to scheduler lock pattern.
   */
  cleanup(): Promise<number>;
  /**
   * Calculate adaptive blocking timeout like BullMQ
   * Returns timeout in seconds
   *
   * Inspiration by BullMQ ⭐️
   */
  private getBlockTimeout;
  /**
   * Check if an error is a Redis connection error (should retry)
   * Conservative approach: only connection closed and ECONNREFUSED
   */
  isConnectionError(err: any): boolean;
  reserveBlocking(timeoutSec?: number, blockUntil?: number, blockingClient?: ioredis0.default): Promise<ReservedJob<T> | null>;
  /**
   * Reserve a job from a specific group atomically (eliminates race conditions)
   * @param groupId - The group to reserve from
   */
  reserveAtomic(groupId: string): Promise<ReservedJob<T> | null>;
  /**
   * Reserve up to maxBatch jobs (one per available group) atomically in Lua.
   */
  reserveBatch(maxBatch?: number): Promise<Array<ReservedJob<T>>>;
  /**
   * Get the number of jobs currently being processed (active jobs)
   */
  getActiveCount(): Promise<number>;
  /**
   * Get the number of jobs waiting to be processed
   */
  getWaitingCount(): Promise<number>;
  /**
   * Get the number of jobs delayed due to backoff
   */
  getDelayedCount(): Promise<number>;
  /**
   * Get list of active job IDs
   */
  getActiveJobs(): Promise<string[]>;
  /**
   * Get list of waiting job IDs
   */
  getWaitingJobs(): Promise<string[]>;
  /**
   * Get list of delayed job IDs
   */
  getDelayedJobs(): Promise<string[]>;
  /**
   * Get list of unique group IDs that have jobs
   */
  getUniqueGroups(): Promise<string[]>;
  /**
   * Get count of unique groups that have jobs
   */
  getUniqueGroupsCount(): Promise<number>;
  /**
   * Fetch a single job by ID with enriched fields for UI/inspection.
   * Attempts to mimic BullMQ's Job shape for fields commonly used by BullBoard.
   */
  getJob(id: string): Promise<Job<T>>;
  /**
   * Fetch jobs by statuses, emulating BullMQ's Queue.getJobs API used by BullBoard.
   * Only getter functionality; ordering is best-effort.
   *
   * Optimized with pagination to reduce Redis load - especially important for BullBoard polling.
   */
  getJobsByStatus(jobStatuses: Array<Status$1>, start?: number, end?: number): Promise<Array<Job<T>>>;
  /**
   * Provide counts structured like BullBoard expects.
   */
  getJobCounts(): Promise<Record<'active' | 'waiting' | 'delayed' | 'completed' | 'failed' | 'paused' | 'waiting-children' | 'prioritized', number>>;
  /**
   * Check for stalled jobs and recover or fail them
   * Returns array of [jobId, groupId, action] tuples
   */
  checkStalledJobs(now: number, gracePeriod: number, maxStalledCount: number): Promise<string[]>;
  /**
   * Start the promoter service for staging system.
   * Promoter listens to Redis keyspace notifications and promotes staged jobs when ready.
   * This is idempotent - calling multiple times has no effect if already running.
   */
  startPromoter(): Promise<void>;
  /**
   * Run a single promotion cycle with distributed locking
   */
  private runPromotion;
  /**
   * Stop the promoter service
   */
  stopPromoter(): Promise<void>;
  /**
   * Close underlying Redis connections
   */
  close(): Promise<void>;
  private get pausedKey();
  pause(): Promise<void>;
  resume(): Promise<void>;
  isPaused(): Promise<boolean>;
  /**
   * Wait for the queue to become empty (no active jobs)
   * @param timeoutMs Maximum time to wait in milliseconds (default: 60 seconds)
   * @returns true if queue became empty, false if timeout reached
   */
  waitForEmpty(timeoutMs?: number): Promise<boolean>;
  private _groupCleanupTracking;
  /**
   * Remove problematic groups from ready queue to prevent infinite loops
   * Handles both poisoned groups (only failed/expired jobs) and locked groups
   *
   * Throttled to 1% sampling rate to reduce Redis overhead
   */
  private cleanupPoisonedGroup;
  /**
   * Distributed one-shot scheduler: promotes delayed jobs and processes repeating jobs.
   * Only proceeds if a short-lived scheduler lock can be acquired.
   */
  private schedulerLockKey;
  acquireSchedulerLock(ttlMs?: number): Promise<boolean>;
  runSchedulerOnce(now?: number): Promise<void>;
  /**
   * Promote up to `limit` delayed jobs that are due now. Uses a small Lua to move one item per tick.
   */
  promoteDelayedJobsBounded(limit?: number, now?: number): Promise<number>;
  /**
   * Process up to `limit` repeating job ticks.
   * Intentionally small per-tick work to keep Redis CPU flat.
   */
  processRepeatingJobsBounded(limit?: number, now?: number): Promise<number>;
  /**
   * Promote delayed jobs that are now ready to be processed
   * This should be called periodically to move jobs from delayed set to ready queue
   */
  promoteDelayedJobs(): Promise<number>;
  /**
   * Change the delay of a specific job
   */
  changeDelay(jobId: string, newDelay: number): Promise<boolean>;
  /**
   * Promote a delayed job to be ready immediately
   */
  promote(jobId: string): Promise<boolean>;
  /**
   * Remove a job from the queue regardless of state (waiting, delayed, processing)
   */
  remove(jobId: string): Promise<boolean>;
  /**
   * Clean jobs of a given status older than graceTimeMs
   * @param graceTimeMs Remove jobs with finishedOn <= now - graceTimeMs (for completed/failed)
   * @param limit Max number of jobs to clean in one call
   * @param status Either 'completed' or 'failed'
   */
  clean(graceTimeMs: number, limit: number, status: 'completed' | 'failed' | 'delayed'): Promise<number>;
  /**
   * Update a job's data payload (BullMQ-style)
   */
  updateData(jobId: string, data: T): Promise<void>;
  /**
   * Add a repeating job (cron job)
   */
  private addRepeatingJob;
  /**
   * Compute next execution time using cron-parser (BullMQ-style)
   */
  private getNextCronTime;
  /**
   * Remove a repeating job
   */
  removeRepeatingJob(groupId: string, repeat: RepeatOptions): Promise<boolean>;
  /**
   * Get the current state of a job
   * Returns 'active', 'delayed', 'waiting', 'completed', 'failed', or 'unknown'
   */
  getJobState(jobId: string): Promise<Status$1 | 'unknown'>;
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
  cancel(jobId: string): Promise<boolean>;
}
//#endregion
//#region src/adapters/groupmq-bullboard-adapter.d.ts
type GroupMQBullBoardAdapterOptions = {
  readOnlyMode?: boolean;
  prefix?: string;
  delimiter?: string;
  description?: string;
  displayName?: string;
};
declare class BullBoardGroupMQAdapter<T = any> extends BaseAdapter {
  private queue;
  private options;
  constructor(queue: Queue<T>, options?: GroupMQBullBoardAdapterOptions);
  getDescription(): string;
  getDisplayName(): string;
  getName(): string;
  getRedisInfo(): Promise<string>;
  getJob(id: string): Promise<QueueJob | undefined | null>;
  getJobs(jobStatuses: JobStatus[], start?: number, end?: number): Promise<QueueJob[]>;
  getJobCounts(): Promise<JobCounts>;
  getJobLogs(_id: string): Promise<string[]>;
  getStatuses(): Status[];
  getJobStatuses(): JobStatus[];
  private assertWritable;
  clean(jobStatus: any, graceTimeMs: number): Promise<void>;
  addJob(_name: string, data: any, options: any): Promise<QueueJob>;
  isPaused(): Promise<boolean>;
  pause(): Promise<void>;
  resume(): Promise<void>;
  empty(): Promise<void>;
  promoteAll(): Promise<void>;
}
//#endregion
//#region src/errors.d.ts
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
declare class JobCancelledError extends Error {
  readonly jobId: string;
  constructor(jobId: string, message?: string);
}
//#endregion
//#region src/worker.d.ts
type BackoffStrategy = (attempt: number) => number;
interface WorkerEvents<T = any> extends Record<string, (...args: any[]) => void> {
  error: (error: Error) => void;
  closed: () => void;
  ready: () => void;
  failed: (job: Job<T>) => void;
  completed: (job: Job<T>) => void;
  'ioredis:close': () => void;
  'graceful-timeout': (job: Job<T>) => void;
  stalled: (jobId: string, groupId: string) => void;
}
declare class TypedEventEmitter<TEvents extends Record<string, (...args: any[]) => void>> {
  private listeners;
  on<K extends keyof TEvents>(event: K, listener: TEvents[K]): this;
  off<K extends keyof TEvents>(event: K, listener: TEvents[K]): this;
  emit<K extends keyof TEvents>(event: K, ...args: Parameters<TEvents[K]>): boolean;
  removeAllListeners<K extends keyof TEvents>(event?: K): this;
}
/**
 * Configuration options for a GroupMQ Worker
 *
 * @template T The type of data stored in jobs
 */
type WorkerOptions<T> = {
  /** The queue instance this worker will process jobs from */
  queue: Queue<T>;
  /**
   * Optional worker name for logging and identification
   * @default queue.name
   */
  name?: string;
  /**
   * The function that processes jobs. Must be async and handle job failures gracefully.
   * @param job The reserved job to process
   * @param signal AbortSignal that is aborted when the job is cancelled
   * @returns Promise that resolves when job is complete
   */
  handler: (job: ReservedJob<T>, signal: AbortSignal) => Promise<unknown>;
  /**
   * Heartbeat interval in milliseconds to keep jobs alive during processing.
   * Prevents jobs from timing out during long-running operations.
   *
   * @default Math.max(1000, queue.jobTimeoutMs / 3)
   * @example 5000 // Heartbeat every 5 seconds
   *
   * **When to adjust:**
   * - Long-running jobs: Increase to reduce Redis overhead
   * - Short jobs: Decrease for faster timeout detection
   * - High job volume: Increase to reduce Redis commands
   */
  heartbeatMs?: number;
  /**
   * Error handler called when job processing fails or worker encounters errors
   * @param err The error that occurred
   * @param job The job that failed (if applicable)
   */
  onError?: (err: unknown, job?: ReservedJob<T>) => void;
  /**
   * Maximum number of retry attempts for failed jobs at the worker level.
   * This overrides the queue's default maxAttempts setting.
   *
   * @default queue.maxAttemptsDefault
   * @example 5 // Retry failed jobs up to 5 times
   *
   * **When to adjust:**
   * - Critical jobs: Increase for more retries
   * - Non-critical jobs: Decrease to fail faster
   * - External API calls: Consider network reliability
   */
  maxAttempts?: number;
  /**
   * Backoff strategy for retrying failed jobs. Determines delay between retries.
   *
   * @default Exponential backoff with jitter (500ms, 1s, 2s, 4s, 8s, 16s, 30s max)
   * @example (attempt) => Math.min(10000, attempt * 1000) // Linear backoff
   *
   * **When to adjust:**
   * - Rate-limited APIs: Use longer delays
   * - Database timeouts: Use shorter delays
   * - External services: Consider their retry policies
   */
  backoff?: BackoffStrategy;
  /**
   * Whether to enable automatic cleanup of expired and completed jobs.
   * Cleanup removes old jobs to prevent Redis memory growth.
   *
   * @default true
   * @example false // Disable if you handle cleanup manually
   *
   * **When to disable:**
   * - Manual cleanup: If you have your own cleanup process
   * - Job auditing: If you need to keep all job history
   * - Development: For debugging job states
   */
  enableCleanup?: boolean;
  /**
   * Interval in milliseconds between cleanup operations.
   * Cleanup removes expired jobs and trims completed/failed job retention.
   *
   * @default 300000 (5 minutes)
   * @example 600000 // Cleanup every 10 minutes
   *
   * **When to adjust:**
   * - High job volume: Increase to reduce Redis overhead
   * - Low job volume: Decrease for more frequent cleanup
   * - Memory constraints: Decrease to prevent Redis memory growth
   * - Job retention needs: Adjust based on keepCompleted/keepFailed settings
   */
  cleanupIntervalMs?: number;
  /**
   * Interval in milliseconds between scheduler operations.
   * Scheduler promotes delayed jobs and processes cron/repeating jobs.
   *
   * @default 5000 (5 seconds)
   * @example 1000 // For fast cron jobs (every minute or less)
   * @example 10000 // For slow cron jobs (hourly or daily)
   *
   * **When to adjust:**
   * - Fast cron jobs: Decrease (1000-2000ms) for sub-minute schedules
   * - Slow cron jobs: Increase (10000-60000ms) to reduce Redis overhead
   * - No cron jobs: Increase (5000-10000ms) since only delayed jobs are affected
   */
  schedulerIntervalMs?: number;
  /**
   * Maximum time in seconds to wait for new jobs when queue is empty.
   * Shorter timeouts make workers more responsive but use more Redis resources.
   *
   * @default 1
   * @example 0.5 // Very responsive, higher Redis usage
   * @example 2 // Less responsive, lower Redis usage
   *
   * **When to adjust:**
   * - High job volume: Use 1s or less for faster job pickup
   * - Low job volume: Increase (2-3s) to reduce Redis overhead
   * - Real-time requirements: Decrease to 0.5-1s for lower latency
   * - Resource constraints: Increase to 2-5s to reduce Redis load
   *
   * **Note:** The actual timeout is adaptive and can go as low as 1ms
   * based on queue activity and delayed job schedules.
   */
  blockingTimeoutSec?: number;
  /**
   * Logger configuration for worker operations and debugging.
   *
   * @default false (no logging)
   * @example true // Enable basic logging
   * @example customLogger // Use custom logger instance
   *
   * **When to enable:**
   * - Development: For debugging job processing
   * - Production monitoring: For operational insights
   * - Troubleshooting: When investigating performance issues
   */
  logger?: LoggerInterface | true;
  /**
   * Number of jobs this worker can process concurrently.
   * Higher concurrency increases throughput but uses more memory and CPU.
   *
   * @default 1
   * @example 4 // Process 4 jobs simultaneously
   * @example 8 // For CPU-intensive jobs on multi-core systems
   *
   * **When to adjust:**
   * - CPU-bound jobs: Set to number of CPU cores
   * - I/O-bound jobs: Set to 2-4x number of CPU cores
   * - Memory constraints: Lower concurrency to reduce memory usage
   * - High job volume: Increase for better throughput
   * - Single-threaded requirements: Keep at 1
   */
  concurrency?: number;
  /**
   * Interval in milliseconds between stalled job checks.
   * Stalled jobs are those whose worker crashed or lost connection.
   *
   * @default 30000 (30 seconds)
   * @example 60000 // Check every minute for lower overhead
   * @example 10000 // Check every 10 seconds for faster recovery
   *
   * **When to adjust:**
   * - Fast recovery needed: Decrease (10-20s)
   * - Lower Redis overhead: Increase (60s+)
   * - Unreliable workers: Decrease for faster detection
   */
  stalledInterval?: number;
  /**
   * Maximum number of times a job can become stalled before being failed.
   * A job becomes stalled when its worker crashes or loses connection.
   *
   * @default 1
   * @example 2 // Allow jobs to stall twice before failing
   * @example 0 // Never fail jobs due to stalling (not recommended)
   *
   * **When to adjust:**
   * - Unreliable infrastructure: Increase to tolerate more failures
   * - Critical jobs: Increase to allow more recovery attempts
   * - Quick failure detection: Keep at 1
   */
  maxStalledCount?: number;
  /**
   * Grace period in milliseconds before a job is considered stalled.
   * Jobs are only marked as stalled if their deadline has passed by this amount.
   *
   * @default 0 (no grace period)
   * @example 5000 // 5 second grace period for clock skew
   * @example 1000 // 1 second grace for network latency
   *
   * **When to adjust:**
   * - Clock skew between servers: Add 1-5s grace
   * - Network latency: Add 1-2s grace
   * - Strict timing: Keep at 0
   */
  stalledGracePeriod?: number;
};
declare class _Worker<T = any> extends TypedEventEmitter<WorkerEvents<T>> {
  private logger;
  readonly name: string;
  private q;
  private handler;
  private hbMs;
  private onError?;
  private stopping;
  private ready;
  private closed;
  private maxAttempts;
  private backoff;
  private enableCleanup;
  private cleanupMs;
  private cleanupTimer?;
  private schedulerTimer?;
  private schedulerMs;
  private blockingTimeoutSec;
  private concurrency;
  private blockingClient;
  private stalledCheckTimer?;
  private stalledInterval;
  private maxStalledCount;
  private stalledGracePeriod;
  private jobsInProgress;
  private abortControllers;
  private cancelSubscriber;
  private lastJobPickupTime;
  private totalJobsProcessed;
  private blockingStats;
  private emptyReserveBackoffMs;
  private redisCloseHandler?;
  private redisErrorHandler?;
  private redisReadyHandler?;
  private runLoopPromise?;
  constructor(opts: WorkerOptions<T>);
  get isClosed(): boolean;
  /**
   * Add jitter to prevent thundering herd problems in high-concurrency environments
   * @param baseInterval The base interval in milliseconds
   * @param jitterPercent Percentage of jitter to add (0-1, default 0.1 for 10%)
   * @returns The interval with jitter applied
   */
  private addJitter;
  private setupRedisEventHandlers;
  /**
   * Set up the Redis subscriber for job cancellation events
   */
  private setupCancelSubscriber;
  run(): Promise<void>;
  private _runLoop;
  private delay;
  /**
   * Process a job and return the next job if atomic completion succeeds
   * This matches BullMQ's processJob signature
   */
  private processJob;
  /**
   * Complete a job and try to atomically get next job from same group
   */
  private completeJob;
  /**
   * Start the stalled job checker
   * Checks periodically for jobs that exceeded their deadline and recovers or fails them
   */
  private startStalledChecker;
  /**
   * Check for stalled jobs and recover or fail them
   * A job is stalled when its worker crashed or lost connection
   */
  private checkStalled;
  /**
   * Get worker performance metrics
   */
  getWorkerMetrics(): {
    name: string;
    totalJobsProcessed: number;
    lastJobPickupTime: number;
    timeSinceLastJob: number | null;
    blockingStats: {
      totalBlockingCalls: number;
      consecutiveEmptyReserves: number;
      lastActivityTime: number;
    };
    isProcessing: boolean;
    jobsInProgressCount: number;
    jobsInProgress: {
      jobId: string;
      groupId: string;
      processingTimeMs: number;
    }[];
  };
  /**
   * Stop the worker gracefully
   * @param gracefulTimeoutMs Maximum time to wait for current job to finish (default: 30 seconds)
   */
  close(gracefulTimeoutMs?: number): Promise<void>;
  /**
   * Get information about the first currently processing job (if any)
   * For concurrency > 1, returns the oldest job in progress
   */
  getCurrentJob(): {
    job: ReservedJob<T>;
    processingTimeMs: number;
  } | null;
  /**
   * Get information about all currently processing jobs
   */
  getCurrentJobs(): Array<{
    job: ReservedJob<T>;
    processingTimeMs: number;
  }>;
  /**
   * Check if the worker is currently processing any jobs
   */
  isProcessing(): boolean;
  add(opts: AddOptions<T>): Promise<Job<T>>;
  private processSingleJob;
  /**
   * Handle job failure: emit events, retry or dead-letter
   */
  private handleJobFailure;
  /**
   * Dead-letter a job that exceeded max attempts
   */
  private deadLetterJob;
  /**
   * Record a failed attempt (not final)
   */
  private recordFailureAttempt;
}
type Worker<T = any> = _Worker<T>;
type WorkerConstructor = new <T>(opts: WorkerOptions<T>) => _Worker<T>;
declare const Worker: WorkerConstructor;
//#endregion
//#region src/helpers.d.ts
/**
 * Wait for a queue to become empty
 * @param queue The queue to monitor
 * @param timeoutMs Maximum time to wait (default: 60 seconds)
 * @returns Promise that resolves when queue is empty or timeout is reached
 */
declare function waitForQueueToEmpty(queue: Queue, timeoutMs?: number): Promise<boolean>;
/**
 * Get status of all workers
 */
declare function getWorkersStatus<T = any>(workers: Worker<T>[]): {
  total: number;
  processing: number;
  idle: number;
  workers: Array<{
    index: number;
    isProcessing: boolean;
    currentJob?: {
      jobId: string;
      groupId: string;
      processingTimeMs: number;
    };
  }>;
};
//#endregion
export { AddOptions, BackoffStrategy, BullBoardGroupMQAdapter, GroupMQBullBoardAdapterOptions, Job, JobCancelledError, Queue, QueueOptions, RepeatOptions, ReservedJob, Worker, WorkerEvents, WorkerOptions, getWorkersStatus, waitForQueueToEmpty };
//# sourceMappingURL=index.d.ts.map