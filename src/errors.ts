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
export class JobCancelledError extends Error {
  public readonly jobId: string;

  constructor(jobId: string, message?: string) {
    super(message ?? `Job ${jobId} was cancelled`);
    this.name = 'JobCancelledError';
    this.jobId = jobId;

    // Maintains proper stack trace for where the error was thrown (only in V8 environments)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, JobCancelledError);
    }
  }
}
