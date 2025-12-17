import Redis from 'ioredis';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { JobCancelledError, Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Job Cancellation Tests', () => {
  const namespace = `test:cancel:${Date.now()}`;

  afterAll(async () => {
    // Cleanup after all tests
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`groupmq:${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  describe('AbortSignal in handler', () => {
    it('should pass AbortSignal to handler', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:signal` });

      let receivedSignal: AbortSignal | null = null;

      const worker = new Worker({
        queue,
        handler: async (_job, signal) => {
          receivedSignal = signal;
          return 'done';
        },
      });

      await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to complete
      await queue.waitForEmpty();

      expect(receivedSignal).not.toBeNull();
      expect(receivedSignal).toBeInstanceOf(AbortSignal);
      expect(receivedSignal!.aborted).toBe(false);

      await worker.close();
      await queue.close();
    });

    it('should start with non-aborted signal', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:nonaborted` });

      let signalAbortedAtStart = true;

      const worker = new Worker({
        queue,
        handler: async (_job, signal) => {
          signalAbortedAtStart = signal.aborted;
          return 'done';
        },
      });

      await queue.add({ groupId: 'test-group', data: { id: 1 } });
      await queue.waitForEmpty();

      expect(signalAbortedAtStart).toBe(false);

      await worker.close();
      await queue.close();
    });
  });

  describe('queue.cancel()', () => {
    it('should return false for non-active job', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:nonactive` });

      // Add a job but don't process it
      const job = await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Job is waiting, not active
      const result = await queue.cancel(job.id);
      expect(result).toBe(false);

      await queue.close();
    });

    it('should return false for non-existent job', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:nonexistent` });

      const result = await queue.cancel('non-existent-job-id');
      expect(result).toBe(false);

      await queue.close();
    });

    it('should return true and abort signal for active job', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:activecancel` });

      let jobStarted = false;
      let signalAborted = false;
      let jobId: string | null = null;

      const worker = new Worker({
        queue,
        handler: async (job, signal) => {
          jobId = job.id;
          jobStarted = true;

          // Wait until cancelled or timeout
          await new Promise<void>((resolve) => {
            const checkInterval = setInterval(() => {
              if (signal.aborted) {
                signalAborted = true;
                clearInterval(checkInterval);
                resolve();
              }
            }, 10);

            // Timeout after 5 seconds
            setTimeout(() => {
              clearInterval(checkInterval);
              resolve();
            }, 5000);
          });

          if (signal.aborted) {
            throw new JobCancelledError(job.id);
          }

          return 'done';
        },
      });

      const addedJob = await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to start
      while (!jobStarted) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      // Verify job is active
      const state = await queue.getJobState(addedJob.id);
      expect(state).toBe('active');

      // Cancel the job
      const cancelResult = await queue.cancel(addedJob.id);
      expect(cancelResult).toBe(true);

      // Wait for abort signal to propagate
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(signalAborted).toBe(true);

      await worker.close(1000);
      await queue.close();
    }, 10000);
  });

  describe('job.cancel()', () => {
    it('should cancel via Job convenience method', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:jobcancel` });

      let jobStarted = false;
      let signalAborted = false;

      const worker = new Worker({
        queue,
        handler: async (job, signal) => {
          jobStarted = true;

          await new Promise<void>((resolve) => {
            const checkInterval = setInterval(() => {
              if (signal.aborted) {
                signalAborted = true;
                clearInterval(checkInterval);
                resolve();
              }
            }, 10);

            setTimeout(() => {
              clearInterval(checkInterval);
              resolve();
            }, 5000);
          });

          if (signal.aborted) {
            throw new JobCancelledError(job.id);
          }

          return 'done';
        },
      });

      const addedJob = await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to start
      while (!jobStarted) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      // Get job and cancel via convenience method
      const job = await queue.getJob(addedJob.id);
      const cancelResult = await job.cancel();
      expect(cancelResult).toBe(true);

      // Wait for abort signal to propagate
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(signalAborted).toBe(true);

      await worker.close(1000);
      await queue.close();
    }, 10000);
  });

  describe('JobCancelledError', () => {
    it('should have correct properties', () => {
      const error = new JobCancelledError('test-job-123');

      expect(error.name).toBe('JobCancelledError');
      expect(error.jobId).toBe('test-job-123');
      expect(error.message).toBe('Job test-job-123 was cancelled');
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(JobCancelledError);
    });

    it('should allow custom message', () => {
      const error = new JobCancelledError('test-job-123', 'Custom cancellation reason');

      expect(error.message).toBe('Custom cancellation reason');
      expect(error.jobId).toBe('test-job-123');
    });

    it('should be throwable and catchable', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({
        redis,
        namespace: `${namespace}:cancelerror`,
        keepFailed: 10,
      });

      let errorCaught = false;
      let caughtError: Error | null = null;

      const worker = new Worker({
        queue,
        maxAttempts: 1, // Don't retry cancelled jobs
        handler: async (job, signal) => {
          // Simulate checking for cancellation
          if (signal.aborted) {
            throw new JobCancelledError(job.id);
          }

          // Manually throw to test error handling
          throw new JobCancelledError(job.id, 'Manually cancelled');
        },
      });

      worker.on('error', (err) => {
        errorCaught = true;
        caughtError = err;
      });

      await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to fail
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(errorCaught).toBe(true);
      expect(caughtError).toBeInstanceOf(JobCancelledError);

      await worker.close();
      await queue.close();
    });
  });

  describe('getJobState()', () => {
    it('should return active for processing jobs', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:stateactive` });

      let jobStarted = false;
      let canComplete = false;

      const worker = new Worker({
        queue,
        handler: async (_job, _signal) => {
          jobStarted = true;
          while (!canComplete) {
            await new Promise((resolve) => setTimeout(resolve, 50));
          }
          return 'done';
        },
      });

      const job = await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to start
      while (!jobStarted) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      const state = await queue.getJobState(job.id);
      expect(state).toBe('active');

      canComplete = true;
      await queue.waitForEmpty();

      await worker.close();
      await queue.close();
    });

    it('should return waiting for queued jobs', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:statewaiting` });

      const job = await queue.add({ groupId: 'test-group', data: { id: 1 } });

      const state = await queue.getJobState(job.id);
      expect(state).toBe('waiting');

      await queue.close();
    });

    it('should return delayed for delayed jobs', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:statedelayed` });

      const job = await queue.add({
        groupId: 'test-group',
        data: { id: 1 },
        delay: 60000, // 60 second delay
      });

      const state = await queue.getJobState(job.id);
      expect(state).toBe('delayed');

      await queue.close();
    });

    it('should return unknown for non-existent jobs', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:stateunknown` });

      const state = await queue.getJobState('non-existent-job');
      expect(state).toBe('unknown');

      await queue.close();
    });
  });

  describe('cancel with multiple workers', () => {
    it('should only abort the specific job that was cancelled', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:multiworker` });

      let job1Started = false;
      let job1Aborted = false;
      let job2Started = false;
      let job2Aborted = false;
      let job1Id: string | null = null;
      let job2Id: string | null = null;

      // Single worker that processes jobs from different groups
      const worker = new Worker({
        queue,
        concurrency: 2, // Process 2 jobs concurrently
        handler: async (job, signal) => {
          const jobNum = (job.data as any).jobNum;

          if (jobNum === 1) {
            job1Id = job.id;
            job1Started = true;
          } else if (jobNum === 2) {
            job2Id = job.id;
            job2Started = true;
          }

          await new Promise<void>((resolve) => {
            const checkInterval = setInterval(() => {
              if (signal.aborted) {
                if (jobNum === 1) job1Aborted = true;
                if (jobNum === 2) job2Aborted = true;
                clearInterval(checkInterval);
                resolve();
              }
            }, 10);
            setTimeout(() => {
              clearInterval(checkInterval);
              resolve();
            }, 3000);
          });

          return 'done';
        },
      });

      // Add jobs to different groups so they can be processed in parallel
      await queue.add({ groupId: 'group-1', data: { jobNum: 1 } });
      await queue.add({ groupId: 'group-2', data: { jobNum: 2 } });

      // Wait for both jobs to start
      let attempts = 0;
      while ((!job1Started || !job2Started) && attempts < 100) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        attempts++;
      }

      expect(job1Started).toBe(true);
      expect(job2Started).toBe(true);

      // Cancel only job1
      const cancelResult = await queue.cancel(job1Id!);
      expect(cancelResult).toBe(true);

      // Wait for signal to propagate
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Only job1 should have been aborted
      expect(job1Aborted).toBe(true);
      expect(job2Aborted).toBe(false);

      // Cancel job2 to clean up
      await queue.cancel(job2Id!);

      await worker.close(1000);
      await queue.close();
    }, 10000);
  });

  describe('cleanup on worker close', () => {
    it('should abort remaining jobs on worker shutdown after graceful timeout', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:closecleanup` });

      let jobStarted = false;
      let signalAborted = false;
      let gracefulTimeoutEmitted = false;

      const worker = new Worker({
        queue,
        handler: async (_job, signal) => {
          jobStarted = true;

          // Long running job that checks for abort
          await new Promise<void>((resolve) => {
            const checkInterval = setInterval(() => {
              if (signal.aborted) {
                signalAborted = true;
                clearInterval(checkInterval);
                resolve();
              }
            }, 10);

            // Don't auto-resolve - wait for abort or external timeout
          });

          return 'done';
        },
      });

      worker.on('graceful-timeout', () => {
        gracefulTimeoutEmitted = true;
      });

      await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to start
      while (!jobStarted) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      // Close with short timeout - should trigger graceful timeout and abort
      await worker.close(200);

      expect(gracefulTimeoutEmitted).toBe(true);
      expect(signalAborted).toBe(true);

      await queue.close();
    }, 10000);
  });

  describe('signal.addEventListener', () => {
    it('should support abort event listener', async () => {
      const redis = new Redis(REDIS_URL);
      const queue = new Queue({ redis, namespace: `${namespace}:eventlistener` });

      let jobStarted = false;
      let abortEventFired = false;

      const worker = new Worker({
        queue,
        handler: async (job, signal) => {
          jobStarted = true;

          signal.addEventListener('abort', () => {
            abortEventFired = true;
          });

          // Wait for cancellation
          await new Promise<void>((resolve) => {
            if (signal.aborted) {
              resolve();
              return;
            }
            signal.addEventListener('abort', () => resolve());
            setTimeout(resolve, 5000);
          });

          if (signal.aborted) {
            throw new JobCancelledError(job.id);
          }

          return 'done';
        },
      });

      const job = await queue.add({ groupId: 'test-group', data: { id: 1 } });

      // Wait for job to start
      while (!jobStarted) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      // Cancel the job
      await queue.cancel(job.id);

      // Wait for abort event to fire
      await new Promise((resolve) => setTimeout(resolve, 300));

      expect(abortEventFired).toBe(true);

      await worker.close(1000);
      await queue.close();
    }, 10000);
  });
});
