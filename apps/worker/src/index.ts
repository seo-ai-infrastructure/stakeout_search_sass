import 'dotenv/config';
import { Queue, Worker, DelayedError, type Job } from 'bullmq';
import { db } from '@stakeout/db';
import { registerStakeoutSchedule } from '@stakeout/core/scheduleHelper';
import {
  redis,
  createRedisClient,
  redactRedisUrl,
} from '@stakeout/core/redis';
import { cloneScheduledRun, settlePin } from '@stakeout/core/scans';
import { acquireDeviceLease } from './duoplus';
import { measureDesktop } from './desktop';
import { measureMobile } from './mobile';

function formatErr(e: unknown) {
  if (e instanceof Error) {
    const err = e as NodeJS.ErrnoException;
    return {
      name: err.name,
      message: err.message,
      code: err.code,
    };
  }
  return { message: String(e) };
}

const queueConnection = redis(),
  deviceWorkerConnection = createRedisClient('worker:stakeout-pins'),
  cronWorkerConnection = createRedisClient('worker:stakeout-recurring-schedules'),
  pins = new Queue('stakeout-pins', { connection: queueConnection }),
  schedules = new Queue('stakeout-recurring-schedules', {
    connection: queueConnection,
  });

async function execute(job: Job, token?: string) {
  const pin = await db.gridPin.findUnique({
    where: { id: job.data.pinId },
    include: { scan: true },
  });
  if (!pin || pin.status !== 'PENDING') return;
  try {
    if (pin.scan.targetType === 'desktop_aimode') {
      await settlePin(pin.id, { data: await measureDesktop(pin) });
      return;
    }
    const d = await acquireDeviceLease(pin.id, pin.scan.phoneModel);
    if (!d) {
      if (Date.now() - job.timestamp > 86400000)
        throw new Error('DEVICE_WAIT_TIMEOUT');
      await job.moveToDelayed(Date.now() + 25000, token);
      throw new DelayedError();
    }
    try {
      const result = await measureMobile(pin, d);
      await d.assertOwned();
      await settlePin(pin.id, { data: result });
    } catch (e) {
      if (e instanceof Error && e.message === 'FLAG_CAPTCHA_DETECTED')
        await db.devicePool.update({
          where: { imageId: d.imageId },
          data: { cooldownUntil: new Date(Date.now() + 35 * 60000) },
        });
      throw e;
    } finally {
      await d.release();
    }
  } catch (e) {
    if (e instanceof DelayedError) throw e;
    if (job.attemptsMade + 1 >= (job.opts.attempts ?? 1))
      await settlePin(pin.id, {
        error: e instanceof Error ? e.message : 'PIN_FAILED',
      });
    throw e;
  }
}

async function repeat(job: Job) {
  if (!job.id) throw new Error('Scheduled job ID required.');
  await cloneScheduledRun(job.data.templateScanId, job.id);
}

const deviceWorker = new Worker('stakeout-pins', execute, {
    connection: deviceWorkerConnection,
    concurrency: Number(process.env.MOBILE_CONCURRENCY ?? 3),
    lockDuration: 300000,
    stalledInterval: 120000,
    drainDelay: 30,
  }),
  cronWorker = new Worker('stakeout-recurring-schedules', repeat, {
    connection: cronWorkerConnection,
    concurrency: 1,
    lockDuration: 120000,
    stalledInterval: 300000,
    drainDelay: 60,
  });

for (const w of [deviceWorker, cronWorker]) {
  w.on('error', (e) => console.error(w.name, formatErr(e)));
  w.on('failed', (job, e) =>
    console.error(w.name, 'job failed', job?.id, formatErr(e)),
  );
}

const jobOptions = {
  attempts: 3,
  backoff: { type: 'exponential', delay: 4000 },
  removeOnComplete: true,
  removeOnFail: { age: 86400 },
};

async function dispatch() {
  const queued = await db.gridScan.findMany({
    where: { dispatchedAt: null, status: 'QUEUED' },
    include: { pins: { select: { id: true } } },
    orderBy: { createdAt: 'asc' },
    take: 10,
  });
  for (const s of queued) {
    if (s.scheduleType !== 'once' && s.scheduleCron) {
      await registerStakeoutSchedule(
        schedules,
        s.id,
        s.scheduleCron.replace(/^PAUSED:/, ''),
      );
    }
    await pins.addBulk(
      s.pins.map((p) => ({
        name: 'measure',
        data: { pinId: p.id },
        opts: { ...jobOptions, jobId: p.id },
      })),
    );
    await db.gridScan.updateMany({
      where: { id: s.id, status: 'QUEUED' },
      data: { dispatchedAt: new Date(), status: 'RUNNING' },
    });
  }
}

async function reconcile() {
  // Only inspect pins untouched for >10 minutes so active scans do not spam Redis
  const staleCutoff = new Date(Date.now() - 10 * 60000);
  const pending = await db.gridPin.findMany({
    where: {
      status: 'PENDING',
      updatedAt: { lt: staleCutoff },
      scan: { status: 'RUNNING' },
    },
    orderBy: { updatedAt: 'asc' },
    take: 50,
    select: { id: true },
  });
  for (const p of pending) {
    const job = await pins.getJob(p.id);
    if (!job)
      await pins.add(
        'measure',
        { pinId: p.id },
        { ...jobOptions, jobId: p.id },
      );
    else if ((await job.getState()) === 'failed')
      await settlePin(p.id, {
        error: job.failedReason || 'WORKER_INTERRUPTED',
      });
    await db.gridPin.updateMany({
      where: { id: p.id, status: 'PENDING' },
      data: { updatedAt: new Date() },
    });
  }
}

let busy = false,
  lastRepair = Date.now();

async function tick() {
  if (busy) return;
  busy = true;
  try {
    await dispatch();
    if (Date.now() - lastRepair > 300000) {
      lastRepair = Date.now();
      await reconcile();
    }
  } catch (e) {
    console.error('Dispatch will retry', formatErr(e));
  } finally {
    busy = false;
  }
}

const timer = setInterval(tick, 10000);
void tick();

async function close() {
  clearInterval(timer);
  await Promise.all([deviceWorker.close(), cronWorker.close()]);
  await Promise.all([pins.close(), schedules.close()]);
  await Promise.allSettled([
    queueConnection.quit(),
    deviceWorkerConnection.quit(),
    cronWorkerConnection.quit(),
  ]);
  await db.$disconnect();
}

process.once('SIGINT', () => void close());
process.once('SIGTERM', () => void close());
console.info(
  `Stakeout Telemetry Engine online · target=${redactRedisUrl()} · stakeout-pins + stakeout-recurring-schedules`,
);
