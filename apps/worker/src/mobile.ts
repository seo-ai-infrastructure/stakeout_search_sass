import { randomBytes, createHash } from 'node:crypto';
import { mkdtemp, writeFile, readdir, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { x as unpack } from 'tar';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { type GridPin, type GridScan } from '@stakeout/db';
import { r2, bucket, put, uploadURL, uploaded } from '@stakeout/core/r2';
import { duoplusPost, type LeasedDevice } from './duoplus';
import { parsePinXmlBundle } from './xmlParser';
import { shellQuote, RUN_PIN_SH_V2 } from '@stakeout/core/runner';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
type Pin = GridPin & { scan: GridScan };

const bootstrappedInMemory = new Set<string>();
const RT_DIR = '/data/local/tmp/rt';
const RUNNER_SHA = createHash('sha256').update(RUN_PIN_SH_V2).digest('hex');

async function pushFileChunked(
  imageId: string,
  path: string,
  content: string,
  mode = '755',
) {
  const tmp = path + '.b64';
  const chunks =
    Buffer.from(content, 'utf8').toString('base64').match(/.{1,700}/g) ?? [];
  const run = (command: string) =>
    duoplusPost('/api/v1/cloudPhone/command', { image_id: imageId, command });

  await run(`mkdir -p ${RT_DIR}`);
  await run(`rm -f ${tmp} ${path}`);
  for (const chunk of chunks) {
    await run(`echo -n ${chunk} >> ${tmp}`);
  }
  await run(`base64 -d ${tmp} > ${path}`);
  await run(`chmod ${mode} ${path}`);
  await run(`rm -f ${tmp}`);
}

async function ensureRunnerInstalled(imageId: string) {
  if (bootstrappedInMemory.has(imageId)) return;

  const check = await duoplusPost('/api/v1/cloudPhone/command', {
    image_id: imageId,
    command: `cat ${RT_DIR}/run_pin.sha`,
  }).catch(() => null);

  if (String(check?.data?.content ?? '').trim() === RUNNER_SHA) {
    bootstrappedInMemory.add(imageId);
    return;
  }

  await pushFileChunked(imageId, `${RT_DIR}/run_pin.sh`, RUN_PIN_SH_V2);
  await duoplusPost('/api/v1/cloudPhone/command', {
    image_id: imageId,
    command: `echo ${RUNNER_SHA} > ${RT_DIR}/run_pin.sha`,
  });
  bootstrappedInMemory.add(imageId);
}

export async function provision(device: LeasedDevice, p: Pin) {
  await device.assertOwned();

  for (let attempt = 0; attempt < 3; attempt++) {
    await device.assertOwned();
    const { data } = await duoplusPost('/api/v1/proxy/check', {
      id: device.proxyId,
      proxy_id: device.proxyId,
      ip_scan_channel: 'ip2location',
    });

    if (
      data?.success &&
      data.ip &&
      /^(us|united states)$/i.test(String(data.country))
    ) {
      return {
        ip: String(data.ip),
        city: String(data.city ?? ''),
        region: String(data.region ?? ''),
        country: String(data.country),
        timezone: String(data.timezone ?? ''),
        rungUsed: 'DEVICE_ROTATING_SLOT',
        ispRequested: p.scan.ispId,
        ispRuleApplied: true,
        port: 10000 + device.slotIndex,
      };
    }
    await sleep(2000);
  }

  throw new Error('PROXY_LADDER_EXHAUSTED');
}

export async function readBundle(key: string, tag: string, dir: string) {
  const obj = await r2().send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  if (!obj.ContentLength || obj.ContentLength > 64 * 1024 ** 2)
    throw new Error('INVALID_BUNDLE_SIZE');
  await writeFile(join(dir, 'raw.tgz'), await obj.Body!.transformToByteArray());
  let bytes = 0,
    files = 0;
  await unpack({
    file: join(dir, 'raw.tgz'),
    cwd: dir,
    strict: true,
    filter: (name, entry) => {
      if (!('type' in entry)) return false;
      if (name === tag || name === tag + '/') return entry.type === 'Directory';
      if (!name.startsWith(tag + '/') || entry.type !== 'File') return false;
      const leaf = name.slice(tag.length + 1);
      if (
        !/^(video\.mp4|dump_[0-7]\.xml|aio_(initial|expanded)\.xml|flags\.txt|meta\.txt)$/.test(
          leaf,
        )
      )
        return false;
      const allowed = leaf === 'video.mp4' ? 40 * 1024 ** 2 : 4000000;
      bytes += entry.size;
      files++;
      return entry.size <= allowed && bytes <= 64 * 1024 ** 2 && files <= 16;
    },
  });
  const folder = join(dir, tag),
    entries = await readdir(folder);
  const xml = async (pattern: RegExp) =>
    Promise.all(
      entries
        .filter((f) => pattern.test(f))
        .sort()
        .map((f) => readFile(join(folder, f), 'utf8')),
    );
  return {
    scroll: await xml(/^dump_\d+\.xml$/),
    ai: await xml(/^aio_.*\.xml$/),
    flags: await readFile(join(folder, 'flags.txt'), 'utf8'),
    video: await readFile(join(folder, 'video.mp4')),
  };
}

export async function measureMobile(p: Pin, d: LeasedDevice) {
  const scan = p.scan,
    tagBase = randomBytes(6).toString('hex'),
    point = p.rowIdx + '_' + p.colIdx,
    tag = tagBase + '_' + point;
  const prefix = 'scans/' + p.scanId + '/pin_' + point + '/' + tagBase,
    tarKey = prefix + '/raw.tar.gz';
  const app: Record<string, string> = {
    mobile_maps: 'maps',
    mobile_google: 'google',
    mobile_chrome: 'chrome',
  };
  if (!app[scan.targetType]) throw new Error('UNSUPPORTED_SURFACE');

  await ensureRunnerInstalled(d.imageId);
  const proof = await provision(d, p);
  await d.assertOwned();

  await duoplusPost('/api/v1/cloudPhone/update', {
    images: [
      {
        image_id: d.imageId,
        proxy: { id: d.proxyId, dns: 1 },
        gps: { type: 2, latitude: String(p.lat), longitude: String(p.lng) },
        locale: {
          type: 2,
          ...(proof.timezone ? { timezone: proof.timezone } : {}),
          language: 'en-US',
        },
      },
    ],
  });
  await sleep(7000);
  await d.assertOwned();

  const args = [
    app[scan.targetType],
    String(p.lat),
    String(p.lng),
    scan.keyword,
    encodeURIComponent(scan.keyword),
    tagBase,
    point,
    scan.trackAiOverviews ? '1' : '0',
  ].map(shellQuote);

  const detachedUpload = process.env.UNBIND_PROXY_BEFORE_UPLOAD === 'true';
  const putUrl = await uploadURL(tarKey);

  const launchScript = [
    '#!/system/bin/sh',
    `export UPLOAD_URL=${shellQuote(detachedUpload ? '' : putUrl)}`,
    `nohup sh ${RT_DIR}/run_pin.sh ${args.join(' ')} >${RT_DIR}/last_run.log 2>&1 &`,
    'exit 0',
  ].join('\n');

  await pushFileChunked(d.imageId, `${RT_DIR}/launch.sh`, launchScript);
  await duoplusPost('/api/v1/cloudPhone/command', {
    image_id: d.imageId,
    command: `sh ${RT_DIR}/launch.sh`,
  });

  const deadline = Date.now() + 240000;
  await sleep(40000);

  async function waitForUpload() {
    while (!(await uploaded(tarKey))) {
      if (Date.now() >= deadline) throw new Error('R2_UPLOAD_TIMEOUT');
      await sleep(3500);
      await d.assertOwned();
    }
  }

  if (detachedUpload) {
    const folder = `${RT_DIR}/${tag}`;
    let ready = false;
    while (Date.now() < deadline) {
      await d.assertOwned();
      const status = await duoplusPost('/api/v1/cloudPhone/command', {
        image_id: d.imageId,
        command: 'cat ' + shellQuote(folder + '/status'),
      }).catch(() => null);
      if (String(status?.data?.content ?? '').trim() === 'done') {
        ready = true;
        break;
      }
      await sleep(5000);
    }
    if (!ready) throw new Error('DEVICE_BUNDLE_TIMEOUT');
    try {
      await duoplusPost('/api/v1/cloudPhone/update', {
        images: [{ image_id: d.imageId, proxy: { id: '', dns: 2 } }],
      });
      await sleep(2000);
      const file = shellQuote(folder + '.tar.gz');
      const sendScript = [
        '#!/system/bin/sh',
        `nohup sh -c "curl --max-time 90 -fsS -H 'Content-Type: application/gzip' -T ${file} ${shellQuote(putUrl)} && rm -f ${file} && rm -rf ${shellQuote(folder)}" >${RT_DIR}/last_send.log 2>&1 &`,
        'exit 0',
      ].join('\n');
      await pushFileChunked(d.imageId, `${RT_DIR}/send.sh`, sendScript);
      await duoplusPost('/api/v1/cloudPhone/command', {
        image_id: d.imageId,
        command: `sh ${RT_DIR}/send.sh`,
      });
      await waitForUpload();
    } finally {
      await d.assertOwned();
      await duoplusPost('/api/v1/cloudPhone/update', {
        images: [{ image_id: d.imageId, proxy: { id: d.proxyId, dns: 1 } }],
      });
    }
  } else {
    await waitForUpload();
  }

  const dir = await mkdtemp(join(tmpdir(), 'stakeout-pin-'));
  try {
    const b = await readBundle(tarKey, tag, dir);
    if (
      /unusual traffic|not a robot|verify you are human/i.test(
        b.scroll.join(' '),
      )
    )
      throw new Error('FLAG_CAPTCHA_DETECTED');
    if (b.flags.trim()) throw new Error('FLAG_MAP_DRIFT_OR_CHALLENGE');
    if (!b.video.length) throw new Error('EMPTY_VIDEO');

    const result = parsePinXmlBundle(
      b.scroll,
      scan.trackAiOverviews ? b.ai : [],
      scan.targetBusiness,
    );
    const videoUrl = prefix + '/video.mp4',
      metaJsonUrl = prefix + '/meta.json';
    await put(videoUrl, b.video, 'video/mp4');
    await put(
      metaJsonUrl,
      JSON.stringify({
        timestamp: new Date(),
        pinId: p.id,
        keyword: scan.keyword,
        targetBusiness: scan.targetBusiness,
        lat: p.lat,
        lng: p.lng,
        zip: p.zip,
        city: p.city,
        requestedHardware: scan.phoneModel,
        observedModel: d.hardwareModel,
        proxyCheck: proof,
        videoKey: videoUrl,
        rawKey: tarKey,
        ...result,
      }),
      'application/json',
    );
    return {
      assignedDeviceId: d.imageId,
      rankFound: result.targetRank,
      matchedDumpIdx: result.matchedDumpIndex,
      topCompetitors: result.orderedBusinesses,
      aiCitations: result.aiCitations,
      hasAiOverview: result.hasAiOverview,
      aiOverviewText: result.aiOverviewText,
      targetCitedInAi: result.targetCitedInAi,
      proxyMeta: proof,
      videoUrl,
      dumpsTarUrl: tarKey,
      metaJsonUrl,
    };
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
