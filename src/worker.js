import { spawn } from "node:child_process";
import crypto from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import {
  access,
  readdir,
  mkdir,
  rename,
  rm,
  stat,
  unlink
} from "node:fs/promises";
import path from "node:path";
import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { Queue, Worker } from "bullmq";
import IORedis from "ioredis";
import { pipeline } from "node:stream/promises";
import { config } from "./config.js";

const RENDITIONS = [
  { name: "1080p", width: 1920, height: 1080, videoRate: "5000k", maxRate: "5350k", buffer: "7500k", audioRate: "192k" },
  { name: "720p", width: 1280, height: 720, videoRate: "2800k", maxRate: "2996k", buffer: "4200k", audioRate: "128k" },
  { name: "480p", width: 854, height: 480, videoRate: "1400k", maxRate: "1498k", buffer: "2100k", audioRate: "96k" }
];

const connection = new IORedis(config.redisUrl, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false
});
const queue = new Queue(config.queueName, { connection });

// Vale anche se, per errore, vengono avviate più istanze del servizio.
await queue.setGlobalConcurrency(1);

const r2Enabled = Boolean(
  config.r2.endpoint &&
  config.r2.accessKeyId &&
  config.r2.secretAccessKey &&
  config.r2.bucket &&
  config.r2.publicUrl
);

const r2 = r2Enabled
  ? new S3Client({
      region: "auto",
      endpoint: config.r2.endpoint,
      credentials: {
        accessKeyId: config.r2.accessKeyId,
        secretAccessKey: config.r2.secretAccessKey
      }
    })
  : null;

function log(level, message, fields = {}) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    ...fields
  }));
}

function safeId(value) {
  const id = String(value ?? "");
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(id)) {
    throw new Error("videoId non valido: usa solo lettere, numeri, trattino e underscore");
  }
  return id;
}

function pathInside(root, candidate) {
  const absolute = path.resolve(candidate);
  const relative = path.relative(root, absolute);
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`Percorso fuori dalla root consentita: ${absolute}`);
  }
  return absolute;
}

function run(command, args, { onStderr } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderrTail = "";

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.on("data", chunk => {
      stderrTail = (stderrTail + chunk).slice(-16_000);
      onStderr?.(chunk);
    });
    child.once("error", reject);
    child.once("close", (code, signal) => {
      if (code === 0) return resolve({ stdout, stderr: stderrTail });
      reject(new Error(
        `${path.basename(command)} terminato con code=${code} signal=${signal ?? "none"}\n${stderrTail}`
      ));
    });
  });
}

async function probeVideo(sourcePath) {
  const { stdout } = await run(config.ffprobePath, [
    "-v", "error",
    "-show_entries", "format=duration:stream=index,codec_type,width,height",
    "-of", "json",
    sourcePath
  ]);
  const data = JSON.parse(stdout);
  const video = data.streams?.find(stream => stream.codec_type === "video");
  if (!video) throw new Error("Il file sorgente non contiene uno stream video");

  return {
    duration: Number(data.format?.duration ?? 0),
    width: Number(video.width ?? 0),
    height: Number(video.height ?? 0),
    hasAudio: data.streams?.some(stream => stream.codec_type === "audio") ?? false
  };
}

function buildFfmpegArgs(sourcePath, outputDir, hasAudio) {
  const splitOutputs = RENDITIONS.map((_, index) => `[v${index}]`).join("");
  const filters = [`[0:v:0]split=${RENDITIONS.length}${splitOutputs}`];

  RENDITIONS.forEach((rendition, index) => {
    filters.push(
      `[v${index}]scale=w=${rendition.width}:h=${rendition.height}:` +
      "force_original_aspect_ratio=decrease:flags=lanczos," +
      `pad=${rendition.width}:${rendition.height}:(ow-iw)/2:(oh-ih)/2:color=black,` +
      `setsar=1[vout${index}]`
    );
  });

  const args = [
    "-hide_banner",
    "-nostdin",
    "-y",
    "-i", sourcePath,
    "-filter_complex_threads", "1",
    "-filter_complex", filters.join(";")
  ];

  RENDITIONS.forEach((_, index) => {
    args.push("-map", `[vout${index}]`);
    if (hasAudio) args.push("-map", "0:a:0");
  });

  args.push(
    "-c:v", "libx264",
    "-preset", config.ffmpegPreset,
    "-profile:v", "high",
    "-pix_fmt", "yuv420p",
    "-sc_threshold", "0",
    "-force_key_frames", `expr:gte(t,n_forced*${config.segmentSeconds})`
  );

  RENDITIONS.forEach((rendition, index) => {
    args.push(
      `-threads:v:${index}`, String(config.videoThreads),
      `-b:v:${index}`, rendition.videoRate,
      `-maxrate:v:${index}`, rendition.maxRate,
      `-bufsize:v:${index}`, rendition.buffer
    );
  });

  if (hasAudio) {
    args.push("-c:a", "aac", "-ar", "48000", "-ac", "2");
    RENDITIONS.forEach((rendition, index) => {
      args.push(`-b:a:${index}`, rendition.audioRate);
    });
  }

  const streamMap = RENDITIONS.map((rendition, index) => (
    hasAudio
      ? `v:${index},a:${index},name:${rendition.name}`
      : `v:${index},name:${rendition.name}`
  )).join(" ");

  args.push(
    "-f", "hls",
    "-hls_time", String(config.segmentSeconds),
    "-hls_playlist_type", "vod",
    "-hls_list_size", "0",
    "-hls_flags", "independent_segments+temp_file",
    "-hls_segment_type", "mpegts",
    "-master_pl_name", "master.m3u8",
    "-var_stream_map", streamMap,
    "-hls_segment_filename", path.join(outputDir, "%v", "segment_%06d.ts"),
    path.join(outputDir, "%v", "index.m3u8")
  );

  return args;
}

async function transcode(sourcePath, tempDir, hasAudio, job) {
  await Promise.all(RENDITIONS.map(({ name }) => mkdir(path.join(tempDir, name), { recursive: true })));
  let lastProgressUpdate = 0;

  await run(config.ffmpegPath, buildFfmpegArgs(sourcePath, tempDir, hasAudio), {
    onStderr(chunk) {
      const now = Date.now();
      if (now - lastProgressUpdate < 5000) return;
      const match = chunk.match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/);
      if (!match) return;
      lastProgressUpdate = now;
      const seconds = Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
      void job.updateProgress({ phase: "transcoding", seconds }).catch(() => {});
    }
  });

  await access(path.join(tempDir, "master.m3u8"));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function r2Key(relativePath) {
  const normalized = relativePath.replace(/^\/+/, "");
  return config.r2.prefix ? `${config.r2.prefix}/${normalized}` : normalized;
}

function stripR2Prefix(objectKey) {
  const normalized = String(objectKey ?? "").replace(/^\/+/, "");
  const prefix = config.r2.prefix ? `${config.r2.prefix}/` : "";
  return prefix && normalized.startsWith(prefix)
    ? normalized.slice(prefix.length)
    : normalized;
}

function contentTypeFor(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  if (extension === ".m3u8") return "application/vnd.apple.mpegurl";
  if (extension === ".ts") return "video/mp2t";
  return "application/octet-stream";
}

async function listFiles(root, current = root) {
  const entries = await readdir(current, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const absolute = path.join(current, entry.name);
    if (entry.isDirectory()) files.push(...await listFiles(root, absolute));
    else if (entry.isFile()) files.push(absolute);
  }
  return files;
}

async function uploadHlsToR2(videoId, finalDir) {
  if (!r2) return null;

  const files = await listFiles(finalDir);
  for (const file of files) {
    const relative = path.relative(finalDir, file).split(path.sep).join("/");
    const key = r2Key(`hls/${videoId}/${relative}`);
    await r2.send(new PutObjectCommand({
      Bucket: config.r2.bucket,
      Key: key,
      Body: createReadStream(file),
      ContentType: contentTypeFor(file),
      CacheControl: file.endsWith(".m3u8")
        ? "public, max-age=60"
        : "public, max-age=31536000, immutable"
    }));
  }

  const masterPath = path.join(finalDir, "master.m3u8");
  await r2.send(new PutObjectCommand({
    Bucket: config.r2.bucket,
    Key: r2Key(`master.m3u8/${videoId}.m3u8`),
    Body: createReadStream(masterPath),
    ContentType: "application/vnd.apple.mpegurl",
    CacheControl: "public, max-age=60"
  }));

  return `${config.r2.publicUrl}/${r2Key(`hls/${videoId}/master.m3u8`)}`;
}

async function resolveSourcePath(job, videoId) {
  const sourceObjectKey = job.data?.sourceObjectKey;
  if (sourceObjectKey && r2) {
    const relativeObjectKey = stripR2Prefix(sourceObjectKey);
    const extension = path.extname(relativeObjectKey) || ".mp4";
    const localSource = pathInside(config.inputRoot, path.join(".r2-cache", `${videoId}${extension}`));
    await mkdir(path.dirname(localSource), { recursive: true });
    await job.updateProgress({ phase: "downloading-r2-source" });
    const response = await r2.send(new GetObjectCommand({
      Bucket: config.r2.bucket,
      Key: sourceObjectKey
    }));
    if (!response.Body) throw new Error("Oggetto R2 sorgente vuoto");
    await pipeline(response.Body, createWriteStream(localSource));
    return localSource;
  }

  if (sourceObjectKey && !r2) {
    throw new Error("Sorgente su R2 ma configurazione R2 worker non disponibile");
  }

  return pathInside(config.inputRoot, job.data?.sourcePath);
}

async function postWebhook(payload) {
  const body = JSON.stringify(payload);
  const signature = config.webhookSecret
    ? crypto.createHmac("sha256", config.webhookSecret).update(body).digest("hex")
    : "";

  let lastError;
  for (let attempt = 1; attempt <= config.webhookMaxAttempts; attempt += 1) {
    try {
      const response = await fetch(config.webhookUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "user-agent": "TVMIX-WORKER/1.0",
          "x-tvmix-signature": signature,
          "x-tvmix-event": "video.ready"
        },
        body,
        signal: AbortSignal.timeout(config.webhookTimeoutMs)
      });
      if (!response.ok) {
        const responseBody = (await response.text()).slice(0, 1000);
        throw new Error(`Webhook HTTP ${response.status}: ${responseBody}`);
      }
      return;
    } catch (error) {
      lastError = error;
      if (attempt < config.webhookMaxAttempts) {
        await sleep(config.webhookRetryBaseMs * (2 ** (attempt - 1)));
      }
    }
  }
  throw lastError;
}

async function processVideo(job) {
  const videoId = safeId(job.data?.videoId);
  const sourcePath = await resolveSourcePath(job, videoId);
  const finalDir = pathInside(config.outputRoot, videoId);
  const tempDir = pathInside(config.outputRoot, `.tmp-${videoId}-${job.id}`);

  const sourceStat = await stat(sourcePath);
  if (!sourceStat.isFile()) throw new Error("sourcePath non è un file");
  if (![".mp4", ".mov", ".mkv"].includes(path.extname(sourcePath).toLowerCase())) {
    throw new Error("Sono accettati solo file sorgente MP4, MOV o MKV");
  }

  log("info", "Transcodifica iniziata", { jobId: job.id, videoId, sourcePath });
  await job.updateProgress({ phase: "probing" });
  const metadata = await probeVideo(sourcePath);

  await rm(tempDir, { recursive: true, force: true });
  await mkdir(tempDir, { recursive: true });

  try {
    await transcode(sourcePath, tempDir, metadata.hasAudio, job);
    await rm(finalDir, { recursive: true, force: true });
    await rename(tempDir, finalDir);

    await job.updateProgress({ phase: "uploading-r2" });
    const r2MasterUrl = await uploadHlsToR2(videoId, finalDir);
    const masterUrl = r2MasterUrl ?? `${config.publicBaseUrl}/${encodeURIComponent(videoId)}/master.m3u8`;
    const payload = {
      event: "video.ready",
      videoId,
      status: "ready",
      masterUrl,
      title: job.data?.title,
      slug: job.data?.slug,
      description: job.data?.description,
      thumbnailUrl: job.data?.thumbnailUrl,
      categorySlug: job.data?.categorySlug,
      categoryName: job.data?.categoryName,
      durationSeconds: metadata.duration,
      source: {
        width: metadata.width,
        height: metadata.height,
        hasAudio: metadata.hasAudio
      },
      renditions: RENDITIONS.map(({ name, width, height }) => ({ name, width, height })),
      completedAt: new Date().toISOString(),
      worker: "TVMIX-WORKER"
    };

    await job.updateProgress({ phase: "webhook" });
    await postWebhook(payload);

    if (config.deleteSourceAfterSuccess) {
      await unlink(sourcePath);
    }
    if (job.data?.sourceObjectKey) {
      await unlink(sourcePath).catch(() => {});
    }

    await job.updateProgress({ phase: "completed", percent: 100 });
    log("info", "Transcodifica completata", { jobId: job.id, videoId, masterUrl });
    return payload;
  } catch (error) {
    await rm(tempDir, { recursive: true, force: true });
    if (job.data?.sourceObjectKey) {
      await unlink(sourcePath).catch(() => {});
    }
    throw error;
  }
}

const worker = new Worker(config.queueName, processVideo, {
  connection,
  concurrency: 1,
  lockDuration: 30 * 60 * 1000,
  stalledInterval: 60 * 1000,
  maxStalledCount: 2
});

worker.on("ready", () => {
  log("info", "Worker pronto", {
    queue: config.queueName,
    concurrency: 1,
    architecture: process.arch,
    videoThreadsPerRendition: config.videoThreads
  });
});

worker.on("failed", (job, error) => {
  log("error", "Job fallito", {
    jobId: job?.id,
    videoId: job?.data?.videoId,
    error: error.message
  });
});

worker.on("error", error => {
  log("error", "Errore BullMQ", { error: error.message });
});

async function shutdown(signal) {
  log("info", "Arresto controllato", { signal });
  await worker.close();
  await queue.close();
  await connection.quit();
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
