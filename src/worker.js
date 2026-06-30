import { spawn } from "node:child_process";
import crypto from "node:crypto";
import {
  access,
  mkdir,
  rename,
  rm,
  stat,
  unlink
} from "node:fs/promises";
import path from "node:path";
import { Queue, Worker } from "bullmq";
import IORedis from "ioredis";
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
  const sourcePath = pathInside(config.inputRoot, job.data?.sourcePath);
  const finalDir = pathInside(config.outputRoot, videoId);
  const tempDir = pathInside(config.outputRoot, `.tmp-${videoId}-${job.id}`);

  const sourceStat = await stat(sourcePath);
  if (!sourceStat.isFile()) throw new Error("sourcePath non è un file");
  if (path.extname(sourcePath).toLowerCase() !== ".mp4") {
    throw new Error("Sono accettati solo file sorgente .mp4");
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

    const masterUrl = `${config.publicBaseUrl}/${encodeURIComponent(videoId)}/master.m3u8`;
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

    await job.updateProgress({ phase: "completed", percent: 100 });
    log("info", "Transcodifica completata", { jobId: job.id, videoId, masterUrl });
    return payload;
  } catch (error) {
    await rm(tempDir, { recursive: true, force: true });
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
