import "dotenv/config";
import path from "node:path";

function integer(name, fallback, min = 1) {
  const value = Number.parseInt(process.env[name] ?? String(fallback), 10);
  if (!Number.isInteger(value) || value < min) {
    throw new Error(`${name} deve essere un intero >= ${min}`);
  }
  return value;
}

function boolean(name, fallback = false) {
  const value = (process.env[name] ?? String(fallback)).toLowerCase();
  return ["1", "true", "yes", "on"].includes(value);
}

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`Variabile d'ambiente obbligatoria mancante: ${name}`);
  return value;
}

function optional(name) {
  const value = process.env[name]?.trim();
  return value || "";
}

export const config = Object.freeze({
  redisUrl: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  queueName: process.env.QUEUE_NAME ?? "tvmix-video-transcoding",
  inputRoot: path.resolve(process.env.INPUT_ROOT ?? "/srv/tvmix/uploads"),
  outputRoot: path.resolve(process.env.OUTPUT_ROOT ?? "/srv/tvmix/hls"),
  publicBaseUrl: required("HLS_PUBLIC_BASE_URL").replace(/\/+$/, ""),
  webhookUrl: required("WEBHOOK_URL"),
  webhookSecret: process.env.WEBHOOK_SECRET ?? "",
  webhookTimeoutMs: integer("WEBHOOK_TIMEOUT_MS", 15_000),
  webhookMaxAttempts: integer("WEBHOOK_MAX_ATTEMPTS", 5),
  webhookRetryBaseMs: integer("WEBHOOK_RETRY_BASE_MS", 1_000),
  ffmpegPath: process.env.FFMPEG_PATH ?? "/usr/bin/ffmpeg",
  ffprobePath: process.env.FFPROBE_PATH ?? "/usr/bin/ffprobe",
  videoThreads: integer("FFMPEG_VIDEO_THREADS", 1),
  ffmpegPreset: process.env.FFMPEG_PRESET ?? "veryfast",
  segmentSeconds: integer("HLS_SEGMENT_SECONDS", 6, 2),
  deleteSourceAfterSuccess: boolean("DELETE_SOURCE_AFTER_SUCCESS"),
  r2: {
    endpoint: optional("R2_ENDPOINT"),
    accessKeyId: optional("R2_ACCESS_KEY_ID"),
    secretAccessKey: optional("R2_SECRET_ACCESS_KEY"),
    bucket: optional("R2_BUCKET"),
    prefix: (process.env.R2_PREFIX ?? "tvmix-media").trim().replace(/^\/+|\/+$/g, ""),
    publicUrl: optional("R2_PUBLIC_URL").replace(/\/+$/, "")
  }
});
