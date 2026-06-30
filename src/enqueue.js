import { Queue } from "bullmq";
import IORedis from "ioredis";
import path from "node:path";
import { config } from "./config.js";

const [videoId, sourcePathArg] = process.argv.slice(2);
if (!videoId || !sourcePathArg) {
  console.error("Uso: npm run enqueue -- <videoId> <sourcePath> [metadataJson]");
  process.exit(1);
}
const metadata = process.argv[4] ? JSON.parse(process.argv[4]) : {};

const connection = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });
const queue = new Queue(config.queueName, { connection });

try {
  const sourcePath = path.resolve(sourcePathArg);
  const job = await queue.add("transcode-hls", { videoId, sourcePath, ...metadata }, {
    jobId: `video-${videoId}`,
    attempts: 3,
    backoff: { type: "exponential", delay: 30_000 },
    removeOnComplete: { age: 7 * 24 * 3600, count: 1000 },
    removeOnFail: { age: 30 * 24 * 3600, count: 5000 }
  });
  console.log(JSON.stringify({ queued: true, jobId: job.id, videoId, sourcePath }));
} finally {
  await queue.close();
  await connection.quit();
}
