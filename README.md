# TVMIX-WORKER

Worker Node.js per convertire file MP4 in HLS adattivo 1080p, 720p e 480p.
È pensato per Hetzner CAX21 ARM64 (4 vCPU): BullMQ esegue un solo video per
volta e ciascuna delle tre rendition x264 usa, per default, un thread.

## Requisiti

- Ubuntu ARM64
- Node.js 20 o successivo
- Redis 7
- FFmpeg con `libx264` e `aac`

Installazione tipica:

```bash
sudo apt update
sudo apt install -y ffmpeg redis-server
node --version
ffmpeg -hide_banner -encoders | grep libx264
```

## Configurazione

```bash
cd /opt/tvmix-worker
npm install --omit=dev
cp .env.example .env
nano .env
sudo mkdir -p /srv/tvmix/uploads /srv/tvmix/hls
sudo chown -R tvmix:tvmix /srv/tvmix/uploads /srv/tvmix/hls /opt/tvmix-worker
```

Avvio manuale:

```bash
npm start
```

Avvio con systemd:

```bash
sudo cp deploy/tvmix-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tvmix-worker
sudo journalctl -u tvmix-worker -f
```

## Inserire un job in coda

Dal backend si deve aggiungere alla coda `tvmix-video-transcoding` un job con:

```json
{
  "videoId": "abc123",
  "sourcePath": "/srv/tvmix/uploads/abc123.mp4"
}
```

Per una prova locale:

```bash
npm run enqueue -- abc123 /srv/tvmix/uploads/abc123.mp4
```

L'output viene pubblicato in:

```text
/srv/tvmix/hls/abc123/master.m3u8
/srv/tvmix/hls/abc123/1080p/index.m3u8
/srv/tvmix/hls/abc123/1080p/segment_000000.ts
/srv/tvmix/hls/abc123/720p/index.m3u8
/srv/tvmix/hls/abc123/480p/index.m3u8
```

## Webhook

Al termine il worker invia `POST WEBHOOK_URL` con un payload simile:

```json
{
  "event": "video.ready",
  "videoId": "abc123",
  "status": "ready",
  "masterUrl": "https://media.tvmix.it/hls/abc123/master.m3u8",
  "durationSeconds": 125.4,
  "completedAt": "2026-06-21T12:00:00.000Z",
  "worker": "TVMIX-WORKER"
}
```

Se `WEBHOOK_SECRET` è configurato, `x-tvmix-signature` contiene
un HMAC-SHA256 del body grezzo, con `WEBHOOK_SECRET` come chiave, in formato
esadecimale.
Il backend deve verificare la firma sul body grezzo prima di decodificare JSON.

## Note operative

- Montare o sincronizzare gli upload del backend sotto `INPUT_ROOT`.
- Servire `OUTPUT_ROOT` tramite Nginx/CDN con MIME type per `.m3u8` e `.ts`.
- Il build FFmpeg di Ubuntu ARM64 usa automaticamente le ottimizzazioni SIMD
  disponibili; non occorre impostare flag CPU manuali.
- Per più velocità, provare `FFMPEG_PRESET=superfast`. Per limitare ulteriormente
  il carico, lasciare `FFMPEG_VIDEO_THREADS=1`.
