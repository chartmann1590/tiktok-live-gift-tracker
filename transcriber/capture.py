import os
import subprocess
import threading
import time
import tempfile
import wave
import shutil

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2
CHANNELS = 1


class AudioCaptureManager:

    def __init__(self, username, stream_url, stream_id, worker, on_transcript):
        self.username = username
        self.stream_url = stream_url
        self.stream_id = stream_id
        self._worker = worker
        self._on_transcript = on_transcript

        self._chunk_seconds = int(os.environ.get("TRANSCRIPT_CHUNK_SECONDS", "30"))
        self._chunk_bytes = SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS * self._chunk_seconds
        self._temp_dir = tempfile.mkdtemp(prefix=f"tts_{username}_")
        self._process = None
        self._stopping = False
        self._thread = None
        self._chunk_index = 0

    def start(self):
        self._stopping = False
        self._thread = threading.Thread(target=self._capture_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stopping = True
        self._kill_ffmpeg()
        try:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
        except Exception:
            pass

    def _kill_ffmpeg(self):
        if self._process and self._process.poll() is None:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
        self._process = None

    def _capture_loop(self):
        while not self._stopping:
            cmd = [
                "ffmpeg",
                "-y",
                "-loglevel", "error",
                "-i", self.stream_url,
                "-vn",
                "-acodec", "pcm_s16le",
                "-ar", str(SAMPLE_RATE),
                "-ac", str(CHANNELS),
                "-f", "s16le",
                "pipe:1",
            ]

            try:
                self._process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )

                while not self._stopping:
                    audio_data = self._read_exact(self._process.stdout, self._chunk_bytes)
                    if not audio_data or len(audio_data) < SAMPLE_RATE * BYTES_PER_SAMPLE:
                        break

                    chunk_path = os.path.join(
                        self._temp_dir, f"chunk_{self._chunk_index:06d}.wav"
                    )
                    self._write_wav(chunk_path, audio_data)

                    idx = self._chunk_index
                    self._chunk_index += 1
                    self._worker.submit(
                        chunk_path,
                        self.username,
                        self.stream_id,
                        idx,
                        self._on_transcript,
                    )

                    self._cleanup_old_chunks()

            except Exception:
                pass

            self._process = None
            if self._stopping:
                break
            time.sleep(5)

    def _read_exact(self, pipe, nbytes):
        data = bytearray()
        remaining = nbytes
        while remaining > 0:
            chunk = pipe.read(min(remaining, 65536))
            if not chunk:
                break
            data.extend(chunk)
            remaining -= len(chunk)
        return bytes(data) if data else b""

    def _write_wav(self, path, raw_pcm):
        with wave.open(path, "wb") as wf:
            wf.setnchannels(CHANNELS)
            wf.setsampwidth(BYTES_PER_SAMPLE)
            wf.setframerate(SAMPLE_RATE)
            wf.writeframes(raw_pcm)

    def _cleanup_old_chunks(self):
        try:
            entries = sorted(os.listdir(self._temp_dir))
            while len(entries) > 10:
                old = os.path.join(self._temp_dir, entries.pop(0))
                try:
                    os.remove(old)
                except OSError:
                    break
        except Exception:
            pass
