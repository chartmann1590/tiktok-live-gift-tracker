import os
import threading
import queue

from faster_whisper import WhisperModel


class TranscriptionWorker:

    def __init__(self):
        model_size = os.environ.get("WHISPER_MODEL", "base")
        device = os.environ.get("WHISPER_DEVICE", "cuda")

        self._model = None
        loaded = False
        for dev, ct in [("cuda", "int8"), ("cpu", "int8")]:
            if dev == "cpu" and device != "cpu":
                continue
            try:
                print(f"[*] Loading Whisper model '{model_size}' on {dev} ({ct})...")
                self._model = WhisperModel(model_size, device=dev, compute_type=ct)
                loaded = True
                print(f"[*] Whisper model loaded on {dev}.")
                break
            except Exception as e:
                print(f"[!] {dev}/{ct} failed ({e}), trying next option...")
        if not loaded:
            raise RuntimeError("Failed to load Whisper model on any device")

        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._process_loop, daemon=True)
        self._thread.start()

    def submit(self, audio_path, username, stream_id, chunk_index, callback):
        self._queue.put((audio_path, username, stream_id, chunk_index, callback))

    def stop(self):
        self._queue.put(None)

    def _process_loop(self):
        while True:
            item = self._queue.get()
            if item is None:
                break

            audio_path, username, stream_id, chunk_index, callback = item
            try:
                segments, info = self._model.transcribe(
                    audio_path,
                    beam_size=5,
                    vad_filter=True,
                )
                text = " ".join(seg.text.strip() for seg in segments).strip()
                detected_language = getattr(info, "language", None) or "unknown"

                if text:
                    callback(username, stream_id, text, detected_language, chunk_index)
            except Exception:
                pass
            finally:
                try:
                    os.remove(audio_path)
                except OSError:
                    pass
