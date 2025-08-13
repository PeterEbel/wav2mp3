#!/usr/bin/env python3
from mutagen.wave import WAVE
import sys

def show_wav_tags(file_path):
    try:
        wav_meta = WAVE(file_path)
        if not wav_meta.tags:
            print(f"Keine Tags gefunden in: {file_path}")
            return

        print(f"Tags für {file_path}:")
        for key, value in wav_meta.tags.items():
            print(f"  {key}: {value}")
    except Exception as e:
        print(f"Fehler beim Lesen von {file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Verwendung: check_wav_tags.py <wav_datei>")
        sys.exit(1)

    file_path = sys.argv[1]
    show_wav_tags(file_path)
