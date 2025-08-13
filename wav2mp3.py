#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from mutagen.wave import WAVE
from pydub import AudioSegment
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table

console = Console()

def setup_logging(log_file):
    """Richtet das Logging in eine Datei ein."""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def read_wav_tags(wav_file):
    """Liest Metadaten aus einer WAV-Datei und gibt sie als pydub-kompatibles Dictionary zurück."""
    try:
        wav_meta = WAVE(wav_file)
        if not wav_meta or not wav_meta.tags:
            return {}

        tag_map = {
            "TIT2": "title", "TPE1": "artist", "TALB": "album", "TDRC": "date",
            "TCON": "genre", "TRCK": "tracknumber", "TPE2": "albumartist", "COMM": "comment",
        }
        
        pydub_tags = {
            pydub_key: str(wav_meta.tags[wav_key][0])
            for wav_key, pydub_key in tag_map.items() if wav_key in wav_meta.tags
        }

        if pydub_tags:
            logging.info(f"Gelesene Tags aus {wav_file.name}: {pydub_tags}")
        return pydub_tags
    except Exception as e:
        logging.warning(f"Tags konnten nicht gelesen werden für {wav_file.name}: {e}")
        return {}

def process_file(wav_file, src_path, dst_path, bitrate):
    """Verarbeitet eine einzelne WAV-Datei: konvertiert sie zu MP3 und kopiert Tags."""
    try:
        relative_path = wav_file.relative_to(src_path)
        mp3_file = dst_path / relative_path.with_suffix(".mp3")
        mp3_file.parent.mkdir(parents=True, exist_ok=True)

        if mp3_file.exists():
            logging.info(f"Übersprungen (bereits vorhanden): {mp3_file.name}")
            return "skipped"

        tags_to_apply = read_wav_tags(wav_file)
        audio = AudioSegment.from_wav(wav_file)
        audio.export(mp3_file, format="mp3", bitrate=bitrate, tags=tags_to_apply)
        
        logging.info(f"Konvertiert: {wav_file.name} -> {mp3_file.name}")
        return "converted"
    except Exception as e:
        logging.error(f"Fehler bei der Konvertierung von {wav_file.name}: {e}")
        return "error"

def convert_wav_to_mp3(src_dir, dst_dir, bitrate):
    """Konvertiert WAV-Dateien parallel in MP3, behält die Ordnerstruktur bei und übernimmt Metadaten."""
    src_path = Path(src_dir)
    dst_path = Path(dst_dir)

    if not src_path.exists():
        console.print(f"[red]Fehler:[/red] Quellverzeichnis {src_dir} existiert nicht.")
        return

    dst_path.mkdir(parents=True, exist_ok=True)
    log_file = dst_path / "conversion.log"
    setup_logging(log_file)
    logging.info("=============================================")
    logging.info(f"Starte parallele Konvertierung (Quelle: {src_path}, Ziel: {dst_path}, Bitrate: {bitrate})")

    wav_files = list(src_path.rglob("*.wav"))
    if not wav_files:
        console.print("[yellow]Keine WAV-Dateien gefunden.[/yellow]")
        return

    console.print(f"[cyan]Gefundene WAV-Dateien:[/cyan] {len(wav_files)}\n")
    
    results = {"converted": 0, "skipped": 0, "error": 0}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console
    ) as progress:
        task = progress.add_task("[green]Konvertiere...", total=len(wav_files))
        
        with ThreadPoolExecutor() as executor:
            # Erstellt für jede Datei einen Future
            futures = {executor.submit(process_file, wav, src_path, dst_path, bitrate): wav for wav in wav_files}
            
            for future in as_completed(futures):
                status = future.result()
                results[status] += 1
                progress.advance(task)

    # Ergebnis-Tabelle
    table = Table(title="Konvertierung abgeschlossen")
    table.add_column("Status", justify="left", style="cyan")
    table.add_column("Anzahl", justify="right", style="magenta")
    table.add_row("Neue MP3s", str(results["converted"]))
    table.add_row("Übersprungen", str(results["skipped"]))
    if results["error"] > 0:
        table.add_row("[red]Fehler[/red]", str(results["error"]))
    console.print(table)

    logging.info(f"Konvertierung abgeschlossen! Neue: {results['converted']}, Übersprungen: {results['skipped']}, Fehler: {results['error']}")
    logging.info("=============================================\n")

def main():
    parser = argparse.ArgumentParser(description="Konvertiert WAV-Dateien parallel in MP3.")
    parser.add_argument("src", help="Pfad zum Quellverzeichnis")
    parser.add_argument("dst", help="Pfad zum Zielverzeichnis")
    parser.add_argument("bitrate", help="Bitrate für MP3 (z.B. 320k)")
    args = parser.parse_args()
    convert_wav_to_mp3(args.src, args.dst, args.bitrate)

if __name__ == "__main__":
    main()