#!/usr/bin/env python3
import argparse
from pathlib import Path
from pydub import AudioSegment
from mutagen.wave import WAVE
import logging
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table

console = Console()

def setup_logging(log_file):
    """Richtet das Logging in eine Datei ein."""
    # Stellt sicher, dass vorherige Handler entfernt werden, um doppeltes Logging zu vermeiden
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def read_wav_tags(wav_file):
    """Liest Metadaten aus einer WAV-Datei und gibt sie als Dictionary zurück,
    das für pydub verständlich ist."""
    try:
        wav_meta = WAVE(wav_file)
        if not wav_meta or not wav_meta.tags:
            logging.info(f"Keine Tags in {wav_file.name} gefunden.")
            return {}

        tags = wav_meta.tags
        pydub_tags = {}

        # Mappt die in den WAV-Dateien gefundenen Tag-Bezeichner 
        # auf die von pydub erwarteten Keys.
        tag_map = {
            "TIT2": "title",        # Titel
            "TPE1": "artist",       # Künstler
            "TALB": "album",        # Album
            "TDRC": "date",         # Jahr/Datum
            "TCON": "genre",        # Genre
            "TRCK": "tracknumber",  # Tracknummer
            "TPE2": "albumartist",  # Album-Künstler
            "COMM": "comment",      # Kommentar
        }

        for wav_key, pydub_key in tag_map.items():
            if wav_key in tags:
                # Mutagen liefert Werte als Liste, wir nehmen das erste Element
                # und konvertieren es sicher in einen String.
                value = str(tags[wav_key][0])
                pydub_tags[pydub_key] = value
        
        if pydub_tags:
            logging.info(f"Gelesene Tags aus {wav_file.name}: {pydub_tags}")
        else:
            logging.info(f"Keine passenden Tags in {wav_file.name} gefunden.")
        return pydub_tags
    except Exception as e:
        logging.warning(f"Tags konnten nicht gelesen werden für {wav_file.name}: {e}")
        return {}

def convert_wav_to_mp3(src_dir, dst_dir, bitrate):
    """Konvertiert WAV-Dateien in MP3, behält die Ordnerstruktur bei und
    übernimmt die Metadaten."""
    src_path = Path(src_dir)
    dst_path = Path(dst_dir)

    if not src_path.exists():
        console.print(f"[red]Fehler:[/red] Quellverzeichnis {src_dir} existiert nicht.")
        return
    
    dst_path.mkdir(parents=True, exist_ok=True)

    log_file = dst_path / "conversion.log"
    setup_logging(log_file)
    logging.info("=============================================")
    logging.info("Starte neue Konvertierung")
    logging.info(f"Quelle: {src_path}")
    logging.info(f"Ziel: {dst_path}")
    logging.info(f"Bitrate: {bitrate}")

    wav_files = list(src_path.rglob("*.wav"))
    if not wav_files:
        console.print("[yellow]Keine WAV-Dateien gefunden.[/yellow]")
        return

    console.print(f"[cyan]Gefundene WAV-Dateien:[/cyan] {len(wav_files)}")
    console.print(f"[cyan]Bitrate:[/cyan] {bitrate}\n")

    converted_count = 0
    skipped_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
        transient=True
    ) as progress:
        task = progress.add_task("[green]Konvertiere...", total=len(wav_files))

        for wav_file in wav_files:
            relative_path = wav_file.relative_to(src_path)
            mp3_file = dst_path / relative_path.with_suffix(".mp3")

            mp3_file.parent.mkdir(parents=True, exist_ok=True)

            if mp3_file.exists():
                logging.info(f"Übersprungen (bereits vorhanden): {mp3_file.name}")
                skipped_count += 1
                progress.advance(task)
                continue

            try:
                # 1. Tags aus der WAV-Datei lesen
                tags_to_apply = read_wav_tags(wav_file)

                # 2. WAV laden und mit Tags nach MP3 exportieren
                audio = AudioSegment.from_wav(wav_file)
                audio.export(
                    mp3_file,
                    format="mp3",
                    bitrate=bitrate,
                    tags=tags_to_apply
                )
                
                logging.info(f"Konvertiert: {wav_file.name} -> {mp3_file.name}")
                converted_count += 1
            except Exception as e:
                logging.error(f"Fehler bei der Konvertierung von {wav_file.name}: {e}")
            
            progress.advance(task)

    # Übersichtliche Tabelle mit dem Ergebnis
    table = Table(title="Konvertierung abgeschlossen")
    table.add_column("Status", justify="left", style="cyan")
    table.add_column("Anzahl", justify="right", style="magenta")
    table.add_row("Neue MP3s", str(converted_count))
    table.add_row("Übersprungen", str(skipped_count))
    console.print(table)

    logging.info(f"Konvertierung abgeschlossen! Neue: {converted_count}, Übersprungen: {skipped_count}")
    logging.info("=============================================\n")

def main():
    parser = argparse.ArgumentParser(description="Konvertiere WAV-Dateien in MP3 und behalte Verzeichnisstruktur bei.")
    parser.add_argument("src", help="Pfad zum Quellverzeichnis (z.B. /home/peter/wav)")
    parser.add_argument("dst", help="Pfad zum Zielverzeichnis (z.B. /home/peter/mp3)")
    parser.add_argument("bitrate", help="Bitrate für MP3 (z.B. 192k, 256k, 320k)")

    args = parser.parse_args()
    convert_wav_to_mp3(args.src, args.dst, args.bitrate)

if __name__ == "__main__":
    main()