#!/usr/bin/env python3
import argparse
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from pydub import AudioSegment
from mutagen.wave import WAVE
from mutagen.mp3 import MP3
from mutagen.id3 import (
    ID3, ID3NoHeaderError,
    TIT2, TALB, TPE1, TPE2, TCON, TRCK, TPOS, TYER, TDRC, TCOM, TSSE, TXXX, COMM, APIC
)

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table
from rich.panel import Panel

console = Console()

# ------------------ Logging ------------------ #
def setup_logging(log_file: Path):
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

# ------------------ Tag Utilities ------------------ #
def _clone_id3_frame_to(mp3_tags: ID3, key: str, frame) -> bool:
    """
    Klont gebräuchliche ID3-Frames aus einer WAV-ID3-Quelle in ein neues ID3-Objekt.
    Gibt True zurück, wenn ein Frame kopiert wurde, sonst False.
    """
    try:
        # Textframes (haben Attribut .text)
        if hasattr(frame, "text"):
            text = frame.text
            cls = frame.__class__
            mp3_tags.add(cls(encoding=3, text=text))
            return True

        # Kommentare
        if isinstance(frame, COMM):
            mp3_tags.add(COMM(encoding=3, lang=frame.lang or "eng", desc=frame.desc or "", text=frame.text))
            return True

        # Cover / Bilder (zur Sicherheit mit kopieren, falls vorhanden)
        if isinstance(frame, APIC):
            mp3_tags.add(APIC(
                encoding=3,
                mime=frame.mime,
                type=frame.type,
                desc=frame.desc or "",
                data=frame.data
            ))
            return True

        # TXXX (benutzerdefiniert)
        if isinstance(frame, TXXX):
            mp3_tags.add(TXXX(encoding=3, desc=frame.desc or "", text=frame.text))
            return True

        # Fallback: für andere bekannte Klassen versuchen wir, minimal zu rekonstruieren
        # (Wenn nicht sicher, überspringen und warnen)
    except Exception as e:
        logging.warning(f"ID3-Frame {key} konnte nicht geklont werden: {e}")
        return False

    logging.info(f"Unbekannter/inkompatibler ID3-Frame übersprungen: {key}")
    return False


def _copy_id3_from_wav_id3(wav_id3: ID3, mp3_audio: MP3):
    """Direktes Kopieren von ID3-Frames aus WAV nach MP3 (neu aufbauen, nicht Referenzen teilen)."""
    if mp3_audio.tags is None:
        mp3_audio.add_tags()
    else:
        mp3_audio.tags.clear()

    copied = 0
    for key, frame in wav_id3.items():
        if _clone_id3_frame_to(mp3_audio.tags, key, frame):
            copied += 1

    mp3_audio.save()
    return copied


def _copy_from_riff_info(wav_meta: WAVE, mp3_audio: MP3):
    """Mapping von RIFF INFO → ID3."""
    tags = wav_meta.tags
    if mp3_audio.tags is None:
        mp3_audio.add_tags()
    else:
        mp3_audio.tags.clear()

    # WAV → ID3 Mapping
    # Übliche RIFF-Keys: INAM (Titel), IART (Artist), IPRD (Album), ICRD (Year), IGNR (Genre),
    # ITRK (Track), ICMT (Comment), ISFT (Software), (optional: TPOS als Disc nicht standardisiert)
    mapping = [
        ("INAM", TIT2),  # Titel
        ("IART", TPE1),  # Künstler
        ("IPRD", TALB),  # Album
        ("ICRD", TYER),  # Jahr (alternativ modern TDRC)
        ("IGNR", TCON),  # Genre
        ("ITRK", TRCK),  # Tracknummer
    ]

    copied = 0
    for wav_key, cls in mapping:
        if wav_key in tags:
            mp3_audio.tags.add(cls(encoding=3, text=tags[wav_key]))
            copied += 1

    # Kommentar
    if "ICMT" in tags:
        mp3_audio.tags.add(COMM(encoding=3, lang="eng", desc="", text=tags["ICMT"]))
        copied += 1
    # Software/Encoder
    if "ISFT" in tags:
        mp3_audio.tags.add(TSSE(encoding=3, text=tags["ISFT"]))
        copied += 1

    mp3_audio.save()
    return copied


def copy_tags(wav_file: Path, mp3_file: Path) -> int:
    """
    Kopiert Tags von WAV → MP3.
    - Falls WAV ID3 enthält: kopiert Frames direkt (rekonstruiert).
    - Sonst RIFF→ID3 Mapping.
    Rückgabe: Anzahl kopierter Frames/Felder.
    """
    try:
        wav_meta = WAVE(str(wav_file))
        if not wav_meta.tags:
            return 0

        mp3_audio = MP3(str(mp3_file))
        # ID3 im WAV?
        if isinstance(wav_meta.tags, ID3):
            return _copy_id3_from_wav_id3(wav_meta.tags, mp3_audio)
        else:
            return _copy_from_riff_info(wav_meta, mp3_audio)

    except Exception as e:
        logging.warning(f"Tags konnten nicht kopiert werden für {wav_file}: {e}")
        return 0


# ------------------ Preview ------------------ #
def preview_tags(files, limit=5):
    shown = 0
    for wav in files[:limit]:
        try:
            meta = WAVE(str(wav))
            console.print(Panel.fit(f"[bold]{wav}[/bold]", title="WAV", border_style="cyan"))
            if not meta.tags:
                console.print("[yellow]Keine Tags vorhanden.[/yellow]")
            else:
                # Zeige als Tabelle
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Key", justify="left")
                table.add_column("Value", justify="left")
                # ID3 oder RIFF-Map
                if isinstance(meta.tags, ID3):
                    for k, frame in meta.tags.items():
                        # Für Textframes freundliche Ausgabe
                        val = ", ".join(frame.text) if hasattr(frame, "text") else repr(frame)
                        table.add_row(k, val)
                else:
                    for k, v in meta.tags.items():
                        table.add_row(k, str(v))
                console.print(table)
            shown += 1
        except Exception as e:
            console.print(f"[red]Fehler beim Lesen:[/red] {wav} - {e}")
    if shown == 0:
        console.print("[yellow]Keine Dateien für Vorschau.[/yellow]")


# ------------------ Konvertierung ------------------ #
def convert_one(wav_file: Path, src_root: Path, dst_root: Path, bitrate: str, dry_run: bool) -> str:
    """
    Konvertiert eine WAV-Datei (oder simuliert im Dry-Run).
    Rückgabe: "converted" | "skipped" | "failed"
    """
    relative_path = wav_file.relative_to(src_root)
    mp3_file = dst_root / relative_path.with_suffix(".mp3")
    mp3_file.parent.mkdir(parents=True, exist_ok=True)

    if mp3_file.exists():
        logging.info(f"Übersprungen (bereits vorhanden): {mp3_file}")
        return "skipped"

    if dry_run:
        return "would_convert"

    try:
        audio = AudioSegment.from_wav(str(wav_file))
        audio.export(str(mp3_file), format="mp3", bitrate=bitrate)
        copied = copy_tags(wav_file, mp3_file)
        logging.info(f"Konvertiert: {wav_file} -> {mp3_file} (Tags kopiert: {copied})")
        return "converted"
    except Exception as e:
        logging.error(f"Fehler bei {wav_file}: {e}")
        return "failed"


def run_conversion(src_dir: str, dst_dir: str, bitrate: str, workers: int, dry_run: bool, preview_count: int):
    src_path = Path(src_dir)
    dst_path = Path(dst_dir)

    if not src_path.exists():
        console.print(f"[red]Fehler:[/red] Quellverzeichnis {src_dir} existiert nicht.")
        return

    dst_path.mkdir(parents=True, exist_ok=True)
    log_file = dst_path / "conversion.log"
    setup_logging(log_file)

    logging.info("Starte Konvertierung")
    logging.info(f"Quelle: {src_path}")
    logging.info(f"Ziel: {dst_path}")
    logging.info(f"Bitrate: {bitrate}")
    logging.info(f"Workers: {workers}")
    logging.info(f"Dry-Run: {dry_run}")

    wav_files = sorted(src_path.rglob("*.wav"))
    if not wav_files:
        console.print("[yellow]Keine WAV-Dateien gefunden.[/yellow]")
        return

    console.print(f"[cyan]Gefundene WAV-Dateien:[/cyan] {len(wav_files)}")
    console.print(f"[cyan]Bitrate:[/cyan] {bitrate}")
    console.print(f"[cyan]Parallelisierung:[/cyan] {workers} Threads")
    if dry_run:
        console.print("[yellow]Dry-Run aktiv – es werden keine Dateien geschrieben.[/yellow]")

    # Vorschau
    if preview_count and preview_count > 0:
        console.rule("[bold]Tag-Vorschau[/bold]")
        preview_tags(wav_files, limit=preview_count)
        console.rule()

    # Dry-run ohne parallelisieren: wir „zählen“ nur
    if dry_run:
        would_convert = 0
        would_skip = 0
        for wav in wav_files:
            relative = wav.relative_to(src_path)
            mp3_file = dst_path / relative.with_suffix(".mp3")
            if mp3_file.exists():
                would_skip += 1
            else:
                would_convert += 1

        table = Table(title="Dry-Run Ergebnis")
        table.add_column("Status", style="cyan")
        table.add_column("Anzahl", style="magenta", justify="right")
        table.add_row("Würde konvertieren", str(would_convert))
        table.add_row("Würde überspringen (MP3 existiert)", str(would_skip))
        console.print(table)
        return

    # Parallele Konvertierung
    converted = 0
    skipped = 0
    failed = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
        transient=True
    ) as progress:
        task = progress.add_task("[green]Konvertiere...", total=len(wav_files))

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(convert_one, wav, src_path, dst_path, bitrate, False) for wav in wav_files]
            for fut in as_completed(futures):
                status = fut.result()
                if status == "converted":
                    converted += 1
                elif status == "skipped":
                    skipped += 1
                elif status == "failed":
                    failed += 1
                progress.advance(task)

    # Ergebnis-Tabelle
    table = Table(title="Konvertierung abgeschlossen")
    table.add_column("Status", justify="left", style="cyan")
    table.add_column("Anzahl", justify="right", style="magenta")
    table.add_row("Neue MP3s", str(converted))
    table.add_row("Übersprungen (bereits vorhanden)", str(skipped))
    table.add_row("Fehler", str(failed))
    console.print(table)

    logging.info(f"Fertig. Neue: {converted}, Übersprungen: {skipped}, Fehler: {failed}")


# ------------------ CLI ------------------ #
def main():
    parser = argparse.ArgumentParser(
        description="Konvertiere WAV → MP3 rekursiv (Struktur beibehalten) mit Tag-Übernahme (ID3 direkt oder RIFF→ID3), "
                    "Parallelisierung, Dry-Run und Tag-Vorschau."
    )
    parser.add_argument("src", help="Pfad zum Quellverzeichnis (z.B. /home/peter/wav)")
    parser.add_argument("dst", help="Pfad zum Zielverzeichnis (z.B. /home/peter/mp3)")
    parser.add_argument("bitrate", help="Bitrate (z.B. 192k, 256k, 320k)")

    parser.add_argument("--workers", type=int, default=min(8, (os.cpu_count() or 4)),
                        help="Anzahl Threads für Parallelisierung (Default: min(8, CPU-Kerne))")
    parser.add_argument("--dry-run", action="store_true",
                        help="Nur anzeigen, was passieren würde – keine Dateien schreiben")
    parser.add_argument("--preview", type=int, default=0,
                        help="Vorab Tags der ersten N WAV-Dateien anzeigen (z.B. --preview 3)")

    args = parser.parse_args()
    run_conversion(args.src, args.dst, args.bitrate, args.workers, args.dry_run, args.preview)


if __name__ == "__main__":
    main()
