"""Send a scrape result CSV via Resend.

Called automatically after each scrape run, or manually to resend a file:

    python scripts/send_report.py output/arbeitsagentur-bau_ausbau-2026-06-17.csv
"""

import os
import sys
from pathlib import Path


def send(csv_path: Path) -> bool:
    """Send csv_path as an email attachment. Returns True on success."""
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    to_email = os.getenv("RESEND_TO", "").strip()
    from_email = os.getenv("RESEND_FROM", "scraper@skrejper.io").strip()

    if not api_key or not to_email:
        print("[email] RESEND_API_KEY ili RESEND_TO nisu postavljeni — preskačem.", flush=True)
        return False

    try:
        import resend  # type: ignore[import]

        resend.api_key = api_key

        with open(csv_path, "rb") as f:
            content = list(f.read())

        # Count data rows (minus header line).
        with open(csv_path, encoding="utf-8-sig") as f:
            n_rows = max(0, sum(1 for _ in f) - 1)

        # Parse source + category + date from filename: <source>-<category>-<date>.csv
        parts = csv_path.stem.split("-", 2)
        source = parts[0].upper() if len(parts) > 0 else "Scraper"
        category = parts[1] if len(parts) > 1 else csv_path.stem
        date = parts[2] if len(parts) > 2 else ""

        subject = f"{source}: {category} — {n_rows} kontakata ({date})"
        body = (
            f"Scrape završen za kategoriju '{category}' ({date}).\n"
            f"Pronađeno {n_rows} kontakata s e-mailom.\n\n"
            f"CSV u prilogu: {csv_path.name}"
        )

        resend.Emails.send({
            "from": from_email,
            "to": [to_email],
            "subject": subject,
            "text": body,
            "attachments": [{"filename": csv_path.name, "content": content}],
        })
        print(f"[email] Poslano na {to_email}  ({csv_path.name}, {n_rows} redova)", flush=True)
        return True

    except Exception as exc:
        print(f"[email] Greška: {exc}", flush=True)
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Korištenje: python scripts/send_report.py <putanja-do-csv>")
        sys.exit(1)

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"Greška: datoteka ne postoji: {path}")
        sys.exit(1)

    ok = send(path)
    sys.exit(0 if ok else 1)
