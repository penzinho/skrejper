import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.gelbeseiten import (  # noqa: E402
    MAX_WEBSITE_PAGES,
    _build_contact_page_candidates,
    _clean_text,
    _extract_email_candidates,
    _fetch_html_via_http_response,
)


GENERIC_LOCAL_PART_PREFERENCES = (
    "kontakt",
    "contact",
    "info",
    "office",
    "mail",
    "service",
    "hello",
    "team",
    "sales",
    "bau",
)
LOW_PRIORITY_LOCAL_PART_KEYWORDS = (
    "noreply",
    "no-reply",
    "newsletter",
    "shop",
    "store",
    "booking",
    "presse",
    "press",
    "marketing",
    "datenschutz",
    "privacy",
    "karriere",
    "career",
    "jobs",
)
CONTACT_PAGE_KEYWORDS = (
    "kontakt",
    "contact",
    "impressum",
    "about",
)


def _normalize_email(value: Any) -> str:
    return _clean_text(value or "").lower()


def _normalize_company(value: Any) -> str:
    return _clean_text(value or "").casefold()


def _normalize_website(value: Any) -> str:
    cleaned = _clean_text(value or "")
    if not cleaned or cleaned in {"http://", "https://"}:
        return ""
    cleaned = "".join(cleaned.split())
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    parsed = urlparse(cleaned)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or ""
    return f"{scheme}://{netloc}{path}"


def _website_host(value: Any) -> str:
    website = _normalize_website(value)
    if not website:
        return ""
    host = urlparse(website).netloc.casefold()
    return host[4:] if host.startswith("www.") else host


def _host_labels(host: str) -> tuple[str, ...]:
    return tuple(label for label in host.split(".") if label)


def _domains_match(email_domain: str, website_host: str) -> bool:
    left = _host_labels(email_domain.casefold())
    right = _host_labels(website_host.casefold())
    if not left or not right:
        return False
    return (
        left[-2:] == right[-2:]
        or email_domain.casefold().endswith(f".{website_host.casefold()}")
        or website_host.casefold().endswith(f".{email_domain.casefold()}")
    )


def _score_email_candidate(email: str, website_host: str, page_url: str) -> tuple[int, int]:
    local_part, _, domain = email.partition("@")
    local_part = local_part.casefold()
    lowered_url = page_url.casefold()
    score = 0

    if _domains_match(domain, website_host):
        score += 100
    else:
        score -= 40

    if any(keyword in local_part for keyword in GENERIC_LOCAL_PART_PREFERENCES):
        score += 15
    if any(keyword in local_part for keyword in LOW_PRIORITY_LOCAL_PART_KEYWORDS):
        score -= 20
    if any(keyword in lowered_url for keyword in CONTACT_PAGE_KEYWORDS):
        score += 10

    return score, -len(email)


def _extract_best_email_from_website(website_url: str) -> str:
    normalized_website = _normalize_website(website_url)
    if not normalized_website:
        return ""

    website_host = _website_host(normalized_website)
    queue = [normalized_website]
    visited: set[str] = set()
    best_email = ""
    best_score: tuple[int, int] | None = None

    while queue and len(visited) < MAX_WEBSITE_PAGES:
        current_url = queue.pop(0)
        if current_url in visited:
            continue
        visited.add(current_url)

        try:
            final_url, body, content_type = _fetch_html_via_http_response(current_url)
        except Exception:
            continue

        if content_type and "html" not in content_type.lower():
            continue

        page_url = final_url or current_url
        for email in _extract_email_candidates(body):
            score = _score_email_candidate(email, website_host, page_url)
            if best_score is None or score > best_score:
                best_email = email
                best_score = score

        if len(visited) == 1:
            queue.extend(
                candidate
                for candidate in _build_contact_page_candidates(page_url, body)
                if candidate not in visited
            )

    return best_email


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = [{key: value or "" for key, value in row.items()} for row in reader]
        return list(reader.fieldnames or []), rows


def _write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def _collect_unique_seed_values(rows: list[dict[str, str]]) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    emails_by_company: dict[str, set[str]] = {}
    emails_by_website: dict[str, set[str]] = {}
    emails_by_host: dict[str, set[str]] = {}

    for row in rows:
        email = _normalize_email(row.get("email"))
        if not email:
            continue
        company_key = _normalize_company(row.get("agency_name"))
        website_key = _normalize_website(row.get("website"))
        host_key = _website_host(row.get("website"))
        if company_key:
            emails_by_company.setdefault(company_key, set()).add(email)
        if website_key:
            emails_by_website.setdefault(website_key, set()).add(email)
        if host_key:
            emails_by_host.setdefault(host_key, set()).add(email)

    def flatten(mapping: dict[str, set[str]]) -> dict[str, str]:
        return {
            key: next(iter(values))
            for key, values in mapping.items()
            if len(values) == 1
        }

    return flatten(emails_by_company), flatten(emails_by_website), flatten(emails_by_host)


def _apply_reuse_maps(
    rows: list[dict[str, str]],
    *,
    emails_by_company: dict[str, str],
    emails_by_website: dict[str, str],
    emails_by_host: dict[str, str],
) -> dict[str, int]:
    stats = {"reused_by_website": 0, "reused_by_company": 0, "reused_by_host": 0}

    for row in rows:
        if _normalize_email(row.get("email")):
            continue

        website_key = _normalize_website(row.get("website"))
        company_key = _normalize_company(row.get("agency_name"))
        host_key = _website_host(row.get("website"))

        email = ""
        source = ""
        if website_key and website_key in emails_by_website:
            email = emails_by_website[website_key]
            source = "reused_by_website"
        elif company_key and company_key in emails_by_company:
            email = emails_by_company[company_key]
            source = "reused_by_company"
        elif host_key and host_key in emails_by_host:
            email = emails_by_host[host_key]
            source = "reused_by_host"

        if email:
            row["email"] = email
            stats[source] += 1

    return stats


def enrich_csv_emails(
    input_path: Path,
    output_path: Path,
    *,
    workers: int,
) -> dict[str, int]:
    fieldnames, rows = _read_rows(input_path)
    if not fieldnames:
        raise RuntimeError(f"No CSV header found in {input_path}")
    if "email" not in fieldnames:
        raise RuntimeError(f"CSV {input_path} does not contain an 'email' column")

    initial_missing = sum(1 for row in rows if not _normalize_email(row.get("email")))
    emails_by_company, emails_by_website, emails_by_host = _collect_unique_seed_values(rows)
    reuse_stats = _apply_reuse_maps(
        rows,
        emails_by_company=emails_by_company,
        emails_by_website=emails_by_website,
        emails_by_host=emails_by_host,
    )

    representative_url_by_host: dict[str, str] = {}
    for row in rows:
        if _normalize_email(row.get("email")):
            continue
        website = _normalize_website(row.get("website"))
        host = _website_host(website)
        if website and host and host not in emails_by_host and host not in representative_url_by_host:
            representative_url_by_host[host] = website

    fetched_by_host: dict[str, str] = {}
    if representative_url_by_host:
        total_hosts = len(representative_url_by_host)
        processed = 0
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            future_map = {
                executor.submit(_extract_best_email_from_website, website): host
                for host, website in representative_url_by_host.items()
            }
            for future in as_completed(future_map):
                host = future_map[future]
                processed += 1
                try:
                    email = _normalize_email(future.result())
                except Exception as exc:
                    print(f"[enrich-csv] Failed to enrich host {host}: {exc}")
                    email = ""

                if email:
                    fetched_by_host[host] = email

                if processed % 100 == 0 or processed == total_hosts:
                    print(
                        f"[enrich-csv] Processed {processed}/{total_hosts} hosts, "
                        f"found {len(fetched_by_host)} emails"
                    )

    fetched_count = 0
    for row in rows:
        if _normalize_email(row.get("email")):
            continue
        host = _website_host(row.get("website"))
        email = fetched_by_host.get(host, "")
        if email:
            row["email"] = email
            fetched_count += 1

    post_fetch_company, post_fetch_website, post_fetch_host = _collect_unique_seed_values(rows)
    follow_up_reuse = _apply_reuse_maps(
        rows,
        emails_by_company=post_fetch_company,
        emails_by_website=post_fetch_website,
        emails_by_host=post_fetch_host,
    )

    final_missing = sum(1 for row in rows if not _normalize_email(row.get("email")))
    _write_rows(output_path, fieldnames, rows)

    return {
        "rows": len(rows),
        "initial_missing": initial_missing,
        "fetched_count": fetched_count,
        "final_missing": final_missing,
        **reuse_stats,
        **{f"follow_up_{key}": value for key, value in follow_up_reuse.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich missing emails in a GelbeSeiten CSV using company websites.")
    parser.add_argument("input_csv")
    parser.add_argument("--output")
    parser.add_argument("--workers", type=int, default=24)
    args = parser.parse_args()

    input_path = Path(args.input_csv)
    output_path = Path(args.output) if args.output else input_path
    stats = enrich_csv_emails(input_path, output_path, workers=args.workers)

    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
