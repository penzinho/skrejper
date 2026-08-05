"""Per-employer frequency scoring.

The lead signal is the *pattern* of hiring, not any single advert: a firm that
posted 24 driver ads in two years needs staffing help; a firm with one ad does
not. Computed from whatever posting history the database has — complete for
Klix (employer pages carry full history), accumulating over time elsewhere.

Components, kept in the ``scores`` table so exports can sort on any of them:

* ``ads_24m``              postings published (or, lacking that, expiring)
                           within the last 24 months
* ``distinct_titles_24m``  distinct normalized position titles in that window
* ``repeated_titles_24m``  titles posted 2+ times — the churn/fluktuacija signal
* ``last_ad`` / ``first_ad``
* ``score``                composite: ads_24m + 4×min(repeated,5) + recency
                           bonus (6/4/2 for a last ad within 1/3/6 months)
"""

from collections import defaultdict
from datetime import date, datetime, timedelta

from app.leadgen.db import LeadDb
from app.leadgen.normalize import norm_text
from app.leadgen.schema import now_iso

WINDOW_DAYS = 730  # 24 months


def _posting_date(posting: dict) -> str:
    # Klix history rows often carry only the expiry date; close enough for a
    # 24-month window (ads run a few weeks).
    return posting.get("published_at") or posting.get("expires_at") or ""


def _months_between(later: date, earlier: date) -> float:
    return (later - earlier).days / 30.44


def compute_scores(db: LeadDb, today: date | None = None) -> int:
    """(Re)compute the scores table from scratch; returns employers scored."""
    today = today or datetime.now().date()
    cutoff = (today - timedelta(days=WINDOW_DAYS)).isoformat()

    by_employer: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for posting in db.postings():
        key = (posting["employer_source"], posting["employer_source_id"])
        if key[0] and key[1]:
            by_employer[key].append(posting)

    rows = []
    for (employer_source, employer_source_id), postings in by_employer.items():
        dated = [(p, _posting_date(p)) for p in postings]
        dates = sorted(d for _, d in dated if d)
        recent = [p for p, d in dated if d >= cutoff]

        title_counts: dict[str, int] = defaultdict(int)
        for posting in recent:
            title = norm_text(posting.get("title", ""))
            if title:
                title_counts[title] += 1
        repeated = sum(1 for count in title_counts.values() if count >= 2)

        last_ad = dates[-1] if dates else ""
        recency_bonus = 0
        if last_ad:
            try:
                months = _months_between(today, date.fromisoformat(last_ad[:10]))
            except ValueError:
                months = 99
            recency_bonus = 6 if months <= 1 else 4 if months <= 3 else 2 if months <= 6 else 0

        rows.append({
            "employer_source": employer_source,
            "employer_source_id": employer_source_id,
            "total_ads": len(postings),
            "ads_24m": len(recent),
            "distinct_titles_24m": len(title_counts),
            "repeated_titles_24m": repeated,
            "first_ad": dates[0] if dates else "",
            "last_ad": last_ad,
            "score": float(len(recent) + 4 * min(repeated, 5) + recency_bonus),
            "computed_at": now_iso(),
        })

    return db.write_scores(rows)
