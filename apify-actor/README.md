# OpenClaw Scraper — Apify Actor

Apify Actor wrapping the HZZ and Meinestadt Playwright scrapers plus the
Arbeitsagentur API scraper, built to avoid the timeout / zombie-process issues
of the FastAPI deployment. Apify manages the browser lifecycle and run timeout
for us.

## Structure

```
apify-actor/
├── .actor/
│   ├── actor.json          # Actor metadata + build config
│   └── input_schema.json   # Input form (source, category, max_pages, ...)
├── src/
│   ├── __main__.py         # python -m src  ->  asyncio.run(main())
│   ├── main.py             # Actor I/O + dedup/filter + orchestration
│   ├── hzz.py              # HZZ scraper (extracted from app/scrapers/hzz.py)
│   ├── meinestadt.py       # Meinestadt scraper (extracted from app/scrapers/meinestadt.py)
│   └── arbeitsagentur.py   # Arbeitsagentur scraper (public REST API, no browser)
├── Dockerfile              # apify/actor-python-playwright base image
└── requirements.txt
```

Unlike HZZ and Meinestadt, the Arbeitsagentur scraper does **not** drive a
browser: the `jobsuche` "infinite scroll" list is backed by a public REST API
(`rest.arbeitsagentur.de/jobboerse/jobsuche-service`), so it talks to that
directly — faster and far more robust than DOM scraping. Categories are broad
groups that each bundle several of the board's *Berufsfelder* (occupational
fields), queried and deduped together; employer e-mails are mined from each
posting's free-text description. (A raw Berufsfeld name is also accepted as a
category for fine-grained scraping.)

The scrapers stay in their own modules because both define overlapping helper
names (`_clean_text`, `_resolve_category`, `BASE_URL`, ...); `src/main.py`
"joins" them at the actor level.

## Input

| Field                   | Type      | Notes                                                                   |
| ----------------------- | --------- | ----------------------------------------------------------------------- |
| `source`                   | string    | `hzz`, `meinestadt`, or `arbeitsagentur` (required)                     |
| `hzz_categories`           | string[]  | HZZ: pick whole categories (multi-select). Empty + no subgroups = all.  |
| `hzz_groups`               | string[]  | HZZ: pick specific subgroups (multi-select); parent category auto-detected. |
| `meinestadt_categories`    | string[]  | Meinestadt: pick categories (multi-select). Empty = default search.     |
| `arbeitsagentur_categories`| string[]  | Arbeitsagentur: pick broad groups (multi-select). Empty = keyword/all.   |
| `arbeitsagentur_keyword`   | string    | Arbeitsagentur: free-text search (`was`), e.g. a job title (optional).  |
| `arbeitsagentur_location`  | string    | Arbeitsagentur: place or postal code (`wo`) (optional).                 |
| `arbeitsagentur_radius`    | integer   | Arbeitsagentur: search radius km around the location (default 25).      |
| `arbeitsagentur_listing_limit`| integer | Arbeitsagentur: cap listings (detail fetches) processed per category.  |
| `output_mode`              | string    | `combined` / `per_category` / `per_subgroup` (default `combined`)        |
| `max_pages`                | integer   | Listing pages to scrape, per category/subgroup (default 1)             |
| `results_per_page`         | integer   | HZZ page size (def 75); Arbeitsagentur page size (≤100). Meinestadt: ignored. |
| `company_limit`            | integer   | Cap on distinct companies, per category/subgroup (optional)            |
| `country`                  | string    | Override for the `country` output field                                 |
| `category` / `group`       | string    | Legacy single-value fields, still accepted and merged with the above    |

The HZZ subgroup list is static (discovered once via `discover_subgroups.py`,
stored in `HZZ_CATEGORY_GROUPS` in `src/hzz.py`). Re-run that script if HZZ
changes its occupational subgroups.

## Output

Each row is one employer. The same filtering as the FastAPI service is applied:
rows without an email are dropped, excluded companies (vrtić/škola/općina) are
skipped, and results are deduped by normalized company name (falling back to the
email when the company name is missing).

`output_mode` controls how results are delivered:

- **`combined`** — all rows in the default dataset (export as a single CSV).
- **`per_category`** — one CSV file per category written to the run's
  key-value store (`<source>-<category>.csv`), plus a combined dataset view.
- **`per_subgroup`** — one CSV file per subgroup
  (`<source>-<category>-<subgroup>.csv`); falls back to per-category for
  Meinestadt, which has no subgroups.

Every row carries `source`, `category` (our key) and, for HZZ, `group`
(subgroup label), so the combined dataset can also be filtered after the fact.

## Implementation note

The scrapers use **synchronous** Playwright, which cannot run inside the asyncio
loop that drives the Apify Actor. `src/main.py` runs them via
`asyncio.to_thread()` so the loop stays responsive.

## Local run

```bash
# from this folder, with the Apify CLI installed

# whole categories, one CSV each
apify run --input '{"source": "hzz", "hzz_categories": ["construction", "hospitality_tourism"], "output_mode": "per_category", "max_pages": 1}'

# specific subgroups only, one CSV each
apify run --input '{"source": "hzz", "hzz_groups": ["Arhitekti/arhitektice", "Zidari/zidarke i srodna zanimanja"], "output_mode": "per_subgroup", "max_pages": 1}'

# everything in a single dataset
apify run --input '{"source": "hzz", "hzz_categories": ["construction"], "output_mode": "combined", "max_pages": 1}'

# Arbeitsagentur: broad groups, one CSV each, capped at 200 listings per group
apify run --input '{"source": "arbeitsagentur", "arbeitsagentur_categories": ["bau_ausbau", "gastronomie_tourismus"], "output_mode": "per_category", "max_pages": 3, "results_per_page": 100, "arbeitsagentur_listing_limit": 200}'

# Arbeitsagentur: keyword + location instead of a category
apify run --input '{"source": "arbeitsagentur", "arbeitsagentur_keyword": "Maurer", "arbeitsagentur_location": "Berlin", "arbeitsagentur_radius": 50, "max_pages": 2}'
```
