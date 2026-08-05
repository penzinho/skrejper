"""B2B lead-generation pipeline for BiH and Serbian job boards.

Unlike ``app/scrapers`` (posting -> e-mail row -> CSV, stateless), this package
is employer-centric and stateful: every scraped employer and posting lands in a
local SQLite database, and leads are derived from the *history* of an
employer's postings (frequency scoring), not from a single advert.

Layout:

* ``schema``        the unified employer/posting record shapes
* ``db``            SQLite store + NDJSON dump/merge (used by Drive sync)
* ``http``          polite fetch layer: rate limit, backoff, on-disk HTML cache
* ``normalize``     transliteration + company-name/city/date normalization
* ``public_sector`` flags public-sector employers (schools, hospitals, ...)
* ``score``         per-employer frequency scoring
* ``dedupe``        cross-source employer matching (tax id, then fuzzy)
* ``export``        ranked leads -> CSV/XLSX
* ``registry``      source adapter registry; a new source is one new module
* ``sources/``      one module per job board (klix, ...)
"""
