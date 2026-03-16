# Certificate Monitor Pipeline

Production certificate expiration monitoring — detects expiring certificates,
matches renewals via fuzzy DN comparison, and sends consolidated + per-source
HTML alert emails.

---

## Folder Structure

```
src/
  batch/
    config/settings/
      cm.py               ← All Pydantic BaseSettings (mongo, email, thresholds, sources, paths)
    models/
      enums.py            ← SourceName, CertStatus, AlertStatus enums
      alerts.py           ← All Pydantic domain models (cert, source variants, final, summary)
    repositories/
      indexes.py          ← MongoDB index creation (run once after deployment)
    services/
      cache.py            ← Parquet cache build / load (FETCH ONCE pattern)
      ignore.py           ← IgnoreAlerts acknowledgement lookup
      alerts.py           ← CertificateAlerts MongoDB upsert
      summary.py          ← RunSummary builder and summary.json writer
      runner.py           ← Main pipeline orchestrator
      sources/
        __init__.py       ← Processor registry (get_processor_registry)
        base.py           ← AbstractBaseSourceProcessor
        ssg.py            ← SSG processor
        hashicorp.py      ← HashiCorp / OpenShift processor (OSE placeholder)
        other_sources.py  ← Evolven, Apigee, Akamai, SSL Tracker processors
      matching/
        fuzzy.py          ← rapidfuzz cdist + length-ratio gate engine
      email/
        builder.py        ← HTML email template builder (all visual logic)
        consolidated.py   ← Consolidated email orchestration
        per_source.py     ← Per-source email orchestration
        sender.py         ← SMTP abstraction + developer alert
    utilities/
      cm.py               ← DN cleaning (Polars-native + lru_cache), split helpers
    runner_cli.py         ← Typer CLI (run, create-indexes commands)
  common/                 ← Your existing base repo, logger, etc.
cache/                    ← Parquet cache (auto-created)
reports/                  ← HTML reports + summary.json (auto-created)
.env.template             ← Copy to .env and fill in values
pyproject.toml            ← Dependencies and tooling config
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -e ".[dev]"
```

# Install uv
```pip install uv```

# Install everything from pyproject.toml
```uv pip install -r pyproject.toml```
# OR if you created requirements.txt
```uv pip install -r requirements.txt```

### 2. Configure environment

```bash
cp config.template .env
# Edit .env with your MongoDB URI, SMTP settings, and recipient lists
```

### 3. Create MongoDB indexes (run once)

```bash
python -m src.batch.runner_cli create-indexes
```

### 4. Wire up your repositories

In `src/batch/runner_cli.py`, replace the three `None` placeholders with your
actual Motor repository instances:

```python
consolidated_repo = CMConsolidatedDataMotorRepository(db)
ignore_repo       = YourIgnoreAlertsRepo(db)
alert_repo        = YourCertificateAlertsRepo(db)
```

### 5. Wire up the HashiCorp OSE API call

In `src/batch/services/sources/hashicorp.py`, replace the body of
`_fetch_replicas()` with your existing async OSE API code.

### 6. Run the pipeline

```bash
# Normal daily run
python -m src.batch.runner_cli

# Force rebuild parquet cache (ignore age)
python -m src.batch.runner_cli --force-refresh

# Dry run (no MongoDB writes, no emails sent)
python -m src.batch.runner_cli --dry-run

# Single source only
python -m src.batch.runner_cli --source SSG

# Verbose logging
python -m src.batch.runner_cli --verbose
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Parquet cache | Single fetch, written once | One connection slot; all processing reads from disk |
| DN cleaning (5M rows) | Polars native expressions (Rust) | No Python GIL; 10–100× faster than `map_elements` |
| DN cleaning (matching) | `lru_cache` Python function | Small batches; cache hit rate is high |
| Fuzzy engine | `rapidfuzz.process.cdist` + `token_sort_ratio` | Vectorised C; (n_alerts × n_candidates) in one call |
| False-positive guard | Length-ratio gate before scoring | Prevents `CRSFSVC` → `163242crsfsvcprodsecret` |
| Renewal scope | Same source + exact key-field match | Semantically correct; no cross-source renewal noise |
| Possible match scope | Cross-source, DN similarity only | Shows users where a cert might already exist |
| `source_details` type | Discriminated union (`source_type` literal) | Type-safe deserialisation; schema-less MongoDB |
| CertificateAlerts TTL | 24h on `log_datetime` | Auto-cleanup; no manual purge needed |
| Per-source email | Always sent (empty = "nothing to report") | Support teams get daily confirmation either way |
| Parquet schema | Explicit, all `sp_*` as Utf8 | Handles missing/null/mixed-type fields from MongoDB |

---

## Adding a New Source

1. Add `MY_SOURCE = "MySource"` to `SourceName` in `models/enums.py`.
2. Create `services/sources/my_source.py` with `MySourceProcessor(BaseSourceProcessor)`.
3. Implement `source_name`, `key_fields`, and `process`.
4. Import and register in `services/sources/__init__.py`.
5. Add the source model to `models/alerts.py` and update `SourceDetailsType` union.
6. Add per-source fields to `PARQUET_SCHEMA` and `_SP_FIELD_MAP` in `services/cache.py`.
7. Add upsert filter logic in `services/alerts.py`.

---

## MongoDB Indexes Created

**ConsolidatedData** (covers `find_eligible_certificates`):
- `idx_eligibility_seek`: `(status, environment, source.name, days_to_expiration, log_date, _id)`
- `idx_eligibility_no_source_seek`: same without `source.name`
- `idx_log_date_desc`: `(log_date DESC)`

**CertificateAlerts**:
- `idx_log_datetime_ttl`: TTL 86400s (24h auto-expire)
- `idx_source_log_datetime`: `(source, log_datetime)`

**IgnoreAlerts**:
- `idx_source_sn_log_datetime`: `(source, serial_number, log_datetime)`

---

## Placeholders to Wire In

| File | What to replace |
|---|---|
| `runner_cli.py` | Motor repository instances (3 repos) |
| `services/sources/hashicorp.py` → `_fetch_replicas()` | Your async OSE API call |
| `services/ignore.py` → `load_ignore_set()` | Your IgnoreAlerts repo `.find_many()` call |
| `services/alerts.py` → `upsert_alert_documents()` | Your CertificateAlerts repo `.upsert_one()` call |
| `services/email/consolidated.py` → `jira_details_fn` | Your JIRA details fetcher |
