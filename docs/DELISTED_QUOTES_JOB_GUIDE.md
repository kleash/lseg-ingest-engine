# Implementation Guide: Isolated "Delisted Quotes" Ingestion Job

A portable, design-level guide for adding a **new file type that is ingested by a
separate, isolated job** into a file-ingestion pipeline. It is written from the LSEG
`lseg-ingest` implementation but deliberately framed so it can be applied to any
codebase with similar logic (file scanner → classifier → per-target loader → DB).

---

## 1. What we are building

A data feed introduced a new QUOTE-file variant that carries **delisted instruments**:

```
EIS_INTDELISTED_<region>_..._QUOTE.INT.<feed>.<yyyymmdd>.<seq>.1.1.txt.zip
```

These rows must land in a **new, separate table** (`lseg_quotes_delisted`) that mirrors
the live quotes table — never mixed into the live `lseg_quotes` data.

**The hard requirement:** the existing **main ingestion job must ignore these files
entirely**. They are only ingested when a **dedicated "delisted" job** is run.

So there are two deliverables:
1. A new file type → new target table.
2. A **job-type** mechanism so one orchestrator can run either the *main* set of targets
   or *only* the delisted target, in isolation.

---

## 2. Why this design (and why not the obvious alternatives)

| Decision | Why | Rejected alternative |
|---|---|---|
| Reuse the existing generic loader/parser for the new file | The delisted file shares the **exact column layout** of the existing quotes file; only the destination table differs. No new parser is justified. | Writing a bespoke delisted parser — needless duplication. |
| Introduce a **job type** on the job/queue row | Cleanly expresses "this run does only delisted; that run does everything else" with one orchestrator, one worker, one lock. | A whole separate service/binary — heavy, duplicates infra, and the cluster lock already serializes runs. |
| **Main job excludes** the delisted target; **delisted job includes only** it | Satisfies "main job ignores the file" literally and centrally — the rule lives in one place, not scattered. | Relying on the classifier to drop the file for main and keep it for delisted — impossible, since the classifier can't see job type. |
| New table **mirrors** the existing quotes table | Same shape ⇒ same unique key, same indexes, same upsert/soft-delete semantics for free. | A differently-shaped table — breaks reuse of the schema/loader. |
| **Default job type = MAIN** + additive, defaulted DB column | Backward compatible: every existing caller, queued row, and test keeps working untouched. | A `NOT NULL` column with no default — breaks existing rows and inserts. |

Two subtle behaviors that fall out **for free** and are worth confirming in any target codebase:
- Any **row-level filtering keyed to the live target** (in LSEG, a "drop rows whose RIC
  contains `^`" filter gated on `target == QUOTES`) will *not* apply to the new target —
  which is exactly what you want, since delisted rows are precisely the filtered-out ones.
- Any **post-load reconciliation step** gated on the live target won't run for the new target.

Verify both in your codebase rather than assuming.

---

## 3. The pipeline shape this guide assumes

Most ingestion pipelines of this kind have these seams. Find their equivalents first:

1. **Job/queue record** — a row representing one run (status, business date, input dir…),
   claimed by a worker.
2. **Orchestrator** — given a job, scans the input dir and runs each *target* pipeline.
3. **File scanner / classifier** — maps a filename to a *target* (which table) and a
   *kind* (full vs delta), or to "ignore".
4. **Target definition** — an enum/registry binding target → table name + unique-key columns.
5. **Column schema** — header → DB column mapping per target.
6. **Generic loader** — parses a file and upserts/soft-deletes by the target's unique key.
7. **DB migrations** — versioned schema changes (Liquibase/Flyway/etc.).

---

## 4. How to implement — step by step

### Step 1 — Add the new target → table binding
In the target enum/registry, add a constant that **mirrors the existing quotes target's
keys** but points at the new table:

```java
QUOTES_DELISTED("lseg_quotes_delisted",
        List.of("asset_id", "quote_id"),     // same unique key as QUOTES
        List.of("Asset_ID", "Quote_ID")),
```

### Step 2 — Reuse the existing column schema for the new target
Don't copy the column list. Extract it once and bind it to both targets so they can't drift:

```java
List<Column> quoteColumns = List.of( /* the existing quote columns */ );
SCHEMAS.put(Target.QUOTES,          quoteColumns);
SCHEMAS.put(Target.QUOTES_DELISTED, quoteColumns);
```
The schema-intersection step (header ∩ schema) will harmlessly drop any extra trailing
columns the new file carries.

### Step 3 — Classify the new filename to the new target
In the classifier, add a rule **before** the existing quotes rules, matched explicitly so
it can't be swallowed by a broader prefix rule:

```java
// New, isolated target — matched before the generic QUOTE rules.
if (dataset.startsWith("EIS_INTDELISTED") &&
        (dataset.endsWith("_QUOTE") || dataset.endsWith("_QUOTES")))
    return Target.QUOTES_DELISTED;
```
> Check the boundary: confirm the new prefix does **not** accidentally match the existing
> rule (here `EIS_INTDELISTED` does *not* match `startsWith("EIS_INT_")` because char 7 is
> `D`, not `_`). Add a regression test for exactly this (Step 8).

### Step 4 — Introduce the job-type constant
```java
public static final String JOB_TYPE_MAIN     = "MAIN";
public static final String JOB_TYPE_DELISTED = "DELISTED";
```

### Step 5 — Persist job type on the queue record (DB migration + DAO)
Additive, defaulted column so existing rows/inserts stay valid:
```sql
ALTER TABLE lseg_jobs ADD COLUMN job_type VARCHAR(32) NOT NULL DEFAULT 'MAIN';
```
DAO:
- Add `jobType` to the insert in `queueJob(...)`.
- Add `getJobType(jobId)` that **defaults to MAIN** when null/empty (defensive for old rows).

### Step 6 — Accept job type at the trigger boundary
On the trigger endpoint/CLI, add an optional `jobType` (default `MAIN`), validate it is one
of the known values, and pass it through to the queue insert. Echo it back in the response.

### Step 7 — Filter targets by job type in the orchestrator (the core of isolation)
Compute the allowed target set once, then apply it everywhere the orchestrator iterates
targets or files:

```java
private static Set<Target> targetsForJobType(String jobType) {
    if (JOB_TYPE_DELISTED.equals(jobType)) return EnumSet.of(Target.QUOTES_DELISTED);
    return EnumSet.complementOf(EnumSet.of(Target.QUOTES_DELISTED));   // MAIN = everything else
}
```
Then:
- After scanning, **drop files** whose target isn't allowed:
  `all.removeIf(f -> !allowed.contains(f.target()));`
  (This keeps sanity-checks, audit, and dedupe limited to relevant files.)
- In each **target loop**, `continue` when `!allowed.contains(t)`.
- Guard any **special async phase** (e.g. a background pricing phase) on
  `allowed.contains(thatTarget)`.

This is the single place that enforces "main ignores delisted; delisted runs only delisted."

### Step 8 — Create the new table (DB migration)
Copy the existing quotes table DDL verbatim; rename the table and **rename all index/constraint
names** to be unique (`uniq_quotes_delisted_*`, `idx_quotes_delisted_*`). Register the new
migration after the job-type migration.

### Step 9 — Extend any external harness/scheduler
Wherever runs are triggered programmatically (a multi-day script, a scheduler), after the
main run reaches a terminal state, trigger the delisted run with `jobType=DELISTED` and wait
for it. Also:
- Scope any "find existing completed job for date" reuse logic to `job_type='MAIN'` so a
  delisted job is never mistaken for the main run.
- Add the new table to any row-count/reporting queries.

### Step 10 — Tests
- **Classifier:** new filename → new target; and a plain quote filename → still the live
  target (the prefix-boundary guard).
- **DAO:** job-type round-trips (insert MAIN/DELISTED, read back).
- **Orchestrator (if you have integration coverage):** a MAIN job leaves the delisted table
  empty; a DELISTED job populates only it and leaves the live table untouched.

---

## 5. Idempotency & ordering checks (don't skip)

- **Dedupe key.** If files are de-duplicated by filename globally, a file ingested by the
  delisted job won't be re-ingested by either job. Confirm the dedupe scope in your codebase.
- **Concurrency.** If a cluster/global lock serializes runs, the main and delisted jobs won't
  collide — desirable. If your pipeline runs jobs concurrently, decide whether the two job
  types may overlap (different tables ⇒ usually safe).
- **Re-runs.** Re-running the delisted job should be a no-op on row counts (upsert on the
  unique key). Verify.

---

## 6. End-to-end verification

1. Build + run migrations; confirm `job_type` column and the new table exist.
2. Point the input at a directory containing the new files.
3. **Main job ignores them:** trigger with no/`MAIN` type → new table count is **0**, live
   table populated as before.
4. **Delisted job ingests them:** trigger with `jobType=DELISTED` → new table count **> 0**;
   spot-check a row that the live-target row-filter would have dropped (e.g. a `^`-RIC) is
   present; live table count unchanged.
5. Re-run the delisted job → row count stable (idempotent).

---

## 7. Checklist (copy into your PR)

- [ ] New target → table binding (mirrors live quotes keys)
- [ ] Column schema shared between live + delisted targets
- [ ] Classifier rule for new filename, matched before generic rules
- [ ] Prefix-boundary regression test (new vs. live)
- [ ] Job-type constants
- [ ] DB migration: additive, defaulted `job_type` column
- [ ] DAO: persist + read job type (defaults to MAIN)
- [ ] Trigger endpoint/CLI: optional, validated `jobType`
- [ ] Orchestrator: allowed-target set filters scan, target loop, and async phases
- [ ] DB migration: new table mirroring live quotes (unique index names)
- [ ] External harness: trigger+await delisted run; scope reuse to MAIN; report new table
- [ ] Confirm row-filter / reconciliation gated on live target won't touch new target
- [ ] Tests: classifier, DAO round-trip, (optional) orchestrator isolation
- [ ] E2E verification per §6

---

## 8. Reference: files touched in `lseg-ingest`

| Concern | File |
|---|---|
| Target → table | `src/main/java/com/lseg/ingest/plan/Target.java` |
| Column schema | `src/main/java/com/lseg/ingest/load/TargetSchema.java` |
| Classifier | `src/main/java/com/lseg/ingest/plan/FileScanner.java` |
| Job-type constants | `src/main/java/com/lseg/ingest/Constants.java` |
| Persist/read job type | `src/main/java/com/lseg/ingest/audit/JobDao.java` |
| Trigger param | `src/main/java/com/lseg/ingest/orchestrator/JobController.java` |
| Target filtering | `src/main/java/com/lseg/ingest/orchestrator/IngestOrchestrator.java` |
| Migrations | `src/main/resources/db/changelog/sql/011-add-job-type.sql`, `012-lseg_quotes_delisted.sql`, `db.changelog-master.xml` |
| External harness | `scripts/multi_day_run.py` |
| Tests | `src/test/java/com/lseg/ingest/plan/FileScannerTest.java`, `.../audit/JobDaoTest.java` |
