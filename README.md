# backup-textit-flows

Cloud Run service: daily restore-capable backup of all TextIt flow definitions.

## What it does (`POST /backup`)

1. `GET /api/v2/flows.json?page_size=250` — paginate, collect + dedupe all flow
   UUIDs. Aborts if zero (empty-backup guard).
2. `GET /api/v2/definitions.json?flow=...&dependencies=all` — batched 40
   UUIDs/call. `dependencies=all` is the UI "Select All" equivalent
   (restore-capable: flows + campaigns + triggers + dependency groups/fields).
3. Merge batches into one workspace-shaped JSON, dedupe per collection by `uuid`
   (SHA-256 hash fallback for objects without a uuid, e.g. triggers/fields).
4. Sanity check: warns if merged flow count != collected UUID count.
5. Upload `<yyyymmdd>_TextIt_backup.json` (UTF-8, no BOM) to the **Text It
   Backups** folder in the Circles of Support Shared Drive via the Drive API
   using the runtime service account (no rclone, no OAuth refresh token).

6. **Retention prune** (ITDO-405 3-tier) runs after the upload succeeds. See below.

`/health` (GET) → `{"status":"ok"}`. Both endpoints require GCP authentication;
`/backup` additionally checks a body password.

## Retention policy (3-tier)

After each successful upload, `/backup` prunes the Text It Backups folder to:

- **Daily** — keep every backup ≤ 30 days old.
- **Weekly** — for 30 days–6 months (≤ 183 days), keep the newest backup in each
  ISO week; trash the rest.
- **Monthly** — older than 6 months (> 183 days), keep the newest backup in each
  calendar month; trash the rest.

Details:

- **Keys on Drive file ID + `modifiedTime`, never filename** — a same-name
  collision was historically possible (local MVP + cloud both wrote
  `<date>_TextIt_backup.json` before the 2026-07-03 orchestrator cutover).
  Filename `yyyymmdd` is used only as a cross-check.
- **Only files ending `_TextIt_backup.json` are touched.** Anything else in the
  folder is left strictly alone.
- **Trash, not hard-delete.** Pruned files are moved to Drive Trash
  (`trashed=true`), recoverable for the Trash-retention window — a mis-prune is
  undoable.
- **Blast-radius guard.** If the computed delete set exceeds
  `RETENTION_MAX_DELETES` (60), the prune deletes **nothing** and returns
  `needs_review: true`. First real prune against a large accumulated backlog may
  exceed the cap — run `/prune` with `dry_run` first, review, and either raise
  the cap deliberately or sweep in passes.
- **A file dated today is never deleted**, independent of tier math.
- **Prune failure never fails the backup** — the backup already landed; a prune
  error is captured into the response under `retention`.

Controls on `/backup` body: `"prune": false` skips pruning; `"prune_dry_run":
true` computes and reports the delete set without trashing anything.

### `POST /prune` — retention without a backup

Runs the policy standalone (manual sweep / preview). Body `{"password": "...",
"dry_run": true|false}`. **`dry_run` defaults to `true`** — a bare authenticated
call is preview-only; deletion is opt-in with `"dry_run": false`.

## Config — Cloud Run console (NOT repo)

Per the cloud_run_service_deploy traps, env vars are console-managed:

- `TEXTIT_TOKEN` — TextIt API token.
- `SYNC_PASSWORD` — POST-body auth password.
- `DRIVE_FOLDER_ID` — default `1WX4ifa_C6-ofVEKLmWg-zL5VxWHgo6cy` (Text It Backups).
- `SHARED_DRIVE_ID` — default `0AIGheCp5gHV6Uk9PVA` (COS Shared Drive).

## Service account requirement

The Cloud Run runtime SA (default compute SA
`853176470965-compute@developer.gserviceaccount.com`) must be added as a member
of the COS Shared Drive — or have the Text It Backups folder shared to it —
with at least **Content Manager**. Otherwise the upload 403s/404s. This is the
service-account Drive path chosen over rclone to avoid the OAuth-token-in-Testing
7-day-expiry trap (see sheet-service).

## Lifts into the nightly orchestrator

`run_backup()` is the callable core, mirroring contacts-sync's `run_sync()`, for
the unified nightly orchestrator (backup → contacts-sync → state/vamc → vamc-sync).

## Retention

Not implemented here (ITDO-405 phase 2 3-tier pruning: daily 30d / weekly 6mo /
monthly thereafter). Files accumulate until pruning is built.
