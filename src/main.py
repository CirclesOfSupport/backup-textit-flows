import os
import io
import json
import hashlib
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
#
# Port of backup_textit_flows.ps1 (ITDO-405 MVP) to a Cloud Run service.
# Same export semantics: dependencies=all (UI "Select All" equivalent,
# restore-capable). Drive upload is done with a service-account (ambient ADC)
# via the Drive API, replacing the local rclone path — no OAuth refresh token,
# so no Testing-status 7-day-expiry trap.
#
# The Cloud Run runtime service account must be a member of (or have the target
# folder shared to it on) the Circles of Support Shared Drive with at least
# Content Manager, or the upload 404s/403s.
#
# Output shape: the restore-capable definitions bundle (flows/campaigns/triggers/
# groups/fields from definitions.json?dependencies=all) PLUS a top-level
# `flow_index` array — per-flow list-metadata from flows.json including the
# `archived` flag, which the definitions bundle does NOT carry. flow_index makes
# the backup (a) a true-state record (which flows were archived at backup time)
# and (b) a next-day reference (greppable uuid→name/status/modified_on map)
# without parsing the full definitions. Costs zero extra API calls — the flows.json
# pagination already runs to collect UUIDs; it now retains the metadata too.
# ---------------------------------------------------------------------------

TEXTIT_BASE = "https://textit.com/api/v2"
TEXTIT_TOKEN = os.environ.get("TEXTIT_TOKEN", "")

PAGE_SIZE = 250        # confirmed TextIt API max page size
BATCH_SIZE = 40        # UUIDs per definitions.json call (matches PS script)

# Drive target: Text It Backups folder inside the Circles of Support Shared Drive.
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "1WX4ifa_C6-ofVEKLmWg-zL5VxWHgo6cy")
SHARED_DRIVE_ID = os.environ.get("SHARED_DRIVE_ID", "0AIGheCp5gHV6Uk9PVA")

SYNC_PASSWORD = os.environ.get("SYNC_PASSWORD", "")

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

COLLECTIONS = ["flows", "campaigns", "triggers", "groups", "fields"]

# ---------------------------------------------------------------------------
# Retention (ITDO-405, 3-tier) — daily 30d / weekly 6mo / monthly thereafter.
#
# Policy (age measured from "now" at prune time, in the service's local tz —
# same tz used for the yyyymmdd filename stamp, so filename date and age agree):
#   Tier 1 (daily):   age <= 30 days           -> KEEP every file.
#   Tier 2 (weekly):  30 days < age <= 6 months -> KEEP the newest file in each
#                     ISO week (year, ISO-week#); delete the rest of that week.
#   Tier 3 (monthly): age > 6 months            -> KEEP the newest file in each
#                     calendar month (year, month); delete the rest of that month.
#
# "6 months" is defined as 183 days (RETENTION_WEEKLY_DAYS) to keep the boundary
# arithmetic pure-integer and tz-safe. 30 days = RETENTION_DAILY_DAYS.
#
# Keying is on Drive file ID + modifiedTime, NOT on filename — the entity
# (textit_flow_backup.md) requires this because a same-name collision was
# historically possible (local MVP + cloud both wrote `<date>_TextIt_backup.json`
# before the 2026-07-03 cutover). Old same-named duplicates may still sit in the
# folder; ID-keying prunes them independently. Filename yyyymmdd is used only as
# a cross-check against modifiedTime, never as the primary date source.
#
# SAFETY: prune only ever runs AFTER a successful upload in run_backup(), and it
# never deletes a file dated today, and it hard-caps how many deletes it will do
# in one run (RETENTION_MAX_DELETES) as a blast-radius guard against a bad clock
# or a mis-parse. If more than the cap would be deleted, it deletes nothing and
# reports needs_review=True for a human to look. Dry-run supported.
# ---------------------------------------------------------------------------

RETENTION_DAILY_DAYS = 30       # tier-1 cutoff (keep all within)
RETENTION_WEEKLY_DAYS = 183     # tier-2 cutoff ("6 months"); beyond -> monthly
RETENTION_MAX_DELETES = 60      # blast-radius guard; over this, prune aborts
BACKUP_NAME_SUFFIX = "_TextIt_backup.json"   # only files matching this are pruned


# ---------------------------------------------------------------------------
# TextIt pull
# ---------------------------------------------------------------------------

def _headers():
    return {"Authorization": f"Token {TEXTIT_TOKEN}"}


# Per-flow metadata fields kept from the flows.json list endpoint. This is the
# flow-LIST metadata (archive state, name, timestamps, run counts) — NOT present
# in the definitions.json?dependencies=all bundle, which carries flow *logic* but
# no `archived` field. Capturing it here adds ZERO extra API calls: the backup
# already paginates flows.json to collect UUIDs; it previously discarded
# everything except the uuid. See RapidPro/TextIt API v2 flows endpoint schema.
FLOW_INDEX_FIELDS = [
    "uuid", "name", "type", "archived",
    "created_on", "modified_on", "expires", "runs", "labels",
]


def collect_flows():
    """GET /flows.json paginated; return (uuids, flow_index).

    uuids: deduped list of flow UUIDs (drives definitions.json batching).
    flow_index: per-flow list-metadata (uuid, name, type, ARCHIVED, timestamps,
    run counts, labels) — the archive-state + next-day-reference layer. Reuses
    the same pagination; no extra API calls.

    Raises if zero flows — empty-backup guard (matches PS script)."""
    uuids = []
    flow_index = []
    seen = set()
    nxt = f"{TEXTIT_BASE}/flows.json?page_size={PAGE_SIZE}"
    while nxt:
        resp = requests.get(nxt, headers=_headers(), timeout=120)
        resp.raise_for_status()
        data = resp.json()
        for f in data.get("results", []):
            uuid = f.get("uuid")
            if not uuid or uuid in seen:
                continue
            seen.add(uuid)
            uuids.append(uuid)
            flow_index.append({k: f.get(k) for k in FLOW_INDEX_FIELDS})
        nxt = data.get("next")
    archived_count = sum(1 for f in flow_index if f.get("archived"))
    logger.info(f"{len(uuids)} flows collected ({archived_count} archived)")
    if not uuids:
        raise RuntimeError("No flows returned from flows.json — aborting (empty backup guard).")
    return uuids, flow_index


def pull_definitions(uuids):
    """GET /definitions.json?dependencies=all batched BATCH_SIZE UUIDs/call."""
    batches = []
    for i in range(0, len(uuids), BATCH_SIZE):
        slice_ = uuids[i:i + BATCH_SIZE]
        query = "&".join(f"flow={u}" for u in slice_)
        uri = f"{TEXTIT_BASE}/definitions.json?{query}&dependencies=all"
        logger.info(f"batch {i // BATCH_SIZE + 1}: {len(slice_)} flows")
        resp = requests.get(uri, headers=_headers(), timeout=300)
        resp.raise_for_status()
        batches.append(resp.json())
    return batches


# ---------------------------------------------------------------------------
# Merge — one workspace-shaped JSON, dedupe per collection by uuid (hash
# fallback for objects without uuid, e.g. triggers). Matches PS Add-Unique.
# ---------------------------------------------------------------------------

def merge_batches(batches):
    merged = {"version": None, "site": None,
              "flows": [], "campaigns": [], "triggers": [], "groups": [], "fields": []}
    seen = {c: set() for c in COLLECTIONS}

    def add_unique(collection, items):
        if not items:
            return
        for item in items:
            uuid = item.get("uuid") if isinstance(item, dict) else None
            if uuid:
                key = str(uuid)
            else:
                key = hashlib.sha256(
                    json.dumps(item, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
            if key not in seen[collection]:
                seen[collection].add(key)
                merged[collection].append(item)

    for b in batches:
        if merged["version"] is None and b.get("version"):
            merged["version"] = b["version"]
        if merged["site"] is None and b.get("site"):
            merged["site"] = b["site"]
        for c in COLLECTIONS:
            add_unique(c, b.get(c))

    return merged


# ---------------------------------------------------------------------------
# Drive upload (service-account ADC). Replaces rclone.
# ---------------------------------------------------------------------------

def get_drive_client():
    creds, _ = google.auth.default(scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)


def upload_to_drive(filename, content_bytes):
    """Create the dated backup file in the Text It Backups Shared Drive folder.
    supportsAllDrives=True is required for Shared Drive (Team Drive) targets."""
    return _upload_with_client(get_drive_client(), filename, content_bytes)


def _upload_with_client(service, filename, content_bytes):
    media = MediaIoBaseUpload(
        io.BytesIO(content_bytes),
        mimetype="application/json",
        resumable=True,
    )
    metadata = {"name": filename, "parents": [DRIVE_FOLDER_ID]}
    created = service.files().create(
        body=metadata,
        media_body=media,
        fields="id,name",
        supportsAllDrives=True,
    ).execute()
    logger.info(f"Uploaded {created.get('name')} (id={created.get('id')})")
    return created.get("id")


# ---------------------------------------------------------------------------
# Retention pruning
# ---------------------------------------------------------------------------

def list_backup_files(service):
    """List every backup file in the Text It Backups folder.

    Returns a list of dicts: {id, name, modifiedTime (datetime, tz-aware UTC)}.
    Only files whose name ends with BACKUP_NAME_SUFFIX are returned — anything
    else in the folder is left strictly alone. Pages through all results
    (page_size 1000). supportsAllDrives/includeItemsFromAllDrives required for
    the Shared Drive.
    """
    files = []
    page_token = None
    # trashed=false so we don't re-count already-trashed files; name filter is a
    # coarse server-side narrow, the suffix check below is the authoritative one.
    q = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, modifiedTime)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="drive",
            driveId=SHARED_DRIVE_ID,
            pageToken=page_token,
        ).execute()
        for f in resp.get("files", []):
            name = f.get("name", "")
            if not name.endswith(BACKUP_NAME_SUFFIX):
                continue
            mt = f.get("modifiedTime")
            if not mt:
                continue
            # Drive returns RFC-3339 UTC, e.g. 2026-07-03T22:08:11.123Z
            dt = datetime.fromisoformat(mt.replace("Z", "+00:00"))
            files.append({"id": f["id"], "name": name, "modifiedTime": dt})
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def _filename_date(name):
    """Parse the leading yyyymmdd from a backup filename. Cross-check only —
    returns a date or None. Never the primary date source (modifiedTime is)."""
    stem = name[:8]
    if len(stem) == 8 and stem.isdigit():
        try:
            return datetime.strptime(stem, "%Y%m%d").date()
        except ValueError:
            return None
    return None


def compute_prune_set(files, now):
    """Given the file list and a tz-aware `now`, return (keep, delete) partitions.

    Deterministic, no side effects — this is the testable core of retention.
    Each file dict is bucketed by age (now - modifiedTime):
      tier 1 (<=30d): always keep.
      tier 2 (>30d..<=183d): keep newest per ISO (year, week); delete rest.
      tier 3 (>183d): keep newest per (year, month); delete rest.
    A file dated today (same local date as now) is never deleted, belt-and-braces.
    """
    daily, weekly, monthly = [], [], []
    today_local = now.astimezone().date()
    for f in files:
        age_days = (now - f["modifiedTime"]).days
        if age_days <= RETENTION_DAILY_DAYS:
            daily.append(f)
        elif age_days <= RETENTION_WEEKLY_DAYS:
            weekly.append(f)
        else:
            monthly.append(f)

    keep, delete = list(daily), []

    def keep_newest_per_bucket(group, key_fn):
        buckets = {}
        for f in group:
            buckets.setdefault(key_fn(f), []).append(f)
        for _, members in buckets.items():
            members.sort(key=lambda x: x["modifiedTime"], reverse=True)
            keep.append(members[0])
            for extra in members[1:]:
                delete.append(extra)

    def iso_week_key(f):
        iso = f["modifiedTime"].astimezone().isocalendar()
        return (iso[0], iso[1])          # (ISO year, ISO week)

    def month_key(f):
        d = f["modifiedTime"].astimezone()
        return (d.year, d.month)

    keep_newest_per_bucket(weekly, iso_week_key)
    keep_newest_per_bucket(monthly, month_key)

    # Never delete a file whose local modified-date is today (guard against a
    # today file somehow landing outside tier 1 via clock skew).
    delete = [f for f in delete
              if f["modifiedTime"].astimezone().date() != today_local]

    return keep, delete


def prune_retention(service, now=None, dry_run=False):
    """Apply the 3-tier retention policy to the Text It Backups folder.

    Runs AFTER a successful upload. Blast-radius guarded: if the computed delete
    set exceeds RETENTION_MAX_DELETES, deletes NOTHING and returns
    needs_review=True. Deletion is Drive trash (files().delete on a Shared Drive
    permanently removes; we use trash via update to keep it recoverable) —
    actually we set trashed=True so a mis-prune is recoverable from Drive Trash.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    files = list_backup_files(service)
    keep, delete = compute_prune_set(files, now)

    result = {
        "total_backup_files": len(files),
        "keep_count": len(keep),
        "delete_count": len(delete),
        "dry_run": dry_run,
        "needs_review": False,
        "deleted": [],
    }

    if len(delete) > RETENTION_MAX_DELETES:
        result["needs_review"] = True
        result["message"] = (
            f"Prune would delete {len(delete)} files (> cap "
            f"{RETENTION_MAX_DELETES}); deleting nothing. Inspect the folder."
        )
        logger.warning(result["message"])
        return result

    if dry_run:
        result["deleted"] = [{"id": f["id"], "name": f["name"],
                              "modifiedTime": f["modifiedTime"].isoformat()}
                             for f in delete]
        return result

    for f in delete:
        try:
            # Trash (recoverable) rather than hard-delete — mis-prune is undoable
            # from Drive Trash for the Trash-retention window.
            service.files().update(
                fileId=f["id"],
                body={"trashed": True},
                supportsAllDrives=True,
            ).execute()
            result["deleted"].append({"id": f["id"], "name": f["name"],
                                      "modifiedTime": f["modifiedTime"].isoformat()})
            logger.info(f"Pruned (trashed) {f['name']} (id={f['id']})")
        except Exception as e:
            logger.warning(f"Failed to trash {f['name']} (id={f['id']}): {e}")

    result["delete_count"] = len(result["deleted"])
    return result


# ---------------------------------------------------------------------------
# Core callable — lifts into the nightly orchestrator
# ---------------------------------------------------------------------------

def run_backup(prune=True, prune_dry_run=False):
    uuids, flow_index = collect_flows()
    batches = pull_definitions(uuids)
    merged = merge_batches(batches)

    # Fold the flow-list metadata (archive state + reference index) in as a
    # top-level key alongside the restore-capable definitions collections. The
    # definitions collections are untouched — restore fidelity is unchanged. A
    # recovery that parses the backup for individual flows reads `flows` and
    # ignores `flow_index`; nothing hands the whole file to TextIt's importer,
    # so the extra key is safe.
    merged["flow_index"] = flow_index
    merged["backed_up_on"] = datetime.now(timezone.utc).isoformat()

    counts = {c: len(merged[c]) for c in COLLECTIONS}
    archived_count = sum(1 for f in flow_index if f.get("archived"))

    # Sanity checks (warn, don't silently pass):
    # 1. every collected flow should appear in the restore-capable definitions.
    count_mismatch = len(merged["flows"]) != len(uuids)
    if count_mismatch:
        logger.warning(
            f"Merged flow count ({len(merged['flows'])}) != collected flow count "
            f"({len(uuids)}). Inspect before trusting this backup."
        )
    # 2. flow_index should have one entry per collected flow.
    index_mismatch = len(flow_index) != len(uuids)
    if index_mismatch:
        logger.warning(
            f"flow_index count ({len(flow_index)}) != collected flow count "
            f"({len(uuids)}). Inspect before trusting this backup."
        )

    # UTF-8, no BOM (matches PS WriteAllText with UTF8Encoding(false)).
    content_bytes = json.dumps(merged, ensure_ascii=False).encode("utf-8")

    stamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d")
    filename = f"{stamp}_TextIt_backup.json"
    service = get_drive_client()
    file_id = _upload_with_client(service, filename, content_bytes)

    result = {
        "status": "success",
        "filename": filename,
        "drive_file_id": file_id,
        "bytes": len(content_bytes),
        "flow_uuids_collected": len(uuids),
        "flow_index_count": len(flow_index),
        "archived_count": archived_count,
        "counts": counts,
        "count_mismatch": count_mismatch,
        "index_mismatch": index_mismatch,
    }

    # Retention runs ONLY after a successful upload, and reuses the same Drive
    # client. A prune failure must not fail the backup (the backup already
    # landed) — capture the error into the result instead of raising.
    if prune:
        try:
            result["retention"] = prune_retention(
                service, dry_run=prune_dry_run
            )
        except Exception as e:
            logger.exception("Retention prune failed (backup already uploaded)")
            result["retention"] = {"status": "error", "message": str(e)}

    return result


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/backup", methods=["POST"])
def backup():
    body = request.get_json(force=True, silent=True) or {}
    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    if not TEXTIT_TOKEN:
        return jsonify({"status": "error", "message": "TEXTIT_TOKEN not set"}), 500
    # prune defaults ON (nightly path). Callers can disable ("prune": false) or
    # preview ("prune_dry_run": true) without touching Drive.
    prune = body.get("prune", True)
    prune_dry_run = body.get("prune_dry_run", False)
    try:
        result = run_backup(prune=prune, prune_dry_run=prune_dry_run)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("Backup failed")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/prune", methods=["POST"])
def prune():
    """Run retention pruning WITHOUT taking a backup. For manual sweeps and for
    a dry-run preview of what the policy would remove. Same password auth.
    Body: {"password": "...", "dry_run": true|false}. dry_run defaults True so a
    bare authenticated call is preview-only — deletion is opt-in."""
    body = request.get_json(force=True, silent=True) or {}
    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    dry_run = body.get("dry_run", True)
    try:
        service = get_drive_client()
        result = prune_retention(service, dry_run=dry_run)
        result["status"] = "success"
        return jsonify(result), 200
    except Exception as e:
        logger.exception("Prune failed")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
