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
# TextIt pull
# ---------------------------------------------------------------------------

def _headers():
    return {"Authorization": f"Token {TEXTIT_TOKEN}"}


def collect_flow_uuids():
    """GET /flows.json paginated; collect + dedupe all flow UUIDs.
    Raises if zero — empty-backup guard (matches PS script)."""
    uuids = []
    nxt = f"{TEXTIT_BASE}/flows.json?page_size={PAGE_SIZE}"
    while nxt:
        resp = requests.get(nxt, headers=_headers(), timeout=120)
        resp.raise_for_status()
        data = resp.json()
        for f in data.get("results", []):
            if f.get("uuid"):
                uuids.append(f["uuid"])
        nxt = data.get("next")
    uuids = list(dict.fromkeys(uuids))  # dedupe, preserve order
    logger.info(f"{len(uuids)} flow UUIDs collected")
    if not uuids:
        raise RuntimeError("No flow UUIDs returned from flows.json — aborting (empty backup guard).")
    return uuids


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
    service = get_drive_client()
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
# Core callable — lifts into the nightly orchestrator
# ---------------------------------------------------------------------------

def run_backup():
    uuids = collect_flow_uuids()
    batches = pull_definitions(uuids)
    merged = merge_batches(batches)

    counts = {c: len(merged[c]) for c in COLLECTIONS}
    count_mismatch = merged["flows"].__len__() != len(uuids)
    if count_mismatch:
        logger.warning(
            f"Merged flow count ({len(merged['flows'])}) != collected UUID count "
            f"({len(uuids)}). Inspect before trusting this backup."
        )

    # UTF-8, no BOM (matches PS WriteAllText with UTF8Encoding(false)).
    content_bytes = json.dumps(merged, ensure_ascii=False).encode("utf-8")

    stamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d")
    filename = f"{stamp}_TextIt_backup.json"
    file_id = upload_to_drive(filename, content_bytes)

    return {
        "status": "success",
        "filename": filename,
        "drive_file_id": file_id,
        "bytes": len(content_bytes),
        "flow_uuids_collected": len(uuids),
        "counts": counts,
        "count_mismatch": count_mismatch,
    }


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
    try:
        result = run_backup()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("Backup failed")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
