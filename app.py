"""
e-GMAT Purchase Tracker
- Fetches purchase history from e-GMAT Student Database API
- Stores data in SQLite, auto-refreshes every 30 minutes
- Flask UI with Dashboard + Add Email tabs
"""

import os
import sqlite3
import json
import time
import threading
import logging
import atexit
from datetime import datetime

import requests
import anthropic
from flask import Flask, render_template, request, redirect, url_for, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_URL = os.environ.get("EGMAT_API_URL", "https://srv1079050.hstgr.cloud/student-db-mcp/")
API_KEY = os.environ.get("EGMAT_API_KEY", "")
DB_PATH = os.environ.get("DB_PATH", "/app/data/purchase_tracker.db")
REFRESH_INTERVAL_MINUTES = int(os.environ.get("REFRESH_INTERVAL_MINUTES", "30"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_prefix=1)

# Anthropic client for AI features (name guessing)
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS tracked_emails (
            email TEXT PRIMARY KEY,
            added_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            purchase_id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            purchase_date TEXT,
            product TEXT,
            product_code TEXT,
            duration_in_days INTEGER,
            amount REAL,
            currency TEXT,
            payment_method TEXT,
            stripe_payment_id TEXT,
            charge_id TEXT,
            receipt_url TEXT,
            status TEXT,
            discount_code TEXT,
            last_updated TEXT NOT NULL,
            FOREIGN KEY (email) REFERENCES tracked_emails(email)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS refresh_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            emails_processed INTEGER,
            purchases_upserted INTEGER,
            errors INTEGER
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS webhook_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            received_at TEXT NOT NULL
        )
    """)
    # Migration: add first_name column if it doesn't exist
    try:
        c.execute("ALTER TABLE tracked_emails ADD COLUMN first_name TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.commit()
    conn.close()


def guess_first_name(email):
    """Use Claude Sonnet 4.6 to guess the first name from an email address."""
    if not anthropic_client:
        log.warning("No ANTHROPIC_API_KEY set, skipping name guess for %s", email)
        return ""
    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=50,
            messages=[{
                "role": "user",
                "content": (
                    f"Given this email address, guess the person's first name. "
                    f"Reply with ONLY the first name, capitalized properly. "
                    f"If you truly cannot guess, reply with just a dash (-). "
                    f"Email: {email}"
                ),
            }],
        )
        name = message.content[0].text.strip()
        log.info("Guessed first name for %s: %s", email, name)
        return name
    except Exception as e:
        log.error("Failed to guess name for %s: %s", email, e)
        return ""


def seed_emails(email_list):
    """Insert initial emails if they don't already exist."""
    conn = get_db()
    now = datetime.utcnow().isoformat()
    for email in email_list:
        email = email.strip().lower()
        if email:
            conn.execute(
                "INSERT OR IGNORE INTO tracked_emails (email, added_at) VALUES (?, ?)",
                (email, now),
            )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# e-GMAT API client (JSON-RPC over HTTP with MCP session)
# ---------------------------------------------------------------------------

class EgmatAPIClient:
    def __init__(self):
        self.session_id = None
        self._lock = threading.Lock()

    def _init_session(self):
        """Initialize an MCP session and store the session ID."""
        resp = requests.post(
            API_URL,
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "purchase-tracker", "version": "1.0"},
                },
            },
            timeout=30,
        )
        # Response is SSE format: "event: message\ndata: {...}\n\n"
        session_id = resp.headers.get("mcp-session-id")
        if not session_id:
            raise RuntimeError(f"Failed to get session ID. Status: {resp.status_code}, Body: {resp.text[:300]}")
        self.session_id = session_id
        log.info("Initialized API session: %s", session_id[:12] + "...")

    def _ensure_session(self):
        if not self.session_id:
            self._init_session()

    def _parse_sse_response(self, text):
        """Parse SSE response to extract JSON data."""
        for line in text.strip().split("\n"):
            if line.startswith("data: "):
                return json.loads(line[6:])
        raise RuntimeError(f"No data line in SSE response: {text[:300]}")

    def get_purchase_history(self, email):
        """Fetch purchase history for a single email. Returns list of purchase dicts."""
        with self._lock:
            self._ensure_session()

        resp = requests.post(
            API_URL,
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
                "Mcp-Session-Id": self.session_id,
            },
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "Get_Purchase_History",
                    "arguments": {"username": email.lower()},
                },
            },
            timeout=30,
        )

        # If session expired, re-init and retry once
        if resp.status_code in (400, 401, 404):
            with self._lock:
                self._init_session()
            resp = requests.post(
                API_URL,
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json",
                    "Mcp-Session-Id": self.session_id,
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "Get_Purchase_History",
                        "arguments": {"username": email.lower()},
                    },
                },
                timeout=30,
            )

        parsed = self._parse_sse_response(resp.text)

        if parsed.get("result", {}).get("isError"):
            raise RuntimeError(f"API error for {email}: {parsed}")

        content = parsed["result"]["content"][0]["text"]
        data = json.loads(content)
        return data.get("result", [])


api_client = EgmatAPIClient()

# ---------------------------------------------------------------------------
# Refresh logic
# ---------------------------------------------------------------------------

def refresh_all_emails():
    """Fetch purchase history for every tracked email and upsert into DB."""
    log.info("Starting refresh cycle...")
    conn = get_db()
    emails = [row["email"] for row in conn.execute("SELECT email FROM tracked_emails").fetchall()]
    conn.close()

    started_at = datetime.utcnow().isoformat()
    now = datetime.utcnow().isoformat()
    total_upserted = 0
    total_errors = 0

    for email in emails:
        try:
            purchases = api_client.get_purchase_history(email)
            conn = get_db()
            for p in purchases:
                conn.execute("""
                    INSERT INTO purchases
                        (purchase_id, email, purchase_date, product, product_code,
                         duration_in_days, amount, currency, payment_method,
                         stripe_payment_id, charge_id, receipt_url, status,
                         discount_code, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(purchase_id) DO UPDATE SET
                        purchase_date=excluded.purchase_date,
                        product=excluded.product,
                        product_code=excluded.product_code,
                        duration_in_days=excluded.duration_in_days,
                        amount=excluded.amount,
                        currency=excluded.currency,
                        payment_method=excluded.payment_method,
                        stripe_payment_id=excluded.stripe_payment_id,
                        charge_id=excluded.charge_id,
                        receipt_url=excluded.receipt_url,
                        status=excluded.status,
                        discount_code=excluded.discount_code,
                        last_updated=excluded.last_updated
                """, (
                    p["purchase_id"], p["email"], p.get("purchase_date"),
                    p.get("product"), p.get("product_code"),
                    p.get("duration_in_days"), p.get("amount"),
                    p.get("currency"), p.get("payment_method"),
                    p.get("stripe_payment_id"), p.get("charge_id"),
                    p.get("receipt_url"), p.get("status"),
                    p.get("discount_code"), now,
                ))
            conn.commit()
            conn.close()
            total_upserted += len(purchases)
            log.info("  %s -> %d purchases", email, len(purchases))
        except Exception as e:
            total_errors += 1
            log.error("  %s -> ERROR: %s", email, e)
        # Small delay to avoid hammering the API
        time.sleep(0.3)

    finished_at = datetime.utcnow().isoformat()
    conn = get_db()
    conn.execute(
        "INSERT INTO refresh_log (started_at, finished_at, emails_processed, purchases_upserted, errors) VALUES (?, ?, ?, ?, ?)",
        (started_at, finished_at, len(emails), total_upserted, total_errors),
    )
    conn.commit()
    conn.close()
    log.info("Refresh done: %d emails, %d purchases upserted, %d errors", len(emails), total_upserted, total_errors)


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return {"status": "ok"}, 200


@app.route("/")
def dashboard():
    conn = get_db()
    # Get all purchases joined with tracked emails
    purchases = conn.execute("""
        SELECT p.email, t.first_name, p.purchase_date, p.product, p.product_code,
               p.amount, p.currency, p.discount_code, p.status, p.receipt_url
        FROM purchases p
        JOIN tracked_emails t ON p.email = t.email
        ORDER BY p.purchase_date DESC
    """).fetchall()

    # Get emails with no purchases
    emails_with_purchases = conn.execute("""
        SELECT DISTINCT email FROM purchases
    """).fetchall()
    emails_with_purchases_set = {row["email"] for row in emails_with_purchases}

    all_emails = conn.execute("SELECT email FROM tracked_emails ORDER BY email").fetchall()
    emails_no_purchases = [row["email"] for row in all_emails if row["email"] not in emails_with_purchases_set]

    # Last refresh info
    last_refresh = conn.execute(
        "SELECT * FROM refresh_log ORDER BY id DESC LIMIT 1"
    ).fetchone()

    total_emails = len(all_emails)
    total_purchases = len(purchases)

    conn.close()
    return render_template(
        "index.html",
        tab="dashboard",
        purchases=purchases,
        emails_no_purchases=emails_no_purchases,
        last_refresh=last_refresh,
        total_emails=total_emails,
        total_purchases=total_purchases,
    )


@app.route("/add-email", methods=["GET"])
def add_email_page():
    conn = get_db()
    all_emails = conn.execute("SELECT email, added_at, first_name FROM tracked_emails ORDER BY email").fetchall()
    conn.close()
    return render_template("index.html", tab="add", all_emails=all_emails)


@app.route("/add-email", methods=["POST"])
def add_email():
    email = request.form.get("email", "").strip().lower()
    if not email or "@" not in email:
        return redirect(url_for("add_email_page"))

    first_name = guess_first_name(email)
    conn = get_db()
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO tracked_emails (email, added_at, first_name) VALUES (?, ?, ?)",
        (email, now, first_name),
    )
    conn.commit()
    conn.close()

    # Fetch this email's data immediately in background
    def fetch_single():
        try:
            purchases = api_client.get_purchase_history(email)
            conn = get_db()
            now_inner = datetime.utcnow().isoformat()
            for p in purchases:
                conn.execute("""
                    INSERT INTO purchases
                        (purchase_id, email, purchase_date, product, product_code,
                         duration_in_days, amount, currency, payment_method,
                         stripe_payment_id, charge_id, receipt_url, status,
                         discount_code, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(purchase_id) DO UPDATE SET
                        purchase_date=excluded.purchase_date,
                        product=excluded.product,
                        last_updated=excluded.last_updated
                """, (
                    p["purchase_id"], p["email"], p.get("purchase_date"),
                    p.get("product"), p.get("product_code"),
                    p.get("duration_in_days"), p.get("amount"),
                    p.get("currency"), p.get("payment_method"),
                    p.get("stripe_payment_id"), p.get("charge_id"),
                    p.get("receipt_url"), p.get("status"),
                    p.get("discount_code"), now_inner,
                ))
            conn.commit()
            conn.close()
            log.info("Fetched %d purchases for newly added %s", len(purchases), email)
        except Exception as e:
            log.error("Error fetching for %s: %s", email, e)

    threading.Thread(target=fetch_single, daemon=True).start()
    return redirect(url_for("add_email_page"))


@app.route("/remove-email", methods=["POST"])
def remove_email():
    email = request.form.get("email", "").strip().lower()
    if email:
        conn = get_db()
        conn.execute("DELETE FROM purchases WHERE email = ?", (email,))
        conn.execute("DELETE FROM tracked_emails WHERE email = ?", (email,))
        conn.commit()
        conn.close()
    return redirect(url_for("add_email_page"))


@app.route("/webhook", methods=["POST"])
def webhook():
    """Receive webhook from Customer.io with an email address."""
    try:
        data = request.get_json(force=True)
        if data is None:
            return jsonify({"error": "invalid JSON"}), 400
    except Exception:
        return jsonify({"error": "could not parse body"}), 400

    email = data.get("email", "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"error": "missing or invalid 'email' field"}), 400

    conn = get_db()
    conn.execute(
        "INSERT INTO webhook_emails (email, received_at) VALUES (?, ?)",
        (email, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()
    log.info("Webhook received: email=%s", email)
    return jsonify({"status": "received", "email": email}), 200


@app.route("/webhooks")
def webhooks_page():
    """Display all emails received via webhook."""
    conn = get_db()
    webhook_emails = conn.execute(
        "SELECT id, email, received_at FROM webhook_emails ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return render_template(
        "index.html",
        tab="webhooks",
        webhook_emails=webhook_emails,
        total_webhook_emails=len(webhook_emails),
    )


@app.route("/refresh", methods=["POST"])
def manual_refresh():
    """Trigger a manual refresh in background."""
    threading.Thread(target=refresh_all_emails, daemon=True).start()
    return redirect(url_for("dashboard"))


@app.route("/api/status")
def api_status():
    conn = get_db()
    total_emails = conn.execute("SELECT COUNT(*) FROM tracked_emails").fetchone()[0]
    total_purchases = conn.execute("SELECT COUNT(*) FROM purchases").fetchone()[0]
    last_refresh = conn.execute("SELECT * FROM refresh_log ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return jsonify({
        "total_emails": total_emails,
        "total_purchases": total_purchases,
        "last_refresh": dict(last_refresh) if last_refresh else None,
    })


# ---------------------------------------------------------------------------
# Initial email list
# ---------------------------------------------------------------------------

INITIAL_EMAILS = [
    "13salonisharma@gmail.com",
    "hitesh.egmat@gmail.com",
    "abhaychaudharydps@gmail.com",
]


# ---------------------------------------------------------------------------
# Startup: init DB, seed emails, start scheduler
# ---------------------------------------------------------------------------

init_db()
seed_emails(INITIAL_EMAILS)
log.info("Seeded %d emails into tracker", len(INITIAL_EMAILS))

scheduler = BackgroundScheduler()
scheduler.add_job(refresh_all_emails, "interval", minutes=REFRESH_INTERVAL_MINUTES)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())
log.info("Scheduler started: refresh every %d minutes", REFRESH_INTERVAL_MINUTES)

# Run initial refresh in background
threading.Thread(target=refresh_all_emails, daemon=True).start()

# ---------------------------------------------------------------------------
# Main (local dev only — production uses gunicorn)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
