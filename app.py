"""
Bulletproof Discord role assignment service (single-file)

Features:
- Flask web app for OAuth + Stripe webhook endpoints
- Discord bot with members intent
- Immediate role assignment after OAuth (and after Stripe events)
- Robust retry logic with exponential backoff (async)
- Persistent retry queue using SQLite so retries survive restarts
- Detailed logging for every step (successes, failures, retries)
- Confirmation check after assigning/removing a role
- Periodic background worker that processes the queue

Notes:
- Make sure the following environment variables are set:
  DISCORD_BOT_TOKEN, DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET, DISCORD_REDIRECT_URI,
  DISCORD_GUILD_ID, ROLE_5K_TO_50K, ROLE_ELITE, ROLE_PLUS,
  STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, GOOGLE_APPS_SCRIPT_URL,
  FLASK_SECRET_KEY (optional)
- Deploy this script as your app entrypoint (e.g., app.py)
"""

import os
import json
import sqlite3
import time
import threading
import requests
import asyncio
from datetime import datetime
from urllib.parse import urlencode
from flask import Flask, request, redirect, session, jsonify
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

# ------------------------------
# Environment / Configuration
# ------------------------------
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI")
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))

# Roles (discord role IDs)
ROLE_5K_TO_50K = int(os.getenv("ROLE_5K_TO_50K", "0"))
ROLE_ELITE = int(os.getenv("ROLE_ELITE", "0"))
ROLE_PLUS = int(os.getenv("ROLE_PLUS", "0"))

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
GOOGLE_APPS_SCRIPT_URL = os.getenv("GOOGLE_APPS_SCRIPT_URL")

# Tuneable retry settings
MAX_ATTEMPTS = 5
INITIAL_BACKOFF = 2  # seconds
BACKOFF_MULTIPLIER = 2

DATABASE_PATH = os.getenv("ASSIGNMENT_DB_PATH", "assignment_queue.db")

# Maps plan name -> role id
PLAN_TO_ROLE = {
    "5K to 50K Challenge": ROLE_5K_TO_50K,
    "MarketWave Elite": ROLE_ELITE,
    "MarketWave Plus": ROLE_PLUS,
}

# Stripe price ids - keep if you use checkout session creation in callback
PLAN_TO_PRICE_ID = {
    "MarketWave Plus": "price_1RdEoc08Ntv6wEBmifMeruFq",
    "MarketWave Elite": "price_1RdEob08Ntv6wEBmT27qALuM",
    "5K to 50K Challenge": "price_1RdEoc08Ntv6wEBmUZOADdMd"
}

# ------------------------------
# SQLite queue persistence
# ------------------------------
def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discord_id TEXT NOT NULL,
            plan TEXT NOT NULL,
            action TEXT NOT NULL, -- 'assign' or 'remove'
            email TEXT,
            stripe_subscription_id TEXT,
            attempts INTEGER DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.commit()
    conn.close()

def add_assignment_to_db(discord_id, plan, action, email=None, stripe_subscription_id=None):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO assignments (discord_id, plan, action, email, stripe_subscription_id) VALUES (?, ?, ?, ?, ?)",
        (str(discord_id), plan, action, email, stripe_subscription_id)
    )
    conn.commit()
    row_id = c.lastrowid
    conn.close()
    print(f"[DB] Queued assignment id={row_id} discord_id={discord_id} plan={plan} action={action}")
    return row_id

def get_pending_assignments(limit=50):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute("SELECT id, discord_id, plan, action, email, stripe_subscription_id, attempts FROM assignments ORDER BY created_at ASC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

def increment_attempts_and_log_error(row_id, error_text):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute("UPDATE assignments SET attempts = attempts + 1, last_error = ? WHERE id = ?", (error_text, row_id))
    conn.commit()
    conn.close()

def remove_assignment(row_id):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM assignments WHERE id = ?", (row_id,))
    conn.commit()
    conn.close()
    print(f"[DB] Removed assignment id={row_id}")

# ------------------------------
# Discord bot setup
# ------------------------------
intents = discord.Intents.default()
intents.members = True  # required to fetch members reliably
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"[Discord] Bot logged in as {bot.user} (id={bot.user.id})")
    # start the background worker once bot is ready
    bot.loop.create_task(queue_processor_loop())

# Core worker: attempts to assign or remove a role and confirms
async def assign_or_remove_role(discord_id: str, plan: str, action: str):
    """
    Returns (True, None) on success.
    Returns (False, "error message") on failure.
    """
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        return False, f"Guild {DISCORD_GUILD_ID} not found"

    try:
        # fetch_member bypasses cache and ensures we have the current member object
        member = await guild.fetch_member(int(discord_id))
    except discord.NotFound:
        return False, f"Member {discord_id} not found in guild {guild.id}"
    except Exception as e:
        return False, f"Error fetching member {discord_id}: {e}"

    role_id = PLAN_TO_ROLE.get(plan)
    if not role_id:
        return False, f"No role mapping for plan '{plan}'"

    role = guild.get_role(int(role_id))
    if not role:
        return False, f"Role object for id {role_id} not found in guild"

    try:
        if action == "assign":
            # If already has role, success
            if role in member.roles:
                return True, None
            await member.add_roles(role, reason="Subscription activation")
            # Confirm
            member = await guild.fetch_member(int(discord_id))
            if role in member.roles:
                return True, None
            else:
                return False, "Role assignment didn't persist (post-check failed)"
        elif action == "remove":
            if role not in member.roles:
                return True, None
            await member.remove_roles(role, reason="Subscription canceled")
            member = await guild.fetch_member(int(discord_id))
            if role not in member.roles:
                return True, None
            else:
                return False, "Role removal didn't persist (post-check failed)"
        else:
            return False, f"Unknown action '{action}'"
    except discord.Forbidden:
        return False, "Discord Forbidden: missing Manage Roles permission or role hierarchy issue"
    except discord.HTTPException as e:
        return False, f"Discord HTTPException: {e}"
    except Exception as e:
        return False, f"Unexpected exception: {e}"

# Exponential backoff wrapper for a single DB assignment row
async def process_assignment_row(row):
    """
    row: (id, discord_id, plan, action, email, stripe_subscription_id, attempts)
    """
    row_id, discord_id, plan, action, email, stripe_subscription_id, attempts = row
    attempts = attempts or 0

    backoff = INITIAL_BACKOFF * (BACKOFF_MULTIPLIER ** attempts)
    if attempts > 0:
        print(f"[Retry] id={row_id} discord_id={discord_id} plan={plan} action={action} attempts={attempts} waiting {backoff}s before retry")
        await asyncio.sleep(backoff)

    success, err = await assign_or_remove_role(discord_id, plan, action)
    if success:
        print(f"[Success] id={row_id} discord_id={discord_id} plan={plan} action={action}")
        remove_assignment(row_id)
    else:
        error_text = f"{datetime.utcnow().isoformat()} | {err}"
        print(f"[Fail] id={row_id} discord_id={discord_id} plan={plan} action={action} attempt={attempts + 1} error={err}")
        increment_attempts_and_log_error(row_id, error_text)
        # If we've reached max attempts, keep it in DB but print an alert and optionally notify admins
        if attempts + 1 >= MAX_ATTEMPTS:
            print(f"[Alert] id={row_id} reached max attempts ({MAX_ATTEMPTS}). Manual intervention may be required. last_error={err}")

# Loop that processes queue periodically
async def queue_processor_loop():
    await bot.wait_until_ready()
    print("[Worker] Starting queue processor loop")
    while True:
        try:
            rows = get_pending_assignments(limit=50)
            if not rows:
                # nothing pending; sleep a bit
                await asyncio.sleep(5)
                continue

            # process tasks concurrently but bounded
            tasks = []
            for row in rows:
                # Each row processed independently
                tasks.append(asyncio.create_task(process_assignment_row(row)))

            # Wait for all tasks from this batch to finish
            if tasks:
                await asyncio.gather(*tasks)
            # Short pause before next loop
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[Worker] Unexpected error in queue loop: {e}")
            await asyncio.sleep(5)

# Utility to schedule a queue processing tick from other threads (like Flask)
def schedule_immediate_processing():
    try:
        bot.loop.call_soon_threadsafe(lambda: bot.loop.create_task(_wake_queue_once()))
    except Exception as e:
        print(f"[Scheduler] Failed to schedule immediate processing: {e}")

# Helper coroutine that triggers one immediate pass (used to make the queue worker check right away)
async def _wake_queue_once():
    rows = get_pending_assignments(limit=50)
    if not rows:
        return
    tasks = [asyncio.create_task(process_assignment_row(row)) for row in rows]
    if tasks:
        await asyncio.gather(*tasks)

# ------------------------------
# Flask app (OAuth + Webhooks)
# ------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "supersecretkey")

# Stripe setup (used only to verify webhook signature)
import stripe
stripe.api_key = STRIPE_SECRET_KEY

SCOPE = "identify email"

@app.route("/")
def index():
    return "OK"

@app.route("/login")
def login():
    plan = request.args.get("plan")
    if plan:
        session["plan"] = plan
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPE,
    }
    return redirect(f"https://discord.com/api/oauth2/authorize?{urlencode(params)}")

def update_subscription_sheet(email, discord_id, plan, status, stripe_customer_id=None, stripe_subscription_id=None):
    payload = {
        "email": email,
        "discord_id": discord_id,
        "plan": plan,
        "status": status,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stripe_customer_id": stripe_customer_id,
        "stripe_subscription_id": stripe_subscription_id
    }
    try:
        r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
        r.raise_for_status()
        print(f"[Sheet] Updated sheet: email={email} plan={plan} status={status}")
        return True
    except Exception as e:
        print(f"[Sheet] Failed to update sheet: {e}")
        return False

def lookup_by_subscription_id(subscription_id):
    try:
        r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"stripe_subscription_id": subscription_id}, timeout=8)
        r.raise_for_status()
        data = r.json()
        return data.get("email"), data.get("discord_id"), data.get("plan")
    except Exception as e:
        print(f"[Sheet] lookup_by_subscription_id failed: {e}")
        return None, None, None

def lookup_by_email(email):
    try:
        r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"email": email}, timeout=8)
        r.raise_for_status()
        data = r.json()
        return data.get("discord_id")
    except Exception as e:
        print(f"[Sheet] lookup_by_email failed: {e}")
        return None

@app.route("/callback")
def callback():
    """
    OAuth callback: exchange code, get user info, update sheet, queue assignment immediately.
    """
    code = request.args.get("code")
    plan = session.get("plan") or request.args.get("plan") or "Unknown"
    if not code:
        return "Missing code", 400

    token_data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post("https://discord.com/api/oauth2/token", data=token_data, headers=headers, timeout=8)
        r.raise_for_status()
        creds = r.json()
    except Exception as e:
        print(f"[OAuth] Token exchange failed: {e}")
        return "OAuth token exchange failed", 500

    try:
        user_resp = requests.get("https://discord.com/api/users/@me", headers={"Authorization": f"Bearer {creds['access_token']}"}, timeout=8)
        user_resp.raise_for_status()
        user_info = user_resp.json()
        discord_id = user_info["id"]
        email = user_info.get("email", "")
    except Exception as e:
        print(f"[OAuth] Failed to fetch user info: {e}")
        return "Failed to fetch user info", 500

    print(f"[OAuth] Success: discord_id={discord_id} email={email} plan={plan}")

    # Update Google Sheet with linked/active status
    sheet_ok = update_subscription_sheet(email, discord_id, plan, "active")

    # Immediately queue assignment task (persisted)
    row_id = add_assignment_to_db(discord_id, plan, "assign", email=email)

    # Schedule the queue worker to process immediately
    schedule_immediate_processing()

    return redirect("https://marketwavetrading.com/success")

@app.route("/webhook", methods=["POST"])
def stripe_webhook():
    """
    Handle Stripe webhook events for subscription creations and deletions.
    Adds to queue and updates Google Sheet.
    """
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        print(f"[Stripe] Webhook signature verification failed: {e}")
        return "Invalid signature", 400

    ev_type = event["type"]
    print(f"[Stripe] Received event: {ev_type}")

    if ev_type == "checkout.session.completed":
        session_obj = event["data"]["object"]
        email = session_obj.get("customer_details", {}).get("email", "")
        stripe_customer_id = session_obj.get("customer")
        plan = session_obj.get("metadata", {}).get("plan", "Unknown")
        stripe_subscription_id = session_obj.get("subscription")

        # Try to get discord_id from sheet first, fallback to metadata
        discord_id = lookup_by_email(email) or session_obj.get("metadata", {}).get("discord_id")

        update_subscription_sheet(email, discord_id, plan, "active", stripe_customer_id, stripe_subscription_id)

        if discord_id and plan:
            add_assignment_to_db(discord_id, plan, "assign", email=email, stripe_subscription_id=stripe_subscription_id)
            schedule_immediate_processing()
        else:
            print(f"[Stripe] Missing discord_id or plan for checkout.session.completed: email={email} metadata={session_obj.get('metadata')}")

        return jsonify({"status":"success"}), 200

    if ev_type == "customer.subscription.deleted":
        subscription = event["data"]["object"]
        stripe_subscription_id = subscription.get("id")
        stripe_customer_id = subscription.get("customer")
        email, discord_id, plan = lookup_by_subscription_id(stripe_subscription_id)

        update_subscription_sheet(email, discord_id, plan, "canceled", stripe_customer_id, stripe_subscription_id)

        if discord_id and plan:
            add_assignment_to_db(discord_id, plan, "remove", email=email, stripe_subscription_id=stripe_subscription_id)
            schedule_immediate_processing()
        else:
            print(f"[Stripe] cancellation webhook missing data: subscription={stripe_subscription_id} lookup result email={email} discord_id={discord_id} plan={plan}")

        return jsonify({"status":"success"}), 200

    # You can add more event types to handle (invoice.payment_failed etc.)
    return jsonify({"status":"ignored"}), 200

# ------------------------------
# Startup wiring
# ------------------------------
def run_flask():
    # Use host 0.0.0.0 and port from env or default 5000
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    # Initialize DB
    init_db()
    # Start flask in a thread (so bot runs in main thread as required by discord.py)
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    # Run discord bot (this call blocks until bot exits)
    bot.run(DISCORD_BOT_TOKEN)
