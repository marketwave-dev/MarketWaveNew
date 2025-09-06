# file: app.py
"""
All-in-one Flask + Discord bot + Stripe webhook + Google Apps Script integration.
Drop this single file onto Render (with your .env configured) and run.

Features:
- Discord OAuth2 to collect Discord ID + email and redirect to Stripe Checkout
- Stripe webhook handling (checkout.session.completed, customer.subscription.deleted, invoice.payment_failed)
- Google Apps Script (sheet) queue for assignments + lookup API calls
- Reliable cross-thread signalling into the bot event loop to assign/remove roles
- Exponential retry and attempt counting via the sheet
- Single-file deploy; logs to stdout (Render-friendly)

Environment variables required (set in Render):
DISCORD_BOT_TOKEN, DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET,
DISCORD_REDIRECT_URI, DISCORD_GUILD_ID, ROLE_5K_TO_50K, ROLE_ELITE, ROLE_PLUS,
STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, GOOGLE_APPS_SCRIPT_URL, FLASK_SECRET_KEY

"""

import os
import time
import threading
import asyncio
import logging
from datetime import datetime
from typing import Optional, Any, Dict, List, Tuple
from urllib.parse import urlencode

from flask import Flask, request, redirect, session, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

import discord
from discord.ext import commands
import stripe
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

# ------------------------------
# Logging
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ------------------------------
# Load env
# ------------------------------
load_dotenv()
REQUIRED_ENV_VARS = [
    "DISCORD_BOT_TOKEN", "DISCORD_CLIENT_ID", "DISCORD_CLIENT_SECRET",
    "DISCORD_REDIRECT_URI", "DISCORD_GUILD_ID", "ROLE_5K_TO_50K",
    "ROLE_ELITE", "ROLE_PLUS", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET",
    "GOOGLE_APPS_SCRIPT_URL", "FLASK_SECRET_KEY"
]
missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI")
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))
ROLE_5K_TO_50K = int(os.getenv("ROLE_5K_TO_50K"))
ROLE_ELITE = int(os.getenv("ROLE_ELITE"))
ROLE_PLUS = int(os.getenv("ROLE_PLUS"))
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
GOOGLE_APPS_SCRIPT_URL = os.getenv("GOOGLE_APPS_SCRIPT_URL")
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")

PLAN_TO_ROLE = {
    "5K to 50K Challenge": ROLE_5K_TO_50K,
    "MarketWave Elite": ROLE_ELITE,
    "MarketWave Plus": ROLE_PLUS,
}
# Replace these with your actual Stripe price IDs if different
PLAN_TO_PRICE_ID = {
    "5K to 50K Challenge": os.getenv("PRICE_5K_TO_50K", "price_1RdEoc08Ntv6wEBmUZOADdMd"),
    "MarketWave Elite": os.getenv("PRICE_ELITE", "price_1RdEob08Ntv6wEBmT27qALuM"),
    "MarketWave Plus": os.getenv("PRICE_PLUS", "price_1RdEoc08Ntv6wEBmifMeruFq"),
}

# Retry settings
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))

# ------------------------------
# Globals for cross-thread signalling
# ------------------------------
_bot_loop: Optional[asyncio.AbstractEventLoop] = None
_queue_wake_event: Optional[asyncio.Event] = None

# ------------------------------
# Sheets (Google Apps Script) helpers
# Expected Apps Script endpoints (simple JSON API):
# action=get_pending_assignments -> { assignments: [ { row_id, discord_id, plan, action, attempts } ] }
# action=insert_assignment (POST) -> { row_id }
# action=update_assignment (POST) -> { ok: true }
# action=remove_assignment (POST) -> { ok: true }
# action=lookup_by_subscription_id -> { email, discord_id, plan }
# action=lookup_by_email -> { discord_id, plan }
# ------------------------------

_HEADERS = {"Content-Type": "application/json"}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_pending_assignments(limit: int = 50) -> List[Dict[str, Any]]:
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "get_pending_assignments", "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json().get("assignments", [])

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def insert_assignment(discord_id: str, plan: str, action: str, email: Optional[str], stripe_subscription_id: Optional[str]) -> Optional[int]:
    data = {
        "discord_id": str(discord_id),
        "plan": plan,
        "action": action,
        "email": email,
        "stripe_subscription_id": stripe_subscription_id,
        "attempts": 0,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json={"action": "insert_assignment", "data": data}, timeout=10)
    r.raise_for_status()
    resp = r.json()
    return resp.get("row_id")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def update_assignment_attempts(row_id: int, attempts: int, last_error: Optional[str] = None) -> None:
    payload = {"action": "update_assignment", "row_id": row_id, "updates": {"attempts": attempts, "last_error": last_error}}
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=10)
    r.raise_for_status()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def remove_assignment(row_id: int) -> None:
    payload = {"action": "remove_assignment", "row_id": row_id}
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=10)
    r.raise_for_status()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def lookup_by_subscription_id(subscription_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_subscription_id", "subscription_id": subscription_id}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("email"), data.get("discord_id"), data.get("plan")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def lookup_by_email(email: str) -> Tuple[Optional[str], Optional[str]]:
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_email", "email": email}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("discord_id"), data.get("plan")

# ------------------------------
# Discord bot
# ------------------------------
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

async def assign_or_remove_role(discord_id: str, plan: str, action: str) -> Tuple[bool, Optional[str]]:
    if plan not in PLAN_TO_ROLE:
        return False, f"unknown plan {plan}"
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        return False, f"guild {DISCORD_GUILD_ID} not found"
    try:
        member = await guild.fetch_member(int(discord_id))
    except discord.NotFound:
        return False, f"member {discord_id} not found"
    except Exception as e:
        return False, f"failed to fetch member: {e}"
    role = guild.get_role(PLAN_TO_ROLE[plan])
    if not role:
        return False, f"role id {PLAN_TO_ROLE[plan]} not found"
    try:
        if action == "assign":
            if role in member.roles:
                return True, None
            await member.add_roles(role, reason="Subscription activated")
            return True, None
        elif action == "remove":
            if role not in member.roles:
                return True, None
            await member.remove_roles(role, reason="Subscription ended")
            return True, None
        else:
            return False, f"unknown action {action}"
    except discord.Forbidden:
        return False, "forbidden: check bot permissions and role hierarchy"
    except Exception as e:
        return False, f"discord error: {e}"

# Worker processing single assignment row
async def process_assignment_row(assignment: Dict[str, Any]) -> None:
    row_id = int(assignment.get("row_id"))
    discord_id = str(assignment.get("discord_id"))
    plan = assignment.get("plan")
    action = assignment.get("action")
    attempts = int(assignment.get("attempts", 0))

    # simple backoff
    if attempts > 0:
        backoff = min(60, 2 ** attempts)
        logger.info(f"[Worker] Backoff {backoff}s for row {row_id} (attempts={attempts})")
        await asyncio.sleep(backoff)

    success, err = await assign_or_remove_role(discord_id, plan, action)
    if success:
        try:
            await asyncio.to_thread(remove_assignment, row_id)
            logger.info(f"[Worker] Success row {row_id} {action} for {discord_id}")
        except Exception as e:
            logger.error(f"[Worker] Failed to remove assignment {row_id}: {e}")
    else:
        attempts += 1
        last_error = f"{datetime.utcnow().isoformat()} | {err}"
        logger.warning(f"[Worker] Failed row {row_id}: {err} (attempt {attempts})")
        try:
            await asyncio.to_thread(update_assignment_attempts, row_id, attempts, last_error)
        except Exception as e:
            logger.error(f"[Worker] Failed to update attempts for {row_id}: {e}")
        if attempts >= MAX_ATTEMPTS:
            logger.error(f"[Worker] Row {row_id} reached max attempts; manual intervention required")

# Worker loop
async def queue_processor_loop() -> None:
    global _queue_wake_event
    if _queue_wake_event is None:
        _queue_wake_event = asyncio.Event()
    logger.info("[Worker] queue processor started")
    while True:
        try:
            if _queue_wake_event.is_set():
                _queue_wake_event.clear()
                assignments = await asyncio.to_thread(get_pending_assignments, 100)
                if assignments:
                    logger.info(f"[Worker] Immediate processing {len(assignments)} assignments")
                    tasks = [asyncio.create_task(process_assignment_row(a)) for a in assignments]
                    await asyncio.gather(*tasks)
                continue

            assignments = await asyncio.to_thread(get_pending_assignments, 50)
            if not assignments:
                await asyncio.sleep(15)
                continue
            logger.info(f"[Worker] Poll processing {len(assignments)} assignments")
            tasks = [asyncio.create_task(process_assignment_row(a)) for a in assignments]
            await asyncio.gather(*tasks)
            await asyncio.sleep(5)
        except Exception as e:
            logger.exception(f"[Worker] Loop exception: {e}")
            await asyncio.sleep(5)

# Thread-safe scheduler to wake worker
def schedule_immediate_processing() -> None:
    try:
        if _bot_loop is None:
            logger.warning("[Scheduler] Bot loop not ready; will be processed by poller")
            return
        def _set_event():
            if _queue_wake_event and not _queue_wake_event.is_set():
                _queue_wake_event.set()
        _bot_loop.call_soon_threadsafe(_set_event)
        # also run one-off wake coroutine
        asyncio.run_coroutine_threadsafe(_run_wake_once(), _bot_loop)
        logger.info("[Scheduler] Scheduled immediate processing")
    except Exception as e:
        logger.exception(f"[Scheduler] Failed to schedule processing: {e}")

async def _run_wake_once() -> None:
    assignments = await asyncio.to_thread(get_pending_assignments, 100)
    if not assignments:
        logger.info("[Scheduler] No pending assignments on immediate wake")
        return
    tasks = [asyncio.create_task(process_assignment_row(a)) for a in assignments]
    await asyncio.gather(*tasks)

# ------------------------------
# Discord bot events
# ------------------------------
@bot.event
async def on_ready():
    global _bot_loop, _queue_wake_event
    _bot_loop = asyncio.get_running_loop()
    if _queue_wake_event is None:
        _queue_wake_event = asyncio.Event()
    logger.info(f"[Discord] Logged in as {bot.user} (id={bot.user.id})")
    # Validate guild and roles
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if guild is None:
        logger.error(f"[Discord] Guild {DISCORD_GUILD_ID} not found; check bot is in the guild")
    else:
        for name, rid in PLAN_TO_ROLE.items():
            if guild.get_role(rid) is None:
                logger.error(f"[Discord] Role id {rid} for plan '{name}' not found in guild {guild.name}")
    # start queue loop
    asyncio.create_task(queue_processor_loop())

# ------------------------------
# Flask app + OAuth + Stripe
# ------------------------------
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
limiter = Limiter(get_remote_address, app=app, storage_uri="memory://")
stripe.api_key = STRIPE_SECRET_KEY
SCOPE = "identify email"

@app.route("/")
def index():
    return "OK"

@app.route("/login")
@limiter.limit("10 per minute")
def login():
    plan = request.args.get("plan")
    if not plan or plan not in PLAN_TO_ROLE:
        return "Invalid plan", 400
    session["plan"] = plan
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPE,
    }
    logger.info(f"[OAuth] Initiating for plan={plan}")
    return redirect(f"https://discord.com/api/oauth2/authorize?{urlencode(params)}")

@app.route("/callback")
@limiter.limit("10 per minute")
def callback():
    code = request.args.get("code")
    plan = session.get("plan")
    if not code or not plan:
        logger.warning("[OAuth] Missing code or plan in callback")
        return "Missing parameters", 400
    token_data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post("https://discord.com/api/oauth2/token", data=token_data, headers=headers, timeout=10)
        r.raise_for_status()
        creds = r.json()
        expected_scopes = set(SCOPE.split())
        received_scopes = set(creds.get("scope", "").split())
        if not expected_scopes.issubset(received_scopes):
            logger.warning(f"[OAuth] Invalid scopes: got {received_scopes}")
            return "Invalid OAuth scope", 400
    except Exception as e:
        logger.exception(f"[OAuth] token exchange failed: {e}")
        return "OAuth token exchange failed", 500
    try:
        user_resp = requests.get("https://discord.com/api/users/@me", headers={"Authorization": f"Bearer {creds['access_token']}"}, timeout=10)
        user_resp.raise_for_status()
        user = user_resp.json()
        discord_id = user["id"]
        email = user.get("email") or ""
    except Exception as e:
        logger.exception(f"[OAuth] failed to fetch user: {e}")
        return "Failed to fetch user", 500
    # Create checkout
    try:
        checkout = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": PLAN_TO_PRICE_ID[plan], "quantity": 1}],
            mode="subscription",
            success_url=os.getenv("SUCCESS_URL", "https://marketwavetrading.com/success"),
            cancel_url=os.getenv("CANCEL_URL", "https://marketwavetrading.com/cancel"),
            metadata={"plan": plan, "discord_id": discord_id},
            customer_email=email,
            allow_promotion_codes=True,
        )
        logger.info(f"[Stripe] Created checkout session for {discord_id} plan={plan}")
        return redirect(checkout.url)
    except Exception as e:
        logger.exception(f"[Stripe] failed to create checkout: {e}")
        return "Failed to create checkout", 500

# Stripe webhook endpoint
@app.route("/webhook", methods=["POST"])
@limiter.limit("200 per minute")
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        logger.warning(f"[Stripe] signature verification failed: {e}")
        return "Invalid signature", 400
    ev_type = event["type"]
    logger.info(f"[Stripe] Event: {ev_type}")

    # checkout.session.completed -> mark active, queue assign
    if ev_type == "checkout.session.completed":
        session_obj = event["data"]["object"]
        email = session_obj.get("customer_details", {}).get("email", "")
        plan = session_obj.get("metadata", {}).get("plan")
        discord_id = session_obj.get("metadata", {}).get("discord_id")
        stripe_subscription_id = session_obj.get("subscription")
        stripe_customer_id = session_obj.get("customer")
        if not email or not plan or not discord_id:
            logger.error("[Stripe] Missing data in checkout.session.completed")
            return jsonify({"status": "error"}), 400
        try:
            update_subscription_payload = {
                "email": email,
                "discord_id": discord_id,
                "plan": plan,
                "status": "active",
                "stripe_customer_id": stripe_customer_id,
                "stripe_subscription_id": stripe_subscription_id,
            }
            requests.post(GOOGLE_APPS_SCRIPT_URL, json={"action": "update_subscription", "data": update_subscription_payload}, timeout=10).raise_for_status()
        except Exception as e:
            logger.exception(f"[Sheets] failed to update subscription row: {e}")
            # continue; still queue assignment so role is given even if sheet write hiccuped
        try:
            row_id = insert_assignment(discord_id, plan, "assign", email, stripe_subscription_id)
            logger.info(f"[Sheets] inserted assignment row {row_id}")
        except Exception as e:
            logger.exception(f"[Sheets] failed to insert assignment: {e}")
            row_id = None
        if row_id is not None:
            schedule_immediate_processing()
        return jsonify({"status": "ok"}), 200

    # subscription deleted -> lookup and queue remove
    elif ev_type == "customer.subscription.deleted":
        subscription = event["data"]["object"]
        subscription_id = subscription.get("id")
        customer_id = subscription.get("customer")
        email, discord_id, plan = None, None, None
        try:
            email, discord_id, plan = lookup_by_subscription_id(subscription_id)
        except Exception as e:
            logger.exception(f"[Sheets] lookup by subscription id failed: {e}")
        if not all([email, discord_id, plan]):
            logger.error(f"[Stripe] Subscription lookup incomplete for {subscription_id}")
            return jsonify({"status": "error"}), 400
        try:
            requests.post(GOOGLE_APPS_SCRIPT_URL, json={"action": "update_subscription", "data": {"email": email, "discord_id": discord_id, "plan": plan, "status": "canceled", "stripe_subscription_id": subscription_id}}, timeout=10).raise_for_status()
        except Exception as e:
            logger.exception(f"[Sheets] failed to mark subscription canceled: {e}")
        try:
            row_id = insert_assignment(discord_id, plan, "remove", email, subscription_id)
            if row_id:
                schedule_immediate_processing()
        except Exception as e:
            logger.exception(f"[Sheets] failed to insert removal assignment: {e}")
        return jsonify({"status": "ok"}), 200

    # invoice.payment_failed -> optionally queue remove (or notify)
    elif ev_type == "invoice.payment_failed":
        invoice = event["data"]["object"]
        subscription_id = invoice.get("subscription")
        if not subscription_id:
            return jsonify({"status": "ignored"}), 200
        try:
            email, discord_id, plan = lookup_by_subscription_id(subscription_id)
            if all([email, discord_id, plan]):
                # For failed payment we choose to queue a removal; if you prefer notify + grace period, change to notify flow
                row_id = insert_assignment(discord_id, plan, "remove", email, subscription_id)
                if row_id:
                    schedule_immediate_processing()
        except Exception as e:
            logger.exception(f"[Stripe] invoice.payment_failed handler error: {e}")
        return jsonify({"status": "ok"}), 200

    logger.info(f"[Stripe] Ignored event {ev_type}")
    return jsonify({"status": "ignored"}), 200

# Manual trigger (useful for debugging)
@app.route("/trigger_assignments", methods=["POST", "GET"])
def trigger_assignments():
    if _bot_loop is None:
        return jsonify({"status": "error", "message": "bot not ready"}), 503
    schedule_immediate_processing()
    return jsonify({"status": "ok"}), 200

# ------------------------------
# Startup helpers
# ------------------------------

def start_discord_bot():
    try:
        bot.run(DISCORD_BOT_TOKEN)
    except Exception as e:
        logger.exception(f"Failed to start Discord bot: {e}")

def start_flask():
    port = int(os.getenv("PORT", "10000"))
    logger.info(f"Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)

# ------------------------------
# Run
# ------------------------------
if __name__ == "__main__":
    # Start bot thread
    bot_thread = threading.Thread(target=start_discord_bot, daemon=True)
    bot_thread.start()
    # Wait up to 20s for bot loop to be set in on_ready; queue will poll in meantime
    for _ in range(20):
        if _bot_loop is not None:
            break
        time.sleep(1)
    # Start Flask (blocking)
    start_flask()

# End of file
