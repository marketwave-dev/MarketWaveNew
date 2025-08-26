# file: app.py
"""
Key fixes:
- Remove reliance on `bot.loop` property (discord.py v2). Capture loop via `setup_hook`/`on_ready` using `asyncio.get_running_loop()`.
- Make queue I/O (requests -> Google Apps Script) non-blocking in bot loop via `asyncio.to_thread`.
- Replace fragile fallback loop + never-awaited coroutine with a simple, thread-safe `run_coroutine_threadsafe` onto the captured bot loop.
- Add `asyncio.Event` wake-up to process assignments immediately upon webhook.
- Fix Flask-Limiter storage URI to `memory://` (or keep Redis if available).
- Enforce `FLASK_SECRET_KEY` usage instead of random fallback.
- Harden error handling and logs.
"""

import os
import json
import time
import threading
import asyncio
from datetime import datetime
from urllib.parse import urlencode
from typing import Optional, Tuple, Any, Dict, List

from flask import Flask, request, redirect, session, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

import discord
from discord.ext import commands
import stripe
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()

# ------------------------------
# Environment / Configuration
# ------------------------------
REQUIRED_ENV_VARS = [
    "DISCORD_BOT_TOKEN", "DISCORD_CLIENT_ID", "DISCORD_CLIENT_SECRET",
    "DISCORD_REDIRECT_URI", "DISCORD_GUILD_ID", "ROLE_5K_TO_50K",
    "ROLE_ELITE", "ROLE_PLUS", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET",
    "GOOGLE_APPS_SCRIPT_URL", "FLASK_SECRET_KEY"
]

def validate_env_vars() -> None:
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"[Startup] Missing required environment variables: {', '.join(missing)}")
    try:
        int(os.getenv("DISCORD_GUILD_ID"))
        for role in ["ROLE_5K_TO_50K", "ROLE_ELITE", "ROLE_PLUS"]:
            int(os.getenv(role))
    except ValueError:
        raise EnvironmentError("[Startup] Invalid role or guild ID format")

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

# Tuneable retry settings
MAX_ATTEMPTS = 5
INITIAL_BACKOFF = 2  # seconds
BACKOFF_MULTIPLIER = 2

# Maps plan name -> role id and Stripe price ID
PLAN_TO_ROLE = {
    "5K to 50K Challenge": ROLE_5K_TO_50K,
    "MarketWave Elite": ROLE_ELITE,
    "MarketWave Plus": ROLE_PLUS,
}
PLAN_TO_PRICE_ID = {
    "5K to 50K Challenge": "price_1RdEoc08Ntv6wEBmUZOADdMd",
    "MarketWave Elite": "price_1RdEob08Ntv6wEBmT27qALuM",
    "MarketWave Plus": "price_1RdEoc08Ntv6wEBmifMeruFq"
}

# ------------------------------
# Globals for cross-thread signalling
# ------------------------------
_bot_loop: Optional[asyncio.AbstractEventLoop] = None  # set once bot is ready
_queue_wake_event: Optional[asyncio.Event] = None      # set inside bot loop

# ------------------------------
# Google Sheets Queue Functions (sync)
# ------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def add_assignment_to_sheet(discord_id: str, plan: str, action: str, email: Optional[str] = None, stripe_subscription_id: Optional[str] = None) -> Optional[int]:
    if plan not in PLAN_TO_ROLE:
        print(f"[Sheets] Invalid plan: {plan}")
        return None
    pending = get_pending_assignments_from_sheet(limit=100)
    for assignment in pending:
        try:
            if (
                assignment.get('discord_id') == str(discord_id) and
                assignment.get('plan') == plan and
                assignment.get('action') == action and
                assignment.get('attempts', 0) < MAX_ATTEMPTS
            ):
                print(f"[Sheets] Skipped duplicate assignment for discord_id={discord_id} plan={plan} action={action}")
                return None
        except KeyError as e:
            print(f"[Sheets] Skipping assignment due to missing key: {e}")
            continue
    payload = {
        "action": "insert_assignment",
        "data": {
            "email": email,
            "discord_id": str(discord_id),
            "plan": plan,
            "status": "pending",
            "action": action,
            "stripe_subscription_id": stripe_subscription_id,
            "attempts": 0,
            "last_error": None,
            "created_at": datetime.utcnow().isoformat(),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        },
    }
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    response = r.json()
    row_id = response.get("row_id")
    print(f"[Sheets] Queued assignment row_id={row_id} discord_id={discord_id} plan={plan} action={action} email={email}")
    return row_id

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_pending_assignments_from_sheet(limit: int = 50) -> List[Dict[str, Any]]:
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "get_pending_assignments", "limit": limit}, timeout=8)
    r.raise_for_status()
    assignments = r.json().get("assignments", [])
    print(f"[Sheets] Fetched {len(assignments)} pending assignments")
    return assignments

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def increment_attempts_and_log_error_in_sheet(row_id: int, error_text: str) -> None:
    payload = {"action": "update_assignment", "row_id": row_id, "updates": {"attempts": "increment", "last_error": error_text}}
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    print(f"[Sheets] Updated assignment row_id={row_id} with error: {error_text}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def remove_assignment_from_sheet(row_id: int) -> None:
    payload = {"action": "remove_assignment", "row_id": row_id}
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    print(f"[Sheets] Removed assignment row_id={row_id}")

# ------------------------------
# Discord Bot Setup
# ------------------------------
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    global _bot_loop, _queue_wake_event
    _bot_loop = asyncio.get_running_loop()  # reliable in v2+
    if _queue_wake_event is None:
        _queue_wake_event = asyncio.Event()
    print(f"[Discord] Bot logged in as {bot.user} (id={bot.user.id})")
    print("[Discord] Event loop captured and wake event ready")
    # Start the queue worker once
    asyncio.create_task(queue_processor_loop())

# ---- role assignment helpers
async def assign_or_remove_role(discord_id: str, plan: str, action: str) -> Tuple[bool, Optional[str]]:
    if plan not in PLAN_TO_ROLE:
        return False, f"No role mapping for plan '{plan}'"
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        return False, f"Guild {DISCORD_GUILD_ID} not found"
    try:
        member = await guild.fetch_member(int(discord_id))
    except discord.NotFound:
        return False, f"Member {discord_id} not found in guild {guild.id}"
    except Exception as e:
        return False, f"Error fetching member {discord_id}: {e}"
    role_id = PLAN_TO_ROLE[plan]
    role = guild.get_role(role_id)
    if not role:
        return False, f"Role {role_id} not found in guild"
    try:
        if action == "assign":
            if role in member.roles:
                print(f"[Discord] Member {discord_id} already has role {plan}")
                return True, None
            await member.add_roles(role, reason="Subscription activation")
            print(f"[Discord] Assigned role {plan} to {discord_id}")
            return True, None
        elif action == "remove":
            if role not in member.roles:
                print(f"[Discord] Member {discord_id} does not have role {plan}")
                return True, None
            await member.remove_roles(role, reason="Subscription canceled")
            print(f"[Discord] Removed role {plan} from {discord_id}")
            return True, None
        else:
            return False, f"Unknown action '{action}'"
    except discord.Forbidden:
        return False, "Missing Manage Roles permission or role hierarchy issue"
    except discord.HTTPException as e:
        return False, f"Discord HTTPException: {e}"
    except Exception as e:
        return False, f"Unexpected exception: {e}"

# ---- queue worker (non-blocking)
async def process_assignment_row(assignment: Dict[str, Any]) -> None:
    row_id = assignment['row_id']
    discord_id = assignment['discord_id']
    plan = assignment['plan']
    action = assignment.get('action', 'unknown')
    attempts = assignment.get('attempts', 0)
    backoff = INITIAL_BACKOFF * (BACKOFF_MULTIPLIER ** attempts)
    if attempts > 0:
        print(f"[Retry] row_id={row_id} discord_id={discord_id} plan={plan} action={action} attempts={attempts} waiting {backoff}s")
        await asyncio.sleep(backoff)

    success, err = await assign_or_remove_role(discord_id, plan, action)
    if success:
        print(f"[Success] row_id={row_id} discord_id={discord_id} plan={plan} action={action}")
        await asyncio.to_thread(remove_assignment_from_sheet, row_id)
    else:
        error_text = f"{datetime.utcnow().isoformat()} | {err}"
        print(f"[Fail] row_id={row_id} discord_id={discord_id} plan={plan} action={action} attempt={attempts + 1} error={err}")
        await asyncio.to_thread(increment_attempts_and_log_error_in_sheet, row_id, error_text)
        if attempts + 1 >= MAX_ATTEMPTS:
            print(f"[Alert] row_id={row_id} reached max attempts ({MAX_ATTEMPTS}). Edit Sheet1 (row {row_id}) to set attempts=0 or status='pending' for retry.")

async def _wake_queue_once() -> None:
    assignments = await asyncio.to_thread(get_pending_assignments_from_sheet, 50)
    if not assignments:
        print("[Scheduler] No pending assignments to process")
        return
    print(f"[Scheduler] Processing {len(assignments)} pending assignments: {[a['row_id'] for a in assignments]}")
    tasks = [asyncio.create_task(process_assignment_row(assignment)) for assignment in assignments]
    if tasks:
        await asyncio.gather(*tasks)

async def queue_processor_loop() -> None:
    global _queue_wake_event
    # Event to signal immediate processing
    if _queue_wake_event is None:
        _queue_wake_event = asyncio.Event()
    print("[Worker] Starting queue processor loop")
    while True:
        try:
            # immediate wake path
            if _queue_wake_event.is_set():
                _queue_wake_event.clear()
                await _wake_queue_once()
                await asyncio.sleep(1)
                continue

            # periodic polling path
            assignments = await asyncio.to_thread(get_pending_assignments_from_sheet, 50)
            if not assignments:
                print("[Worker] No pending assignments, sleeping for 30s")
                try:
                    await asyncio.wait_for(_queue_wake_event.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass
                continue

            print(f"[Worker] Processing {len(assignments)} pending assignments: {[a['row_id'] for a in assignments]}")
            tasks = [asyncio.create_task(process_assignment_row(assignment)) for assignment in assignments]
            if tasks:
                await asyncio.gather(*tasks)
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[Worker] Unexpected error in queue loop: {e}")
            await asyncio.sleep(5)

# ---- thread-safe trigger called from webhook
def schedule_immediate_processing() -> None:
    try:
        if _bot_loop is None:
            print("[Scheduler] Bot loop not ready; queue will pick up on next poll")
            return
        # set the wake event inside bot loop safely
        def _set_event():
            if _queue_wake_event and not _queue_wake_event.is_set():
                _queue_wake_event.set()
        _bot_loop.call_soon_threadsafe(_set_event)
        # also schedule an immediate run
        asyncio.run_coroutine_threadsafe(_wake_queue_once(), _bot_loop)
        print("[Scheduler] Immediate processing scheduled")
    except Exception as e:
        print(f"[Scheduler] Failed to schedule immediate processing: {e}")

# ------------------------------
# Flask App
# ------------------------------
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY  # enforce configured key

# Use memory backend (ephemeral). To persist, use Redis: redis://<host>:<port>/0
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
        print(f"[OAuth] Invalid or missing plan: {plan}")
        return "Invalid plan", 400
    session["plan"] = plan
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPE,
    }
    print(f"[OAuth] Initiating OAuth for plan={plan}")
    return redirect(f"https://discord.com/api/oauth2/authorize?{urlencode(params)}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def update_subscription_sheet(email: str, discord_id: str, plan: str, status: str, stripe_customer_id: Optional[str] = None, stripe_subscription_id: Optional[str] = None, coupon_id: Optional[str] = None) -> bool:
    payload = {
        "action": "update_subscription",
        "data": {
            "email": email,
            "discord_id": discord_id,
            "plan": plan,
            "status": status,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "coupon_id": coupon_id,
        },
    }
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    print(f"[Sheets] Updated subscription: email={email} discord_id={discord_id} plan={plan} status={status} coupon_id={coupon_id}")
    return True

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def lookup_by_subscription_id(subscription_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_subscription_id", "subscription_id": subscription_id}, timeout=8)
    r.raise_for_status()
    data = r.json()
    email = data.get("email")
    discord_id = data.get("discord_id")
    plan = data.get("plan")
    if not all([email, discord_id, plan]):
        print(f"[Sheets] Incomplete lookup result for subscription_id={subscription_id}: {data}")
        return None, None, None
    print(f"[Sheets] Lookup success: subscription_id={subscription_id} email={email} discord_id={discord_id} plan={plan}")
    return email, discord_id, plan

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def lookup_by_email(email: str) -> Tuple[Optional[str], Optional[str]]:
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_email", "email": email}, timeout=8)
    r.raise_for_status()
    data = r.json()
    discord_id = data.get("discord_id")
    plan = data.get("plan")
    if not discord_id or not plan:
        print(f"[Sheets] Incomplete lookup for email={email}: discord_id={discord_id}, plan={plan}")
        return None, None
    print(f"[Sheets] Lookup success: email={email} discord_id={discord_id} plan={plan}")
    return discord_id, plan

@app.route("/callback")
@limiter.limit("10 per minute")
def callback():
    code = request.args.get("code")
    plan = session.get("plan")
    if not code:
        print("[OAuth] Missing code in callback")
        return "Missing code", 400
    if not plan or plan not in PLAN_TO_ROLE:
        print(f"[OAuth] Invalid or missing plan in callback: {plan}")
        return "Invalid plan", 400
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
        expected_scopes = set(SCOPE.split())
        received_scopes = set(creds.get("scope", "").split())
        if not expected_scopes.issubset(received_scopes):
            print(f"[OAuth] Invalid scope: expected {expected_scopes}, got {received_scopes}")
            return "Invalid OAuth scope", 400
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
    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": PLAN_TO_PRICE_ID[plan], "quantity": 1}],
            mode="subscription",
            success_url="https://marketwavetrading.com/success",
            cancel_url="https://marketwavetrading.com/cancel",
            metadata={"plan": plan, "discord_id": discord_id},
            customer_email=email,
            allow_promotion_codes=True,
        )
        print(f"[Stripe] Created checkout session for discord_id={discord_id} plan={plan}")
        return redirect(checkout_session.url)
    except Exception as e:
        print(f"[Stripe] Failed to create checkout session: {e}")
        return "Failed to create checkout session", 500

@app.route("/webhook", methods=["POST"])
@limiter.limit("100 per minute")
def stripe_webhook():
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
        plan = session_obj.get("metadata", {}).get("plan", None)
        discord_id = session_obj.get("metadata", {}).get("discord_id")
        stripe_subscription_id = session_obj.get("subscription")
        coupon_id = None
        if session_obj.get("total_details", {}).get("amount_discount", 0) > 0:
            try:
                subscription = stripe.Subscription.retrieve(stripe_subscription_id)
                coupon_id = (
                    subscription.get("discount", {}).get("coupon", {}).get("id")
                    or subscription.get("discount", {}).get("promotion_code")
                )
                print(f"[Stripe] Coupon applied: coupon_id={coupon_id}")
            except Exception as e:
                print(f"[Stripe] Failed to retrieve coupon details: {e}")
        if not email or not plan or plan not in PLAN_TO_ROLE or not discord_id:
            print(f"[Stripe] Invalid data in checkout.session.completed: email={email} plan={plan} discord_id={discord_id}")
            return jsonify({"status": "error", "message": "Invalid email, plan, or discord_id"}), 400
        try:
            update_subscription_sheet(email, discord_id, plan, "active", stripe_customer_id, stripe_subscription_id, coupon_id)
        except Exception as e:
            print(f"[Stripe] Failed to update sheet for email={email}: {e}")
            return jsonify({"status": "error", "message": "Failed to update sheet"}), 500
        row_id = add_assignment_to_sheet(discord_id, plan, "assign", email, stripe_subscription_id)
        if row_id is not None:
            schedule_immediate_processing()
        return jsonify({"status": "success"}), 200

    elif ev_type == "customer.subscription.deleted":
        subscription = event["data"]["object"]
        stripe_subscription_id = subscription.get("id")
        stripe_customer_id = subscription.get("customer")
        email, discord_id, plan = lookup_by_subscription_id(stripe_subscription_id)
        if not all([email, discord_id, plan]) or plan not in PLAN_TO_ROLE:
            print(f"[Stripe] Invalid lookup for subscription_id={stripe_subscription_id}: email={email} discord_id={discord_id} plan={plan}")
            return jsonify({"status": "error", "message": "Invalid subscription data"}), 400
        try:
            update_subscription_sheet(email, discord_id, plan, "canceled", stripe_customer_id, stripe_subscription_id)
        except Exception as e:
            print(f"[Stripe] Failed to update sheet for subscription_id={stripe_subscription_id}: {e}")
            return jsonify({"status": "error", "message": "Failed to update sheet"}), 500
        row_id = add_assignment_to_sheet(discord_id, plan, "remove", email, stripe_subscription_id)
        if row_id is not None:
            schedule_immediate_processing()
        return jsonify({"status": "success"}), 200

    elif ev_type == "invoice.payment_failed":
        invoice = event["data"]["object"]
        stripe_subscription_id = invoice.get("subscription")
        if stripe_subscription_id:
            email, discord_id, plan = lookup_by_subscription_id(stripe_subscription_id)
            if all([email, discord_id, plan]):
                print(f"[Stripe] Payment failed for subscription {stripe_subscription_id}; queuing removal")
                row_id = add_assignment_to_sheet(discord_id, plan, "remove", email, stripe_subscription_id)
                if row_id is not None:
                    schedule_immediate_processing()
        return jsonify({"status": "handled"}), 200

    print(f"[Stripe] Ignored event: {ev_type}")
    return jsonify({"status": "ignored"}), 200

@app.route("/trigger_assignments", methods=["GET"])
@limiter.limit("5 per minute")
def trigger_assignments():
    print("[Manual] Triggering manual assignment processing")
    try:
        if _bot_loop is None:
            print("[Manual] Bot loop not ready; background poller will process later")
            return jsonify({"status": "error", "message": "Bot not ready"}), 503
        schedule_immediate_processing()
        return jsonify({"status": "success", "message": "Triggered assignment processing"}), 200
    except Exception as e:
        print(f"[Manual] Failed to trigger assignments: {e}")
        return jsonify({"status": "error", "message": f"Failed to trigger: {str(e)}"}), 500

# ------------------------------
# Startup Wiring
# ------------------------------

def start_discord_bot() -> None:
    print("[Discord] Preparing to start bot")
    try:
        @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=5, max=60))
        def run_bot():
            print("[Discord] Attempting to run bot")
            try:
                bot.run(DISCORD_BOT_TOKEN)
            except discord.errors.LoginFailure as e:
                print(f"[Discord] Login failed: Invalid DISCORD_BOT_TOKEN: {e}")
                raise
            except discord.errors.HTTPException as e:
                print(f"[Discord] HTTP error during bot startup: {e}")
                raise
            except Exception as e:
                print(f"[Discord] Unexpected error running bot: {e}")
                raise
        run_bot()
    except Exception as e:
        print(f"[Discord] Failed to start bot after retries: {e}")

def run_flask() -> None:
    port = int(os.environ.get("PORT", 10000))
    print(f"[Startup] Starting Flask server on port {port}")
    # threaded=True so webhook and login don't block each other
    app.run(host="0.0.0.0", port=port, threaded=True)

if __name__ == "__main__":
    validate_env_vars()
    print("[Startup] Starting Discord bot thread")
    bot_thread = threading.Thread(target=start_discord_bot, daemon=True)
    bot_thread.start()

    # Give the bot a short head start, but don't hard-block for long
    for i in range(15):
        if _bot_loop is not None:
            break
        time.sleep(1)

    print("[Startup] Starting Flask thread")
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    bot_thread.join()
