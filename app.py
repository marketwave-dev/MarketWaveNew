import os
import json
import time
import threading
import asyncio
from datetime import datetime
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

def validate_env_vars():
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
# Google Sheets Queue Functions
# ------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def add_assignment_to_sheet(discord_id, plan, action, email=None, stripe_subscription_id=None):
    if plan not in PLAN_TO_ROLE:
        print(f"[Sheets] Invalid plan: {plan}")
        return None
    pending = get_pending_assignments_from_sheet(limit=100)
    for assignment in pending:
        if (assignment['discord_id'] == str(discord_id) and
            assignment['plan'] == plan and
            assignment['action'] == action and
            assignment['attempts'] < MAX_ATTEMPTS):
            print(f"[Sheets] Skipped duplicate assignment for discord_id={discord_id} plan={plan} action={action}")
            return None
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
            "created_at": datetime.now().isoformat(),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    response = r.json()
    row_id = response.get("row_id")
    print(f"[Sheets] Queued assignment row_id={row_id} discord_id={discord_id} plan={plan} action={action} email={email}")
    return row_id

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_pending_assignments_from_sheet(limit=50):
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "get_pending_assignments", "limit": limit}, timeout=8)
    r.raise_for_status()
    assignments = r.json().get("assignments", [])
    print(f"[Sheets] Fetched {len(assignments)} pending assignments")
    return assignments

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def increment_attempts_and_log_error_in_sheet(row_id, error_text):
    payload = {
        "action": "update_assignment",
        "row_id": row_id,
        "updates": {
            "attempts": "increment",
            "last_error": error_text
        }
    }
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    print(f"[Sheets] Updated assignment row_id={row_id} with error: {error_text}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def remove_assignment_from_sheet(row_id):
    payload = {
        "action": "remove_assignment",
        "row_id": row_id
    }
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
    print(f"[Discord] Bot logged in as {bot.user} (id={bot.user.id})")
    bot.loop.create_task(queue_processor_loop())

async def assign_or_remove_role(discord_id: str, plan: str, action: str):
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

async def process_assignment_row(assignment):
    row_id = assignment['row_id']
    discord_id = assignment['discord_id']
    plan = assignment['plan']
    action = assignment['action']
    attempts = assignment.get('attempts', 0)
    backoff = INITIAL_BACKOFF * (BACKOFF_MULTIPLIER ** attempts)
    if attempts > 0:
        print(f"[Retry] row_id={row_id} discord_id={discord_id} plan={plan} action={action} attempts={attempts} waiting {backoff}s")
        await asyncio.sleep(backoff)
    success, err = await assign_or_remove_role(discord_id, plan, action)
    if success:
        print(f"[Success] row_id={row_id} discord_id={discord_id} plan={plan} action={action}")
        remove_assignment_from_sheet(row_id)
    else:
        error_text = f"{datetime.utcnow().isoformat()} | {err}"
        print(f"[Fail] row_id={row_id} discord_id={discord_id} plan={plan} action={action} attempt={attempts + 1} error={err}")
        increment_attempts_and_log_error_in_sheet(row_id, error_text)
        if attempts + 1 >= MAX_ATTEMPTS:
            print(f"[Alert] row_id={row_id} reached max attempts ({MAX_ATTEMPTS}). Edit Sheet1 (row {row_id}) to set attempts=0 or status='pending' for retry.")

async def queue_processor_loop():
    await bot.wait_until_ready()
    print("[Worker] Starting queue processor loop")
    while True:
        try:
            assignments = get_pending_assignments_from_sheet(limit=50)
            if not assignments:
                await asyncio.sleep(30)
                continue
            tasks = [asyncio.create_task(process_assignment_row(assignment)) for assignment in assignments]
            if tasks:
                await asyncio.gather(*tasks)
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[Worker] Unexpected error in queue loop: {e}")
            await asyncio.sleep(5)

def schedule_immediate_processing():
    try:
        if bot.is_ready():
            bot.loop.call_soon_threadsafe(lambda: bot.loop.create_task(_wake_queue_once()))
        else:
            print("[Scheduler] Bot not ready, deferring immediate processing")
            bot.loop.create_task(_wake_queue_once())
    except Exception as e:
        print(f"[Scheduler] Failed to schedule immediate processing: {e}")

async def _wake_queue_once():
    assignments = get_pending_assignments_from_sheet(limit=50)
    if not assignments:
        print("[Scheduler] No pending assignments to process")
        return
    tasks = [asyncio.create_task(process_assignment_row(assignment)) for assignment in assignments]
    await asyncio.gather(*tasks)

# ------------------------------
# Flask App
# ------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or os.urandom(24).hex()
limiter = Limiter(
    get_remote_address,
    app=app,
    storage_uri="memory:////opt/render/project/src/rate_limit.db"
)
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
def update_subscription_sheet(email, discord_id, plan, status, stripe_customer_id=None, stripe_subscription_id=None):
    payload = {
        "action": "update_subscription",
        "data": {
            "email": email,
            "discord_id": discord_id,
            "plan": plan,
            "status": status,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id
        }
    }
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=8)
    r.raise_for_status()
    print(f"[Sheets] Updated subscription: email={email} discord_id={discord_id} plan={plan} status={status}")
    return True

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def lookup_by_subscription_id(subscription_id):
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
def lookup_by_email(email):
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
    # Create Stripe checkout session
    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[
                {
                    "price": PLAN_TO_PRICE_ID[plan],
                    "quantity": 1,
                }
            ],
            mode="subscription",
            success_url="https://marketwavetrading.com/success",
            cancel_url="https://marketwavetrading.com/cancel",
            metadata={"plan": plan, "discord_id": discord_id},
            customer_email=email,
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
        if not email or not plan or plan not in PLAN_TO_ROLE or not discord_id:
            print(f"[Stripe] Invalid data in checkout.session.completed: email={email} plan={plan} discord_id={discord_id}")
            return jsonify({"status": "error", "message": "Invalid email, plan, or discord_id"}), 400
        try:
            update_subscription_sheet(email, discord_id, plan, "active", stripe_customer_id, stripe_subscription_id)
        except Exception as e:
            print(f"[Stripe] Failed to update sheet for email={email}: {e}")
            return jsonify({"status": "error", "message": "Failed to update sheet"}), 500
        row_id = add_assignment_to_sheet(discord_id, plan, "assign", email, stripe_subscription_id)
        if row_id:
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
        if row_id:
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
                if row_id:
                    schedule_immediate_processing()
        return jsonify({"status": "handled"}), 200
    print(f"[Stripe] Ignored event: {ev_type}")
    return jsonify({"status": "ignored"}), 200

# ------------------------------
# Startup Wiring
# ------------------------------
def run_flask():
    port = int(os.environ.get("PORT", 5000))
    print(f"[Startup] Starting Flask server on port {port}")
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    validate_env_vars()
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    bot.run(DISCORD_BOT_TOKEN)