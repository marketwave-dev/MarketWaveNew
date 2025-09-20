import os
import logging
import asyncio
import requests
import stripe
from flask import Flask, request, redirect, session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from discord.ext import commands
import discord
from datetime import datetime
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Validate environment variables
REQUIRED_ENV_VARS = [
    "DISCORD_BOT_TOKEN", "DISCORD_CLIENT_ID", "DISCORD_CLIENT_SECRET",
    "DISCORD_GUILD_ID", "DISCORD_REDIRECT_URI", "FLASK_SECRET_KEY",
    "GOOGLE_APPS_SCRIPT_URL", "ROLE_5K_TO_50K", "ROLE_ELITE", "ROLE_PLUS",
    "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET"
]
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        logger.error("Missing environment variable: %s", var)
        raise ValueError(f"Missing environment variable: {var}")

# Load environment variables
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI")
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")
GOOGLE_APPS_SCRIPT_URL = os.getenv("GOOGLE_APPS_SCRIPT_URL")
ROLE_5K_TO_50K = int(os.getenv("ROLE_5K_TO_50K"))
ROLE_ELITE = int(os.getenv("ROLE_ELITE"))
ROLE_PLUS = int(os.getenv("ROLE_PLUS"))
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

# Stripe config
stripe.api_key = STRIPE_SECRET_KEY

# Price mapping
PLAN_TO_PRICE_ID = {
    "5K to 50K Challenge": "price_1RdEoc08Ntv6wEBmUZOADdMd",
    "MarketWave Elite": "price_1RdEob08Ntv6wEBmT27qALuM",
    "MarketWave Plus": "price_1RdEoc08Ntv6wEBmifMeruFq"
}

# Flask app
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

limiter = Limiter(get_remote_address, app=app, default_limits=["100 per minute"])

# Discord bot
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Role mapping
ROLE_MAP = {
    "5K to 50K Challenge": ROLE_5K_TO_50K,
    "MarketWave Elite": ROLE_ELITE,
    "MarketWave Plus": ROLE_PLUS,
}

# --- Google Sheet Integration ---
def make_request(method, url, **kwargs):
    for attempt in range(3):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and attempt < 2:
                logger.warning("Rate limit hit, retrying after %s seconds", 2 ** attempt)
                time.sleep(2 ** attempt)
            else:
                raise
        except Exception as e:
            raise

def insert_assignment(data):
    return make_request("POST", GOOGLE_APPS_SCRIPT_URL, json={"action": "insert_assignment", "data": data})

def get_pending_assignments(limit=50):
    return make_request("GET", GOOGLE_APPS_SCRIPT_URL, params={"action": "get_pending_assignments", "limit": limit})

def update_assignment(row_id, updates):
    return make_request("POST", GOOGLE_APPS_SCRIPT_URL, json={"action": "update_assignment", "row_id": row_id, "updates": updates})

def remove_assignment(row_id):
    return make_request("POST", GOOGLE_APPS_SCRIPT_URL, json={"action": "remove_assignment", "row_id": row_id})

def lookup_by_subscription_id(subscription_id):
    return make_request("GET", GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_subscription_id", "subscription_id": subscription_id})

def lookup_by_email(email):
    return make_request("GET", GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_email", "email": email})

# --- Role Verification Helpers ---
async def verify_role(member, role, should_have=True):
    await asyncio.sleep(30)  # wait before checking
    refreshed = member.guild.get_member(member.id)
    if not refreshed:
        return False
    has_role = role in refreshed.roles
    return has_role if should_have else not has_role

async def assign_role_with_check(discord_id, plan, retries=3):
    guild = bot.get_guild(DISCORD_GUILD_ID)
    member = guild.get_member(int(discord_id))
    role = guild.get_role(ROLE_MAP[plan])

    for attempt in range(1, retries + 1):
        await member.add_roles(role)
        logger.info("[Discord] Attempt %s: Assigned role %s to %s", attempt, role.name, member.display_name)
        if await verify_role(member, role, should_have=True):
            logger.info("[Discord] Verified role %s for %s", role.name, member.display_name)
            return True
        await asyncio.sleep(10 * attempt)  # backoff

    logger.error("[Discord] Failed to assign role %s to %s after %s attempts", role.name, member.display_name, retries)
    return False

async def remove_role_with_check(discord_id, plan, retries=3):
    guild = bot.get_guild(DISCORD_GUILD_ID)
    member = guild.get_member(int(discord_id))
    role = guild.get_role(ROLE_MAP[plan])

    for attempt in range(1, retries + 1):
        await member.remove_roles(role)
        logger.info("[Discord] Attempt %s: Removed role %s from %s", attempt, role.name, member.display_name)
        if await verify_role(member, role, should_have=False):
            logger.info("[Discord] Verified removal of role %s from %s", role.name, member.display_name)
            return True
        await asyncio.sleep(10 * attempt)

    logger.error("[Discord] Failed to remove role %s from %s after %s attempts", role.name, member.display_name, retries)
    return False

# --- Worker Loop ---
async def queue_processor_loop():
    backoff = 15
    max_backoff = 300
    while True:
        try:
            logger.info("[Worker] Polling pending assignments (interval=%ss)", backoff)
            assignments = await asyncio.to_thread(get_pending_assignments, 50)
            count = len(assignments.get("assignments", []))
            logger.info("[Worker] Retrieved %s assignments", count)

            for row in assignments.get("assignments", []):
                try:
                    await process_assignment(row)
                except Exception as e:
                    logger.warning("[Worker] Error processing row %s: %s", row.get("row_id"), e)

            backoff = 60
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("[Worker] Hit Google rate limit (429). Backing off...")
                backoff = min(backoff * 2, max_backoff)
            else:
                logger.error("[Worker] HTTP error during poll: %s", e)
        except Exception as e:
            logger.error("[Worker] Unexpected loop error: %s", e, exc_info=True)

        await asyncio.sleep(backoff)

async def process_assignment(row):
    discord_id = row.get("discord_id")
    plan = row.get("plan")
    action = row.get("action")
    row_id = row.get("row_id")
    attempts = int(row.get("attempts") or 0)

    try:
        success = False
        if action == "assign":
            success = await assign_role_with_check(discord_id, plan)
            status = "assigned" if success else "failed"
        elif action == "remove":
            success = await remove_role_with_check(discord_id, plan)
            status = "removed" if success else "failed"
        else:
            raise ValueError(f"Unknown action {action}")

        await asyncio.to_thread(update_assignment, row_id, {"status": status})
        logger.info("[Worker] Row %s completed with status=%s", row_id, status)
    except Exception as e:
        logger.warning("[Worker] Failed row %s: %s (attempt %s)", row_id, e, attempts + 1)
        await asyncio.to_thread(update_assignment, row_id, {"attempts": "increment", "last_error": str(e)})

# --- Flask Routes ---
@app.route("/")
def home():
    return "Service is running"

@app.route("/login")
def login():
    plan = request.args.get("plan")
    if not plan or plan not in PLAN_TO_PRICE_ID:
        logger.error("[OAuth] Invalid or missing plan: %s", plan)
        return "Invalid plan", 400
    session["plan"] = plan
    logger.info("[OAuth] Initiating for plan=%s", plan)
    return redirect(
        f"https://discord.com/api/oauth2/authorize?client_id={DISCORD_CLIENT_ID}&redirect_uri={DISCORD_REDIRECT_URI}&response_type=code&scope=identify%20email"
    )

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        logger.error("[OAuth] Missing code parameter")
        return "Missing code", 400

    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "scope": "identify email",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post("https://discord.com/api/oauth2/token", data=data, headers=headers)
        r.raise_for_status()
        credentials = r.json()
    except requests.exceptions.RequestException as e:
        logger.error("[OAuth] Error exchanging code for token: %s", e)
        return "OAuth error", 400

    access_token = credentials["access_token"]
    try:
        user = requests.get("https://discord.com/api/users/@me", headers={"Authorization": f"Bearer {access_token}"}).json()
    except requests.exceptions.RequestException as e:
        logger.error("[OAuth] Error fetching user info: %s", e)
        return "OAuth error", 400

    discord_id = user["id"]
    email = user.get("email")
    plan = session.get("plan")
    if not plan:
        logger.error("[Stripe] No plan in session for %s", discord_id)
        return "No plan specified", 400

    price_id = PLAN_TO_PRICE_ID.get(plan)
    if not price_id:
        logger.error("[Stripe] No price ID mapped for plan %s", plan)
        return "Plan not supported", 400

    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            allow_promotion_codes=True,
            success_url="https://marketwavebot-f0tu.onrender.com/success",
            cancel_url="https://marketwavebot-f0tu.onrender.com/cancel",
            metadata={"discord_id": discord_id, "email": email or "", "plan": plan},
            customer_email=email if email else None,
        )
        logger.info("[Stripe] Created checkout session for %s plan=%s", discord_id, plan)
        return redirect(checkout_session.url, code=303)
    except stripe.error.StripeError as e:
        logger.error("[Stripe] Error creating checkout session: %s", e)
        return "Payment error", 400

@app.route("/success")
def success():
    return "Payment successful! Your subscription is being processed."

@app.route("/cancel")
def cancel():
    return "Payment cancelled. Please try again."

@app.route("/webhook", methods=["POST"])
@limiter.limit("200 per minute")
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        logger.error("[Stripe] Webhook error: %s", e)
        return str(e), 400

    logger.info("[Stripe] Event: %s", event["type"])

    if event["type"] == "checkout.session.completed":
        data = event["data"]["object"]
        metadata = data.get("metadata", {})
        discord_id = metadata.get("discord_id")
        email = metadata.get("email") or data.get("customer_details", {}).get("email")
        plan = metadata.get("plan")
        subscription_id = data.get("subscription")

        if not discord_id or not plan:
            logger.error("[Stripe] Missing required metadata in checkout.session.completed")
            return "Missing metadata", 400

        insert_assignment({
            "email": email,
            "discord_id": discord_id,
            "plan": plan,
            "status": "pending",
            "action": "assign",
            "attempts": 0,
            "last_error": "",
            "stripe_subscription_id": subscription_id,
            "created_at": datetime.utcnow().isoformat(),
        })
        logger.info("[Sheets] Inserted assignment row for %s", discord_id)

    elif event["type"] in ["customer.subscription.deleted", "invoice.payment_failed"]:
        subscription_id = event["data"]["object"]["id"]
        sub_info = lookup_by_subscription_id(subscription_id)
        if sub_info:
            insert_assignment({
                "email": sub_info.get("email"),
                "discord_id": sub_info.get("discord_id"),
                "plan": sub_info.get("plan"),
                "status": "pending",
                "action": "remove",
                "attempts": 0,
                "last_error": "",
                "stripe_subscription_id": subscription_id,
                "created_at": datetime.utcnow().isoformat(),
            })
            logger.info("[Sheets] Queued removal for subscription %s", subscription_id)

    return "", 200

# --- Startup ---
@bot.event
async def on_ready():
    logger.info("[Discord] Logged in as %s (id=%s)", bot.user, bot.user.id)
    bot.loop.create_task(queue_processor_loop())

if __name__ == "__main__":
    import threading

    def run_flask():
        app.run(host="0.0.0.0", port=10000)

    threading.Thread(target=run_flask, daemon=True).start()
    bot.run(DISCORD_BOT_TOKEN)
