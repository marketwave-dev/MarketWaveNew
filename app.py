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

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

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

# Flask app
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

limiter = Limiter(get_remote_address, app=app, default_limits=["100 per minute"])

# Stripe config
stripe.api_key = STRIPE_SECRET_KEY

# Discord bot
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Role mapping
ROLE_MAP = {
    "MarketWave 5K-50K": ROLE_5K_TO_50K,
    "MarketWave Elite": ROLE_ELITE,
    "MarketWave Plus": ROLE_PLUS,
}

# --- Google Sheet Integration ---
def insert_assignment(data):
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json={"action": "insert_assignment", "data": data})
    r.raise_for_status()
    return r.json()

def get_pending_assignments(limit=50):
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "get_pending_assignments", "limit": limit})
    r.raise_for_status()
    return r.json()

def update_assignment(row_id, updates):
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json={"action": "update_assignment", "row_id": row_id, "updates": updates})
    r.raise_for_status()
    return r.json()

def remove_assignment(row_id):
    r = requests.post(GOOGLE_APPS_SCRIPT_URL, json={"action": "remove_assignment", "row_id": row_id})
    r.raise_for_status()
    return r.json()

def lookup_by_subscription_id(subscription_id):
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_subscription_id", "subscription_id": subscription_id})
    r.raise_for_status()
    return r.json()

def lookup_by_email(email):
    r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"action": "lookup_by_email", "email": email})
    r.raise_for_status()
    return r.json()

# --- Discord Role Management ---
async def assign_role(discord_id, plan):
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        raise ValueError("Guild not found")
    member = guild.get_member(int(discord_id))
    if not member:
        raise ValueError(f"Member {discord_id} not found")
    role_id = ROLE_MAP.get(plan)
    if not role_id:
        raise ValueError(f"Plan {plan} not mapped to a role")
    role = guild.get_role(role_id)
    if not role:
        raise ValueError(f"Role ID {role_id} not found in guild")
    await member.add_roles(role)
    logger.info("[Discord] Assigned role %s to %s", role.name, member.display_name)

async def remove_role(discord_id, plan):
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        raise ValueError("Guild not found")
    member = guild.get_member(int(discord_id))
    if not member:
        raise ValueError(f"Member {discord_id} not found")
    role_id = ROLE_MAP.get(plan)
    if not role_id:
        raise ValueError(f"Plan {plan} not mapped to a role")
    role = guild.get_role(role_id)
    if not role:
        raise ValueError(f"Role ID {role_id} not found in guild")
    await member.remove_roles(role)
    logger.info("[Discord] Removed role %s from %s", role.name, member.display_name)

# --- Worker Loop with Backoff ---
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

            backoff = 60  # normal after success
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
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
        if action == "assign":
            await assign_role(discord_id, plan)
        elif action == "remove":
            await remove_role(discord_id, plan)
        else:
            raise ValueError(f"unknown action {action}")
        remove_assignment(row_id)
        logger.info("[Worker] Completed row %s (%s %s)", row_id, action, plan)
    except Exception as e:
        logger.warning("[Worker] Failed row %s: %s (attempt %s)", row_id, e, attempts+1)
        update_assignment(row_id, {"attempts": "increment", "last_error": str(e)})

# --- Flask Routes ---
@app.route("/")
def home():
    return "Service is running"

@app.route("/login")
def login():
    plan = request.args.get("plan")
    session["plan"] = plan
    logger.info("[OAuth] Initiating for plan=%s", plan)
    return redirect(
        f"https://discord.com/api/oauth2/authorize?client_id={DISCORD_CLIENT_ID}&redirect_uri={DISCORD_REDIRECT_URI}&response_type=code&scope=identify%20email"
    )

@app.route("/callback")
def callback():
    code = request.args.get("code")
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "scope": "identify email",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post("https://discord.com/api/oauth2/token", data=data, headers=headers)
    r.raise_for_status()
    credentials = r.json()
    access_token = credentials["access_token"]
    user = requests.get("https://discord.com/api/users/@me", headers={"Authorization": f"Bearer {access_token}"}).json()

    discord_id = user["id"]
    email = user["email"]
    plan = session.get("plan")

    checkout_session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "product_data": {"name": plan},
                "unit_amount": 1000,
            },
            "quantity": 1,
        }],
        mode="subscription",
        success_url="https://example.com/success",
        cancel_url="https://example.com/cancel",
        metadata={"discord_id": discord_id, "email": email, "plan": plan},
    )

    logger.info("[Stripe] Created checkout session for %s plan=%s", discord_id, plan)
    return redirect(checkout_session.url, code=303)

@app.route("/webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        return str(e), 400

    logger.info("[Stripe] Event: %s", event["type"])

    if event["type"] == "checkout.session.completed":
        data = event["data"]["object"]
        discord_id = data["metadata"]["discord_id"]
        email = data["metadata"]["email"]
        plan = data["metadata"]["plan"]
        subscription_id = data.get("subscription")

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
        logger.info("[Sheets] inserted assignment row for %s", discord_id)

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
            logger.info("[Sheets] queued removal for subscription %s", subscription_id)

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
