import os
import threading
import requests
import json
import asyncio
from datetime import datetime
from flask import Flask, request, redirect, session, jsonify
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

# ENV VARS
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI")
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))
ROLE_5K_TO_50K = int(os.getenv("ROLE_5K_TO_50K"))
ROLE_ELITE = int(os.getenv("ROLE_ELITE"))
ROLE_PLUS = int(os.getenv("ROLE_PLUS"))
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
GOOGLE_APPS_SCRIPT_URL = os.getenv("GOOGLE_APPS_SCRIPT_URL")

# DISCORD BOT 
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

PLAN_TO_ROLE = {
    "5K to 50K Challenge": ROLE_5K_TO_50K,
    "MarketWave Elite": ROLE_ELITE,
    "MarketWave Plus": ROLE_PLUS,
}

PLAN_TO_PRICE_ID = {
    "MarketWave Plus": "price_1RdEoc08Ntv6wEBmifMeruFq",
    "MarketWave Elite": "price_1RdEob08Ntv6wEBmT27qALuM",
    "5K to 50K Challenge": "price_1RdEoc08Ntv6wEBmUZOADdMd"
}

@bot.event
async def on_ready():
    print(f"✅ Discord bot logged in as {bot.user}")

async def assign_or_remove_role(discord_id, plan, action):
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        print(f"❌ Guild {DISCORD_GUILD_ID} not found")
        return

    try:
        member = await guild.fetch_member(int(discord_id))
    except discord.NotFound:
        print(f"❌ Member with ID {discord_id} not found in guild {guild.name}")
        return
    except Exception as e:
        print(f"❌ Error fetching member {discord_id}: {e}")
        return

    role_id = PLAN_TO_ROLE.get(plan)
    if not role_id:
        print(f"❌ Role ID not found for plan: '{plan}'")
        return

    role = guild.get_role(role_id)
    if not role:
        print(f"❌ Role object not found in guild for ID: {role_id}")
        return

    try:
        if action == "assign":
            await member.add_roles(role)
            print(f"✅ Assigned {role.name} to {member.display_name}")
        elif action == "remove":
            await member.remove_roles(role)
            print(f"✅ Removed {role.name} from {member.display_name}")
    except Exception as e:
        print(f"❌ Role {action} failed for {member.display_name} - {e}")

# FLASK APP SETUP
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'supersecretkey')

DISCORD_API_BASE_URL = "https://discord.com/api"
SCOPE = "identify email"

def update_subscription(email, discord_id, plan, status, stripe_customer_id=None, stripe_subscription_id=None):
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
        r = requests.post(GOOGLE_APPS_SCRIPT_URL, json=payload, timeout=5)
        r.raise_for_status()
        print(f"✅ Updated Google Sheet for {email} - {status}")
        return True
    except Exception as e:
        print(f"❌ Failed to update Google Sheet: {e}")
        return False

def lookup_by_subscription_id(subscription_id):
    try:
        r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"stripe_subscription_id": subscription_id}, timeout=5)
        r.raise_for_status()
        data = r.json()
        return data.get("email"), data.get("discord_id"), data.get("plan")
    except Exception as e:
        print(f"❌ Failed to lookup by subscription ID: {e}")
        return None, None, None

def lookup_by_email(email):
    try:
        r = requests.get(GOOGLE_APPS_SCRIPT_URL, params={"email": email}, timeout=5)
        r.raise_for_status()
        data = r.json()
        return data.get("discord_id")
    except Exception as e:
        print(f"❌ Failed to lookup by email: {e}")
        return None

@app.route('/')
def index():
    return 'OAuth system working!'

@app.route('/login')
def login():
    plan = request.args.get("plan")
    if plan:
        session["plan"] = plan
    return redirect(
        f"{DISCORD_API_BASE_URL}/oauth2/authorize?client_id={DISCORD_CLIENT_ID}&redirect_uri={DISCORD_REDIRECT_URI}&response_type=code&scope={SCOPE}"
    )

import stripe
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

@app.route('/callback')
def callback():
    code = request.args.get("code")
    plan = session.get("plan")
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post(f"{DISCORD_API_BASE_URL}/oauth2/token", data=data, headers=headers)
        r.raise_for_status()
        credentials = r.json()

        user_response = requests.get(
            f"{DISCORD_API_BASE_URL}/users/@me",
            headers={"Authorization": f"Bearer {credentials['access_token']}"}
        )
        user_response.raise_for_status()
        user_info = user_response.json()
        discord_id = user_info["id"]
        email = user_info.get("email", "")
        session["discord_id"] = discord_id
        session["email"] = email

        update_subscription(email, discord_id, "None", "linked")

        if plan:
            stripe_price_id = PLAN_TO_PRICE_ID[plan]
            checkout_session = stripe.checkout.Session.create(
                payment_method_types=['card'],
                mode='subscription',
                line_items=[{
                    'price': stripe_price_id,
                    'quantity': 1,
                }],
                customer_email=email,
                metadata={
                    'plan': plan,
                    'discord_id': discord_id,
                    'email': email
                },
                allow_promotion_codes=True,
                success_url="https://marketwavetrading.com/success",
                cancel_url="https://marketwavetrading.com/cancel",
            )
            return redirect(checkout_session.url)

        return redirect("https://marketwavetrading.com/success")

    except Exception as e:
        import traceback
        print("❌ Error in OAuth/Stripe flow:", e)
        traceback.print_exc()
        return jsonify({"error": "Authentication failed", "details": str(e)}), 500

@app.route('/webhook', methods=['POST'])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get('Stripe-Signature')
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        print("❌ Webhook signature verification failed:", e)
        return "Invalid signature", 400

    if event['type'] == "checkout.session.completed":
        session_obj = event["data"]["object"]
        email = session_obj.get("customer_details", {}).get("email", "")
        stripe_customer_id = session_obj.get("customer")
        plan = session_obj.get("metadata", {}).get("plan", "Unknown")
        stripe_subscription_id = session_obj.get("subscription")

        discord_id = lookup_by_email(email) or session_obj.get("metadata", {}).get("discord_id")

        update_subscription(email, discord_id, plan, "active", stripe_customer_id, stripe_subscription_id)

        if discord_id and plan:
            bot.loop.create_task(assign_or_remove_role(discord_id, plan, "assign"))
        else:
            print(f"❌ [Webhook] Missing discord_id or plan for email {email}")

        return jsonify({"status": "success"}), 200

    if event['type'] == "customer.subscription.deleted":
        subscription = event["data"]["object"]
        stripe_subscription_id = subscription["id"]
        print(f"📩 Received cancellation webhook for subscription {stripe_subscription_id}")
        email, discord_id, plan = lookup_by_subscription_id(stripe_subscription_id)

        update_subscription(email, discord_id, plan, "canceled", subscription.get("customer"), stripe_subscription_id)

        if discord_id and plan:
            async def delayed_removal():
                await asyncio.sleep(2)
                await assign_or_remove_role(discord_id, plan, "remove")
            bot.loop.create_task(delayed_removal())
        else:
            print(f"❌ Missing data for cancellation webhook: discord_id={discord_id}, plan={plan}")

        return jsonify({"status": "success"}), 200

    return jsonify({"status": "ignored"}), 200

def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    bot.run(DISCORD_BOT_TOKEN)

