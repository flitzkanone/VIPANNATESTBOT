import os
import logging
import json
import random
from dotenv import load_dotenv
from datetime import datetime, timedelta
from io import BytesIO
import asyncio
import re
from math import ceil

from fpdf import FPDF
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error, InputMediaPhoto, InputMediaVideo, User
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.helpers import escape_markdown

# --- Configuration ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PAYPAL_USER = os.getenv("PAYPAL_USER")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
NOTIFICATION_GROUP_ID = os.getenv("NOTIFICATION_GROUP_ID")
TELEGRAM_USERNAME = os.getenv("TELEGRAM_USERNAME", "ANNASPICY")

AGE_ANNA = os.getenv("AGE_ANNA", "18")

BTC_WALLET = "1FcgMLNBDLiuDSDip7AStuP19sq47LJB12"
ETH_WALLET = "0xeeb8FDc4aAe71B53934318707d0e9747C5c66f6e"

PRICES = {
    "bilder": {10: 5, 25: 10, 35: 15},
    "videos": {10: 15, 25: 25, 35: 30},
    "livecall": {10: 10, 15: 15, 20: 20, 30: 30, 60: 50, 120: 80},
    "treffen": {60: 200, 120: 300, 240: 400, 1440: 600, 2880: 800}
}
VOUCHER_FILE = "vouchers.json"
STATS_FILE = "stats.json"
MEDIA_DIR = "image"
DISCOUNT_MSG_HEADER = "--- BOT DISCOUNT DATA (DO NOT DELETE) ---"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


# --- I18N and Text Management ---

texts = {
    # General
    "back_button": {"de": "« Zurück", "en": "« Back"},
    "main_menu_button": {"de": "« Zurück zum Hauptmenü", "en": "« Back to Main Menu"},
    "cancel_button": {"de": "Abbrechen", "en": "Cancel"},
    "error_occurred": {"de": "Ups! Es ist ein Fehler aufgetreten. Bitte versuche es erneut, indem du /start sendest.", "en": "Oops! An error occurred. Please try again by sending /start."},
    "banned_user_message": {"de": "Du bist von der Nutzung dieses Bots ausgeschlossen.", "en": "You are banned from using this bot."},
    "banned_user_alert": {"de": "Du bist von der Nutzung dieses Bots ausgeschlossen.", "en": "You are banned from using this bot."},

    # Language Selection
    "language_selection_prompt": {"de": "Bitte wähle deine Sprache:", "en": "Please select your language:"},

    # Start/Main Menu
    "welcome_text": {"de": "Herzlich Willkommen! ✨\n\nHier kannst du eine Vorschau meiner Inhalte sehen oder direkt ein Paket auswählen. Die gesamte Bedienung erfolgt über die Buttons.", "en": "Welcome! ✨\n\nHere you can see a preview of my content or select a package directly. The entire operation is done via the buttons."},
    "preview_button": {"de": "🖼️ Vorschau", "en": "🖼️ Preview"},
    "packages_button": {"de": "🛍️ Bilder & Videos", "en": "🛍️ Pictures & Videos"},
    "live_call_button": {"de": "📞 Live Call", "en": "📞 Live Call"},
    "meeting_button": {"de": "📅 Treffen buchen", "en": "📅 Book a Meeting"},

    # Discount Message
    "discount_offer_text": {
        "de": "🎁 Wir haben dich vermisst! 🎁\n\nAls Willkommensgruß erhältst du einen exklusiven **10% Rabatt** auf alle Pakete!\n\nPLUS: Unser **2-für-1 PayPal-Angebot** gilt weiterhin für dich. Nutze die Chance!",
        "en": "🎁 We've missed you! 🎁\n\nAs a welcome back gift, you receive an exclusive **10% discount** on all packages!\n\nPLUS: Our **2-for-1 PayPal offer** is still valid for you. Take the chance!"
    },
    "discount_text": {"de": "Rabatt", "en": "Discount"},
    "discount_offer_button": {"de": "💸 Zu meinen exklusiven Preisen 💸", "en": "💸 To my exclusive prices 💸"},

    # Preview
    "preview_caption": {"de": "Hier ist eine Vorschau. Ich bin {age_anna} Jahre alt. Klicke auf 'Nächstes Medium' für mehr.", "en": "Here is a preview. I am {age_anna} years old. Click 'Next Medium' for more."},
    "no_preview_content": {"de": "Ups! Ich konnte gerade keine passenden Inhalte finden...", "en": "Oops! I couldn't find any suitable content right now..."},
    "next_medium_button": {"de": "🖼️ Nächstes Medium", "en": "🖼️ Next Medium"},
    "prices_and_packages_button": {"de": "🛍️ Preise & Pakete", "en": "🛍️ Prices & Packages"},
    "preview_limit_reached_alert": {"de": "Du hast dein Vorschau-Limit von 25 Klicks bereits erreicht.", "en": "You have already reached your preview limit of 25 clicks."},
    "preview_limit_reached_text": {"de": "Du hast dein Vorschau-Limit von 25 Klicks erreicht. Sieh dir jetzt die Preise an, um mehr zu sehen!", "en": "You have reached your preview limit of 25 clicks. Check out the prices now to see more!"},
    "view_prices_button": {"de": "🛍️ Preise ansehen", "en": "🛍️ View Prices"},

    # Prices Page
    "select_package_caption": {"de": "Wähle dein gewünschtes Paket:", "en": "Choose your desired package:"},
    "package_button_text_bilder": {"de": "{amount} Bilder", "en": "{amount} Pictures"},
    "package_button_text_videos": {"de": "{amount} Videos", "en": "{amount} Videos"},

    # Package Selection
    "package_selection_text": {"de": "Du hast das Paket **{amount} {media_type}** für {price_str} ausgewählt.\n\nWie möchtest du bezahlen?", "en": "You have selected the **{amount} {media_type}** package for {price_str}.\n\nHow would you like to pay?"},
    "paypal_offer_text": {"de": "\n\n🔥 *PayPal-Aktion: Kaufe 1, erhalte 2!* 🔥", "en": "\n\n🔥 *PayPal Offer: Buy 1, get 2!* 🔥"},
    "paypal_button": {"de": " PayPal", "en": " PayPal"},
    "voucher_button": {"de": " Gutschein (Amazon)", "en": " Voucher (Amazon)"},
    "crypto_button": {"de": "🪙 Krypto", "en": "🪙 Crypto"},
    "back_to_prices_button": {"de": "« Zurück zu den Preisen", "en": "« Back to Prices"},

    # Payment
    "paypal_payment_text": {"de": "Super! Klicke auf den Link, um die Zahlung für **{package_info_text}** in Höhe von **{price}€** abzuschließen...\n\n➡️ [Hier sicher bezahlen]({paypal_link})\n\n", "en": "Great! Click the link to complete the payment for **{package_info_text}** amounting to **{price}€**...\n\n➡️ [Pay securely here]({paypal_link})\n\n"},
    "contact_after_payment_text": {"de": "📲 *Melde dich danach bei @{TELEGRAM_USERNAME} mit einem Screenshot!*", "en": "📲 *Contact @{TELEGRAM_USERNAME} with a screenshot afterwards!*"},
    "voucher_prompt_text": {"de": "Bitte sende mir jetzt deinen Amazon-Gutschein-Code als einzelne Nachricht.", "en": "Please send me your Amazon voucher code as a single message now."},
    "crypto_prompt_text": {"de": "Bitte wähle die gewünschte Kryptowährung:", "en": "Please choose the desired cryptocurrency:"},
    "crypto_payment_text": {"de": "Zahlung mit **{crypto_name}** für **{price}€**.\n\n`{wallet_address}`", "en": "Payment with **{crypto_name}** for **{price}€**.\n\n`{wallet_address}`"},

    # Vouchers
    "voucher_submitted_text": {"de": "✅ Vielen Dank! Dein Gutschein wurde übermittelt.\n\nDie manuelle Überprüfung dauert ca. **10-20 Minuten**. Sobald dein Code verifiziert ist, melde ich mich bei dir.", "en": "✅ Thank you! Your voucher has been submitted.\n\nThe manual verification takes about **10-20 minutes**. I will contact you as soon as your code is verified."},

    # Live Call
    "live_call_menu_text": {"de": "📞 Wähle die gewünschte Dauer für deinen Live Call:", "en": "📞 Choose the desired duration for your Live Call:"},
    "live_call_unit_min": {"de": "{duration} Min", "en": "{duration} min"},
    "live_call_unit_hr": {"de": "{hours} Std", "en": "{hours} hr"},
    "live_call_available_text": {"de": "✅ Ich bin für deinen Call verfügbar!", "en": "✅ I am available for your call!"},
    "live_call_selection_text": {"de": "Du hast einen **Live Call** für **{amount} Minuten** für *{price}€* ausgewählt.\n\nBitte schließe die Bezahlung ab und melde dich danach bei **@{TELEGRAM_USERNAME}** mit einem Screenshot.", "en": "You have selected a **Live Call** for **{amount} minutes** for *{price}€*.\n\nPlease complete the payment and then contact **@{TELEGRAM_USERNAME}** with a screenshot."},
    "package_info_live_call": {"de": "{amount} Min Live Call", "en": "{amount} min Live Call"},


    # Meeting
    "meeting_menu_text": {"de": "📅 Wähle die gewünschte Dauer für dein Treffen:", "en": "📅 Choose the desired duration for your meeting:"},
    "meeting_duration_1_hour": {"de": "1 Stunde", "en": "1 hour"},
    "meeting_duration_2_hours": {"de": "2 Stunden", "en": "2 hours"},
    "meeting_duration_4_hours": {"de": "4 Stunden", "en": "4 hours"},
    "meeting_duration_1_day": {"de": "1 Tag", "en": "1 day"},
    "meeting_duration_2_days": {"de": "2 Tage", "en": "2 days"},
    "meeting_deposit_info_button": {"de": "🤔 Warum eine Anzahlung?", "en": "🤔 Why a deposit?"},
    "meeting_deposit_info_text": {
        "de": ("🤔 **Warum eine kleine Anzahlung?** 🤔\n\n"
               "Ganz einfach: Sie ist eine kleine Sicherheit für uns beide! 🤝\n\n"
               "1️⃣ **Für dich:** Dein Termin ist damit fest für dich geblockt und niemand kann ihn dir wegschnappen. 🔒\n"
               "2️⃣ **Für mich:** Sie hilft mir, meine Anreise zu planen ✈️ und schützt mich vor Spaßbuchungen. So weiß ich, dass du es auch wirklich ernst meinst. 😊\n\n"
               "Den großen Rest zahlst du dann ganz entspannt und diskret in bar, wenn wir uns sehen. 💸"),
        "en": ("🤔 **Why a small deposit?** 🤔\n\n"
               "It's simple: it's a small security for both of us! 🤝\n\n"
               "1️⃣ **For you:** Your appointment is firmly booked for you and nobody can take it away. 🔒\n"
               "2️⃣ **For me:** It helps me plan my travel ✈️ and protects me from fake bookings. This way I know you are serious about it. 😊\n\n"
               "You'll pay the rest relaxed and discreetly in cash when we meet. 💸")
    },
    "understood_back_button": {"de": "« Verstanden & zurück", "en": "« Understood & back"},
    "understood_back_to_payment_button": {"de": "« Verstanden & zurück zur Zahlung", "en": "« Understood & back to payment"},
    "meeting_date_prompt": {"de": "📅 Bitte gib dein Wunschdatum ein (z.B. `24.12`):", "en": "📅 Please enter your desired date (e.g., `24.12`):"},
    "invalid_date_prompt": {"de": "Das war leider kein gültiges Datum. 😕\n\nBitte gib dein Wunschdatum nochmal ein (z.B. `24.12`):", "en": "That was not a valid date. 😕\n\nPlease enter your desired date again (e.g., `24.12`):"},
    "meeting_location_prompt": {"de": "📍 Super! Und an welchem Ort (z.B. Stadt)?", "en": "📍 Great! And at what location (e.g., city)?"},
    "meeting_summary_error": {"de": "Ein Fehler ist aufgetreten. Bitte beginne die Buchung erneut.", "en": "An error has occurred. Please start the booking again."},
    "back_to_meeting_menu_button": {"de": "« Zum Treffen-Menü", "en": "« To Meeting Menu"},
    "meeting_available_status": {"de": "Status: ✅ Dein Wunschtermin ist verfügbar!", "en": "Status: ✅ Your desired date is available!"},
    "meeting_summary_title": {"de": "📅 **Deine Terminanfrage:**\n\n", "en": "📅 **Your Appointment Request:**\n\n"},
    "meeting_summary_duration": {"de": "**Dauer:** {duration_text}\n", "en": "**Duration:** {duration_text}\n"},
    "meeting_summary_date": {"de": "**Datum:** {date}\n", "en": "**Date:** {date}\n"},
    "meeting_summary_location": {"de": "**Ort:** {location}\n\n", "en": "**Location:** {location}\n\n"},
    "meeting_summary_total_price": {"de": "**Gesamtpreis:** {full_price}€\n", "en": "**Total Price:** {full_price}€\n"},
    "meeting_summary_cash_discount": {"de": "**Barzahler-Rabatt (10%):** -{discount_amount:.2f}€\n", "en": "**Cash Payment Discount (10%):** -{discount_amount:.2f}€\n"},
    "meeting_summary_final_price": {"de": "**Neuer Endpreis (bei Barzahlung):** **{cash_price:.2f}€**\n\n", "en": "**New Final Price (with cash payment):** **{cash_price:.2f}€**\n\n"},
    "meeting_summary_deposit_info": {"de": "Zur Verifizierung ist eine **Anzahlung von 25% ({deposit}€)** erforderlich. Der Restbetrag wird in bar beim Treffen bezahlt.", "en": "A **deposit of 25% ({deposit}€)** is required for verification. The remaining amount will be paid in cash at the meeting."},
    "deposit_info_button_summary": {"de": "🤔 Warum diese Anzahlung?", "en": "🤔 Why this deposit?"},
    "deposit_paypal_button": {"de": "💸 Anzahlung ({deposit}€) per PayPal", "en": "💸 Deposit ({deposit}€) via PayPal"},
    "deposit_voucher_button": {"de": "🎟️ Anzahlung ({deposit}€) per Gutschein", "en": "🎟️ Deposit ({deposit}€) via Voucher"},
    "deposit_crypto_button": {"de": "🪙 Anzahlung ({deposit}€) per Krypto", "en": "🪙 Deposit ({deposit}€) via Crypto"},
    "cancel_booking_button": {"de": "« Buchung abbrechen", "en": "« Cancel Booking"},
    "package_info_meeting_deposit": {"de": "Anzahlung Treffen ({duration_text})", "en": "Deposit for Meeting ({duration_text})"},
}

def get_text(key: str, context: ContextTypes.DEFAULT_TYPE, **kwargs) -> str:
    """Fetches a string in the user's chosen language."""
    lang = context.user_data.get('language', 'de')  # Default to German
    text_template = texts.get(key, {}).get(lang) or texts.get(key, {}).get('en', f"<{key}_{lang}_NOT_FOUND>")
    return text_template.format(**kwargs) if kwargs else text_template

# --- Helper Functions ---
def load_vouchers():
    try:
        with open(VOUCHER_FILE, "r") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return {"amazon": []}

def save_vouchers(vouchers):
    with open(VOUCHER_FILE, "w") as f: json.dump(vouchers, f, indent=2)

def load_stats():
    try:
        with open(STATS_FILE, "r") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"users": {}, "admin_logs": {}, "events": {}}

def save_stats(stats):
    with open(STATS_FILE, "w") as f: json.dump(stats, f, indent=4)

def ensure_user_in_stats(user_id: int, stats: dict) -> dict:
    user_id_str = str(user_id)
    if user_id_str not in stats.get("users", {}):
        stats.setdefault("users", {})[user_id_str] = {
            "first_start": datetime.now().isoformat(),
            "last_start": datetime.now().isoformat(),
            "discount_sent": False,
            "preview_clicks": 0,
            "payments_initiated": [],
            "banned": False,
            "paypal_offer_sent": False
        }
        save_stats(stats)
    return stats

async def save_discounts_to_telegram(context: ContextTypes.DEFAULT_TYPE):
    if not NOTIFICATION_GROUP_ID: return
    stats = load_stats(); discounts_to_save = {}
    for user_id, user_data in stats.get("users", {}).items():
        if "discounts" in user_data: discounts_to_save[user_id] = user_data["discounts"]
    json_string = json.dumps(discounts_to_save, indent=2); message_text = f"{DISCOUNT_MSG_HEADER}\n<tg-spoiler>{json_string}</tg-spoiler>"; discount_message_id = stats.get("discount_message_id")
    try:
        if discount_message_id: await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=discount_message_id, text=message_text, parse_mode='HTML')
        else: raise error.BadRequest("No discount message ID found")
    except error.BadRequest:
        logger.warning("Discount message not found or invalid, creating a new one.")
        try:
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=message_text, parse_mode='HTML')
            stats["discount_message_id"] = sent_message.message_id; save_stats(stats)
        except Exception as e: logger.error(f"Could not create a new discount persistence message: {e}")

async def load_discounts_from_telegram(application: Application):
    if not NOTIFICATION_GROUP_ID: return
    try:
        stats = load_stats()
        discount_message_id = stats.get("discount_message_id")
        if not discount_message_id: return
        message = await application.bot.get_message(chat_id=NOTIFICATION_GROUP_ID, message_id=discount_message_id)
        json_match = re.search(r'<tg-spoiler>(.*)</tg-spoiler>', message.text_html, re.DOTALL)
        if not json_match: return
        discounts_data = json.loads(json_match.group(1)); users_updated = 0
        for user_id, discounts in discounts_data.items():
            if user_id in stats["users"]: stats["users"][user_id]["discounts"] = discounts; users_updated += 1
        if users_updated > 0: save_stats(stats); logger.info(f"Successfully restored discounts for {users_updated} users.")
    except Exception as e: logger.error(f"An unexpected error occurred during discount restore: {e}")

async def track_event(event_name: str, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    if str(user_id) == ADMIN_USER_ID: return
    stats = load_stats(); stats["events"][event_name] = stats["events"].get(event_name, 0) + 1; save_stats(stats)

def is_user_banned(user_id: int) -> bool:
    stats = load_stats(); user_data = stats.get("users", {}).get(str(user_id), {}); return user_data.get("banned", False)

def get_discounted_price(base_price: int, discount_data: dict, package_key: str) -> int:
    if not discount_data: return -1
    discount_type = discount_data.get("type")

    if discount_type == "percent":
        value = discount_data.get("value", 0); new_price = base_price * (1 - value / 100); return ceil(new_price)
    elif discount_type == "euro_packages":
        packages = discount_data.get("packages", {});
        if package_key in packages: return max(1, base_price - packages[package_key])
    elif discount_type == "percent_packages":
        packages = discount_data.get("packages", {});
        if package_key in packages: value = packages[package_key]; new_price = base_price * (1 - value / 100); return ceil(new_price)
    return -1

def get_package_button_text(media_type: str, amount: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    stats = load_stats(); user_data = stats.get("users", {}).get(str(user_id), {}); base_price = PRICES[media_type][amount]; package_key = f"{media_type}_{amount}"

    duration_text = ""
    if media_type == "livecall":
        if amount < 60: duration_text = get_text("live_call_unit_min", context, duration=amount)
        else: duration_text = get_text("live_call_unit_hr", context, hours=amount//60)
    elif media_type == "treffen":
        if amount == 60: duration_text = get_text("meeting_duration_1_hour", context)
        elif amount == 120: duration_text = get_text("meeting_duration_2_hours", context)
        elif amount == 240: duration_text = get_text("meeting_duration_4_hours", context)
        elif amount == 1440: duration_text = get_text("meeting_duration_1_day", context)
        elif amount == 2880: duration_text = get_text("meeting_duration_2_days", context)
    else:
        key = f"package_button_text_{media_type.lower()}"
        duration_text = get_text(key, context, amount=amount)

    if media_type not in ["livecall", "treffen"]:
        discount_price = get_discounted_price(base_price, user_data.get("discounts"), package_key)
        if discount_price != -1:
            return f"{duration_text} ~{base_price}~{discount_price}€ ✨"

    return f"{duration_text} {base_price}€"

async def check_user_status(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    if str(user_id) == ADMIN_USER_ID: return "admin", False, None
    stats = load_stats()
    user_id_str = str(user_id)
    now = datetime.now()

    if user_id_str not in stats.get("users", {}):
        stats = ensure_user_in_stats(user_id, stats)
        return "new", True, stats["users"][user_id_str]

    user_data = stats["users"][user_id_str]
    last_start_dt = datetime.fromisoformat(user_data.get("last_start"))

    if now - last_start_dt > timedelta(hours=24):
        return "returning", True, user_data

    return "active", False, user_data

async def send_or_update_admin_log(context: ContextTypes.DEFAULT_TYPE, user: User, event_text: str = ""):
    if not NOTIFICATION_GROUP_ID or str(user.id) == ADMIN_USER_ID: return
    try:
        user_id_str = str(user.id); stats = load_stats(); admin_logs = stats.get("admin_logs", {}); user_data = stats.get("users", {}).get(user_id_str, {}); log_message_id = admin_logs.get(user_id_str, {}).get("message_id")
        user_mention = f"[{escape_markdown(user.first_name, version=2)}](tg://user?id={user.id})"; discount_emoji = "💸" if user_data.get("discount_sent") or "discounts" in user_data else ""; banned_emoji = "🚫" if user_data.get("banned") else ""
        first_start_str = "N/A"
        if user_data.get("first_start"): first_start_str = datetime.fromisoformat(user_data["first_start"]).strftime('%Y-%m-%d %H:%M')
        preview_clicks = user_data.get("preview_clicks", 0); payments = user_data.get("payments_initiated", []); payments_str = "\n".join(f"   • {p}" for p in payments) if payments else "   • Keine"
        base_text = (f"👤 *Nutzer-Aktivität* {discount_emoji}{banned_emoji}\n\n" f"*Nutzer:* {user_mention} (`{user.id}`)\n" f"*Erster Start:* `{first_start_str}`\n\n" f"🖼️ *Vorschau-Klicks:* {preview_clicks}/25\n\n" f"💰 *Bezahlversuche*\n{payments_str}")
        final_text = f"{base_text}\n\n`Letzte Aktion: {event_text}`".strip()
        if log_message_id: await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id, text=final_text, parse_mode='Markdown')
        else:
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
            admin_logs.setdefault(user_id_str, {})["message_id"] = sent_message.message_id; stats["admin_logs"] = admin_logs; save_stats(stats)
    except error.BadRequest as e:
        if "chat not found" in str(e).lower(): logger.warning(f"Admin log group '{NOTIFICATION_GROUP_ID}' not found.")
        elif "message to edit not found" in str(e): logger.warning(f"Admin log for user {user.id} not found.")
        else: logger.error(f"BadRequest on admin log for user {user.id}: {e}")
    except error.TelegramError as e:
        if 'message is not modified' not in str(e): logger.warning(f"Temporary error updating admin log for user {user.id}: {e}")

def get_media_files(media_type: str, purpose: str) -> list:
    matching_files = []
    if media_type == 'combined' and purpose == 'vorschau':
        for mt in ['bilder', 'videos']:
            target_prefix = f"{mt.lower()}_{purpose.lower()}"
            if not os.path.isdir(MEDIA_DIR): continue
            for filename in os.listdir(MEDIA_DIR):
                normalized_filename = filename.lower().lstrip('•-_ ').replace(' ', '_')
                if normalized_filename.startswith(target_prefix): matching_files.append(os.path.join(MEDIA_DIR, filename))
    else:
        target_prefix = f"{media_type.lower()}_{purpose.lower()}"
        if not os.path.isdir(MEDIA_DIR): return []
        for filename in os.listdir(MEDIA_DIR):
            normalized_filename = filename.lower().lstrip('•-_ ').replace(' ', '_')
            if normalized_filename.startswith(target_prefix): matching_files.append(os.path.join(MEDIA_DIR, filename))
    matching_files.sort()
    return matching_files

async def cleanup_bot_messages(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    if 'tracked_message_ids' in context.chat_data:
        message_ids = context.chat_data['tracked_message_ids']
        for msg_id in list(message_ids):
            try:
                await context.bot.delete_message(chat_id, msg_id)
            except error.TelegramError:
                pass
        context.chat_data['tracked_message_ids'] = []
    context.chat_data.pop('media_message_id', None)
    context.chat_data.pop('control_message_id', None)

def track_message(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    if 'tracked_message_ids' not in context.chat_data:
        context.chat_data['tracked_message_ids'] = []
    if message_id not in context.chat_data['tracked_message_ids']:
        context.chat_data['tracked_message_ids'].append(message_id)

async def send_tracked_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, **kwargs):
    message = await context.bot.send_message(chat_id=chat_id, **kwargs)
    track_message(context, message.message_id)
    return message

async def send_tracked_photo(context: ContextTypes.DEFAULT_TYPE, chat_id: int, **kwargs):
    message = await context.bot.send_photo(chat_id=chat_id, **kwargs)
    track_message(context, message.message_id)
    return message

async def send_tracked_video(context: ContextTypes.DEFAULT_TYPE, chat_id: int, **kwargs):
    message = await context.bot.send_video(chat_id=chat_id, **kwargs)
    track_message(context, message.message_id)
    return message

# --- FIXED FUNCTION ---
async def query_or_message_edit(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    """Edits a message if the update is a callback query, or sends a new one if it's a message."""
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, **kwargs)
            # The original message ID is already being tracked if it was sent via a tracked method
            # Re-tracking an edited message is often not necessary unless you replace the message entirely
        except error.BadRequest as e:
            if "message is not modified" not in str(e):
                logger.warning(f"Could not edit message, sending new one. Error: {e}")
                await send_tracked_message(context, chat_id=update.effective_chat.id, text=text, **kwargs)
    elif update.message:
        await send_tracked_message(context, chat_id=update.effective_chat.id, text=text, **kwargs)


async def send_preview_message(update: Update, context: ContextTypes.DEFAULT_TYPE, media_type: str, start_index: int = 0):
    chat_id = update.effective_chat.id
    await cleanup_bot_messages(chat_id, context)

    media_paths = get_media_files(media_type, "vorschau")
    if media_type == 'combined': random.shuffle(media_paths)
    context.user_data['preview_gallery'] = media_paths

    if not media_paths:
        text = get_text("no_preview_content", context)
        keyboard = [[InlineKeyboardButton(get_text("back_button", context), callback_data="main_menu")]]
        await send_tracked_message(context, chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    start_index %= len(media_paths)
    context.user_data[f'preview_index_{media_type}'] = start_index
    media_path = media_paths[start_index]
    file_extension = os.path.splitext(media_path)[1].lower()

    try:
        with open(media_path, 'rb') as media_file:
            media_message = None
            if file_extension in ['.jpg', '.jpeg', '.png']:
                media_message = await send_tracked_photo(context, chat_id=chat_id, photo=media_file, protect_content=True)
            elif file_extension in ['.mp4', '.mov', '.m4v']:
                media_message = await send_tracked_video(context, chat_id=chat_id, video=media_file, protect_content=True, supports_streaming=True)
            if media_message:
                context.chat_data['media_message_id'] = media_message.message_id

        caption = get_text("preview_caption", context, age_anna=AGE_ANNA)
        keyboard = [
            [InlineKeyboardButton(get_text("next_medium_button", context), callback_data=f"next_preview:{media_type}")],
            [InlineKeyboardButton(get_text("prices_and_packages_button", context), callback_data="show_price_options")],
            [InlineKeyboardButton(get_text("live_call_button", context), callback_data="live_call_menu")],
            [InlineKeyboardButton(get_text("meeting_button", context), callback_data="treffen_menu")],
            [InlineKeyboardButton(get_text("main_menu_button", context), callback_data="main_menu")]
        ]
        await send_tracked_message(context, chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard))
    except error.TelegramError as e:
        logger.error(f"Error sending preview file {media_path}: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = update.effective_chat.id

    if update.callback_query:
        await cleanup_bot_messages(chat_id, context)
    
    if 'language' not in context.user_data:
        await cleanup_bot_messages(chat_id, context)
        keyboard = [
            [
                InlineKeyboardButton("Deutsch 🇩🇪", callback_data="select_lang:de"),
                InlineKeyboardButton("English 🇬🇧", callback_data="select_lang:en")
            ]
        ]
        await send_tracked_message(context, chat_id=chat_id, text="Bitte wähle deine Sprache / Please select your language:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if is_user_banned(user.id):
        await send_tracked_message(context, chat_id=chat_id, text=get_text("banned_user_message", context))
        return

    try:
        status, should_notify, user_data = await check_user_status(user.id, context)
        await track_event("start_command", context, user.id)
        if user_data and not user_data.get("discount_sent"):
            last_start_dt = datetime.fromisoformat(user_data.get("last_start"))
            if datetime.now() - last_start_dt > timedelta(hours=2):
                stats = load_stats()
                stats["users"][str(user.id)]["discounts"] = {"type": "percent", "value": 10}
                stats["users"][str(user.id)]["discount_sent"] = True
                save_stats(stats)
                await save_discounts_to_telegram(context)
                discount_text = get_text("discount_offer_text", context)
                keyboard = [[InlineKeyboardButton(get_text("discount_offer_button", context), callback_data="show_price_options")]]
                await send_tracked_message(context, chat_id=chat_id, text=discount_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                return
        if should_notify:
            event_text = "Bot gestartet (neuer Nutzer)" if status == "new" else "Bot erneut gestartet"
            await send_or_update_admin_log(context, user, event_text=event_text)
    except Exception as e:
        logger.error(f"Error in start logic for user {user.id}: {e}")

    stats = load_stats()
    ensure_user_in_stats(user.id, stats)
    stats["users"][str(user.id)]["last_start"] = datetime.now().isoformat()
    save_stats(stats)

    welcome_text = get_text("welcome_text", context)
    keyboard = [
        [InlineKeyboardButton(get_text("preview_button", context), callback_data="show_preview:combined")],
        [InlineKeyboardButton(get_text("packages_button", context), callback_data="show_price_options")],
        [InlineKeyboardButton(get_text("live_call_button", context), callback_data="live_call_menu")],
        [InlineKeyboardButton(get_text("meeting_button", context), callback_data="treffen_menu")]
    ]
    await query_or_message_edit(update, context, welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_prices_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    
    try:
        await cleanup_bot_messages(chat_id, context)
        await track_event("prices_viewed", context, user.id)
        await send_or_update_admin_log(context, user, event_text="Schaut sich die Preise an")

        caption = get_text("select_package_caption", context)
        keyboard = get_price_keyboard(user.id, context)
        
        media_paths = get_media_files("videos", "preis")
        
        if media_paths:
            random_media_path = random.choice(media_paths)
            try:
                with open(random_media_path, 'rb') as media_file:
                    await send_tracked_video(context, chat_id=chat_id, video=media_file, caption=caption, reply_markup=InlineKeyboardMarkup(keyboard), protect_content=True, supports_streaming=True)
                return
            except Exception as e_video:
                logger.error(f"Could not send price video {random_media_path}, falling back to text: {e_video}")
        
        await send_tracked_message(context, chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        logger.error(f"FATAL ERROR in show_prices_page: {e}")
        try:
            await send_tracked_message(context, chat_id=chat_id, text=get_text("error_occurred", context))
        except Exception as e_send:
            logger.error(f"Could not even send error message to user {chat_id}: {e_send}")

async def show_treffen_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    buchung = context.user_data.get('treffen_buchung', {})
    chat_id = update.effective_chat.id
    await cleanup_bot_messages(chat_id, context)

    if not all(k in buchung for k in ['duration', 'date', 'location']):
        text = get_text("meeting_summary_error", context)
        keyboard = [[InlineKeyboardButton(get_text("back_to_meeting_menu_button", context), callback_data="treffen_menu")]]
        await send_tracked_message(context, chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    duration = buchung['duration']
    duration_text = get_package_button_text('treffen', duration, user.id, context).split(' ')[0]
    full_price = PRICES['treffen'][duration]
    deposit = ceil(full_price / 4)
    cash_price = full_price * 0.9
    
    summary_text = get_text("meeting_summary_title", context)
    summary_text += get_text("meeting_summary_duration", context, duration_text=duration_text)
    summary_text += get_text("meeting_summary_date", context, date=buchung['date'])
    summary_text += get_text("meeting_summary_location", context, location=buchung['location'])
    summary_text += get_text("meeting_summary_total_price", context, full_price=full_price)
    summary_text += get_text("meeting_summary_cash_discount", context, discount_amount=full_price * 0.1)
    summary_text += get_text("meeting_summary_final_price", context, cash_price=cash_price)
    summary_text += get_text("meeting_summary_deposit_info", context, deposit=deposit)

    keyboard = [
        [InlineKeyboardButton(get_text("deposit_info_button_summary", context), callback_data="treffen_info_anzahlung_summary")],
        [InlineKeyboardButton(get_text("deposit_paypal_button", context, deposit=deposit), callback_data=f"pay_paypal:treffen:{duration}")],
        [InlineKeyboardButton(get_text("deposit_voucher_button", context, deposit=deposit), callback_data=f"pay_voucher:treffen:{duration}")],
        [InlineKeyboardButton(get_text("deposit_crypto_button", context, deposit=deposit), callback_data=f"pay_crypto:treffen:{duration}")],
        [InlineKeyboardButton(get_text("cancel_booking_button", context), callback_data="treffen_menu")]
    ]
    await send_tracked_message(context, chat_id=chat_id, text=get_text("meeting_available_status", context))
    await send_tracked_message(context, chat_id=chat_id, text=summary_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

def get_price_keyboard(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    return [
        [InlineKeyboardButton(get_package_button_text("bilder", 10, user_id, context), callback_data="select_package:bilder:10"), InlineKeyboardButton(get_package_button_text("videos", 10, user_id, context), callback_data="select_package:videos:10")],
        [InlineKeyboardButton(get_package_button_text("bilder", 25, user_id, context), callback_data="select_package:bilder:25"), InlineKeyboardButton(get_package_button_text("videos", 25, user_id, context), callback_data="select_package:videos:25")],
        [InlineKeyboardButton(get_package_button_text("bilder", 35, user_id, context), callback_data="select_package:bilder:35"), InlineKeyboardButton(get_package_button_text("videos", 35, user_id, context), callback_data="select_package:videos:35")],
        [InlineKeyboardButton(get_text("main_menu_button", context), callback_data="main_menu")]
    ]
    
# --- Admin Menu Functions (remains in German for the admin) ---
async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔️ Du hast keine Berechtigung für diesen Befehl.")
        return
    await show_admin_menu(update, context)

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🔒 *Admin-Menü*\n\nWähle eine Option:"
    keyboard = [
        [InlineKeyboardButton("📊 Nutzer-Statistiken", callback_data="admin_stats_users"), InlineKeyboardButton("🖱️ Klick-Statistiken", callback_data="admin_stats_clicks")],
        [InlineKeyboardButton("🎟️ Gutscheine", callback_data="admin_show_vouchers")],
        [InlineKeyboardButton("👤 Nutzer verwalten", callback_data="admin_user_manage")]
    ]
    await query_or_message_edit(update, context, text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_user_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "👤 *Nutzerverwaltung*\n\nWähle eine Aktion aus:"
    keyboard = [
        [InlineKeyboardButton("🚫 Nutzer sperren", callback_data="admin_user_ban_start")],
        [InlineKeyboardButton("✅ Nutzer entsperren", callback_data="admin_user_unban_start")],
        [InlineKeyboardButton("🖼️ Vorschau-Limit anpassen", callback_data="admin_preview_limit_start")],
        [InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]
    ]
    await query_or_message_edit(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_vouchers_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    vouchers = load_vouchers()
    amazon_codes = "\n".join([f"- `{code}`" for code in vouchers.get("amazon", [])]) or "Keine"
    text = f"*Eingelöste Gutscheine*\n\n*Amazon:*\n{amazon_codes}"
    keyboard = [
        [InlineKeyboardButton("📄 Vouchers als PDF laden", callback_data="download_vouchers_pdf")],
        [InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]
    ]
    await query_or_message_edit(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_manage_discounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "💸 *Rabatte verwalten*\n\nHier kannst du aktive, vom Admin vergebene Rabatte einsehen und löschen."
    keyboard = [
        [InlineKeyboardButton("🗑️ Alle Rabatte löschen", callback_data="admin_delete_all_discounts_confirm")],
        [InlineKeyboardButton("👤 Rabatt für Nutzer löschen", callback_data="admin_delete_user_discount_start")],
        [InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]
    ]
    await query_or_message_edit(update, context, text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id
    user = update.effective_user

    if data.startswith("select_lang:"):
        lang_code = data.split(":")[1]
        context.user_data['language'] = lang_code
        await start(update, context)
        return

    stats = load_stats()
    ensure_user_in_stats(user.id, stats)
    user_data = stats["users"][str(user.id)]

    if is_user_banned(user.id):
        await query.answer(get_text("banned_user_alert", context), show_alert=True)
        return

    if data == "main_menu":
        await start(update, context)
        return
    
    # --- ADMIN CALLBACKS ---
    if data.startswith("admin_"):
        if str(user.id) != ADMIN_USER_ID:
            await query.answer("⛔️ Keine Berechtigung.", show_alert=True)
            return

        # Navigation
        if data == "admin_main_menu": await show_admin_menu(update, context)
        elif data == "admin_user_manage": await show_user_management_menu(update, context)
        
        # Anzeigen von Daten
        elif data == "admin_stats_users":
            await query.edit_message_text(f"Gesamtzahl der Nutzer: {len(stats.get('users', {}))}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]))
        elif data == "admin_stats_clicks":
            events = stats.get("events", {})
            text = "Klick-Statistiken:\n" + "\n".join(f"- {key}: {value}" for key, value in events.items()) if events else "Noch keine Klicks erfasst."
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]))
        elif data == "admin_show_vouchers": await show_vouchers_panel(update, context)

        # Starten von Aktionen mit Texteingabe
        elif data == "admin_user_ban_start":
            context.user_data['awaiting_user_id_for_sperren'] = True
            await query.edit_message_text("Bitte sende mir die numerische Nutzer-ID der Person, die du sperren möchtest.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Abbrechen", callback_data="admin_user_manage")]]))
        elif data == "admin_user_unban_start":
            context.user_data['awaiting_user_id_for_entsperren'] = True
            await query.edit_message_text("Bitte sende mir die numerische Nutzer-ID der Person, die du entsperren möchtest.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Abbrechen", callback_data="admin_user_manage")]]))
        elif data == "admin_preview_limit_start":
            context.user_data['awaiting_user_id_for_preview_limit'] = True
            await query.edit_message_text("Bitte sende mir die Nutzer-ID, deren Vorschau-Limit du verwalten möchtest.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Abbrechen", callback_data="admin_user_manage")]]))

        # Ausführen von Aktionen
        elif data.startswith("admin_preview_"):
            _, action, user_id_str = data.split(":")
            await execute_manage_preview_limit(update, context, user_id_str, action)

        # Rabatt-Management Callbacks
        elif data == "admin_manage_discounts": await show_manage_discounts_menu(update, context)
        elif data == "admin_delete_all_discounts_confirm":
            await query.edit_message_text("Bist du sicher, dass du ALLE Rabatte von ALLEN Nutzern löschen möchtest?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Ja, alle löschen", callback_data="admin_delete_all_discounts_execute")], [InlineKeyboardButton("Abbrechen", callback_data="admin_manage_discounts")]]))
        elif data == "admin_delete_all_discounts_execute": await execute_delete_all_discounts(update, context)
        elif data == "admin_delete_user_discount_start":
            context.user_data['awaiting_user_id_for_discount_deletion'] = True
            await query.edit_message_text("Sende mir die Nutzer-ID, deren Rabatte du löschen möchtest.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Abbrechen", callback_data="admin_manage_discounts")]]))
        elif data.startswith("admin_delete_user_discount_execute:"):
            _, user_id_to_clear = data.split(":")
            await execute_delete_user_discount(update, context, user_id_to_clear)
        
        return

    # --- USER CALLBACKS ---
    
    if data == "download_vouchers_pdf":
        vouchers = load_vouchers(); pdf = FPDF(); pdf.add_page(); pdf.set_font("Arial", size=12)
        pdf.cell(0, 10, "Amazon Gutschein Report", ln=True, align='C')
        if vouchers.get("amazon"):
            for code in vouchers["amazon"]: pdf.cell(0, 8, f"- {code.encode('latin-1', 'ignore').decode('latin-1')}", ln=True)
        else: pdf.cell(0, 8, "Keine Gutscheine vorhanden.", ln=True)
        pdf_buffer = BytesIO(pdf.output(dest='S').encode('latin-1')); pdf_buffer.seek(0)
        await context.bot.send_document(chat_id=chat_id, document=pdf_buffer, filename=f"Gutschein-Report_{datetime.now().strftime('%Y-%m-%d')}.pdf")
        return
        
    if data.startswith("show_preview:"):
        _, media_type = data.split(":")
        if user_data.get("preview_clicks", 0) >= 25:
            await query.answer(get_text("preview_limit_reached_alert", context), show_alert=True)
            return
        await track_event(f"preview_{media_type}", context, user.id)
        await send_or_update_admin_log(context, user, event_text="Schaut sich Vorschau an")
        await send_preview_message(update, context, media_type)
        return

    elif data == "show_price_options":
        await show_prices_page(update, context)
        return

    elif data == "live_call_menu":
        await cleanup_bot_messages(chat_id, context)
        text = get_text("live_call_menu_text", context)
        keyboard = []
        row = []
        for duration, price in PRICES['livecall'].items():
            duration_text = get_text("live_call_unit_min", context, duration=duration) if duration < 60 else get_text("live_call_unit_hr", context, hours=duration//60)
            row.append(InlineKeyboardButton(f"{duration_text} - {price}€", callback_data=f"select_package:livecall:{duration}"))
            if len(row) == 2: keyboard.append(row); row = []
        if row: keyboard.append(row)
        keyboard.append([InlineKeyboardButton(get_text("main_menu_button", context), callback_data="main_menu")])
        await send_tracked_message(context, chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data == "treffen_menu":
        await cleanup_bot_messages(chat_id, context)
        text = get_text("meeting_menu_text", context)
        keyboard = []
        row = []
        for duration in sorted(PRICES['treffen'].keys()):
            button_text = get_package_button_text('treffen', duration, user.id, context)
            row.append(InlineKeyboardButton(button_text, callback_data=f"select_treffen_duration:{duration}"))
            if len(row) == 2: keyboard.append(row); row = []
        if row: keyboard.append(row)
        keyboard.append([InlineKeyboardButton(get_text("meeting_deposit_info_button", context), callback_data="treffen_info_anzahlung_menu")])
        keyboard.append([InlineKeyboardButton(get_text("main_menu_button", context), callback_data="main_menu")])
        await send_tracked_message(context, chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data in ["treffen_info_anzahlung_menu", "treffen_info_anzahlung_summary"]:
        text = get_text("meeting_deposit_info_text", context)
        if data == "treffen_info_anzahlung_menu":
            keyboard = [[InlineKeyboardButton(get_text("understood_back_button", context), callback_data="treffen_menu")]]
        else:
            keyboard = [[InlineKeyboardButton(get_text("understood_back_to_payment_button", context), callback_data="back_to_treffen_summary")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    elif data == "back_to_treffen_summary":
        await show_treffen_summary(update, context)
        return

    elif data.startswith("select_treffen_duration:"):
        await cleanup_bot_messages(chat_id, context)
        _, duration_str = data.split(":")
        context.user_data['treffen_buchung'] = {'duration': int(duration_str)}
        context.user_data['awaiting_input'] = 'treffen_date'
        text = get_text("meeting_date_prompt", context)
        await send_tracked_message(context, chat_id, text=text, parse_mode='Markdown')
        return

    elif data.startswith("next_preview:"):
        if user_data.get("preview_clicks", 0) >= 25:
            await query.answer(get_text("preview_limit_reached_alert", context), show_alert=True)
            await cleanup_bot_messages(chat_id, context)
            limit_text = get_text("preview_limit_reached_text", context)
            keyboard = [[InlineKeyboardButton(get_text("view_prices_button", context), callback_data="show_price_options")], [InlineKeyboardButton(get_text("main_menu_button", context), callback_data="main_menu")]]
            await send_tracked_message(context, chat_id, text=limit_text, reply_markup=InlineKeyboardMarkup(keyboard))
            return

        stats["users"][str(user.id)]["preview_clicks"] = user_data.get("preview_clicks", 0) + 1
        save_stats(stats)
        await track_event("next_preview", context, user.id)
        _, media_type = data.split(":")
        await send_or_update_admin_log(context, user, event_text=f"Nächstes Medium ({media_type})")

        media_paths = context.user_data.get('preview_gallery', [])
        if not media_paths: return

        index_key = f'preview_index_{media_type}'
        current_index = context.user_data.get(index_key, 0)
        next_index = (current_index + 1) % len(media_paths)
        context.user_data[index_key] = next_index

        media_path = media_paths[next_index]
        media_message_id = context.chat_data.get("media_message_id")
        if not media_message_id:
            await send_preview_message(update, context, media_type, start_index=next_index)
            return

        try:
            with open(media_path, 'rb') as media_file:
                is_video = any(media_path.lower().endswith(ext) for ext in ['.mp4', '.mov', '.m4v'])
                new_media = InputMediaVideo(media=media_file) if is_video else InputMediaPhoto(media=media_file)
                await context.bot.edit_message_media(chat_id=chat_id, message_id=media_message_id, media=new_media)
        except error.BadRequest as e:
            if "message is not modified" not in str(e):
                await send_preview_message(update, context, media_type, start_index=next_index)
        except Exception:
            await send_preview_message(update, context, media_type, start_index=next_index)
        return

    elif data.startswith("select_package:"):
        await track_event("package_selected", context, user.id)
        _, media_type, amount_str = data.split(":")
        amount = int(amount_str)
        await cleanup_bot_messages(chat_id, context)

        if media_type == "livecall":
            await send_tracked_message(context, chat_id, text=get_text("live_call_available_text", context))
            price = PRICES[media_type][amount]
            text = get_text("live_call_selection_text", context, amount=amount, price=price, TELEGRAM_USERNAME=TELEGRAM_USERNAME)
            keyboard = [
                [InlineKeyboardButton(f"💸 {price}€ per PayPal", callback_data=f"pay_paypal:{media_type}:{amount}")],
                [InlineKeyboardButton(f"🎟️ {price}€ per Gutschein", callback_data=f"pay_voucher:{media_type}:{amount}")],
                [InlineKeyboardButton(f"🪙 {price}€ per Krypto", callback_data=f"pay_crypto:{media_type}:{amount}")],
                [InlineKeyboardButton(get_text("back_button", context), callback_data="live_call_menu")]
            ]
            await send_tracked_message(context, chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        base_price = PRICES[media_type][amount]
        package_key = f"{media_type}_{amount}"
        price = get_discounted_price(base_price, user_data.get("discounts"), package_key)
        if price == -1: price = base_price
        price_str = f"~{base_price}€~ *{price}€* ({get_text('discount_text', context)})" if price != base_price else f"*{price}€*"
        
        media_type_str = get_text(f"package_button_text_{media_type.lower()}", context, amount="").replace(str(amount), "").strip()
        text = get_text("package_selection_text", context, amount=amount, media_type=media_type_str, price_str=price_str)

        if not user_data.get("paypal_offer_sent"):
            text += get_text("paypal_offer_text", context)
            stats["users"][str(user.id)]["paypal_offer_sent"] = True; save_stats(stats)
            
        keyboard = [
            [InlineKeyboardButton(get_text("paypal_button", context), callback_data=f"pay_paypal:{media_type}:{amount}")],
            [InlineKeyboardButton(get_text("voucher_button", context), callback_data=f"pay_voucher:{media_type}:{amount}")],
            [InlineKeyboardButton(get_text("crypto_button", context), callback_data=f"pay_crypto:{media_type}:{amount}")],
            [InlineKeyboardButton(get_text("back_to_prices_button", context), callback_data="show_price_options")]
        ]
        await send_tracked_message(context, chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    async def update_payment_log(payment_method: str, price_val: int, package_info: str):
        stats_log = load_stats(); user_data_log = stats_log.get("users", {}).get(str(user.id))
        if user_data_log:
            payment_info = f"{payment_method} ({package_info}): {price_val}€"
            if payment_info not in user_data_log.get("payments_initiated", []):
                user_data_log.setdefault("payments_initiated", []).append(payment_info); save_stats(stats_log)
        await send_or_update_admin_log(context, user, event_text=f"Bezahlmethode '{payment_method}' für {price_val}€ gewählt")

    if data.startswith(("pay_paypal:", "pay_voucher:", "pay_crypto:")):
        _, media_type, amount_str = data.split(":")
        amount = int(amount_str)
        original_message = query.message

        if media_type == "livecall":
            price = PRICES[media_type][amount]
            package_info_text = get_text("package_info_live_call", context, amount=amount)
        elif media_type == "treffen":
            price = ceil(PRICES[media_type][amount] / 4)
            duration_text = get_package_button_text('treffen', amount, user.id, context).split(' ')[0]
            package_info_text = get_text("package_info_meeting_deposit", context, duration_text=duration_text)
        else:
            base_price = PRICES[media_type][amount]
            package_key = f"{media_type}_{amount}"
            price = get_discounted_price(base_price, user_data.get("discounts"), package_key)
            if price == -1: price = base_price
            package_info_text = f"{amount} {media_type.capitalize()}"

        back_button_data = "back_to_treffen_summary" if media_type == "treffen" else (f"select_package:{media_type}:{amount}" if media_type == "livecall" else "show_price_options")

        if data.startswith("pay_paypal:"):
            await track_event(f"payment_{media_type}", context, user.id); await update_payment_log("PayPal", price, package_info_text)
            paypal_link = f"https://paypal.me/{PAYPAL_USER}/{price}"
            text = get_text("paypal_payment_text", context, package_info_text=package_info_text, price=price, paypal_link=paypal_link)
            if media_type in ["livecall", "treffen"]:
                text += get_text("contact_after_payment_text", context, TELEGRAM_USERNAME=TELEGRAM_USERNAME)
            await original_message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text("back_button", context), callback_data=back_button_data)]]), parse_mode='Markdown', disable_web_page_preview=True)
        
        elif data.startswith("pay_voucher:"):
            await track_event(f"payment_{media_type}", context, user.id); await update_payment_log("Gutschein", price, package_info_text)
            context.user_data["awaiting_voucher"] = "amazon"
            text = get_text("voucher_prompt_text", context)
            await original_message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text("cancel_button", context), callback_data=back_button_data)]]))
        
        elif data.startswith("pay_crypto:"):
            await track_event(f"payment_{media_type}", context, user.id); await update_payment_log("Krypto", price, package_info_text)
            text = get_text("crypto_prompt_text", context)
            keyboard = [
                [InlineKeyboardButton("Bitcoin (BTC)", callback_data=f"show_wallet:btc:{media_type}:{amount}"), InlineKeyboardButton("Ethereum (ETH)", callback_data=f"show_wallet:eth:{media_type}:{amount}")],
                [InlineKeyboardButton(get_text("back_button", context), callback_data=f"select_package:{media_type}:{amount}")]
            ]
            await original_message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("show_wallet:"):
        _, crypto_type, media_type, amount_str = data.split(":")
        amount = int(amount_str); price = 0
        if media_type == "livecall": price = PRICES[media_type][amount]
        elif media_type == "treffen": price = ceil(PRICES[media_type][amount] / 4)
        else:
            base_price = PRICES[media_type][amount]; package_key = f"{media_type}_{amount}"
            price = get_discounted_price(base_price, user_data.get("discounts"), package_key)
            price = price if price != -1 else base_price
        
        wallet_address = BTC_WALLET if crypto_type == "btc" else ETH_WALLET
        crypto_name = "Bitcoin (BTC)" if crypto_type == "btc" else "Ethereum (ETH)"
        text = get_text("crypto_payment_text", context, crypto_name=crypto_name, price=price, wallet_address=wallet_address)
        keyboard = [[InlineKeyboardButton(get_text("back_button", context), callback_data=f"pay_crypto:{media_type}:{amount}")]]
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    text_input = update.message.text
    chat_id = update.effective_chat.id

    if str(user.id) == ADMIN_USER_ID:
        try: await context.bot.delete_message(chat_id, update.message.message_id)
        except error.TelegramError: pass
        
        if context.user_data.get('awaiting_user_id_for_sperren'): await handle_admin_user_management_input(update, context, "sperren"); return
        if context.user_data.get('awaiting_user_id_for_entsperren'): await handle_admin_user_management_input(update, context, "entsperren"); return
        if context.user_data.get('awaiting_user_id_for_preview_limit'): await handle_admin_preview_limit_input(update, context); return
        if context.user_data.get('awaiting_user_id_for_discount_deletion'): await handle_admin_delete_user_discount_input(update, context); return
    
    try: await context.bot.delete_message(chat_id, update.message.message_id)
    except error.TelegramError: pass

    if context.user_data.get('awaiting_input') == 'treffen_date':
        await cleanup_bot_messages(chat_id, context)
        buchung = context.user_data.get('treffen_buchung', {})
        match = re.match(r"^\s*(\d{1,2}\s*\.\s*\d{1,2})\s*\.?\s*$", text_input)
        if match:
            buchung['date'] = match.group(1).replace(" ", "")
            context.user_data['treffen_buchung'] = buchung
            context.user_data['awaiting_input'] = 'treffen_location'
            await send_tracked_message(context, chat_id, text=get_text("meeting_location_prompt", context))
        else:
            await send_tracked_message(context, chat_id, text=get_text("invalid_date_prompt", context), parse_mode='Markdown')
        return

    if context.user_data.get('awaiting_input') == 'treffen_location':
        buchung = context.user_data.get('treffen_buchung', {})
        buchung['location'] = text_input
        context.user_data['awaiting_input'] = None
        await show_treffen_summary(update, context)
        return

    if context.user_data.get("awaiting_voucher"):
        await cleanup_bot_messages(chat_id, context)
        provider = context.user_data.pop("awaiting_voucher")
        code = text_input
        vouchers = load_vouchers()
        vouchers.setdefault(provider, []).append(code)
        save_vouchers(vouchers)
        notification_text = (f"📬 *Neuer Gutschein erhalten!* 📬\n\n*Anbieter:* {provider.capitalize()}\n*Code:* `{code}`\n*Von Nutzer:* {escape_markdown(user.first_name, version=2)} (`{user.id}`)\n\n⚠️ *AKTION ERFORDERLICH:* Code prüfen!")
        if NOTIFICATION_GROUP_ID: await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=notification_text, parse_mode='Markdown')
        await send_or_update_admin_log(context, user, event_text=f"Gutschein '{provider}' eingereicht")
        user_confirmation_text = get_text("voucher_submitted_text", context)
        keyboard = [[InlineKeyboardButton(get_text("main_menu_button", context), callback_data="main_menu")]]
        await send_tracked_message(context, chat_id, text=user_confirmation_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

async def handle_admin_user_management_input(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    user_id_to_manage = update.message.text; context.user_data[f'awaiting_user_id_for_{action}'] = False
    await cleanup_bot_messages(update.effective_chat.id, context)
    if not user_id_to_manage.isdigit(): await send_tracked_message(context, update.effective_chat.id, text="⚠️ Ungültige ID."); await show_admin_menu(update, context); return
    stats = load_stats()
    if user_id_to_manage not in stats.get("users", {}): await send_tracked_message(context, update.effective_chat.id, text=f"⚠️ Nutzer mit ID `{user_id_to_manage}` nicht gefunden."); await show_admin_menu(update, context); return
    stats["users"][user_id_to_manage]["banned"] = True if action == "sperren" else False; save_stats(stats)
    verb = "gesperrt" if action == "sperren" else "entsperrt"; await send_tracked_message(context, update.effective_chat.id, text=f"✅ Nutzer `{user_id_to_manage}` wurde erfolgreich *{verb}*."); await show_admin_menu(update, context)

async def handle_admin_preview_limit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['awaiting_user_id_for_preview_limit'] = False; user_id_to_manage = update.message.text
    await cleanup_bot_messages(update.effective_chat.id, context)
    if not user_id_to_manage.isdigit(): await send_tracked_message(context, update.effective_chat.id, text="⚠️ Ungültige ID."); await show_admin_menu(update, context); return
    stats = load_stats()
    if user_id_to_manage not in stats["users"]: await send_tracked_message(context, update.effective_chat.id, text=f"⚠️ Nutzer mit ID `{user_id_to_manage}` nicht gefunden."); await show_admin_menu(update, context); return
    current_clicks = stats['users'][user_id_to_manage].get('preview_clicks', 0)
    text = f"Nutzer `{user_id_to_manage}` hat *{current_clicks}* Klicks.\n\nWas tun?"; keyboard = [[InlineKeyboardButton("Auf 0 setzen", callback_data=f"admin_preview_reset:{user_id_to_manage}")], [InlineKeyboardButton("Um 25 erhöhen", callback_data=f"admin_preview_increase:{user_id_to_manage}")], [InlineKeyboardButton("❌ Abbrechen", callback_data="admin_user_manage")]];
    await send_tracked_message(context, update.effective_chat.id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def execute_manage_preview_limit(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: str, action: str):
    stats = load_stats(); user_data = stats.get("users", {}).get(user_id)
    if not user_data: await query_or_message_edit(update, context, f"Fehler: Nutzer {user_id} nicht gefunden.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_user_manage")]])); return
    current_clicks = user_data.get('preview_clicks', 0)
    new_clicks = 0 if action == 'reset' else current_clicks + 25
    stats["users"][user_id]['preview_clicks'] = new_clicks; save_stats(stats)
    text = f"✅ Vorschau-Limit für `{user_id}` ist jetzt *{new_clicks}*."
    await query_or_message_edit(update, context, text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_user_manage")]]))

async def execute_delete_all_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = load_stats(); cleared_count = 0
    for user_id in stats["users"]:
        if "discounts" in stats["users"][user_id]:
            del stats["users"][user_id]["discounts"]; cleared_count += 1
    save_stats(stats); await save_discounts_to_telegram(context)
    text = f"✅ Alle Rabatte von *{cleared_count}* Nutzern entfernt."
    await query_or_message_edit(update, context, text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_manage_discounts")]]))

async def handle_admin_delete_user_discount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['awaiting_user_id_for_discount_deletion'] = False; user_id_to_clear = update.message.text
    await cleanup_bot_messages(update.effective_chat.id, context)
    if not user_id_to_clear.isdigit(): await send_tracked_message(context, update.effective_chat.id, text="⚠️ Ungültige ID."); await show_admin_menu(update, context); return
    stats = load_stats(); user_data = stats.get("users", {}).get(user_id_to_clear)
    if not user_data or "discounts" not in user_data: await send_tracked_message(context, update.effective_chat.id, f"ℹ️ Nutzer `{user_id_to_clear}` hat keine Rabatte."); await show_admin_menu(update, context); return
    text = f"Nutzer `{user_id_to_clear}` hat Rabatte. Löschen?"; keyboard = [[InlineKeyboardButton("✅ Ja, löschen", callback_data=f"admin_delete_user_discount_execute:{user_id_to_clear}")], [InlineKeyboardButton("❌ Abbrechen", callback_data="admin_manage_discounts")]];
    await send_tracked_message(context, update.effective_chat.id, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def execute_delete_user_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id_to_clear: str):
    stats = load_stats()
    if user_id_to_clear in stats["users"] and "discounts" in stats["users"][user_id_to_clear]:
        del stats["users"][user_id_to_clear]["discounts"]; save_stats(stats); await save_discounts_to_telegram(context)
        text = f"✅ Rabatte für `{user_id_to_clear}` entfernt."
        await query_or_message_edit(update, context, text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_manage_discounts")]]))
    else: await query_or_message_edit(update, context, f"ℹ️ Fehler: Nutzer `{user_id_to_clear}` hat keine Rabatte.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_manage_discounts")]]))

async def post_init(application: Application):
    await load_discounts_from_telegram(application)

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    if WEBHOOK_URL:
        port = int(os.environ.get("PORT", 8443))
        logger.info(f"Starting bot in webhook mode on port {port}")
        application.run_webhook(listen="0.0.0.0", port=port, url_path=BOT_TOKEN, webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}")
    else:
        logger.info("Starting bot in polling mode")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
