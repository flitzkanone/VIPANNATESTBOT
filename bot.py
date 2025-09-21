import os
import logging
import json
import random
import traceback
from dotenv import load_dotenv
from datetime import datetime, timedelta
from io import BytesIO
import asyncio
import re

from fpdf import FPDF
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error, InputMediaPhoto, User
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.helpers import escape_markdown

# --- Konfiguration ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PAYPAL_USER = os.getenv("PAYPAL_USER")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
AGE_ANNA = os.getenv("AGE_ANNA", "18")
AGE_LUNA = os.getenv("AGE_LUNA", "21")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "@Admin") # NEU: Für den /support Befehl
NOTIFICATION_GROUP_ID = os.getenv("NOTIFICATION_GROUP_ID")

BTC_WALLET = "1FcgMLNBDLiuDSDip7AStuP19sq47LJB12"
ETH_WALLET = "0xeeb8FDc4aAe71B53934318707d0e9747C5c66f6e"

PRICES = {"bilder": {10: 5, 25: 10, 35: 15}, "videos": {10: 15, 25: 25, 35: 30}}
VOUCHER_FILE = "vouchers.json"
STATS_FILE = "stats.json"
MEDIA_DIR = "image"

creating_log_for_user = set()
BOT_START_TIME = datetime.now() # NEU: Für Uptime im /ping Befehl

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Hilfsfunktionen ---
def load_vouchers():
    try:
        with open(VOUCHER_FILE, "r") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return {"amazon": [], "paysafe": []}

def save_vouchers(vouchers):
    with open(VOUCHER_FILE, "w") as f: json.dump(vouchers, f, indent=2)

def load_stats():
    try:
        with open(STATS_FILE, "r") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"pinned_message_id": None, "users": {}, "admin_logs": {}, "events": {}}

def save_stats(stats):
    with open(STATS_FILE, "w") as f: json.dump(stats, f, indent=4)

async def track_event(event_name: str, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    if str(user_id) == ADMIN_USER_ID: return
    stats = load_stats()
    stats["events"][event_name] = stats["events"].get(event_name, 0) + 1
    save_stats(stats)
    await update_pinned_summary(context)

async def check_user_status(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    if str(user_id) == ADMIN_USER_ID: return "admin", False, None
    stats = load_stats()
    user_id_str = str(user_id)
    now = datetime.now()
    user_data = stats.get("users", {}).get(user_id_str)

    if user_data is None:
        stats.get("users", {})[user_id_str] = {
            "first_start": now.isoformat(), "last_start": now.isoformat(),
            "discount_sent": False, "preview_clicks": 0, "viewed_sisters": [],
            "payments_initiated": [], "discounts": {}
        }
        save_stats(stats)
        await update_pinned_summary(context)
        return "new", True, stats["users"][user_id_str]

    last_start_dt = datetime.fromisoformat(user_data.get("last_start"))
    if now - last_start_dt > timedelta(hours=24):
        stats["users"][user_id_str]["last_start"] = now.isoformat()
        save_stats(stats)
        return "returning", True, stats["users"][user_id_str]

    stats["users"][user_id_str]["last_start"] = now.isoformat()
    save_stats(stats)
    return "active", False, stats["users"][user_id_str]

async def send_or_update_admin_log(context: ContextTypes.DEFAULT_TYPE, user: User, event_text: str = "", new_discounts: dict = None):
    if not NOTIFICATION_GROUP_ID or str(user.id) == ADMIN_USER_ID:
        return

    user_id_str = str(user.id)
    while user_id_str in creating_log_for_user:
        await asyncio.sleep(0.2)

    stats = load_stats()
    admin_logs = stats.get("admin_logs", {})
    user_data = stats.get("users", {}).get(user_id_str, {})
    log_message_id = admin_logs.get(user_id_str, {}).get("message_id")

    final_discounts = new_discounts if new_discounts is not None else user_data.get("discounts", {})
    discount_emoji = "💸" if user_data.get("discount_sent") or final_discounts else ""
    first_start_str = "N/A"
    if user_data.get("first_start"):
        first_start_dt = datetime.fromisoformat(user_data["first_start"])
        first_start_str = first_start_dt.strftime('%Y-%m-%d %H:%M')
    
    username_str = f"@{user.username}" if user.username else "N/A"
    viewed_sisters_list = user_data.get("viewed_sisters", [])
    viewed_sisters_str = f"(Gesehen: {', '.join(s.upper() for s in sorted(viewed_sisters_list))})" if viewed_sisters_list else ""
    preview_clicks = user_data.get("preview_clicks", 0)
    payments = user_data.get("payments_initiated", [])
    payments_str = "\n".join(f"   • {p}" for p in payments) if payments else "   • Keine"

    base_text = (
        f"👤 Nutzer-Aktivität {discount_emoji}\n\n"
        f"ID: `{user.id}`\n"
        f"Name: {escape_markdown(user.first_name, version=2)}\n"
        f"Username: `{username_str}`\n"
        f"Erster Start: `{first_start_str}`\n\n"
        f"🖼️ Vorschau-Klicks: {preview_clicks}/25 {viewed_sisters_str}\n\n"
        f"💰 Bezahlversuche\n{payments_str}"
    )
    
    if final_discounts:
        discount_lines = [f"   • {k.replace('_', ' ').capitalize()}: {v}€" for k, v in final_discounts.items() if v > 0]
        if discount_lines:
            base_text += "\n\n💰 Individuelle Rabatte\n" + "\n".join(discount_lines)

    final_text = f"{base_text}\n\n`Letzte Aktion: {event_text}`"

    try:
        if log_message_id:
            await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id, text=final_text, parse_mode='Markdown')
        else:
            creating_log_for_user.add(user_id_str)
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
            stats = load_stats()
            stats.setdefault("admin_logs", {}).setdefault(user_id_str, {})["message_id"] = sent_message.message_id
            save_stats(stats)
    except error.BadRequest as e:
        if "message to edit not found" in str(e):
            logger.warning(f"Admin log for user {user.id} not found. Sending a new one.")
            creating_log_for_user.add(user_id_str)
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
            stats = load_stats()
            stats.setdefault("admin_logs", {}).setdefault(user_id_str, {})["message_id"] = sent_message.message_id
            save_stats(stats)
    except error.TelegramError as e:
        if 'message is not modified' not in str(e):
            logger.warning(f"Temporary error updating admin log for user {user.id}: {e}")
    finally:
        if user_id_str in creating_log_for_user:
            creating_log_for_user.remove(user_id_str)

async def send_permanent_admin_notification(context: ContextTypes.DEFAULT_TYPE, message: str):
    if NOTIFICATION_GROUP_ID:
        try:
            await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=message, parse_mode='Markdown')
        except Exception as e: logger.error(f"Konnte permanente Benachrichtigung nicht senden: {e}")

async def update_pinned_summary(context: ContextTypes.DEFAULT_TYPE):
    if not NOTIFICATION_GROUP_ID: return
    stats = load_stats()
    user_count = len(stats.get("users", {}))
    active_users_24h = 0
    now = datetime.now()
    for user_data in stats.get("users", {}).values():
        last_start_dt = datetime.fromisoformat(user_data.get("last_start", "1970-01-01T00:00:00"))
        if now - last_start_dt <= timedelta(hours=24):
            active_users_24h += 1
    events = stats.get("events", {})
    text = (
        f"📊 *Bot-Statistik Dashboard*\n"
        f"🕒 _Letztes Update:_ `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
        f"👥 *Nutzerübersicht*\n"
        f"   • Gesamt: *{user_count}*\n"
        f"   • Aktiv (24h): *{active_users_24h}*\n"
        f"   • Starts: *{events.get('start_command', 0)}*\n\n"
        f"💰 *Bezahl-Interesse*\n"
        f"   • PayPal: *{events.get('payment_paypal', 0)}*\n"
        f"   • Krypto: *{events.get('payment_crypto', 0)}*\n"
        f"   • Gutschein: *{events.get('payment_voucher', 0)}*\n\n"
        f"🖱️ *Klick-Verhalten*\n"
        f"   • Vorschau (KS): *{events.get('preview_ks', 0)}*\n"
        f"   • Vorschau (GS): *{events.get('preview_gs', 0)}*\n"
        f"   • Preise (KS): *{events.get('prices_ks', 0)}*\n"
        f"   • Preise (GS): *{events.get('prices_gs', 0)}*\n"
        f"   • 'Nächstes Bild': *{events.get('next_preview', 0)}*\n"
        f"   • Paketauswahl: *{events.get('package_selected', 0)}*"
    )
    pinned_id = stats.get("pinned_message_id")
    try:
        if pinned_id: await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=pinned_id, text=text, parse_mode='Markdown')
        else: raise error.BadRequest("Keine ID")
    except (error.BadRequest, error.Forbidden):
        logger.warning("Konnte Dashboard nicht bearbeiten, erstelle neu.")
        try:
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=text, parse_mode='Markdown')
            stats["pinned_message_id"] = sent_message.message_id; save_stats(stats)
            await context.bot.pin_chat_message(chat_id=NOTIFICATION_GROUP_ID, message_id=sent_message.message_id, disable_notification=True)
        except Exception as e_new: logger.error(f"Konnte Dashboard nicht erstellen/anpinnen: {e_new}")

async def restore_stats_from_pinned_message(application: Application):
    if not NOTIFICATION_GROUP_ID:
        logger.info("Keine NOTIFICATION_GROUP_ID gesetzt, Wiederherstellung übersprungen."); return
    logger.info("Versuche, Statistiken wiederherzustellen...")
    try:
        chat = await application.bot.get_chat(chat_id=NOTIFICATION_GROUP_ID)
        if not chat.pinned_message or "Bot-Statistik Dashboard" not in chat.pinned_message.text:
            logger.warning("Keine passende Dashboard-Nachricht gefunden."); return
        pinned_text = chat.pinned_message.text; stats = load_stats()
        def extract(p, t): return int(re.search(p, t, re.DOTALL).group(1)) if re.search(p, t, re.DOTALL) else 0
        user_count = extract(r"Gesamt:\s*\*(\d+)\*", pinned_text)
        if len(stats.get("users", {})) < user_count:
            for i in range(user_count - len(stats.get("users", {}))):
                stats["users"][f"restored_user_{i}"] = {"first_start": "1970-01-01T00:00:00", "last_start": "1970-01-01T00:00:00"}
        stats['events']['start_command'] = extract(r"Starts:\s*\*(\d+)\*", pinned_text)
        stats['events']['payment_paypal'] = extract(r"PayPal:\s*\*(\d+)\*", pinned_text)
        stats['events']['payment_crypto'] = extract(r"Krypto:\s*\*(\d+)\*", pinned_text)
        stats['events']['payment_voucher'] = extract(r"Gutschein:\s*\*(\d+)\*", pinned_text)
        stats['events']['preview_ks'] = extract(r"Vorschau \(KS\):\s*\*(\d+)\*", pinned_text)
        stats['events']['preview_gs'] = extract(r"Vorschau \(GS\):\s*\*(\d+)\*", pinned_text)
        stats['events']['prices_ks'] = extract(r"Preise \(KS\):\s*\*(\d+)\*", pinned_text)
        stats['events']['prices_gs'] = extract(r"Preise \(GS\):\s*\*(\d+)\*", pinned_text)
        stats['events']['next_preview'] = extract(r"'Nächstes Bild':\s*\*(\d+)\*", pinned_text)
        stats['events']['package_selected'] = extract(r"Paketauswahl:\s*\*(\d+)\*", pinned_text)
        stats['pinned_message_id'] = chat.pinned_message.message_id
        save_stats(stats); logger.info("Statistiken erfolgreich wiederhergestellt.")
    except Exception as e: logger.error(f"Fehler bei Wiederherstellung: {e}")

def get_media_files(schwester_code: str, media_type: str) -> list:
    matching_files = []; target_prefix = f"{schwester_code.lower()}_{media_type.lower()}"
    if not os.path.isdir(MEDIA_DIR):
        logger.error(f"Media-Verzeichnis '{MEDIA_DIR}' nicht gefunden!"); return []
    for filename in os.listdir(MEDIA_DIR):
        normalized_filename = filename.lower().lstrip('•-_ ').replace(' ', '_')
        if normalized_filename.startswith(target_prefix): matching_files.append(os.path.join(MEDIA_DIR, filename))
    return matching_files

async def cleanup_previous_messages(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    if "messages_to_delete" in context.user_data:
        for msg_id in context.user_data["messages_to_delete"]:
            try: await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except error.TelegramError: pass
        context.user_data["messages_to_delete"] = []

async def send_preview_message(update: Update, context: ContextTypes.DEFAULT_TYPE, schwester_code: str):
    chat_id = update.effective_chat.id
    image_paths = get_media_files(schwester_code, "vorschau"); image_paths.sort()
    if not image_paths:
        await context.bot.send_message(chat_id=chat_id, text="Ups! Ich konnte gerade keine passenden Inhalte finden...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="main_menu")]])); return
    context.user_data[f'preview_index_{schwester_code}'] = 0
    image_to_show_path = image_paths[0]
    with open(image_to_show_path, 'rb') as photo_file:
        photo_message = await context.bot.send_photo(chat_id=chat_id, photo=photo_file, protect_content=True)
    if schwester_code == 'gs':
        caption = f"Anna ({AGE_ANNA})"
    else:
        caption = f"Luna ({AGE_LUNA})"
    keyboard_buttons = [[InlineKeyboardButton("🛍️ Zu den Preisen", callback_data=f"select_schwester:{schwester_code}:prices")], [InlineKeyboardButton("🖼️ Nächstes Bild", callback_data=f"next_preview:{schwester_code}")], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
    text_message = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard_buttons))
    await cleanup_previous_messages(chat_id, context)
    context.user_data["messages_to_delete"] = [photo_message.message_id, text_message.message_id]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = update.effective_chat.id
    try:
        status, should_notify, user_data = await check_user_status(user.id, context)
        await track_event("start_command", context, user.id)
        if user_data and not user_data.get("discount_sent") and not user_data.get("discounts"):
            first_start_dt = datetime.fromisoformat(user_data.get("first_start"))
            if datetime.now() - first_start_dt > timedelta(hours=2):
                context.user_data['discount_active'] = True
                stats = load_stats()
                stats["users"][str(user.id)]["discount_sent"] = True
                save_stats(stats)
                await send_or_update_admin_log(context, user, event_text="Rabatt angeboten (Inaktivität >2h)")
                discount_message = "Willkommen zurück!\n\nAls Dankeschön für dein Interesse erhältst du einen *einmaligen Rabatt von 1€* auf alle Pakete bei deinem nächsten Kauf."
                await context.bot.send_message(chat_id, discount_message, parse_mode='Markdown')
        if should_notify:
            event_text = "Bot gestartet (neuer Nutzer)" if status == "new" else "Bot erneut gestartet"
            await send_or_update_admin_log(context, user, event_text=event_text)
    except Exception as e:
        logger.error(f"Error in start admin logic for user {user.id}: {e}")
        await error_handler(update, context) # Melde den Fehler an den Admin
    
    await cleanup_previous_messages(chat_id, context)
    welcome_text = f"Hier sind Anna ({AGE_ANNA}) und Luna ({AGE_LUNA}), eure heißesten Begleiterinnen! 😈\n\nWir freuen uns, dir unsere exklusiven Bilder und Videos zu präsentieren. Lass dich von unserer Leidenschaft und Erotik verzaubern! 🔥😘"
    keyboard = [[InlineKeyboardButton(" Vorschau", callback_data="show_preview_options")], [InlineKeyboardButton(" Preise & Pakete", callback_data="show_price_options")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        query = update.callback_query; await query.answer()
        try: await query.edit_message_text(welcome_text, reply_markup=reply_markup)
        except error.TelegramError:
            try: await query.delete_message()
            except: pass
            msg = await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup)
            context.user_data["messages_to_delete"] = [msg.message_id]
    else:
        msg = await update.message.reply_text(welcome_text, reply_markup=reply_markup)
        context.user_data["messages_to_delete"] = [msg.message_id]

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer(); data = query.data
    chat_id = update.effective_chat.id; user = update.effective_user

    if data == "main_menu": await start(update, context); return
    if data.startswith("admin_"): pass # Handled below
    if data == "download_vouchers_pdf": pass # Handled below

    if data in ["show_preview_options", "show_price_options"]:
        action = "preview" if "preview" in data else "prices"
        text = "Für wen interessierst du dich?"; keyboard = [[InlineKeyboardButton("Kleine Schwester", callback_data=f"select_schwester:ks:{action}"), InlineKeyboardButton("Große Schwester", callback_data=f"select_schwester:gs:{action}")], [InlineKeyboardButton("« Zurück", callback_data="main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # ... Rest of the handle_callback_query function

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if str(user.id) == ADMIN_USER_ID and context.user_data.get('rabatt_in_progress'):
        await handle_admin_discount_input(update, context)
        return
    if context.user_data.get("awaiting_voucher"):
        provider = context.user_data.pop("awaiting_voucher"); code = update.message.text
        vouchers = load_vouchers(); vouchers[provider].append(code); save_vouchers(vouchers)
        notification_text = f"📬 *Neuer Gutschein erhalten!*\n\n*Anbieter:* {provider.capitalize()}\n*Code:* `{code}`\n*Von Nutzer:* {escape_markdown(user.first_name, version=2)} (`{user.id}`)"
        await send_permanent_admin_notification(context, notification_text)
        await send_or_update_admin_log(context, user, event_text=f"Gutschein '{provider}' eingereicht")
        await update.message.reply_text("Vielen Dank! Dein Gutschein wurde übermittelt und wird nun geprüft."); await asyncio.sleep(2)
        await start(update, context)

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None: await show_admin_menu(update, context)
async def add_voucher(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None: pass
async def set_summary_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None: pass
async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
async def show_vouchers_panel(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
async def start_discount_process(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
async def select_discount_target(update: Update, context: ContextTypes.DEFAULT_TYPE, target: str): pass
async def select_discount_package(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str): pass
async def show_discount_package_menu(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
async def handle_admin_discount_input(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
async def finalize_discount_action(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
async def broadcast_discount_task(context: ContextTypes.DEFAULT_TYPE, target_ids: list, rabatt_data: dict, message_text: str, reply_markup: InlineKeyboardMarkup): pass

# --- (Rest des Codes, der unverändert bleibt) ---

async def main() -> None:
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    # Alle Handler hinzufügen...
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
