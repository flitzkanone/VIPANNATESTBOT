# --- START OF FILE bot.py ---

import os
import logging
import json
import random
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

# --- Konfiguration ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PAYPAL_USER = os.getenv("PAYPAL_USER")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
AGE_ANNA = os.getenv("AGE_ANNA", "18")
AGE_LUNA = os.getenv("AGE_LUNA", "21")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
NOTIFICATION_GROUP_ID = os.getenv("NOTIFICATION_GROUP_ID")

BTC_WALLET = "bc1q6dkmy9lj0v0rrsgnwvjsxc0lzyfx9cyvejzkcy"
ETH_WALLET = "0x394EC23E0CD08899c653e6F85987B4A1d1cA6865"

PRICES = {"bilder": {10: 5, 25: 10, 35: 15}, "videos": {10: 15, 25: 25, 35: 30}}
PREVIEW_LIMIT = 25
VOUCHER_FILE = "vouchers.json"
STATS_FILE = "stats.json"
MEDIA_DIR = "image"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Hilfsfunktionen ---
def escape_markdown_v2(text: str) -> str:
    """Escapes strings for Telegram's MarkdownV2 parser, but this bot uses the legacy parser."""
    # For legacy Markdown, we only need to escape `_`, `*`, `` ` ``, `[`
    escape_chars = r'_*`['
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

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

async def check_and_send_inactivity_discount(context: ContextTypes.DEFAULT_TYPE, user: User):
    if str(user.id) == ADMIN_USER_ID or not NOTIFICATION_GROUP_ID: return
    stats = load_stats()
    user_id_str = str(user.id)
    user_data = stats.get("users", {}).get(user_id_str)
    user_log = stats.get("admin_logs", {}).get(user_id_str)
    if not user_data or "first_start" not in user_data or not user_log or "message_id" not in user_log: return
    log_message_id = user_log["message_id"]
    try:
        admin_message = await context.bot.get_chat_message(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id)
        if "💸" in admin_message.text: return
        first_start_dt = datetime.fromisoformat(user_data["first_start"])
        if datetime.now() - first_start_dt >= timedelta(hours=2):
            text = (
                "👋 Hey, wir vermissen dich!\n\n"
                "Als kleines Dankeschön für dein Interesse schenken wir dir **1€ Rabatt** auf dein nächstes Paket. ✨\n\n"
                "Schau doch mal wieder rein!"
            )
            keyboard = [[InlineKeyboardButton("Zurück zum Bot", callback_data="main_menu")]]
            await context.bot.send_message(chat_id=user.id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            new_text = admin_message.text + "\n\n💸 `Rabatt gesendet`"
            await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id, text=new_text, parse_mode='Markdown')
            logger.info(f"Inaktivitäts-Rabatt an Nutzer {user.id} gesendet.")
    except Exception as e:
        logger.error(f"Fehler beim Senden des Inaktivitäts-Rabatts an {user.id}: {e}")

async def track_event(event_name: str, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    if str(user.id) == ADMIN_USER_ID: return
    stats = load_stats()
    stats["events"][event_name] = stats["events"].get(event_name, 0) + 1
    save_stats(stats)
    await update_pinned_summary(context)

async def check_user_status(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    if str(user.id) == ADMIN_USER_ID: return "admin", False
    stats = load_stats()
    user_id_str = str(user.id)
    now = datetime.now()
    user_data = stats.get("users", {}).get(user_id_str)
    if user_data is None:
        stats["users"][user_id_str] = {"last_start": now.isoformat(), "first_start": now.isoformat()}
        save_stats(stats)
        await update_pinned_summary(context)
        return "new", True
    last_start_dt = datetime.fromisoformat(user_data.get("last_start"))
    if now - last_start_dt > timedelta(hours=24):
        stats["users"][user_id_str]["last_start"] = now.isoformat()
        save_stats(stats)
        return "returning", True
    stats["users"][user_id_str]["last_start"] = now.isoformat()
    save_stats(stats)
    return "active", False

async def send_or_update_admin_log(context: ContextTypes.DEFAULT_TYPE, user: User, event_text: str = ""):
    if not NOTIFICATION_GROUP_ID or str(user.id) == ADMIN_USER_ID: return
    user_id_str = str(user.id)
    stats = load_stats()
    user_log = stats.get("admin_logs", {}).get(user_id_str, {})
    log_message_id = user_log.get("message_id")
    try:
        if log_message_id:
            message = await context.bot.get_chat_message(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id)
            old_text = message.text
            new_action_line = f"\n\n`Aktion: {event_text}`"
            if '`Aktion:' in old_text:
                new_text = re.sub(r"\n\n`Aktion:.*?`", new_action_line, old_text, flags=re.DOTALL)
            else:
                new_text = old_text + new_action_line
            if new_text != old_text:
                await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id, text=new_text, parse_mode='Markdown')
        else:
            escaped_name = escape_markdown_v2(user.first_name)
            username_str = f"\n*Benutzername:* `@{user.username}`" if user.username else ""
            base_text = f"👤 *Nutzer-Aktivität*\n\n*ID:* `{user.id}`\n*Name:* {escaped_name}{username_str}"
            final_text = f"{base_text}\n\n`Aktion: {event_text}`"
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
            user_log["message_id"] = sent_message.message_id
            stats["admin_logs"][user_id_str] = user_log
            save_stats(stats)
    except Exception as e:
        logger.error(f"Konnte Admin-Log für {user.id} nicht erstellen/aktualisieren: {e}")
        if log_message_id and isinstance(e, error.BadRequest):
            del stats["admin_logs"][user_id_str]
            save_stats(stats)
            await send_or_update_admin_log(context, user, event_text)

def get_preview_status(admin_message_text: str) -> dict:
    match = re.search(r"📊 Vorschau-Status: (.*?)\n", admin_message_text)
    if not match:
        return {"clicks": 0, "viewed": set(), "limit_reached": False}
    status_str = match.group(1)
    limit_reached = "🚫" in status_str
    clicks_match = re.search(r"(\d+)/\d+", status_str)
    clicks = int(clicks_match.group(1)) if clicks_match else 0
    viewed_match = re.search(r"\(Gesehen: (.*?)\)", status_str)
    viewed = set(viewed_match.group(1).split(', ')) if viewed_match and viewed_match.group(1) else set()
    return {"clicks": clicks, "viewed": viewed, "limit_reached": limit_reached}

async def update_admin_log_preview_status(context: ContextTypes.DEFAULT_TYPE, user: User, new_status: dict):
    stats = load_stats()
    user_log = stats.get("admin_logs", {}).get(str(user.id))
    if not user_log or "message_id" not in user_log: return
    try:
        message = await context.bot.get_chat_message(chat_id=NOTIFICATION_GROUP_ID, message_id=user_log["message_id"])
        base_text = re.sub(r"\n\n📊 Vorschau-Status:.*", "", message.text, flags=re.DOTALL)
        limit_str = "🚫 Limit Erreicht" if new_status["limit_reached"] else f"{new_status['clicks']}/{PREVIEW_LIMIT}"
        viewed_str = f" (Gesehen: {', '.join(sorted(list(new_status['viewed'])))})" if new_status['viewed'] else ""
        status_block = f"\n\n📊 Vorschau-Status: {limit_str}{viewed_str}"
        new_text = base_text + status_block
        if new_text != message.text:
            await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=user_log["message_id"], text=new_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Fehler beim Update des Vorschau-Status für {user.id}: {e}")

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
        if now - last_start_dt <= timedelta(hours=24): active_users_24h += 1
    events = stats.get("events", {})
    text = (
        f"📊 *Bot-Statistik Dashboard*\n_(Letztes Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})_\n\n"
        f"👤 *Nutzer Gesamt:* {user_count}\n"
        f"🟢 *Aktive Nutzer (24h):* {active_users_24h}\n"
        f"🚀 *Starts insgesamt:* {events.get('start_command', 0)}\n\n"
        f"--- *Bezahl-Interesse* ---\n💰 *PayPal Klicks:* {events.get('payment_paypal', 0)}\n🪙 *Krypto Klicks:* {events.get('payment_crypto', 0)}\n🎟️ *Gutschein Klicks:* {events.get('payment_voucher', 0)}\n\n"
        f"--- *Klick-Verhalten* ---\n▪️ Vorschau (KS): {events.get('preview_ks', 0)}\n▪️ Vorschau (GS): {events.get('preview_gs', 0)}\n"
        f"▪️ Preise (KS): {events.get('prices_ks', 0)}\n▪️ Preise (GS): {events.get('prices_gs', 0)}\n"
        f"▪️ 'Nächstes Bild' Klicks: {events.get('next_preview', 0)}\n"
        f"▪️ Paketauswahl: {events.get('package_selected', 0)}"
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
    if not NOTIFICATION_GROUP_ID: return
    logger.info("Versuche, Statistiken wiederherzustellen...")
    try:
        chat = await application.bot.get_chat(chat_id=NOTIFICATION_GROUP_ID)
        if not chat.pinned_message or "Bot-Statistik Dashboard" not in chat.pinned_message.text: return
        pinned_text = chat.pinned_message.text; stats = load_stats()
        def extract(p, t): return int(re.search(p, t).group(1)) if re.search(p, t) else 0
        user_count = extract(r"Nutzer Gesamt:\s*(\d+)", pinned_text)
        if len(stats.get("users", {})) < user_count:
            for i in range(user_count - len(stats.get("users", {}))):
                stats["users"][f"restored_user_{i}"] = {"last_start": "1970-01-01T00:00:00"}
        stats['events']['start_command'] = extract(r"Starts insgesamt:\s*(\d+)", pinned_text)
        # ... (restliche extractions)
        stats['pinned_message_id'] = chat.pinned_message.message_id
        save_stats(stats); logger.info("Statistiken erfolgreich wiederhergestellt.")
    except Exception as e: logger.error(f"Fehler bei Wiederherstellung: {e}")

async def cleanup_previous_messages(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    if "messages_to_delete" in context.user_data:
        for msg_id in context.user_data["messages_to_delete"]:
            try: await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except error.TelegramError: pass
        del context.user_data["messages_to_delete"]

async def send_preview_message(update: Update, context: ContextTypes.DEFAULT_TYPE, schwester_code: str):
    chat_id = update.effective_chat.id; image_paths = get_media_files(schwester_code, "vorschau"); image_paths.sort()
    if not image_paths:
        await context.bot.send_message(chat_id=chat_id, text="Ups! Ich konnte gerade keine passenden Inhalte finden...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="main_menu")]])); return
    context.user_data[f'preview_index_{schwester_code}'] = 0
    image_to_show_path = image_paths[0]
    with open(image_to_show_path, 'rb') as photo_file:
        photo_message = await context.bot.send_photo(chat_id=chat_id, photo=photo_file, protect_content=True)
    if schwester_code == 'gs': caption = f"Heyy ich bin Anna, ich bin {AGE_ANNA} Jahre alt und mache mit meiner Schwester zusammen 🌶️ videos und Bilder falls du lust hast speziele videos zu bekommen schreib mir 😏 @Anna_2008_030"
    else: caption = f"Heyy, mein name ist Luna ich bin {AGE_LUNA} Jahre alt und mache 🌶️ videos und Bilder. wenn du Spezielle wünsche hast schreib meiner Schwester für mehr.\nMeine Schwester: @Anna_2008_030"
    keyboard_buttons = [[InlineKeyboardButton("🛍️ Zu den Preisen", callback_data=f"select_schwester:{schwester_code}:prices")], [InlineKeyboardButton("🖼️ Nächstes Bild", callback_data=f"next_preview:{schwester_code}")], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
    text_message = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard_buttons))
    context.user_data["messages_to_delete"] = [photo_message.message_id, text_message.message_id]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        user = update.effective_user
        await check_and_send_inactivity_discount(context, user)
        status, should_notify = await check_user_status(user.id, context)
        await track_event("start_command", context, user.id)
        if should_notify:
            if status == "new":
                await send_or_update_admin_log(context, user, "Gestartet (Neu)")
            elif status == "returning":
                await send_or_update_admin_log(context, user, "Gestartet (Wiederkehrend)")
        context.user_data.clear(); chat_id = update.effective_chat.id; await cleanup_previous_messages(chat_id, context)
        welcome_text = ( "Herzlich Willkommen! ✨\n\n" "Hier kannst du eine Vorschau meiner Inhalte sehen oder direkt ein Paket auswählen. " "Die gesamte Bedienung erfolgt über die Buttons.")
        keyboard = [[InlineKeyboardButton(" Vorschau", callback_data="show_preview_options")], [InlineKeyboardButton(" Preise & Pakete", callback_data="show_price_options")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            query = update.callback_query; await query.answer()
            try: await query.edit_message_text(welcome_text, reply_markup=reply_markup)
            except error.TelegramError:
                try: await query.delete_message()
                except Exception: pass
                await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup)
        else: await update.message.reply_text(welcome_text, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ein kritischer Fehler ist in start() aufgetreten: {e}", exc_info=True)
        try:
            await update.effective_message.reply_text("Ups! Etwas ist schiefgelaufen. Bitte versuche es später erneut.")
        except Exception:
            pass

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer(); data = query.data; chat_id = update.effective_chat.id; user = update.effective_user
    
    if data == "download_vouchers_pdf":
        await query.answer("PDF wird erstellt..."); vouchers = load_vouchers(); pdf = FPDF()
        pdf.add_page(); pdf.set_font("Arial", size=16); pdf.cell(0, 10, "Gutschein Report", ln=True, align='C'); pdf.ln(10)
        pdf.set_font("Arial", 'B', size=14); pdf.cell(0, 10, "Amazon Gutscheine", ln=True); pdf.set_font("Arial", size=12)
        if vouchers.get("amazon", []):
            for code in vouchers["amazon"]: pdf.cell(0, 8, f"- {code.encode('latin-1', 'ignore').decode('latin-1')}", ln=True)
        else: pdf.cell(0, 8, "Keine vorhanden.", ln=True)
        pdf.ln(5); pdf.set_font("Arial", 'B', size=14); pdf.cell(0, 10, "Paysafe Gutscheine", ln=True); pdf.set_font("Arial", size=12)
        if vouchers.get("paysafe", []):
            for code in vouchers["paysafe"]: pdf.cell(0, 8, f"- {code.encode('latin-1', 'ignore').decode('latin-1')}", ln=True)
        else: pdf.cell(0, 8, "Keine vorhanden.", ln=True)
        pdf_buffer = BytesIO(pdf.output(dest='S').encode('latin-1')); pdf_buffer.seek(0)
        today_str = datetime.now().strftime("%Y-%m-%d"); await context.bot.send_document(chat_id=chat_id, document=pdf_buffer, filename=f"Gutschein-Report_{today_str}.pdf", caption="Hier ist dein aktueller Gutschein-Report.")
        return
    if data in ["main_menu", "show_price_options"]:
        await cleanup_previous_messages(chat_id, context)
        try: await query.edit_message_text(text="⏳"); await asyncio.sleep(0.5)
        except Exception: pass
    if data == "main_menu": await start(update, context)
    elif data == "admin_main_menu": await show_admin_menu(update, context)
    elif data == "admin_show_vouchers": await show_vouchers_panel(update, context)
    elif data == "admin_stats_users":
        stats = load_stats(); user_count = len(stats.get("users", {}))
        text = f"📊 *Nutzer-Statistiken*\n\nGesamtzahl der Nutzer: *{user_count}*"; keyboard = [[InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    elif data == "admin_stats_clicks":
        stats = load_stats(); events = stats.get("events", {}); text = "🖱️ *Klick-Statistiken*\n\n"
        if not events: text += "Noch keine Klicks erfasst."
        else:
            for event, count in sorted(events.items()): text += f"- `{event}`: *{count}* Klicks\n"
        keyboard = [[InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    elif data == "admin_reset_stats":
        text = "⚠️ *Bist du sicher?*\n\nAlle Statistiken werden unwiderruflich auf Null zurückgesetzt."
        keyboard = [[InlineKeyboardButton("✅ Ja, zurücksetzen", callback_data="admin_reset_stats_confirm")], [InlineKeyboardButton("❌ Nein, abbrechen", callback_data="admin_main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    elif data == "admin_reset_stats_confirm":
        stats = load_stats(); stats["users"] = {}; stats["admin_logs"] = {}; stats["events"] = {key: 0 for key in stats["events"]}; save_stats(stats)
        await update_pinned_summary(context)
        await query.edit_message_text("✅ Alle Statistiken wurden zurückgesetzt.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]]))
    
    elif data in ["show_preview_options", "show_price_options"]:
        action = "preview" if "preview" in data else "prices"; text = "Für wen interessierst du dich?"
        keyboard = [[InlineKeyboardButton("Kleine Schwester", callback_data=f"select_schwester:ks:{action}"), InlineKeyboardButton("Große Schwester", callback_data=f"select_schwester:gs:{action}")], [InlineKeyboardButton("« Zurück", callback_data="main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data.startswith("select_schwester:"):
        await cleanup_previous_messages(chat_id, context)
        try: await query.message.delete()
        except Exception: pass
        _, schwester_code, action = data.split(":")
        await track_event(f"{action}_{schwester_code}", context, user.id)
        if action == "preview":
            status = {}
            stats = load_stats()
            user_log = stats.get("admin_logs", {}).get(str(user.id))
            if user_log and "message_id" in user_log:
                try:
                    admin_message = await context.bot.get_chat_message(chat_id=NOTIFICATION_GROUP_ID, message_id=user_log["message_id"])
                    status = get_preview_status(admin_message.text)
                except Exception: status = get_preview_status("")
            if status.get("limit_reached", False):
                if schwester_code.upper() in status.get("viewed", set()):
                    await send_or_update_admin_log(context, user, f"Versucht Vorschau {schwester_code.upper()} (gesperrt)")
                    limit_text = "Du hast das Vorschau-Limit bereits erreicht. 😥\n\nBitte wähle nun ein Paket aus, um die volle Auswahl zu sehen."
                    limit_keyboard = [[InlineKeyboardButton("Zu den Preisen", callback_data="show_price_options")], [InlineKeyboardButton("« Zurück", callback_data="main_menu")]]
                    await context.bot.send_message(chat_id=chat_id, text=limit_text, reply_markup=InlineKeyboardMarkup(limit_keyboard))
                    return
                else:
                    await send_or_update_admin_log(context, user, f"Startet Vorschau {schwester_code.upper()} (Ausnahme)")
                    context.user_data['preview_clicks'] = status.get("clicks", 0)
                    await send_preview_message(update, context, schwester_code)
                    return
            await send_or_update_admin_log(context, user, f"Startet Vorschau ({schwester_code.upper()})")
            context.user_data['preview_clicks'] = status.get("clicks", 0)
            await send_preview_message(update, context, schwester_code)
        elif action == "prices":
            await send_or_update_admin_log(context, user, f"Schaut sich Preise an ({schwester_code.upper()})")
            image_paths = get_media_files(schwester_code, "preis"); image_paths.sort()
            if not image_paths:
                await context.bot.send_message(chat_id=chat_id, text="Ups! Ich konnte gerade keine passenden Inhalte finden...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="main_menu")]])); return
            random_image_path = random.choice(image_paths)
            with open(random_image_path, 'rb') as photo_file:
                photo_message = await context.bot.send_photo(chat_id=chat_id, photo=photo_file, protect_content=True)
            caption = "Wähle dein gewünschtes Paket:"
            keyboard_buttons = [[InlineKeyboardButton("10 Bilder", callback_data="select_package:bilder:10"), InlineKeyboardButton("10 Videos", callback_data="select_package:videos:10")], [InlineKeyboardButton("25 Bilder", callback_data="select_package:bilder:25"), InlineKeyboardButton("25 Videos", callback_data="select_package:videos:25")], [InlineKeyboardButton("35 Bilder", callback_data="select_package:bilder:35"), InlineKeyboardButton("35 Videos", callback_data="select_package:videos:35")], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
            text_message = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard_buttons))
            context.user_data["messages_to_delete"] = [photo_message.message_id, text_message.message_id]

    elif data.startswith("next_preview:"):
        await track_event("next_preview", context, user.id)
        _, schwester_code = data.split(":")
        current_clicks = context.user_data.get('preview_clicks', 0) + 1
        context.user_data['preview_clicks'] = current_clicks
        status = {}
        stats = load_stats()
        user_log = stats.get("admin_logs", {}).get(str(user.id))
        if user_log and "message_id" in user_log:
            try:
                admin_message = await context.bot.get_chat_message(chat_id=NOTIFICATION_GROUP_ID, message_id=user_log["message_id"])
                status = get_preview_status(admin_message.text)
            except Exception: status = get_preview_status("")
        status["clicks"] = current_clicks
        status["viewed"].add(schwester_code.upper())
        status["limit_reached"] = (status["clicks"] >= PREVIEW_LIMIT)
        await update_admin_log_preview_status(context, user, status)
        await send_or_update_admin_log(context, user, f"Nächstes Bild ({status['clicks']}/{PREVIEW_LIMIT})")
        if status["limit_reached"]:
            await query.answer("Vorschau-Limit erreicht!", show_alert=True)
            await cleanup_previous_messages(chat_id, context)
            try: await query.message.delete()
            except error.TelegramError: pass
            limit_text = "Du hast das Vorschau-Limit von 25 Bildern erreicht. 😥\n\nBitte wähle nun ein Paket aus, um die volle Auswahl zu sehen."
            limit_keyboard = [[InlineKeyboardButton("Zu den Preisen", callback_data="show_price_options")], [InlineKeyboardButton("« Zurück", callback_data="main_menu")]]
            await context.bot.send_message(chat_id=chat_id, text=limit_text, reply_markup=InlineKeyboardMarkup(limit_keyboard))
            return
        image_paths = get_media_files(schwester_code, "vorschau"); image_paths.sort()
        index_key = f'preview_index_{schwester_code}'; current_index = context.user_data.get(index_key, 0); next_index = current_index + 1
        if next_index >= len(image_paths): next_index = 0
        context.user_data[index_key] = next_index
        image_to_show_path = image_paths[next_index]
        if "messages_to_delete" in context.user_data and len(context.user_data["messages_to_delete"]) > 0:
            photo_message_id = context.user_data["messages_to_delete"][0]
            try:
                with open(image_to_show_path, 'rb') as photo_file:
                    await context.bot.edit_message_media(chat_id=chat_id, message_id=photo_message_id, media=InputMediaPhoto(photo_file))
            except error.TelegramError as e:
                logger.warning(f"Konnte Bild nicht bearbeiten, sende neu: {e}")
                await cleanup_previous_messages(chat_id, context)
                await send_preview_message(update, context, schwester_code)

    elif data.startswith("select_package:"):
        await track_event("package_selected", context, user.id)
        await cleanup_previous_messages(chat_id, context);
        try: await query.message.delete()
        except Exception: pass
        _, media_type, amount_str = data.split(":"); amount = int(amount_str); price = PRICES[media_type][amount]
        await send_or_update_admin_log(context, user, f"Paket ausgewählt: {amount} {media_type} für {price}€")
        text = f"Du hast das Paket **{amount} {media_type.capitalize()}** für **{price}€** ausgewählt.\n\nWie möchtest du bezahlen?"
        keyboard = [[InlineKeyboardButton(" PayPal", callback_data=f"pay_paypal:{media_type}:{amount}")], [InlineKeyboardButton(" Gutschein", callback_data=f"pay_voucher:{media_type}:{amount}")], [InlineKeyboardButton("🪙 Krypto", callback_data=f"pay_crypto:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück zu den Preisen", callback_data="show_price_options")]]
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif data.startswith(("pay_paypal:", "pay_voucher:", "pay_crypto:", "show_wallet:", "voucher_provider:")):
        try: await query.edit_message_text(text="⏳"); await asyncio.sleep(1)
        except Exception: pass
        parts = data.split(":")
        escaped_name = escape_markdown_v2(user.first_name)
        username_str = f"\n*Benutzername:* `@{user.username}`" if user.username else ""
        user_details = f"*ID:* `{user.id}`\n*Name:* {escaped_name}{username_str}"
        if data.startswith("pay_paypal:"):
            _, media_type, amount_str = parts; amount = int(amount_str); price = PRICES[media_type][amount]
            await track_event("payment_paypal", context, user.id)
            notification_text = f"💰 *PayPal-Klick*\n\n{user_details}\n*Paket:* {amount} {media_type.capitalize()} für {price}€"
            await send_permanent_admin_notification(context, notification_text)
            paypal_link = f"https://paypal.me/{PAYPAL_USER}/{price}"; text = (f"Super! Klicke auf den Link, um die Zahlung für **{amount} {media_type.capitalize()}** in Höhe von **{price}€** abzuschließen.\n\nGib als Verwendungszweck bitte deinen Telegram-Namen an.\n\n➡️ [Hier sicher bezahlen]({paypal_link})")
            keyboard = [[InlineKeyboardButton("« Zurück zur Bezahlwahl", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown', disable_web_page_preview=True)
        elif data.startswith("pay_voucher:"):
            _, media_type, amount_str = parts; amount = int(amount_str); price = PRICES[media_type][amount]
            await track_event("payment_voucher", context, user.id)
            notification_text = f"🎟️ *Gutschein-Klick*\n\n{user_details}\n*Paket:* {amount} {media_type.capitalize()} für {price}€"
            await send_permanent_admin_notification(context, notification_text)
            text = "Welchen Gutschein möchtest du einlösen?"; keyboard = [[InlineKeyboardButton("Amazon", callback_data=f"voucher_provider:amazon:{media_type}:{amount}"), InlineKeyboardButton("Paysafe", callback_data=f"voucher_provider:paysafe:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück zur Bezahlwahl", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif data.startswith("pay_crypto:"):
            _, media_type, amount_str = parts; amount = int(amount_str); price = PRICES[media_type][amount]
            await track_event("payment_crypto", context, user.id)
            notification_text = f"🪙 *Krypto-Klick*\n\n{user_details}\n*Paket:* {amount} {media_type.capitalize()} für {price}€"
            await send_permanent_admin_notification(context, notification_text)
            text = "Bitte wähle die gewünschte Kryptowährung:"; keyboard = [[InlineKeyboardButton("Bitcoin (BTC)", callback_data=f"show_wallet:btc:{media_type}:{amount}"), InlineKeyboardButton("Ethereum (ETH)", callback_data=f"show_wallet:eth:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück zur Bezahlwahl", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif data.startswith("show_wallet:"):
            _, crypto_type, media_type, amount_str = parts; amount = int(amount_str); price = PRICES[media_type][amount]
            wallet_address = BTC_WALLET if crypto_type == "btc" else ETH_WALLET
            crypto_name = "Bitcoin (BTC)" if crypto_type == "btc" else "Ethereum (ETH)"
            text = (f"Super! Um deine Bestellung (**{amount} {media_type.capitalize()}** für **{price}€**) mit **{crypto_name}** zu bezahlen, sende den entsprechenden Betrag an die folgende Adresse.\n\n**Wichtig:** Melde dich nach der Zahlung bei @Anna_2008_030 mit deinem Telegram-Namen und der Transaktions-ID, damit wir deine Bestellung zuordnen können.\n\n**Wallet-Adresse zum Kopieren:**\n`{wallet_address}`")
            keyboard = [[InlineKeyboardButton("« Zurück zur Krypto-Wahl", callback_data=f"pay_crypto:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        elif data.startswith("voucher_provider:"):
            _, provider, media_type, amount_str = parts
            context.user_data["awaiting_voucher"] = provider
            text = f"Bitte sende mir jetzt deinen {provider.capitalize()}-Gutschein-Code als einzelne Nachricht."
            keyboard = [[InlineKeyboardButton("Abbrechen", callback_data=f"pay_voucher:{media_type}:{amount_str}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🔒 *Admin-Menü*\n\nWähle eine Option:"
    keyboard = [[InlineKeyboardButton("📊 Nutzer-Statistiken", callback_data="admin_stats_users")], [InlineKeyboardButton("🖱️ Klick-Statistiken", callback_data="admin_stats_clicks")], [InlineKeyboardButton("🎟️ Gutscheine anzeigen", callback_data="admin_show_vouchers")], [InlineKeyboardButton("🔄 Statistiken zurücksetzen", callback_data="admin_reset_stats")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query: await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else: await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_vouchers_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    vouchers = load_vouchers(); amazon_codes = "\n".join([f"- `{code}`" for code in vouchers.get("amazon", [])]) or "Keine"; paysafe_codes = "\n".join([f"- `{code}`" for code in vouchers.get("paysafe", [])]) or "Keine"
    text = (f"*Eingelöste Gutscheine*\n\n*Amazon:*\n{amazon_codes}\n\n*Paysafe:*\n{paysafe_codes}")
    keyboard = [[InlineKeyboardButton("📄 Vouchers als PDF laden", callback_data="download_vouchers_pdf")], [InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("awaiting_voucher"):
        user = update.effective_user
        provider = context.user_data.pop("awaiting_voucher")
        code = update.message.text
        vouchers = load_vouchers()
        vouchers.setdefault(provider, []).append(code)
        save_vouchers(vouchers)
        escaped_name = escape_markdown_v2(user.first_name)
        username_str = f"\n*Benutzername:* `@{user.username}`" if user.username else ""
        user_details = f"*ID:* `{user.id}`\n*Name:* {escaped_name}{username_str}"
        notification_text = (f"📬 *Neuer Gutschein erhalten!*\n\n*Anbieter:* {provider.capitalize()}\n*Code:* `{code}`\n\n*Von Nutzer:*\n{user_details}")
        await send_permanent_admin_notification(context, notification_text)
        await update.message.reply_text("Vielen Dank! Dein Gutschein wurde übermittelt...")
        await start(update, context)

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    if not ADMIN_USER_ID or user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔️ Du hast keine Berechtigung für diesen Befehl.")
        return
    await show_admin_menu(update, context)

async def add_voucher(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    if not ADMIN_USER_ID or user_id != ADMIN_USER_ID: await update.message.reply_text("⛔️ Du hast keine Berechtigung."); return
    if len(context.args) < 2: await update.message.reply_text("⚠️ Falsches Format:\n`/addvoucher <anbieter> <code...>`", parse_mode='Markdown'); return
    provider = context.args[0].lower()
    if provider not in ["amazon", "paysafe"]: await update.message.reply_text("Fehler: Anbieter muss 'amazon' oder 'paysafe' sein."); return
    code = " ".join(context.args[1:]); vouchers = load_vouchers(); vouchers.setdefault(provider, []).append(code); save_vouchers(vouchers)
    await update.message.reply_text(f"✅ Gutschein für **{provider.capitalize()}** hinzugefügt:\n`{code}`", parse_mode='Markdown')

async def set_summary_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    if not ADMIN_USER_ID or user_id != ADMIN_USER_ID: await update.message.reply_text("⛔️ Du hast keine Berechtigung."); return
    if str(update.effective_chat.id) != NOTIFICATION_GROUP_ID:
        await update.message.reply_text("⚠️ Dieser Befehl geht nur in der Admin-Gruppe."); return
    await update.message.reply_text("🔄 Erstelle Dashboard...")
    stats = load_stats(); stats["pinned_message_id"] = None; save_stats(stats)
    await update_pinned_summary(context)

async def post_init(application: Application):
    await restore_stats_from_pinned_message(application)

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("addvoucher", add_voucher))
    application.add_handler(CommandHandler("setsummary", set_summary_message))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    
    if WEBHOOK_URL:
        port = int(os.environ.get("PORT", 8443))
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}"
        )
    else:
        logger.info("Starte Bot im Polling-Modus")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
# --- END OF FILE bot.py ---
