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
from telegram.helpers import escape_markdown

# --- Konfiguration ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PAYPAL_USER = os.getenv("PAYPAL_USER")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
AGE_ANNA = os.getenv("AGE_ANNA", "18")
AGE_LUNA = os.getenv("AGE_LUNA", "21")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
NOTIFICATION_GROUP_ID = os.getenv("NOTIFICATION_GROUP_ID")

BTC_WALLET = "1FcgMLNBDLiuDSDip7AStuP19sq47LJB12"
ETH_WALLET = "0xeeb8FDc4aAe71B53934318707d0e9747C5c66f6e"

PRICES = {"bilder": {10: 5, 25: 10, 35: 15}, "videos": {10: 15, 25: 25, 35: 30}}
VOUCHER_FILE = "vouchers.json"
STATS_FILE = "stats.json"
MEDIA_DIR = "image"

admin_notification_ids = {}

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

def is_user_banned(user_id: int) -> bool:
    stats = load_stats()
    return stats.get("users", {}).get(str(user_id), {}).get("banned", False)

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
        stats.setdefault("users", {})[user_id_str] = {
            "first_start": now.isoformat(),
            "last_start": now.isoformat(),
            "discount_sent": False,
            "preview_clicks": 0,
            "viewed_sisters": [],
            "payments_initiated": [],
            "banned": False
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

async def send_or_update_admin_log(context: ContextTypes.DEFAULT_TYPE, user: User, event_text: str = ""):
    if not NOTIFICATION_GROUP_ID or str(user.id) == ADMIN_USER_ID: return
    user_id_str = str(user.id)
    stats = load_stats()
    admin_logs = stats.get("admin_logs", {})
    user_data = stats.get("users", {}).get(user_id_str, {})
    log_message_id = admin_logs.get(user_id_str, {}).get("message_id")
    user_mention = f"[{escape_markdown(user.first_name, version=2)}](tg://user?id={user.id})"
    discount_emoji = "💸" if user_data.get("discount_sent") or "discounts" in user_data else ""
    banned_emoji = "🚫" if user_data.get("banned") else ""
    first_start_str = "N/A"
    if user_data.get("first_start"):
        first_start_dt = datetime.fromisoformat(user_data["first_start"])
        first_start_str = first_start_dt.strftime('%Y-%m-%d %H:%M')
    viewed_sisters_list = user_data.get("viewed_sisters", [])
    viewed_sisters_str = f"(Gesehen: {', '.join(s.upper() for s in sorted(viewed_sisters_list))})" if viewed_sisters_list else ""
    preview_clicks = user_data.get("preview_clicks", 0)
    payments = user_data.get("payments_initiated", [])
    payments_str = "\n".join(f"   • {p}" for p in payments) if payments else "   • Keine"
    base_text = (f"👤 *Nutzer-Aktivität* {discount_emoji} {banned_emoji}\n\n*Nutzer:* {user_mention} (`{user.id}`)\n🗓️ *Erster Start:* `{first_start_str}`\n\n🖼️ *Vorschau-Klicks:* {preview_clicks}/25 {viewed_sisters_str}\n\n💰 *Bezahlversuche*\n{payments_str}")
    final_text = f"{base_text}\n\n`➡️ Letzte Aktion: {event_text}`".strip()
    try:
        if log_message_id: await context.bot.edit_message_text(chat_id=NOTIFICATION_GROUP_ID, message_id=log_message_id, text=final_text, parse_mode='Markdown')
        else:
            sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
            admin_logs.setdefault(user_id_str, {})["message_id"] = sent_message.message_id
            stats["admin_logs"] = admin_logs
            save_stats(stats)
    except error.BadRequest as e:
        if "message to edit not found" in str(e):
            logger.warning(f"Admin log for user {user.id} not found. Sending new.")
            try:
                sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
                admin_logs.setdefault(user_id_str, {})["message_id"] = sent_message.message_id
                stats["admin_logs"] = admin_logs
                save_stats(stats)
            except Exception as e_new: logger.error(f"Failed to send replacement admin log: {e_new}")
        else: logger.error(f"BadRequest on admin log: {e}")
    except error.TelegramError as e:
        if 'message is not modified' not in str(e): logger.warning(f"Temporary error updating admin log: {e}")

async def send_permanent_admin_notification(context: ContextTypes.DEFAULT_TYPE, message: str):
    if NOTIFICATION_GROUP_ID:
        try: await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=message, parse_mode='Markdown')
        except Exception as e: logger.error(f"Konnte permanente Benachrichtigung nicht senden: {e}")

async def update_pinned_summary(context: ContextTypes.DEFAULT_TYPE):
    if not NOTIFICATION_GROUP_ID: return
    stats = load_stats()
    user_count = len(stats.get("users", {}))
    banned_count = sum(1 for u in stats.get("users", {}).values() if u.get("banned"))
    active_users_24h = 0
    now = datetime.now()
    for user_data in stats.get("users", {}).values():
        last_start_dt = datetime.fromisoformat(user_data.get("last_start", "1970-01-01T00:00:00"))
        if now - last_start_dt <= timedelta(hours=24): active_users_24h += 1
    events = stats.get("events", {})
    text = (f"📊 *Bot-Statistik Dashboard*\n🕒 _Letztes Update:_ `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n👥 *Nutzerübersicht*\n   • Gesamt: *{user_count}* (davon {banned_count} gebannt)\n   • Aktiv (24h): *{active_users_24h}*\n   • Starts: *{events.get('start_command', 0)}*\n\n💰 *Bezahl-Interesse*\n   • PayPal: *{events.get('payment_paypal', 0)}*\n   • Krypto: *{events.get('payment_crypto', 0)}*\n   • Gutschein: *{events.get('payment_voucher', 0)}*\n\n🖱️ *Klick-Verhalten*\n   • Vorschau (KS): *{events.get('preview_ks', 0)}*\n   • Vorschau (GS): *{events.get('preview_gs', 0)}*\n   • Preise (KS): *{events.get('prices_ks', 0)}*\n   • Preise (GS): *{events.get('prices_gs', 0)}*\n   • 'Nächstes Bild': *{events.get('next_preview', 0)}*\n   • Paketauswahl: *{events.get('package_selected', 0)}*")
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
    if not NOTIFICATION_GROUP_ID: logger.info("Keine NOTIFICATION_GROUP_ID, Wiederherstellung übersprungen."); return
    logger.info("Versuche, Statistiken wiederherzustellen...")
    try:
        chat = await application.bot.get_chat(chat_id=NOTIFICATION_GROUP_ID)
        if not chat.pinned_message or "Bot-Statistik Dashboard" not in chat.pinned_message.text: logger.warning("Keine passende Dashboard-Nachricht gefunden."); return
        pinned_text = chat.pinned_message.text; stats = load_stats()
        def extract(p, t): return int(re.search(p, t, re.DOTALL).group(1)) if re.search(p, t, re.DOTALL) else 0
        user_count = extract(r"Gesamt:\s*\*(\d+)\*", pinned_text)
        if len(stats.get("users", {})) < user_count:
            for i in range(user_count - len(stats.get("users", {}))): stats["users"][f"restored_user_{i}"] = {"first_start": "1970-01-01T00:00:00", "last_start": "1970-01-01T00:00:00"}
        for key, pattern in {'start_command': r"Starts:\s*\*(\d+)\*", 'payment_paypal': r"PayPal:\s*\*(\d+)\*", 'payment_crypto': r"Krypto:\s*\*(\d+)\*", 'payment_voucher': r"Gutschein:\s*\*(\d+)\*", 'preview_ks': r"Vorschau \(KS\):\s*\*(\d+)\*", 'preview_gs': r"Vorschau \(GS\):\s*\*(\d+)\*", 'prices_ks': r"Preise \(KS\):\s*\*(\d+)\*", 'prices_gs': r"Preise \(GS\):\s*\*(\d+)\*", 'next_preview': r"'Nächstes Bild':\s*\*(\d+)\*", 'package_selected': r"Paketauswahl:\s*\*(\d+)\*"}.items():
            stats['events'][key] = extract(pattern, pinned_text)
        stats['pinned_message_id'] = chat.pinned_message.message_id
        save_stats(stats); logger.info("Statistiken erfolgreich wiederhergestellt.")
    except Exception as e: logger.error(f"Fehler bei Wiederherstellung: {e}")

def get_media_files(schwester_code: str, media_type: str) -> list:
    matching_files = []; target_prefix = f"{schwester_code.lower()}_{media_type.lower()}"
    if not os.path.isdir(MEDIA_DIR): logger.error(f"Media-Verzeichnis '{MEDIA_DIR}' nicht gefunden!"); return []
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
    await cleanup_previous_messages(update.effective_chat.id, context)
    chat_id = update.effective_chat.id; image_paths = get_media_files(schwester_code, "vorschau"); image_paths.sort()
    if not image_paths:
        logger.error(f"Keine Vorschau-Bilder für '{schwester_code}' im Ordner '{MEDIA_DIR}' gefunden!")
        await context.bot.send_message(chat_id=chat_id, text="Ups! Ich konnte gerade keine passenden Inhalte finden...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="main_menu")]])); return
    context.user_data[f'preview_index_{schwester_code}'] = 0
    with open(image_paths[0], 'rb') as photo_file: photo_message = await context.bot.send_photo(chat_id=chat_id, photo=photo_file, protect_content=True)
    caption = f"Heyy ich bin Anna, ich bin {AGE_ANNA} Jahre alt..." if schwester_code == 'gs' else f"Heyy, mein name ist Luna ich bin {AGE_LUNA} Jahre alt..."
    keyboard_buttons = [[InlineKeyboardButton("🛍️ Zu den Preisen", callback_data=f"select_schwester:{schwester_code}:prices")], [InlineKeyboardButton("🖼️ Nächstes Bild", callback_data=f"next_preview:{schwester_code}")], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
    text_message = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard_buttons))
    context.user_data["messages_to_delete"] = [photo_message.message_id, text_message.message_id]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user; chat_id = update.effective_chat.id
    if is_user_banned(user.id): logger.warning(f"Gebannter Nutzer {user.id} versuchte /start."); return
    try:
        status, should_notify, user_data = await check_user_status(user.id, context)
        await track_event("start_command", context, user.id)
        if user_data and not user_data.get("discount_sent") and "discounts" not in user_data:
            if datetime.now() - datetime.fromisoformat(user_data.get("first_start")) > timedelta(hours=2):
                context.user_data['discount_active'] = True
                stats = load_stats(); stats["users"][str(user.id)]["discount_sent"] = True; save_stats(stats)
                await send_or_update_admin_log(context, user, event_text="Rabatt angeboten (Inaktivität >2h)")
                discount_message = "🎉 **WILLKOMMEN ZURÜCK!** 🎉\n\nAls Dankeschön haben wir einen ✨ *einmaligen Rabatt von 1€* ✨ auf **alle Pakete** für dich freigeschaltet!"
                keyboard = [[InlineKeyboardButton("🛍️ Jetzt Angebote ansehen!", callback_data="show_price_options")]]
                msg_method = update.message.reply_text if update.message else context.bot.send_message
                await msg_method(chat_id, discount_message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        if should_notify:
            await send_or_update_admin_log(context, user, event_text="Bot gestartet (neuer/wiederkehrender Nutzer)")
    except Exception as e: logger.error(f"Fehler in start() für {user.id}: {e}")
    await cleanup_previous_messages(chat_id, context)
    welcome_text = "Herzlich Willkommen! ✨\n\nHier kannst du eine Vorschau meiner Inhalte sehen oder direkt ein Paket auswählen."
    keyboard = [[InlineKeyboardButton("🖼️ Vorschau ansehen", callback_data="show_preview_options")], [InlineKeyboardButton("🛍️ Preise & Pakete", callback_data="show_price_options")]]
    if update.callback_query:
        query = update.callback_query; await query.answer()
        try: await query.edit_message_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))
        except error.TelegramError:
            await query.delete_message()
            msg = await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=InlineKeyboardMarkup(keyboard)); context.user_data["messages_to_delete"] = [msg.message_id]
    else:
        msg = await update.message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard)); context.user_data["messages_to_delete"] = [msg.message_id]

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer(); data = query.data; chat_id = update.effective_chat.id; user = update.effective_user
    if is_user_banned(user.id): logger.warning(f"Gebannter Nutzer {user.id} nutzte Callback."); return
    if data == "main_menu": await start(update, context); return
    if data.startswith("admin_"):
        if str(user.id) != ADMIN_USER_ID: await query.answer("⛔️ Keine Berechtigung.", show_alert=True); return
        if data in ["admin_main_menu", "admin_user_management"]:
            if 'admin_awaiting_input' in context.user_data: del context.user_data['admin_awaiting_input']
        if data == "admin_main_menu": await show_admin_menu(update, context)
        elif data == "admin_show_vouchers": await show_vouchers_panel(update, context)
        elif data == "admin_stats_users":
            stats = load_stats(); user_count = len(stats.get("users", {})); banned_count = sum(1 for u in stats.get("users", {}).values() if u.get("banned"))
            text = f"📊 *Nutzer-Statistiken*\n\nGesamtzahl der Nutzer: *{user_count}*\nGebannte Nutzer: *{banned_count}*"; keyboard = [[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))
        elif data == "admin_stats_clicks":
            stats = load_stats(); events = stats.get("events", {}); text = "🖱️ *Klick-Statistiken*\n\n" + ("Noch keine erfasst." if not events else "\n".join([f"- `{e}`: *{c}*" for e, c in sorted(events.items())]))
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]))
        elif data == "admin_reset_stats":
            text = "⚠️ *Bist du sicher?* Alle Statistiken werden unwiderruflich zurückgesetzt."; keyboard = [[InlineKeyboardButton("✅ Ja, zurücksetzen", callback_data="admin_reset_stats_confirm")], [InlineKeyboardButton("❌ Abbrechen", callback_data="admin_main_menu")]]
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))
        elif data == "admin_reset_stats_confirm":
            stats = load_stats(); stats["users"] = {}; stats["admin_logs"] = {}; stats["events"] = {key: 0 for key in stats["events"]}; save_stats(stats); await update_pinned_summary(context)
            await send_or_edit_admin_message(update, context, "✅ Alle Statistiken wurden zurückgesetzt.", InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]))
        elif data == "admin_discount_start":
            context.user_data['rabatt_in_progress'] = True; context.user_data['rabatt_data'] = {}; text = "💸 *Rabatt senden - Schritt 1: Zielgruppe*"; keyboard = [[InlineKeyboardButton("Alle Nutzer", callback_data="admin_discount_target_all")], [InlineKeyboardButton("Bestimmter Nutzer", callback_data="admin_discount_target_specific")], [InlineKeyboardButton("« Zurück", callback_data="admin_user_management")]]
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))
        elif data == "admin_discount_target_all": context.user_data['rabatt_target_type'] = 'all'; await show_discount_package_menu(update, context)
        elif data == "admin_discount_target_specific":
            context.user_data['rabatt_target_type'] = 'specific'; context.user_data['rabatt_awaiting'] = 'user_id'; text = "Bitte sende mir die numerische ID des Nutzers.";
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_user_management")]]))
        elif data.startswith("admin_discount_select_package:"):
            package_key = data.split(":")[1]; context.user_data['rabatt_awaiting'] = f'discount_amount_{package_key}'; package_name = package_key.replace("_", " ").capitalize()
            text = f"Wie hoch soll der Rabatt für *{package_name}* in Euro sein? (z.B. `2`)"; await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_discount_back_to_packages")]]))
        elif data == "admin_discount_back_to_packages": context.user_data['rabatt_awaiting'] = None; await show_discount_package_menu(update, context)
        elif data == "admin_discount_finalize": await query.answer(); await finalize_discount_action(update, context)
        elif data == "admin_broadcast_start":
            context.user_data['admin_awaiting_input'] = 'broadcast_message'; text = "📢 Bitte sende mir die Nachricht für den Broadcast.";
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]))
        elif data == "confirm_broadcast": await do_broadcast(update, context)
        elif data == "cancel_broadcast":
            if 'broadcast_message' in context.user_data: del context.user_data['broadcast_message']
            if 'admin_awaiting_input' in context.user_data: del context.user_data['admin_awaiting_input']
            await send_or_edit_admin_message(update, context, "✅ Broadcast abgebrochen.", InlineKeyboardMarkup([[InlineKeyboardButton("« Zum Hauptmenü", callback_data="admin_main_menu")]]))
        elif data == "admin_user_management": await show_user_management_menu(update, context)
        elif data == "admin_ban_user":
            context.user_data['admin_awaiting_input'] = 'ban_user_id'; text = "🚫 Bitte sende die ID des zu sperrenden Nutzers."
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_user_management")]]))
        elif data == "admin_unban_user":
            context.user_data['admin_awaiting_input'] = 'unban_user_id'; text = "✅ Bitte sende die ID des zu entsperrenden Nutzers."
            await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_user_management")]]))
        return
    if data == "download_vouchers_pdf":
        await query.answer("PDF wird erstellt..."); vouchers = load_vouchers(); pdf = FPDF(); pdf.add_page(); pdf.set_font("Arial", size=16); pdf.cell(0, 10, "Gutschein Report", ln=True, align='C'); pdf.ln(10)
        for provider in ["amazon", "paysafe"]:
            pdf.set_font("Arial", 'B', size=14); pdf.cell(0, 10, f"{provider.capitalize()} Gutscheine", ln=True); pdf.set_font("Arial", size=12)
            codes = vouchers.get(provider, []); pdf.cell(0, 8, "\n".join([f"- {c.encode('latin-1', 'ignore').decode('latin-1')}" for c in codes]) or "Keine vorhanden.", ln=True)
        pdf_buffer = BytesIO(pdf.output(dest='S').encode('latin-1')); pdf_buffer.seek(0)
        await context.bot.send_document(chat_id=chat_id, document=pdf_buffer, filename=f"Gutschein-Report_{datetime.now().strftime('%Y-%m-%d')}.pdf"); return
    if data in ["show_preview_options", "show_price_options"]:
        action = "preview" if "preview" in data else "prices"; text = "Für wen interessierst du dich?"; keyboard = [[InlineKeyboardButton("Kleine Schwester", callback_data=f"select_schwester:ks:{action}"), InlineKeyboardButton("Große Schwester", callback_data=f"select_schwester:gs:{action}")], [InlineKeyboardButton("« Zurück", callback_data="main_menu")]]; await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    elif data.startswith("select_schwester:"):
        await cleanup_previous_messages(chat_id, context); await query.message.delete()
        _, schwester_code, action = data.split(":"); stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); preview_clicks = user_data.get("preview_clicks", 0); viewed_sisters = user_data.get("viewed_sisters", [])
        if action == "preview" and preview_clicks >= 25 and schwester_code in viewed_sisters:
            msg = await context.bot.send_message(chat_id, "Du hast dein Vorschau-Limit erreicht.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zum Hauptmenü", callback_data="main_menu")]])); context.user_data["messages_to_delete"] = [msg.message_id]; return
        if schwester_code not in viewed_sisters: viewed_sisters.append(schwester_code); user_data["viewed_sisters"] = viewed_sisters; stats["users"][str(user.id)] = user_data; save_stats(stats)
        await track_event(f"{action}_{schwester_code}", context, user.id); await send_or_update_admin_log(context, user, event_text=f"Schaut {action} von {schwester_code.upper()} an")
        if action == "preview": await send_preview_message(update, context, schwester_code)
        elif action == "prices":
            image_paths = get_media_files(schwester_code, "preis");
            if not image_paths: await context.bot.send_message(chat_id, "Ups! Konnte keine Inhalte finden.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="main_menu")]])); return
            with open(random.choice(image_paths), 'rb') as photo_file: photo_message = await context.bot.send_photo(chat_id=chat_id, photo=photo_file, protect_content=True)
            stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); specific_discounts = user_data.get("discounts", {}); welcome_discount_active = context.user_data.get('discount_active', False)
            def create_package_button(media_type, amount):
                package_key, base_price = f"{media_type}_{amount}", PRICES[media_type][amount]; button_text = f"{amount} {media_type.capitalize()} ({base_price}€)"
                if package_key in specific_discounts: final_price = max(1, base_price - specific_discounts[package_key]); button_text = f"{amount} {media_type.capitalize()} (~{base_price}€~ {final_price}€ 🎁)"
                elif welcome_discount_active: final_price = max(1, base_price - 1); button_text = f"{amount} {media_type.capitalize()} (~{base_price}€~ {final_price}€ ✨)"
                return InlineKeyboardButton(button_text, callback_data=f"select_package:{media_type}:{amount}")
            keyboard_buttons = [[create_package_button("bilder", 10), create_package_button("videos", 10)], [create_package_button("bilder", 25), create_package_button("videos", 25)], [create_package_button("bilder", 35), create_package_button("videos", 35)], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
            caption = "Wähle dein Paket:" + ("\n\nDeine Rabatte wurden bereits markiert! ✨" if bool(specific_discounts) or welcome_discount_active else "")
            text_message = await context.bot.send_message(chat_id, caption, reply_markup=InlineKeyboardMarkup(keyboard_buttons)); context.user_data["messages_to_delete"] = [photo_message.message_id, text_message.message_id]
    elif data.startswith("next_preview:"):
        stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); preview_clicks = user_data.get("preview_clicks", 0)
        if preview_clicks >= 25:
            await query.answer("Vorschau-Limit erreicht!", show_alert=True); await cleanup_previous_messages(chat_id, context); _, schwester_code = data.split(":")
            limit_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(f"🛍️ Preise für {schwester_code.upper()}", callback_data=f"select_schwester:{schwester_code}:prices")], [InlineKeyboardButton("« Hauptmenü", callback_data="main_menu")]]); msg = await context.bot.send_message(chat_id, "Du hast dein Vorschau-Limit erreicht.", reply_markup=limit_keyboard); context.user_data["messages_to_delete"] = [msg.message_id]; return
        user_data["preview_clicks"] = preview_clicks + 1; stats["users"][str(user.id)] = user_data; save_stats(stats); await track_event("next_preview", context, user.id); _, schwester_code = data.split(":")
        await send_or_update_admin_log(context, user, event_text=f"Nächstes Bild ({schwester_code.upper()})"); image_paths = get_media_files(schwester_code, "vorschau"); image_paths.sort(); index_key = f'preview_index_{schwester_code}'; current_index = context.user_data.get(index_key, 0); next_index = (current_index + 1) % len(image_paths) if image_paths else 0; context.user_data[index_key] = next_index
        if image_paths and (photo_message_id := context.user_data.get("messages_to_delete", [None])[0]):
            try:
                with open(image_paths[next_index], 'rb') as photo_file: await context.bot.edit_message_media(chat_id, photo_message_id, InputMediaPhoto(photo_file))
            except error.TelegramError as e: logger.warning(f"Bild bearbeiten fehlgeschlagen: {e}"); await send_preview_message(update, context, schwester_code)
    elif data.startswith("select_package:"):
        await cleanup_previous_messages(chat_id, context); await query.message.delete()
        await track_event("package_selected", context, user.id); _, media_type, amount_str = data.split(":"); amount = int(amount_str); base_price = PRICES[media_type][amount]; price = base_price; price_str = f"*{price}€*"; stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); package_key = f"{media_type}_{amount}"
        if "discounts" in user_data and package_key in user_data["discounts"]: price = max(1, base_price - user_data["discounts"][package_key]); price_str = f"~{base_price}€~ *{price}€* (Exklusiv-Rabatt)"
        elif context.user_data.get('discount_active'): price = max(1, base_price - 1); price_str = f"~{base_price}€~ *{price}€* (Rabatt)"
        text = f"Du hast **{amount} {media_type.capitalize()}** für {price_str} gewählt.\nWie möchtest du bezahlen?"; keyboard = [[InlineKeyboardButton("PayPal", callback_data=f"pay_paypal:{media_type}:{amount}")], [InlineKeyboardButton("Gutschein", callback_data=f"pay_voucher:{media_type}:{amount}")], [InlineKeyboardButton("🪙 Krypto", callback_data=f"pay_crypto:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data="show_price_options")]]; msg = await context.bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'); context.user_data["messages_to_delete"] = [msg.message_id]
    elif data.startswith(("pay_", "show_wallet:", "voucher_provider:")):
        parts = data.split(":"); media_type = parts[1]; amount = int(parts[2]); base_price = PRICES[media_type][amount]; price = base_price; stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); package_key = f"{media_type}_{amount}"
        if "discounts" in user_data and package_key in user_data["discounts"]: price = max(1, base_price - user_data["discounts"][package_key])
        elif context.user_data.get('discount_active'): price = max(1, base_price - 1)
        async def update_payment_log(payment_method: str, price_val: int):
            stats_log = load_stats(); user_data_log = stats_log.get("users", {}).get(str(user.id))
            if user_data_log:
                payment_info = f"{payment_method}: {price_val}€"
                if payment_info not in user_data_log.get("payments_initiated", []): user_data_log.setdefault("payments_initiated", []).append(payment_info); save_stats(stats_log)
            await send_or_update_admin_log(context, user, event_text=f"Zahlung '{payment_method}' für {price_val}€ gewählt")
        if data.startswith("pay_paypal:"):
            await track_event("payment_paypal", context, user.id); await update_payment_log("PayPal", price); paypal_link = f"https://paypal.me/{PAYPAL_USER}/{price}"; text = f"Super! Klicke auf den Link, um die Zahlung für **{amount} {media_type.capitalize()}** über **{price}€** abzuschließen.\n\n➡️ [Hier sicher via PayPal bezahlen]({paypal_link})"; await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data=f"select_package:{media_type}:{amount}")]]), parse_mode='Markdown', disable_web_page_preview=True)
        elif data.startswith("pay_voucher:"):
            await track_event("payment_voucher", context, user.id); await update_payment_log("Gutschein", price); text = "Welchen Gutschein möchtest du einlösen?"; keyboard = [[InlineKeyboardButton("Amazon", callback_data=f"voucher_provider:amazon:{media_type}:{amount}"), InlineKeyboardButton("Paysafe", callback_data=f"voucher_provider:paysafe:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif data.startswith("pay_crypto:"):
            await track_event("payment_crypto", context, user.id); await update_payment_log("Krypto", price); text = "Wähle die Kryptowährung:"; keyboard = [[InlineKeyboardButton("Bitcoin (BTC)", callback_data=f"show_wallet:btc:{media_type}:{amount}"), InlineKeyboardButton("Ethereum (ETH)", callback_data=f"show_wallet:eth:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif data.startswith("show_wallet:"):
            wallet_address = BTC_WALLET if parts[1] == "btc" else ETH_WALLET; crypto_name = "Bitcoin (BTC)" if parts[1] == "btc" else "Ethereum (ETH)"
            text = f"Zahlung mit **{crypto_name}** für **{price}€**.\n\nSende den Betrag an:\n`{wallet_address}`"; await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data=f"pay_crypto:{media_type}:{amount}")]]), parse_mode='Markdown')
        elif data.startswith("voucher_provider:"):
            context.user_data["awaiting_voucher"] = parts[1]; text = f"Sende mir jetzt deinen {parts[1].capitalize()}-Gutschein-Code."
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Abbrechen", callback_data=f"pay_voucher:{media_type}:{amount}")]]))

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if is_user_banned(user.id): return
    if str(user.id) == ADMIN_USER_ID:
        if context.user_data.get('admin_awaiting_input'): await handle_admin_text_input(update, context); return
        if context.user_data.get('rabatt_in_progress'): await handle_admin_discount_input(update, context); return
    if provider := context.user_data.pop("awaiting_voucher", None):
        code = update.message.text; vouchers = load_vouchers(); vouchers.setdefault(provider, []).append(code); save_vouchers(vouchers)
        notification_text = f"📬 *Neuer Gutschein:*\n\n*Anbieter:* {provider.capitalize()}\n*Code:* `{code}`\n*Von:* {escape_markdown(user.first_name, version=2)} (`{user.id}`)"; await send_permanent_admin_notification(context, notification_text)
        await send_or_update_admin_log(context, user, event_text=f"Gutschein '{provider}' eingereicht"); await update.message.reply_text("Danke! Dein Gutschein wird geprüft."); await asyncio.sleep(2); await start(update, context)

# --- Admin-Funktionen ---
async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) != ADMIN_USER_ID: return
    await show_admin_menu(update, context)

async def send_or_edit_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text, reply_markup):
    if update.callback_query:
        try: await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        except error.BadRequest as e:
            if "message is not modified" not in str(e): logger.error(f"Admin message edit failed: {e}")
    else: await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for key in list(context.user_data.keys()):
        if key.startswith(('rabatt_', 'admin_awaiting', 'broadcast_')): del context.user_data[key]
    text = "🔒 *Admin-Hauptmenü*\n\nWähle eine Aktion:"; keyboard = [[InlineKeyboardButton("📊 Nutzer-Statistiken", callback_data="admin_stats_users"), InlineKeyboardButton("🖱️ Klick-Statistiken", callback_data="admin_stats_clicks")], [InlineKeyboardButton("🎟️ Gutscheine anzeigen", callback_data="admin_show_vouchers"), InlineKeyboardButton("📢 Broadcast senden", callback_data="admin_broadcast_start")], [InlineKeyboardButton("👤 Nutzer verwalten", callback_data="admin_user_management"), InlineKeyboardButton("🔄 Statistiken zurücksetzen", callback_data="admin_reset_stats")]]
    await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))

async def show_user_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "👤 *Nutzer Verwaltung*"; keyboard = [[InlineKeyboardButton("🚫 Nutzer sperren", callback_data="admin_ban_user")], [InlineKeyboardButton("✅ Nutzer entsperren", callback_data="admin_unban_user")], [InlineKeyboardButton("💸 Rabatt an Nutzer senden", callback_data="admin_discount_start")], [InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]]
    await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))

async def show_vouchers_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    vouchers = load_vouchers(); amazon_codes = "\n".join([f"- `{c}`" for c in vouchers.get("amazon", [])]) or "Keine"; paysafe_codes = "\n".join([f"- `{c}`" for c in vouchers.get("paysafe", [])]) or "Keine"; text = f"*Eingelöste Gutscheine*\n\n*Amazon:*\n{amazon_codes}\n\n*Paysafe:*\n{paysafe_codes}"; keyboard = [[InlineKeyboardButton("📄 PDF laden", callback_data="download_vouchers_pdf")], [InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]
    await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))

async def set_summary_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) != ADMIN_USER_ID or str(update.effective_chat.id) != NOTIFICATION_GROUP_ID: return
    await update.message.reply_text("🔄 Erstelle Dashboard..."); stats = load_stats(); stats["pinned_message_id"] = None; save_stats(stats); await update_pinned_summary(context)

async def show_discount_package_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rabatt_data = context.user_data.get('rabatt_data', {})
    def get_btn_txt(pkg, name): return f"{name} (Rabatt: {rabatt_data.get(pkg)}€)" if rabatt_data.get(pkg) is not None else name
    keyboard = [[InlineKeyboardButton(get_btn_txt(f'{t}_{a}', f'{t.capitalize()} {a}'), callback_data=f"admin_discount_select_package:{t}_{a}") for a in PRICES[t]] for t in PRICES]
    keyboard.extend([[InlineKeyboardButton("✅ Aktion abschließen & senden", callback_data="admin_discount_finalize")], [InlineKeyboardButton("« Zurück", callback_data="admin_user_management")]])
    target_desc = "Alle Nutzer" if context.user_data.get('rabatt_target_type') == 'all' else f"Nutzer `{context.user_data.get('rabatt_target_id')}`"; text = f"💸 *Rabatt senden - Schritt 2: Pakete*\n\nZiel: {target_desc}"
    await send_or_edit_admin_message(update, context, text, InlineKeyboardMarkup(keyboard))

async def handle_admin_discount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_input = update.message.text; awaiting = context.user_data.get('rabatt_awaiting')
    if awaiting == 'user_id':
        if not text_input.isdigit() or text_input not in load_stats()["users"]: await update.message.reply_text("⚠️ Ungültige oder unbekannte User ID."); return
        context.user_data['rabatt_target_id'] = text_input; context.user_data.pop('rabatt_awaiting'); await show_discount_package_menu(update, context)
    elif awaiting and awaiting.startswith('discount_amount_'):
        if not text_input.isdigit(): await update.message.reply_text("⚠️ Bitte eine ganze Zahl eingeben."); return
        package_key = awaiting.replace('discount_amount_', ''); context.user_data.setdefault('rabatt_data', {})[package_key] = int(text_input); context.user_data.pop('rabatt_awaiting')
        await update.message.reply_text(f"✅ Rabatt für `{package_key}` auf *{text_input}€* gesetzt."); await show_discount_package_menu(update, context)

async def finalize_discount_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rabatt_data = context.user_data.get('rabatt_data', {});
    if not rabatt_data: await update.callback_query.answer("Es wurden keine Rabatte festgelegt.", show_alert=True); return
    await send_or_edit_admin_message(update, context, "Sende Rabatte...", None)
    stats = load_stats(); target_ids = list(stats["users"].keys()) if context.user_data.get('rabatt_target_type') == 'all' else [context.user_data.get('rabatt_target_id')]
    if not target_ids or not target_ids[0]: await send_or_edit_admin_message(update, context, "Fehler: Kein Ziel gefunden.", InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_user_management")]])); return
    success_count, fail_count = 0, 0
    rabatt_message = "🎁 **DEIN PERSÖNLICHES ANGEBOT!** 🎁\n\nWir haben dir einen exklusiven Rabatt gutgeschrieben! Schau dir deine neuen Preise an:"
    keyboard = [[InlineKeyboardButton("💸 Zu meinen exklusiven Preisen 💸", callback_data="show_price_options")]];
    for user_id in target_ids:
        if user_id in stats["users"] and not stats["users"][user_id].get("banned"):
            stats["users"][user_id].setdefault("discounts", {}).update(rabatt_data)
            try: await context.bot.send_message(user_id, rabatt_message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'); success_count += 1
            except (error.Forbidden, error.BadRequest): fail_count += 1
            await asyncio.sleep(0.1)
    save_stats(stats);
    for key in list(context.user_data.keys()):
        if key.startswith('rabatt_'): del context.user_data[key]
    final_text = f"✅ Aktion abgeschlossen!\n\n- Gesendet an: *{success_count} Nutzer*\n- Fehlgeschlagen: *{fail_count} Nutzer*"
    await send_or_edit_admin_message(update, context, final_text, InlineKeyboardMarkup([[InlineKeyboardButton("« Zum Hauptmenü", callback_data="admin_main_menu")]]))

async def handle_admin_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    awaiting = context.user_data.get('admin_awaiting_input'); text_input = update.message.text
    if awaiting == 'broadcast_message':
        context.user_data['broadcast_message'] = text_input; keyboard = [[InlineKeyboardButton("✅ Ja, senden", callback_data="confirm_broadcast")], [InlineKeyboardButton("❌ Abbrechen", callback_data="cancel_broadcast")]]; user_count = len([u for u in load_stats().get("users", {}).values() if not u.get("banned")])
        await update.message.reply_text(f"Willst du das wirklich an *{user_count}* Nutzer senden?\n\n---\n{text_input}\n---", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    elif awaiting in ['ban_user_id', 'unban_user_id']:
        context.user_data.pop('admin_awaiting_input')
        if not text_input.isdigit(): await update.message.reply_text("⚠️ Ungültige User ID."); return
        stats = load_stats()
        if text_input not in stats["users"]: await update.message.reply_text("⚠️ Nutzer nicht gefunden."); return
        is_banning = awaiting == 'ban_user_id'; stats["users"][text_input]["banned"] = is_banning; save_stats(stats); await update_pinned_summary(context)
        action_text = "gesperrt" if is_banning else "entsperrt"; emoji = "🚫" if is_banning else "✅"
        await update.message.reply_text(f"{emoji} Nutzer `{text_input}` wurde {action_text}."); await send_or_update_admin_log(context, User(id=int(text_input), first_name="N/A", is_bot=False), event_text=f"Wurde {action_text}")
        await show_admin_menu(update, context)

async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message_to_send = context.user_data.pop('broadcast_message', None)
    context.user_data.pop('admin_awaiting_input', None)
    if not message_to_send: await query.edit_message_text("Fehler: Nachricht nicht gefunden."); return
    await query.edit_message_text("🚀 Sende Broadcast...")
    stats = load_stats(); all_users = list(stats.get("users", {}).keys()); success_count, fail_count = 0, 0
    for user_id in all_users:
        if not stats["users"][user_id].get("banned"):
            try: await context.bot.send_message(user_id, message_to_send); success_count += 1
            except (error.Forbidden, error.BadRequest): fail_count += 1
            await asyncio.sleep(0.1)
    final_text = f"✅ Broadcast beendet!\n\n- Erfolgreich: *{success_count}*\n- Fehlgeschlagen: *{fail_count}*"
    await query.edit_message_text(final_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zum Hauptmenü", callback_data="admin_main_menu")]]), parse_mode='Markdown')

async def post_init(application: Application): await restore_stats_from_pinned_message(application)

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("setsummary", set_summary_message))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    if WEBHOOK_URL:
        port = int(os.environ.get("PORT", 8443))
        application.run_webhook(listen="0.0.0.0", port=port, url_path=BOT_TOKEN, webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}")
    else: logger.info("Starte Bot im Polling-Modus"); application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__": main()
