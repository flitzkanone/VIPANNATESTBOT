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

creating_log_for_user = set()

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
    
    if schwester_code == 'gs': caption = f"Heyy ich bin Anna, ich bin {AGE_ANNA} Jahre alt und mache mit meiner Schwester zusammen 🌶️ videos und Bilder falls du lust hast speziele videos zu bekommen schreib mir 😏 @Anna_2008_030"
    else: caption = f"Heyy, mein name ist Luna ich bin {AGE_LUNA} Jahre alt und mache 🌶️ videos und Bilder. wenn du Spezielle wünsche hast schreib meiner Schwester für mehr.\nMeine Schwester: @Anna_2008_030"
    
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
        try:
            error_message = "Hoppla! Im Hintergrund ist ein kleiner technischer Fehler aufgetreten. Das beeinträchtigt dich aber nicht. Viel Spaß!"
            if update.callback_query: await context.bot.send_message(chat_id, error_message)
            else: await update.message.reply_text(error_message)
        except Exception as e_reply:
            logger.error(f"Could not even send error reply to user {user.id}: {e_reply}")

    await cleanup_previous_messages(chat_id, context)
    welcome_text = f"Hier sind Anna ({AGE_ANNA}) und Luna ({AGE_LUNA}), eure heißesten Begleiterinnen! 😈\n\nWir freuen uns, dir unsere exklusiven Bilder und Videos zu präsentieren. Lass dich von unserer Leidenschaft und Erotik verzaubern! 🔥😘"
    keyboard = [[InlineKeyboardButton(" Vorschau", callback_data="show_preview_options")], [InlineKeyboardButton(" Preise & Pakete", callback_data="show_price_options")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        query = update.callback_query; await query.answer()
        try:
            await query.edit_message_text(welcome_text, reply_markup=reply_markup)
        except error.TelegramError:
            try: await query.delete_message()
            except Exception: pass
            msg = await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup)
            context.user_data["messages_to_delete"] = [msg.message_id]
    else:
        msg = await update.message.reply_text(welcome_text, reply_markup=reply_markup)
        context.user_data["messages_to_delete"] = [msg.message_id]

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id
    user = update.effective_user

    if data == "main_menu":
        await start(update, context)
        return

    if data.startswith("admin_"):
        if str(user.id) != ADMIN_USER_ID:
            await query.answer("⛔️ Keine Berechtigung.", show_alert=True); return
        if data == "admin_main_menu":
            for key in list(context.user_data.keys()):
                if key.startswith('rabatt_'): del context.user_data[key]
            await show_admin_menu(update, context)
        elif data == "admin_show_vouchers": await show_vouchers_panel(update, context)
        elif data == "admin_stats_users": pass 
        elif data == "admin_stats_clicks": pass 
        elif data == "admin_reset_stats": pass 
        elif data == "admin_reset_stats_confirm": pass 
        elif data == "admin_discount_start": await start_discount_process(update, context)
        elif data == "admin_discount_target_all": await select_discount_target(update, context, 'all')
        elif data == "admin_discount_target_specific": await select_discount_target(update, context, 'specific')
        elif data.startswith("admin_discount_select_package:"): await select_discount_package(update, context, data)
        elif data == "admin_discount_back_to_packages": await show_discount_package_menu(update, context)
        elif data == "admin_discount_finalize": await finalize_discount_action(update, context)
        return

    if data == "download_vouchers_pdf": pass 

    if data in ["show_preview_options", "show_price_options"]:
        action = "preview" if "preview" in data else "prices"
        text = "Für wen interessierst du dich?"
        keyboard = [[InlineKeyboardButton("Kleine Schwester", callback_data=f"select_schwester:ks:{action}"), InlineKeyboardButton("Große Schwester", callback_data=f"select_schwester:gs:{action}")], [InlineKeyboardButton("« Zurück", callback_data="main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("select_schwester:"):
        _, schwester_code, action = data.split(":")
        new_messages = []
        try:
            if action == "preview": await send_preview_message(update, context, schwester_code)
            elif action == "prices":
                image_paths = get_media_files(schwester_code, "preis"); image_paths.sort()
                if not image_paths:
                    msg = await context.bot.send_message(chat_id=chat_id, text="Ups! Ich konnte gerade keine passenden Inhalte finden...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="main_menu")]]))
                    new_messages.append(msg.message_id)
                else:
                    random_image_path = random.choice(image_paths)
                    with open(random_image_path, 'rb') as photo_file: photo_message = await context.bot.send_photo(chat_id=chat_id, photo=photo_file, protect_content=True)
                    caption = "Wähle dein gewünschtes Paket:"
                    keyboard_buttons = [[InlineKeyboardButton("10 Bilder", callback_data="select_package:bilder:10"), InlineKeyboardButton("10 Videos", callback_data="select_package:videos:10")], [InlineKeyboardButton("25 Bilder", callback_data="select_package:bilder:25"), InlineKeyboardButton("25 Videos", callback_data="select_package:videos:25")], [InlineKeyboardButton("35 Bilder", callback_data="select_package:bilder:35"), InlineKeyboardButton("35 Videos", callback_data="select_package:videos:35")], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
                    text_message = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=InlineKeyboardMarkup(keyboard_buttons))
                    new_messages.extend([photo_message.message_id, text_message.message_id])
            
            await cleanup_previous_messages(chat_id, context); await query.message.delete(); context.user_data["messages_to_delete"] = new_messages
            stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {})
            if schwester_code not in user_data.get("viewed_sisters", []): user_data.setdefault("viewed_sisters", []).append(schwester_code); save_stats(stats)
            await track_event(f"{action}_{schwester_code}", context, user.id); await send_or_update_admin_log(context, user, event_text=f"Schaut sich {action} von {schwester_code.upper()} an")
        except Exception as e: logger.error(f"Error in select_schwester handler: {e}")

    elif data.startswith("next_preview:"):
        stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); preview_clicks = user_data.get("preview_clicks", 0)
        if preview_clicks >= 25:
            await query.answer("Vorschau-Limit erreicht!", show_alert=True); _, schwester_code = data.split(":")
            limit_text = "Du hast dein Vorschau-Limit von 25 Klicks erreicht. Sieh dir jetzt die Preise an, um mehr zu sehen!"
            limit_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(f"🛍️ Preise für {schwester_code.upper()} ansehen", callback_data=f"select_schwester:{schwester_code}:prices")], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]])
            msg = await context.bot.send_message(chat_id, text=limit_text, reply_markup=limit_keyboard); await cleanup_previous_messages(chat_id, context); context.user_data["messages_to_delete"] = [msg.message_id]
            return
        user_data["preview_clicks"] = preview_clicks + 1; stats["users"][str(user.id)] = user_data; save_stats(stats)
        await track_event("next_preview", context, user.id); _, schwester_code = data.split(":")
        await send_or_update_admin_log(context, user, event_text=f"Nächstes Bild ({schwester_code.upper()})"); image_paths = get_media_files(schwester_code, "vorschau"); image_paths.sort()
        index_key = f'preview_index_{schwester_code}'; current_index = context.user_data.get(index_key, 0); next_index = (current_index + 1) % len(image_paths) if image_paths else 0
        context.user_data[index_key] = next_index
        if not image_paths: return
        image_to_show_path = image_paths[next_index]; photo_message_id = context.user_data.get("messages_to_delete", [None])[0]
        if photo_message_id:
            try:
                with open(image_to_show_path, 'rb') as photo_file: media = InputMediaPhoto(photo_file); await context.bot.edit_message_media(chat_id=chat_id, message_id=photo_message_id, media=media)
            except error.TelegramError as e: logger.warning(f"Konnte Bild nicht bearbeiten, sende neu: {e}"); await send_preview_message(update, context, schwester_code)

    elif data.startswith("select_package:"):
        try:
            await track_event("package_selected", context, user.id); _, media_type, amount_str = data.split(":"); amount = int(amount_str); base_price = PRICES[media_type][amount]; price = base_price; price_str = f"*{price}€*"
            stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); package_key = f"{media_type}_{amount}"
            if user_data.get("discounts", {}).get(package_key):
                discount = user_data["discounts"][package_key]; price = max(1, base_price - discount); price_str = f"~{base_price}€~ *{price}€* (Exklusiv-Rabatt)"
            elif context.user_data.get('discount_active'): price = max(1, base_price - 1); price_str = f"~{base_price}€~ *{price}€* (Rabatt)"
            text = f"Du hast das Paket **{amount} {media_type.capitalize()}** für {price_str} ausgewählt.\n\nWie möchtest du bezahlen?"
            keyboard = [[InlineKeyboardButton(" PayPal", callback_data=f"pay_paypal:{media_type}:{amount}")], [InlineKeyboardButton(" Gutschein", callback_data=f"pay_voucher:{media_type}:{amount}")], [InlineKeyboardButton("🪙 Krypto", callback_data=f"pay_crypto:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück zu den Preisen", callback_data="show_price_options")]]
            msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            await cleanup_previous_messages(chat_id, context); await query.message.delete(); context.user_data["messages_to_delete"] = [msg.message_id]
        except Exception as e: logger.error(f"Error in select_package handler: {e}")

    elif data.startswith("payment_confirmed:"):
        _, media_type, amount_str = data.split(":")
        await send_or_update_admin_log(context, user, event_text="✅ Zahlung bestätigt (Klick)")
        text = "⏳ Deine Zahlung wird nun verifiziert, dies kann einen Moment dauern..."
        await query.edit_message_text(text=text, reply_markup=None)
        asyncio.create_task(send_failed_payment_message(context, chat_id, media_type, amount_str))

    elif data.startswith(("pay_paypal:", "pay_voucher:", "pay_crypto:", "show_wallet:", "voucher_provider:")):
        parts = data.split(":"); media_type = parts[1]; amount_str = parts[2]; amount = int(amount_str); base_price = PRICES[media_type][amount]; price = base_price
        stats = load_stats(); user_data = stats.get("users", {}).get(str(user.id), {}); package_key = f"{media_type}_{amount}"
        if user_data.get("discounts", {}).get(package_key): price = max(1, base_price - user_data["discounts"][package_key])
        elif context.user_data.get('discount_active'): price = max(1, base_price - 1)
        async def update_payment_log(payment_method: str, price_val: int):
            stats_log = load_stats(); user_data_log = stats_log.get("users", {}).get(str(user.id))
            if user_data_log:
                payment_info = f"{payment_method}: {price_val}€"; user_data_log.setdefault("payments_initiated", []).append(payment_info); save_stats(stats_log)
            await send_or_update_admin_log(context, user, event_text=f"Bezahlmethode '{payment_method}' für {price_val}€ gewählt")
        if data.startswith("pay_paypal:"):
            await track_event("payment_paypal", context, user.id); await update_payment_log("PayPal", price)
            paypal_link = f"https://paypal.me/{PAYPAL_USER}/{price}"
            text = f"Super! Klicke auf den Link, um die Zahlung für **{amount} {media_type.capitalize()}** in Höhe von **{price}€** abzuschließen.\n\nGib als Verwendungszweck bitte deinen Telegram-Namen an.\n\n➡️ [Hier sicher bezahlen]({paypal_link})"
            keyboard = [[InlineKeyboardButton("✅ Ich habe bezahlt", callback_data=f"payment_confirmed:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown', disable_web_page_preview=True)
        elif data.startswith("pay_crypto:"):
            await track_event("payment_crypto", context, user.id); await update_payment_log("Krypto", price); text = "Bitte wähle die gewünschte Kryptowährung:"; keyboard = [[InlineKeyboardButton("Bitcoin (BTC)", callback_data=f"show_wallet:btc:{media_type}:{amount}"), InlineKeyboardButton("Ethereum (ETH)", callback_data=f"show_wallet:eth:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data=f"select_package:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif data.startswith("show_wallet:"):
            _, crypto_type, _, _ = parts; wallet_address = BTC_WALLET if crypto_type == "btc" else ETH_WALLET; crypto_name = "Bitcoin (BTC)" if crypto_type == "btc" else "Ethereum (ETH)"
            text = f"Zahlung mit **{crypto_name}** für **{price}€**.\n\nBitte sende den Betrag an die folgende Adresse:\n\n`{wallet_address}`"
            keyboard = [[InlineKeyboardButton("✅ Ich habe bezahlt", callback_data=f"payment_confirmed:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data=f"pay_crypto:{media_type}:{amount}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        elif data.startswith("pay_voucher:"): await track_event("payment_voucher", context, user.id); await update_payment_log("Gutschein", price); text = "Welchen Gutschein möchtest du einlösen?"; keyboard = [[InlineKeyboardButton("Amazon", callback_data=f"voucher_provider:amazon:{media_type}:{amount}"), InlineKeyboardButton("Paysafe", callback_data=f"voucher_provider:paysafe:{media_type}:{amount}")], [InlineKeyboardButton("« Zurück", callback_data=f"select_package:{media_type}:{amount}")]]; await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif data.startswith("voucher_provider:"): _, provider, _, _ = parts; context.user_data["awaiting_voucher"] = provider; text = f"Bitte sende mir jetzt deinen {provider.capitalize()}-Gutschein-Code als einzelne Nachricht."; keyboard = [[InlineKeyboardButton("Abbrechen", callback_data=f"pay_voucher:{media_type}:{amount_str}")]]; await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def send_failed_payment_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, media_type: str, amount_str: str):
    await asyncio.sleep(30)
    text = "❌ Kauf fehlgeschlagen!\n\nDein vorheriger Betrag wird dir selbstverständlich zurückerstattet. Bitte kaufe das Paket erneut."
    last_package_callback = f"select_package:{media_type}:{amount_str}"
    keyboard = [[InlineKeyboardButton("🛍️ Erneut kaufen", callback_data=last_package_callback)], [InlineKeyboardButton("« Zurück zum Hauptmenü", callback_data="main_menu")]]
    try:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.warning(f"Could not send failed payment message to {chat_id}: {e}")

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if str(user.id) == ADMIN_USER_ID and context.user_data.get('rabatt_in_progress'):
        await handle_admin_discount_input(update, context)
        return
    if context.user_data.get("awaiting_voucher"):
        provider = context.user_data.pop("awaiting_voucher"); code = update.message.text
        vouchers = load_vouchers(); vouchers[provider].append(code); save_vouchers(vouchers)
        notification_text = (f"📬 *Neuer Gutschein erhalten!*\n\n*Anbieter:* {provider.capitalize()}\n*Code:* `{code}`\n*Von Nutzer:* {escape_markdown(user.first_name, version=2)} (`{user.id}`)")
        await send_permanent_admin_notification(context, notification_text)
        await send_or_update_admin_log(context, user, event_text=f"Gutschein '{provider}' eingereicht")
        await update.message.reply_text("Vielen Dank! Dein Gutschein wurde übermittelt und wird nun geprüft. Ich melde mich bei dir."); await asyncio.sleep(2)
        await start(update, context)

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    if not ADMIN_USER_ID or user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔️ Du hast keine Berechtigung für diesen Befehl."); return
    await show_admin_menu(update, context)

async def add_voucher(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass

async def set_summary_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass

async def show_discount_package_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rabatt_data = context.user_data.get('rabatt_data', {})
    def get_button_text(pkg, name):
        discount = rabatt_data.get(pkg); return f"{name} (Rabatt: {discount}€)" if discount is not None else name
    keyboard = [[InlineKeyboardButton(get_button_text('bilder_10', "Bilder 10"), callback_data="admin_discount_select_package:bilder_10"), InlineKeyboardButton(get_button_text('videos_10', "Videos 10"), callback_data="admin_discount_select_package:videos_10")], [InlineKeyboardButton(get_button_text('bilder_25', "Bilder 25"), callback_data="admin_discount_select_package:bilder_25"), InlineKeyboardButton(get_button_text('videos_25', "Videos 25"), callback_data="admin_discount_select_package:videos_25")], [InlineKeyboardButton(get_button_text('bilder_35', "Bilder 35"), callback_data="admin_discount_select_package:bilder_35"), InlineKeyboardButton(get_button_text('videos_35', "Videos 35"), callback_data="admin_discount_select_package:videos_35")], [InlineKeyboardButton("✅ Aktion abschließen & senden", callback_data="admin_discount_finalize")], [InlineKeyboardButton("❌ Abbrechen", callback_data="admin_main_menu")]]
    target_type = context.user_data.get('rabatt_target_type'); target_id = context.user_data.get('rabatt_target_id'); target_desc = "Alle Nutzer" if target_type == 'all' else f"Nutzer `{target_id}`"
    text = f"💸 *Rabatt-Manager - Schritt 2: Pakete*\n\nZiel: {target_desc}\n\nWähle ein Paket, um einen Rabatt festzulegen oder zu ändern."
    query = update.callback_query
    if query: await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else: await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_discount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_input = update.message.text; awaiting = context.user_data.get('rabatt_awaiting')
    if awaiting == 'user_id':
        if not text_input.isdigit(): await update.message.reply_text("⚠️ Bitte gib eine gültige, numerische Nutzer-ID ein."); return
        stats = load_stats()
        if text_input not in stats["users"]: await update.message.reply_text(f"⚠️ Nutzer mit der ID `{text_input}` wurde nicht gefunden."); return
        context.user_data['rabatt_target_id'] = text_input; context.user_data['rabatt_awaiting'] = None
        await show_discount_package_menu(update, context)
    elif awaiting and awaiting.startswith('discount_amount_'):
        if not text_input.isdigit(): await update.message.reply_text("⚠️ Bitte gib einen gültigen Rabattbetrag als ganze Zahl ein (z.B. `2`)."); return
        package_key = awaiting.replace('discount_amount_', ''); discount_amount = int(text_input)
        context.user_data.setdefault('rabatt_data', {})[package_key] = discount_amount; context.user_data['rabatt_awaiting'] = None
        await update.message.reply_text(f"✅ Rabatt für `{package_key}` auf *{discount_amount}€* gesetzt."); await show_discount_package_menu(update, context)

async def broadcast_discount_task(context: ContextTypes.DEFAULT_TYPE, target_ids: list, rabatt_data: dict, message_text: str, reply_markup: InlineKeyboardMarkup):
    logger.info(f"Starting broadcast to {len(target_ids)} users."); success_count, fail_count = 0, 0
    stats = load_stats()
    for user_id in target_ids:
        if user_id in stats["users"]: stats["users"][user_id]["discounts"] = rabatt_data
    save_stats(stats); logger.info("Stats file updated with new discounts for all targets.")
    for user_id in target_ids:
        try:
            user_obj = await context.bot.get_chat(user_id); await send_or_update_admin_log(context, user_obj, event_text="Individueller Rabatt zugewiesen", new_discounts=rabatt_data)
            await context.bot.send_message(chat_id=user_id, text=message_text, reply_markup=reply_markup); success_count += 1
        except error.Forbidden: fail_count += 1; logger.warning(f"Broadcast failed for user {user_id}: Bot was blocked.")
        except Exception as e: fail_count += 1; logger.error(f"Error processing broadcast for {user_id}: {e}")
        await asyncio.sleep(0.1)
    logger.info(f"Broadcast finished. Success: {success_count}, Failed: {fail_count}.")
    if ADMIN_USER_ID:
        try: await context.bot.send_message(chat_id=ADMIN_USER_ID, text=f"📢 Rabatt-Broadcast abgeschlossen!\n\n✅ Erfolgreich: {success_count}\n❌ Fehlgeschlagen/Blockiert: {fail_count}")
        except: pass

async def finalize_discount_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; rabatt_data = context.user_data.get('rabatt_data', {})
    if not any(v > 0 for v in rabatt_data.values()): await query.answer("Es wurden keine Rabatte > 0€ festgelegt.", show_alert=True); return
    target_ids = []; target_type = context.user_data.get('rabatt_target_type')
    if target_type == 'all': stats = load_stats(); target_ids = list(stats["users"].keys())
    elif target_type == 'specific': target_id = context.user_data.get('rabatt_target_id');
        if target_id: target_ids.append(target_id)
    if not target_ids: await query.edit_message_text("Fehler: Kein Ziel gefunden.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]])); return
    rabatt_message_text = "🎉 Du hast exklusive Rabatte erhalten!\n\nWähle dein gewünschtes Paket direkt aus:"; keyboard_buttons = []
    for package_key, discount_value in rabatt_data.items():
        if discount_value > 0:
            media_type, amount_str = package_key.split('_'); amount = int(amount_str); base_price = PRICES[media_type][amount]
            new_price = max(1, base_price - discount_value); button_text = f"{media_type.capitalize()} {amount} - nur {new_price}€"
            callback = f"select_package:{media_type}:{amount}"; keyboard_buttons.append([InlineKeyboardButton(button_text, callback_data=callback)])
    if not keyboard_buttons: await query.answer("Keine Rabatte > 0€, es wird keine Nachricht gesendet.", show_alert=True); return
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)
    asyncio.create_task(broadcast_discount_task(context, target_ids, rabatt_data, rabatt_message_text, reply_markup))
    for key in list(context.user_data.keys()):
        if key.startswith('rabatt_'): del context.user_data[key]
    final_text = f"✅ Broadcast gestartet!\n\nDie {len(target_ids)} Nachrichten werden im Hintergrund versendet. Du erhältst nach Abschluss eine private Nachricht mit dem Ergebnis."
    await query.edit_message_text(final_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück zum Admin-Menü", callback_data="admin_main_menu")]]), parse_mode='Markdown')

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
        port = int(os.environ.get("PORT", 8443)); application.run_webhook(listen="0.0.0.0", port=port, url_path=BOT_TOKEN, webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}")
    else:
        logger.info("Starte Bot im Polling-Modus"); application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
