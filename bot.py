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

# --- Global Lock for Admin Log Creation ---
admin_log_locks = {}

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
    lock = admin_log_locks.setdefault(user_id_str, asyncio.Lock())
    
    async with lock:
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
                sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
                admin_logs.setdefault(user_id_str, {})["message_id"] = sent_message.message_id
                stats["admin_logs"] = admin_logs
                save_stats(stats)
        except error.BadRequest as e:
            if "message to edit not found" in str(e):
                logger.warning(f"Admin log for user {user.id} not found. Sending a new one.")
                sent_message = await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=final_text, parse_mode='Markdown')
                admin_logs.setdefault(user_id_str, {})["message_id"] = sent_message.message_id
                stats["admin_logs"] = admin_logs
                save_stats(stats)
        except error.TelegramError as e:
            if 'message is not modified' not in str(e):
                logger.warning(f"Temporary error updating admin log for user {user.id}: {e}")

async def send_permanent_admin_notification(context: ContextTypes.DEFAULT_TYPE, message: str):
    if NOTIFICATION_GROUP_ID:
        try:
            await context.bot.send_message(chat_id=NOTIFICATION_GROUP_ID, text=message, parse_mode='Markdown')
        except Exception as e: logger.error(f"Konnte permanente Benachrichtigung nicht senden: {e}")

async def update_pinned_summary(context: ContextTypes.DEFAULT_TYPE):
    pass # Implementation omitted for brevity

async def restore_stats_from_pinned_message(application: Application):
    pass # Implementation omitted for brevity

def get_media_files(schwester_code: str, media_type: str) -> list:
    pass # Implementation omitted for brevity

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
    pass # Implementation omitted for brevity

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass # Implementation omitted for brevity

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🔒 *Admin-Menü*\n\nWähle eine Option:"
    keyboard = [
        [InlineKeyboardButton("📊 Nutzer-Statistiken", callback_data="admin_stats_users"), InlineKeyboardButton("🖱️ Klick-Statistiken", callback_data="admin_stats_clicks")],
        [InlineKeyboardButton("🎟️ Gutscheine anzeigen", callback_data="admin_show_vouchers"), InlineKeyboardButton("💸 Rabatt senden", callback_data="admin_discount_start")],
        [InlineKeyboardButton("🔄 Statistiken zurücksetzen", callback_data="admin_reset_stats")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query: await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else: await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def show_vouchers_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass # Implementation omitted for brevity

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
        await update.message.reply_text("Vielen Dank! Dein Gutschein wurde übermittelt und wird nun geprüft. Ich melde mich bei dir.");
        await asyncio.sleep(2)
        await start(update, context)

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    if not ADMIN_USER_ID or user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔️ Du hast keine Berechtigung für diesen Befehl.")
        return
    await show_admin_menu(update, context)

async def add_voucher(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass # Implementation omitted for brevity

async def set_summary_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass # Implementation omitted for brevity

# --- Discount Menu Helper Functions ---
async def show_discount_package_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rabatt_data = context.user_data.get('rabatt_data', {})
    
    def get_button_text(pkg, name):
        discount = rabatt_data.get(pkg)
        return f"{name} (Rabatt: {discount}€)" if discount is not None else name

    keyboard = [
        [
            InlineKeyboardButton(get_button_text('bilder_10', "Bilder 10"), callback_data="admin_discount_select_package:bilder_10"),
            InlineKeyboardButton(get_button_text('videos_10', "Videos 10"), callback_data="admin_discount_select_package:videos_10"),
        ],
        [
            InlineKeyboardButton(get_button_text('bilder_25', "Bilder 25"), callback_data="admin_discount_select_package:bilder_25"),
            InlineKeyboardButton(get_button_text('videos_25', "Videos 25"), callback_data="admin_discount_select_package:videos_25"),
        ],
        [
            InlineKeyboardButton(get_button_text('bilder_35', "Bilder 35"), callback_data="admin_discount_select_package:bilder_35"),
            InlineKeyboardButton(get_button_text('videos_35', "Videos 35"), callback_data="admin_discount_select_package:videos_35"),
        ],
        [InlineKeyboardButton("✅ Aktion abschließen & senden", callback_data="admin_discount_finalize")],
        [InlineKeyboardButton("❌ Abbrechen", callback_data="admin_main_menu")],
    ]
    
    target_type = context.user_data.get('rabatt_target_type')
    target_id = context.user_data.get('rabatt_target_id')
    target_desc = "Alle Nutzer" if target_type == 'all' else f"Nutzer `{target_id}`"
    
    text = f"💸 *Rabatt-Manager - Schritt 2: Pakete*\n\nZiel: {target_desc}\n\nWähle ein Paket, um einen Rabatt festzulegen oder zu ändern."
    
    query = update.callback_query
    if query:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_discount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_input = update.message.text
    awaiting = context.user_data.get('rabatt_awaiting')
    
    if awaiting == 'user_id':
        if not text_input.isdigit():
            await update.message.reply_text("⚠️ Bitte gib eine gültige, numerische Nutzer-ID ein.")
            return
        stats = load_stats()
        if text_input not in stats["users"]:
            await update.message.reply_text(f"⚠️ Nutzer mit der ID `{text_input}` wurde nicht gefunden. Bitte überprüfe die ID.")
            return
        context.user_data['rabatt_target_id'] = text_input
        context.user_data['rabatt_awaiting'] = None
        await show_discount_package_menu(update, context)

    elif awaiting and awaiting.startswith('discount_amount_'):
        if not text_input.isdigit():
            await update.message.reply_text("⚠️ Bitte gib einen gültigen Rabattbetrag als ganze Zahl ein (z.B. `2`).")
            return
        
        package_key = awaiting.replace('discount_amount_', '')
        discount_amount = int(text_input)
        
        context.user_data.setdefault('rabatt_data', {})[package_key] = discount_amount
        context.user_data['rabatt_awaiting'] = None
        
        await update.message.reply_text(f"✅ Rabatt für `{package_key}` auf *{discount_amount}€* gesetzt.")
        await show_discount_package_menu(update, context)

async def finalize_discount_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    rabatt_data = context.user_data.get('rabatt_data', {})
    
    if not any(v > 0 for v in rabatt_data.values()):
        await query.answer("Es wurden keine Rabatte > 0€ festgelegt.", show_alert=True)
        return

    stats = load_stats()
    target_ids = []
    target_type = context.user_data.get('rabatt_target_type')
    
    if target_type == 'all':
        target_ids = list(stats["users"].keys())
    elif target_type == 'specific':
        target_id = context.user_data.get('rabatt_target_id')
        if target_id:
            target_ids.append(target_id)

    if not target_ids:
        await query.edit_message_text("Fehler: Kein Ziel für den Rabatt gefunden.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Zurück", callback_data="admin_main_menu")]]))
        return
        
    success_count = 0
    fail_count = 0
    
    rabatt_message_text = "🎉 Du hast exklusive Rabatte erhalten!\n\nWähle dein gewünschtes Paket direkt aus:"
    keyboard_buttons = []
    
    for package_key, discount_value in rabatt_data.items():
        if discount_value > 0:
            media_type, amount_str = package_key.split('_')
            amount = int(amount_str)
            base_price = PRICES[media_type][amount]
            new_price = max(1, base_price - discount_value)
            button_text = f"{media_type.capitalize()} {amount} - nur {new_price}€"
            callback = f"select_package:{media_type}:{amount}"
            keyboard_buttons.append([InlineKeyboardButton(button_text, callback_data=callback)])
            
    if not keyboard_buttons:
        await query.answer("Keine Rabatte > 0€, es wird keine Nachricht gesendet.", show_alert=True)
        return
        
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    for user_id in target_ids:
        if user_id in stats["users"]:
            stats["users"][user_id]["discounts"] = rabatt_data
            try:
                user_obj = await context.bot.get_chat(user_id)
                await send_or_update_admin_log(context, user_obj, event_text="Individueller Rabatt zugewiesen", new_discounts=rabatt_data)
                await context.bot.send_message(chat_id=user_id, text=rabatt_message_text, reply_markup=reply_markup)
                success_count += 1
            except error.Forbidden:
                fail_count += 1
            except Exception as e:
                logger.error(f"Error processing discount for {user_id}: {e}")
                fail_count += 1

    save_stats(stats)
    
    for key in list(context.user_data.keys()):
        if key.startswith('rabatt_'):
            del context.user_data[key]
            
    final_text = f"✅ Rabatt-Aktion abgeschlossen!\n\n- Erfolgreich gesendet an: *{success_count} Nutzer*\n- Fehlgeschlagen/Blockiert: *{fail_count} Nutzer*"
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
