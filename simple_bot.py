# simple_bot.py - Minimal Telegram bot with one button (python-telegram-bot v13)
# NOTE: This file contains a placeholder token. Replace it with your real token if you want to run it.
# To run: pip install -r requirements.txt && python simple_bot.py
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext

# === HARD-CODED TOKEN (placeholder) ===
BOT_TOKEN = "8307376353:AAEwJCcVdwUuEUTqGRdmYjU419nJ2GJPX_E"

def start(update: Update, context: CallbackContext):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Press me", callback_data="press")]])
    update.message.reply_text("Hello! Press the button:", reply_markup=kb)

def button(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    # You can edit message or send a new one
    query.edit_message_text("Button pressed ✅")

def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(button, pattern="press"))
    updater.start_polling()
    print("Bot started. Press Ctrl+C to stop.")
    updater.idle()

if __name__ == "__main__":
    main()
