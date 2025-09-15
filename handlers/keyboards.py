from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def get_position_keyboard(positions, fiscal_doc):
    kb = []
    for i, pos in enumerate(positions):
        kb.append([InlineKeyboardButton(text=f"⬜️ {pos['name']} — {pos['sum']:.2f} RUB", callback_data=f"return_pos_{i}")])
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_return")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_confirm_keyboard(action):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=action)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_return")]
    ])