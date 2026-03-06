from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sheets import (
    is_user_allowed, 
    save_receipt, 
    is_fiscal_doc_unique,
    async_sheets_call,
    sheets_service,  # Если используется
    SHEET_NAME,  # Если используется
    get_monthly_balance,  # Для других частей, если нужно
    compute_delta_balance,
    update_balance_cache_with_delta,
    batch_update_sheets
)
from utils import safe_float, parse_qr_from_photo, reset_keyboard
from handlers.notifications import send_notification
from config import SHEET_NAME  # Для spreadsheetId
from googleapiclient.errors import HttpError
import logging
from difflib import SequenceMatcher
from datetime import datetime
import urllib.parse

logger = logging.getLogger("AccountingBot")
expenses_router = Router()

class ConfirmDelivery(StatesGroup):
    SELECT_RECEIPT = State()
    SELECT_ITEMS = State()
    UPLOAD_FULL_QR = State()
    CONFIRM_ACTION = State()

def _norm_name(s: str) -> str:
    s = (s or "").lower().strip()
    return " ".join(s.split())

def _rub(val) -> float:
    if val is None:
        return 0.0
    try:
        v = float(val)
        return v / 100.0 if (v > 500 and float(v).is_integer()) else v
    except Exception:
        return 0.0

def _item_sum_from_qr(item: dict) -> float:
    if "sum" in item and item["sum"] is not None:
        return _rub(item["sum"])
    price = _rub(item.get("price", 0))
    qty = float(item.get("quantity", 1) or 1)
    return price * qty

@expenses_router.message(Command("expenses"))
async def list_pending_receipts(message: Message, state: FSMContext) -> None:
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        return

    try:
        res = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:P"
        )
        rows = res.get("values", [])[1:]

        groups = {}
        for i, row in enumerate(rows, start=2):
            status = (row[8] if len(row) > 8 else "").strip().lower()
            if status != "ожидает":
                continue
            fiscal_doc = (row[12] if len(row) > 12 else "").strip()
            item_name = (row[10] if len(row) > 10 else "").strip()
            if not fiscal_doc or not item_name:
                continue
            try:
                item_sum = safe_float(row[2] if len(row) > 2 else "0")
            except Exception:
                item_sum = 0.0
            groups.setdefault(fiscal_doc, []).append({
                "row_index": i,
                "name": item_name,
                "sum": item_sum,
                "date": row[1] if len(row) > 1 else "",
                "user": row[5] if len(row) > 5 else "",
                "store": row[6] if len(row) > 6 else ""
            })

        if not groups:
            await message.answer("Нет чеков со статусом «Ожидает».")
            return

        kb_rows = [
            [InlineKeyboardButton(text=f"{fd} — позиций: {len(items)}", callback_data=f"choose_fd:{fd}")]
            for fd, items in groups.items()
        ]

        await state.update_data(pending_groups=groups)
        await message.answer(
            "Выберите чек (fiscal_doc), в котором хотите подтвердить доставку:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows)
        )
        await state.set_state(ConfirmDelivery.SELECT_RECEIPT)
    except HttpError as e:
        await message.answer(f"Ошибка Google Sheets: {e.status_code} - {e.reason}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}")

@expenses_router.callback_query(ConfirmDelivery.SELECT_RECEIPT, F.data.startswith("choose_fd:"))
async def choose_receipt(callback: CallbackQuery, state: FSMContext) -> None:
    fiscal_doc = callback.data.split(":", 1)[-1]
    data = await state.get_data()
    groups = data.get("pending_groups", {})
    items = groups.get(fiscal_doc, [])
    if not items:
        await callback.message.edit_text("Позиции не найдены.")
        await callback.answer()
        return

    await state.update_data(items=items, selected=set(), fd=fiscal_doc)

    def build_kb(items: list, selected_idxs: set) -> InlineKeyboardMarkup:
        rows = []
        for idx, it in enumerate(items):
            checked = "☑️" if idx in selected_idxs else "⬜️"
            rows.append([
                InlineKeyboardButton(
                    text=f"{checked} {it['name']} — {it['sum']:.2f} RUB (стр. {it['row_index']})",
                    callback_data=f"sel:toggle:{idx}"
                )
            ])
        rows.append([InlineKeyboardButton(text="Далее ▶️", callback_data="sel:done")])
        rows.append([InlineKeyboardButton(text="Отмена", callback_data="sel:cancel")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    kb = build_kb(items, set())
    await callback.message.edit_text(
        f"Чек {fiscal_doc}. Выберите позиции для подтверждения:",
        reply_markup=kb
    )
    await state.set_state(ConfirmDelivery.SELECT_ITEMS)
    await callback.answer()

@expenses_router.callback_query(ConfirmDelivery.SELECT_ITEMS, F.data.startswith("sel:"))
async def select_items_toggle(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    items = data.get("items", [])
    selected = set(data.get("selected", set()))

    cmd = callback.data
    if cmd == "sel:cancel":
        await callback.message.edit_text("Отменено.")
        await state.clear()
        await callback.answer()
        return

    if cmd == "sel:done":
        if not selected:
            await callback.answer("Ничего не выбрано.", show_alert=True)
            return
        await state.update_data(selected=selected)
        await callback.message.edit_text("Отправьте фото QR-кода ЧЕКА ПОЛНОГО РАСЧЁТА (operationType=1).")
        await state.set_state(ConfirmDelivery.UPLOAD_FULL_QR)
        await callback.answer()
        return

    try:
        _, _, sidx = cmd.split(":", 2)
        idx = int(sidx)
        if idx < 0 or idx >= len(items):
            raise ValueError("bad index")
        if idx in selected:
            selected.remove(idx)
        else:
            selected.add(idx)
        await state.update_data(selected=selected)
    except Exception:
        await callback.answer("Некорректный индекс.", show_alert=True)
        return

    def build_kb(items: list, selected_idxs: set) -> InlineKeyboardMarkup:
        rows = []
        for i, it in enumerate(items):
            checked = "☑️" if i in selected_idxs else "⬜️"
            rows.append([
                InlineKeyboardButton(
                    text=f"{checked} {it['name']} — {it['sum']:.2f} RUB (стр. {it['row_index']})",
                    callback_data=f"sel:toggle:{i}"
                )
            ])
        rows.append([InlineKeyboardButton(text="Далее ▶️", callback_data="sel:done")])
        rows.append([InlineKeyboardButton(text="Отмена", callback_data="sel:cancel")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    kb = build_kb(items, selected)
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()

@expenses_router.message(ConfirmDelivery.UPLOAD_FULL_QR)
async def upload_full_qr(message: Message, state: FSMContext, bot: Bot) -> None:
    loading = await message.answer("⌛ Проверяю чек...")

    if not message.photo:
        await loading.edit_text("Пожалуйста, пришлите фото QR-кода чека полного расчёта.")
        return

    parsed = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed:
        await loading.edit_text("Не удалось распознать QR. Проверьте качество фото.")
        return

    if parsed.get("operation_type") != 1:
        await loading.edit_text("Это не чек полного расчёта (operationType должен быть 1).")
        return

    data = await state.get_data()
    items = data.get("items", [])
    selected = sorted(list(data.get("selected", set())))
    sel_items = [items[i] for i in selected]

    qr_items = parsed.get("items", [])
    missing = []
    for it in sel_items:
        need_name = _norm_name(it["name"])
        matched = any(
            SequenceMatcher(None, need_name, _norm_name(q.get("name", ""))).ratio() > 0.8
            for q in qr_items
        )
        if not matched:
            missing.append(it["name"])

    if missing:
        await loading.edit_text(
            "❌ Проверка провалена. Не найдены в QR:\n• " + "\n• ".join(missing),
            reply_markup=reset_keyboard()
        )
        return

    await state.update_data(qr_parsed=parsed)
    total = sum(it["sum"] for it in sel_items)
    details = [
        f"Чек (fiscal_doc): {parsed.get('fiscal_doc')}",
        f"Позиции ({len(sel_items)} шт., итого {total:.2f} RUB):"
    ] + [f"• {it['name']} — {it['sum']:.2f} RUB (строка {it['row_index']})" for it in sel_items]

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить запись", callback_data="confirm:delivery_many")],
        [InlineKeyboardButton(text="Отмена", callback_data="confirm:cancel")]
    ])
    await loading.edit_text("✅ Проверка пройдена.\n" + "\n".join(details), reply_markup=kb)
    await state.set_state(ConfirmDelivery.CONFIRM_ACTION)

@expenses_router.callback_query(ConfirmDelivery.CONFIRM_ACTION, F.data.in_(["confirm:delivery_many", "confirm:cancel"]))
async def confirm_delivery_many(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    if callback.data == "confirm:cancel":
        await callback.message.edit_text("🚫 Доставка отменена.")
        await state.clear()
        return

    data = await state.get_data()
    items = data.get("items", [])
    selected = sorted(list(data.get("selected", set())))
    sel_items = [items[i] for i in selected]
    parsed = data.get("qr_parsed", {})
    new_fd = parsed.get("fiscal_doc", "")
    
    # ✅ НОВОЕ: Извлекаем ссылку на PDF полного чека и готовим кнопку
    pdf_url = parsed.get("pdf_url", "")
    qr_str = parsed.get("qr_string", "")
    
    if pdf_url:
        qr_cell_value = f'=HYPERLINK("{pdf_url}"; "📄 Открыть PDF")'
    else:
        safe_qr = urllib.parse.quote(qr_str)
        fallback_link = f"https://proverkacheka.com/qrcode/generate?text={safe_qr}"
        qr_cell_value = f'=HYPERLINK("{fallback_link}"; "⏳ PDF готовится (QR)")'

    updates = []
    updated_items = []
    ok, fail, errors = 0, 0, []

    for it in sel_items:
        row_index = it["row_index"]
        try:
            res = await async_sheets_call(
                sheets_service.spreadsheets().values().get,
                spreadsheetId=SHEET_NAME, range=f"Чеки!A{row_index}:Q{row_index}"
            )
            row = res.get("values", [[]])[0] if res.get("values") else []
            while len(row) < 17:
                row.append("")

            row[8] = "Доставлено"
            row[11] = "Полный"
            row[12] = str(new_fd)
            
            # ✅ ИЗМЕНЕНО: Записываем готовую формулу в столбец N (индекс 13)
            row[13] = qr_cell_value 

            updates.append({"range": f"Чеки!A{row_index}:Q{row_index}", "values": [row]})

            updated_items.append({
                "name": it.get("name", "—"),
                "sum": safe_float(it.get("sum", 0)),
                "quantity": int(it.get("quantity", 1) or 1),
                "link": (row[15] or "").strip() if len(row) > 15 else "",
                "comment": (row[16] or "").strip() if len(row) > 16 else "",
                "delivery_date": (row[7] or "").strip() if len(row) > 7 else ""
            })
            ok += 1
        except Exception as e:
            fail += 1
            errors.append(f"Строка {row_index}: {str(e)}")

    if updates:
        await batch_update_sheets(updates)

    balance_data = await get_monthly_balance(force_refresh=True)
    balance = balance_data.get("balance", 0.0) if balance_data else 0.0

    user_name = await is_user_allowed(callback.from_user.id) or callback.from_user.full_name
    operation_date = datetime.now().strftime("%d.%m.%Y")

    if fail == 0:
        await send_notification(
            bot=callback.bot,
            action="📦 Подтверждена доставка",
            items=updated_items,
            user_name=user_name,
            fiscal_doc=new_fd,
            operation_date=operation_date,
            balance=balance,
            is_group=True,
            pdf_url=pdf_url  # ✅ НОВОЕ: Передаем ссылку на чек полного расчета
        )
        await send_notification(
            bot=callback.bot,
            action="📦 Доставка подтверждена",
            items=updated_items,
            user_name=user_name,
            fiscal_doc=new_fd,
            operation_date=operation_date,
            balance=balance,
            is_group=False,
            chat_id=callback.message.chat.id,
            pdf_url=pdf_url  # ✅ НОВОЕ: Передаем ссылку на чек полного расчета
        )
        await callback.message.edit_text(f"✅ Доставка подтверждена ({ok} позиций). Баланс: {balance:.2f} ₽")
    else:
        details = "\n".join(errors[:5])
        await callback.message.edit_text(f"⚠️ Частично: {ok} ок, {fail} ошибок.\n{details}\nБаланс: {balance:.2f} ₽")

    await state.clear()