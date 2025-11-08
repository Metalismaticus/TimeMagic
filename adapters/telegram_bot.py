import asyncio
import logging
import os
import sys
import re
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.client.default import DefaultBotProperties

# === Путь к корню проекта и загрузка конфига ===

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))
load_dotenv(ROOT_DIR / "config.env")

from core import service, storage  # noqa: E402
from core.storage import get_conn  # noqa: E402

# === Конфиг ===

BOT_TOKEN = os.getenv("BOT_TOKEN")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

_daily_digest_raw = os.getenv("DAILY_DIGEST_TIME", "").strip()
if _daily_digest_raw:
    try:
        _h, _m = map(int, _daily_digest_raw.split(":"))
        DAILY_DIGEST_ENABLED = True
        DAILY_DIGEST_HOUR = _h
        DAILY_DIGEST_MINUTE = _m
    except ValueError:
        DAILY_DIGEST_ENABLED = False
        DAILY_DIGEST_HOUR = DAILY_DIGEST_MINUTE = None
else:
    DAILY_DIGEST_ENABLED = False
    DAILY_DIGEST_HOUR = DAILY_DIGEST_MINUTE = None

_event_rem_raw = (
    os.getenv("EVENT_REMIND_BEFORE_MINUTES", "").strip()
    or os.getenv("REMIND_BEFORE_MINUTES", "").strip()
)
EVENT_REMIND_BEFORE_MINUTES = int(_event_rem_raw) if _event_rem_raw else None

_task_rem_raw = os.getenv("TASK_REMIND_BEFORE_MINUTES", "").strip()
TASK_REMIND_BEFORE_MINUTES = int(_task_rem_raw) if _task_rem_raw else None

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан (проверь config.env в корне проекта)")

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
)
dp = Dispatcher()

# === Регекс и константы ===

TIME_REGEX = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")  # чистое время
TIME_ANY_REGEX = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
DATE_REGEX = re.compile(
    r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b"
)

WEEKDAY_IN_TEXT_PATTERN = re.compile(
    r"\bво?\s+("
    r"понедельник|вторник|среду|среда|четверг|пятницу|пятница|"
    r"субботу|суббота|воскресенье|"
    r"пн|вт|ср|чт|пт|сб|вс"
    r")\b",
    re.IGNORECASE,
)

TIME_WORDS_EVENT = (
    "сегодня",
    "завтра",
    "послезавтра",
    "понедельник", "вторник", "среда", "среду",
    "четверг", "пятница", "пятницу",
    "суббота", "субботу",
    "воскресенье",
    "пн", "вт", "ср", "чт", "пт", "сб", "вс",
)

# pending конфликты: user_id -> {day:str, title:str, duration:int}
PENDING_CONFLICTS: dict[str, dict] = {}

# === Хелперы ===


def has_explicit_date_or_time(text: str) -> bool:
    t = text.lower()
    if TIME_ANY_REGEX.search(t):
        return True
    if DATE_REGEX.search(t):
        return True
    return False


def has_event_time_phrase(text: str) -> bool:
    t = text.lower()
    if has_explicit_date_or_time(t):
        return True
    if any(w in t for w in TIME_WORDS_EVENT):
        return True
    return False


def format_date_ru(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y")


def format_datetime_ru(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y %H:%M")


def is_end_of_day(dt: datetime) -> bool:
    return dt.hour == 23 and dt.minute == 59 and dt.second == 0


def is_default_morning(dt: datetime) -> bool:
    return dt.hour == 10 and dt.minute == 0 and dt.second == 0


def strip_weekday_phrase(text: str, default_label: str = "Задача") -> str:
    clean = WEEKDAY_IN_TEXT_PATTERN.sub("", text).strip(" ,.-")
    return clean or default_label


def clean_for_reschedule(text: str) -> str:
    """
    Очищаем заголовок для повторного планирования:
    убираем дни недели, даты, время и висящее 'в'.
    """
    b = (text or "").strip()
    b = WEEKDAY_IN_TEXT_PATTERN.sub("", b)
    b = DATE_REGEX.sub("", b)
    b = TIME_ANY_REGEX.sub("", b)
    b = re.sub(r"\bв$", "", b)
    b = b.strip(" ,.-")
    return b or "Без названия"


def format_timed_line(base: str, start_dt: datetime | None, end_dt: datetime | None) -> str:
    """
    Слот:
      📆 16:00-18:00 текст
    без дат и дней недели внутри текста.
    """
    b = (base or "").strip() or "Без названия"

    b = WEEKDAY_IN_TEXT_PATTERN.sub("", b).strip(" ,.-")
    b = DATE_REGEX.sub("", b).strip(" ,.-")
    b = TIME_ANY_REGEX.sub("", b).strip(" ,.-")
    b = re.sub(r"\bв$", "", b).strip(" ,.-")
    if not b:
        b = "Без названия"

    if not start_dt:
        return f"📆 {b}"

    if end_dt and end_dt > start_dt:
        label = f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
    else:
        label = start_dt.strftime("%H:%M")

    return f"📆 <b>{label}</b> {b}"


def format_event_reminder(title: str, start_dt: datetime) -> str:
    base = (title or "").strip() or "Событие"
    if not has_event_time_phrase(base):
        base = f"{base} в {start_dt.strftime('%H:%M')}"
    return f"Напоминание: {base}"


def format_task_reminder(title: str, due_dt: datetime) -> str:
    base = (title or "").strip() or "Задача"
    if is_end_of_day(due_dt):
        due_str = format_date_ru(due_dt)
    else:
        due_str = format_datetime_ru(due_dt)
    return f"Задача к сроку: {base} (дедлайн {due_str})"


# === Разделение по дню ===


def split_items_for_day(events, tasks, day_start: datetime, day_end: datetime):
    """
    events: (title, start_at, end_at)
    tasks: (title, due_at)
    """
    timed: list[str] = []
    day_tasks: list[str] = []

    # События
    for title, start_at, end_at in events:
        if not start_at:
            continue
        try:
            sdt = datetime.fromisoformat(start_at)
            edt = datetime.fromisoformat(end_at) if end_at else None
        except Exception:
            continue
        if not (day_start <= sdt < day_end):
            continue

        base = (title or "").strip() or "Без названия"

        if is_default_morning(sdt) and not has_explicit_date_or_time(base):
            day_tasks.append(strip_weekday_phrase(base, default_label="Запись"))
        else:
            timed.append(format_timed_line(base, sdt, edt))

    # Задачи
    for title, due_at in tasks:
        base = (title or "").strip() or "Задача"
        if not due_at:
            continue
        try:
            dt = datetime.fromisoformat(due_at)
        except Exception:
            continue
        if not (day_start <= dt < day_end):
            continue

        if not is_end_of_day(dt):
            timed.append(format_timed_line(base, dt, None))
            continue

        if TIME_ANY_REGEX.search(base):
            timed.append(format_timed_line(base, None, None))
        else:
            day_tasks.append(strip_weekday_phrase(base, default_label="Задача"))

    def extract_time_prefix(s: str):
        m = TIME_ANY_REGEX.search(s)
        if m:
            try:
                h = int(m.group(1))
                mn = int(m.group(2))
                return h * 60 + mn
            except Exception:
                return 9999
        return 9999

    timed_sorted = sorted(timed, key=extract_time_prefix)
    return timed_sorted, day_tasks


# === Сводки ===


def build_day_plan_text(user_id: str, day: datetime.date) -> str:
    day_start = datetime(day.year, day.month, day.day, 0, 0, 0)
    day_end = day_start + timedelta(days=1)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT title, start_at, end_at FROM items
        WHERE user_id = ?
          AND type = 'event'
          AND start_at IS NOT NULL
          AND start_at >= ?
          AND start_at < ?
        ORDER BY start_at
        """,
        (user_id, day_start.isoformat(), day_end.isoformat()),
    )
    events = cur.fetchall()

    cur.execute(
        """
        SELECT title, due_at FROM items
        WHERE user_id = ?
          AND type = 'task'
          AND status = 'active'
          AND due_at IS NOT NULL
          AND due_at >= ?
          AND due_at < ?
        ORDER BY due_at
        """,
        (user_id, day_start.isoformat(), day_end.isoformat()),
    )
    tasks = cur.fetchall()

    conn.close()

    timed, day_tasks = split_items_for_day(events, tasks, day_start, day_end)

    if not timed and not day_tasks:
        return f"План на {format_date_ru(day_start)}: пусто."

    lines = [f"План на {format_date_ru(day_start)}:"]
    for item in timed:
        lines.append(f"  {item}")
    if day_tasks:
        lines.append("  🧾 Задачи на день:")
        for item in day_tasks:
            lines.append(f"    - {item}")
    return "\n".join(lines)


def build_today_summary_text(user_id: str) -> str | None:
    now = datetime.now()
    txt = build_day_plan_text(user_id, now.date())
    if "пусто." in txt:
        return None
    lines = txt.splitlines()
    if not lines:
        return None
    lines[0] = "Сегодня:"
    return "\n".join(lines)


def get_period_items(user_id: str, days: int):
    now = datetime.now()
    start = datetime(now.year, now.month, now.day, 0, 0, 0)
    end = start + timedelta(days=days)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT title, start_at, end_at FROM items
        WHERE user_id = ?
          AND type = 'event'
          AND start_at IS NOT NULL
          AND start_at >= ?
          AND start_at < ?
        ORDER BY start_at
        """,
        (user_id, start.isoformat(), end.isoformat()),
    )
    events = cur.fetchall()

    cur.execute(
        """
        SELECT title, due_at FROM items
        WHERE user_id = ?
          AND type = 'task'
          AND status = 'active'
          AND due_at IS NOT NULL
          AND due_at >= ?
          AND due_at < ?
        ORDER BY due_at
        """,
        (user_id, start.isoformat(), end.isoformat()),
    )
    tasks = cur.fetchall()

    conn.close()
    return events, tasks, start


def build_period_schedule_text(user_id: str, days: int, header: str) -> str:
    events, tasks, start = get_period_items(user_id, days)

    if not events and not tasks:
        return f"{header}\n\nНет запланированных дел."

    schedule: dict[str, dict[str, list[str]]] = {}

    for offset in range(days):
        day_start = start + timedelta(days=offset)
        day_end = day_start + timedelta(days=1)
        day_key = format_date_ru(day_start)

        day_events = [
            (t, s, e)
            for (t, s, e) in events
            if s and day_start <= datetime.fromisoformat(s) < day_end
        ]
        day_tasks = [
            (t, d)
            for (t, d) in tasks
            if d and day_start <= datetime.fromisoformat(d) < day_end
        ]

        timed, day_only = split_items_for_day(day_events, day_tasks, day_start, day_end)
        if timed or day_only:
            schedule[day_key] = {"timed": timed, "day_tasks": day_only}

    if not schedule:
        return f"{header}\n\nНет запланированных дел."

    lines = [header]
    for day in sorted(schedule.keys(), key=lambda d: datetime.strptime(d, "%d.%m.%Y")):
        block = schedule[day]
        lines.append(f"\n<b>{day}</b>:")
        for item in block["timed"]:
            lines.append(f"  {item}")
        if block["day_tasks"]:
            lines.append("  🧾 Задачи на день:")
            for item in block["day_tasks"]:
                lines.append(f"    - {item}")

    return "\n".join(lines)


# === Конфликт: текст + кнопки ===


def build_conflict_message(user_id: str, payload: str):
    """
    payload:
      __CONFLICT__|day|conf_title|conf_start|conf_end|new_title|duration_min
    """
    parts = payload.split("|")
    if len(parts) < 7:
        text = "Не могу добавить: время занято."
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="conf_help_cancel")]
            ]
        )
        return text, kb, None

    _, day_iso, conf_title, conf_start, conf_end, new_title, duration_str = parts

    # duration_min может быть 0 (точка), не поднимаем до 30.
    try:
        duration_min = max(0, int(duration_str))
    except Exception:
        duration_min = 0

    try:
        day = datetime.fromisoformat(day_iso).date()
    except Exception:
        day = datetime.now().date()

    conflict_line = "Не могу добавить: в это время уже есть другое событие."
    try:
        if conf_title and conf_start:
            cs = datetime.fromisoformat(conf_start)
            ce = datetime.fromisoformat(conf_end) if conf_end else cs
            if ce > cs:
                conflict_line = (
                    f"Не могу добавить: в это время уже есть '{conf_title}' "
                    f"({cs.strftime('%d.%m %H:%M')}-{ce.strftime('%H:%M')})."
                )
            else:
                conflict_line = (
                    f"Не могу добавить: в это время уже есть '{conf_title}' "
                    f"({cs.strftime('%d.%m %H:%M')})."
                )
    except Exception:
        pass

    plan_text = build_day_plan_text(user_id, day)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⏰ Указать другое время", callback_data="conf_help_time")],
            [InlineKeyboardButton(text="📅 Выбрать другой день", callback_data="conf_help_day")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="conf_help_cancel")],
        ]
    )

    text = (
        f"{conflict_line}\n"
        f"{plan_text}\n\n"
        f"Выбери действие кнопкой ниже.\n"
        f"При выборе времени введи только новое время.\n"
        f"При выборе другой даты введи только дату или дату и время."
    )

    pending = {
        "day": day_iso,
        "title": clean_for_reschedule(new_title),
        "duration": duration_min,
    }

    return text, kb, pending


# === Команды ===


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Секретарь.\n"
        "- Пиши текст — разложу на событие, задачу или заметку.\n"
        "- /today — сводка на сегодня.\n"
        "- /week — расписание на 7 дней.\n"
        "- /month — расписание на 30 дней.\n"
        "- /tasks — задачи без жёсткого времени и с крайним сроком.\n"
        "- /notes — заметки."
    )


@dp.message(Command("today"))
async def cmd_today(message: Message):
    user_id = str(message.from_user.id)
    text = build_today_summary_text(user_id)
    if text is None:
        await message.answer("На сегодня ничего не запланировано.")
    else:
        await message.answer(text)


@dp.message(Command("week"))
async def cmd_week(message: Message):
    user_id = str(message.from_user.id)
    text = build_period_schedule_text(user_id, days=7, header="Расписание на 7 дней:")
    await message.answer(text)


@dp.message(Command("month"))
async def cmd_month(message: Message):
    user_id = str(message.from_user.id)
    text = build_period_schedule_text(user_id, days=30, header="Расписание на 30 дней:")
    await message.answer(text)


@dp.message(Command("tasks"))
async def cmd_tasks(message: Message):
    user_id = str(message.from_user.id)
    rows = storage.get_tasks(user_id)
    if not rows:
        await message.answer("Активных задач нет.")
        return

    filtered: list[tuple[str, datetime | None]] = []

    for _item_id, title, due_at in rows:
        base = (title or "").strip() or "Задача"
        lower = base.lower()

        if not due_at:
            filtered.append((base, None))
            continue

        try:
            dt = datetime.fromisoformat(due_at)
        except Exception:
            if "до " in lower and not TIME_ANY_REGEX.search(base):
                filtered.append((base, None))
            continue

        if not is_end_of_day(dt):
            continue

        if TIME_ANY_REGEX.search(base):
            continue

        if WEEKDAY_IN_TEXT_PATTERN.search(base) and "до " not in lower:
            continue

        if "до " in lower:
            filtered.append((base, dt))

    if not filtered:
        await message.answer("Нет задач без фиксированного времени. Всё разнесено по дням.")
        return

    lines = ["Активные задачи (без точного времени):"]
    for idx, (base, due) in enumerate(filtered, start=1):
        if due is None:
            lines.append(f"{idx}. {base}")
        else:
            due_str = format_date_ru(due)
            lines.append(f"{idx}. до <b>{due_str}</b> {base}")

    await message.answer("\n".join(lines))


@dp.message(Command("notes"))
async def cmd_notes(message: Message):
    user_id = str(message.from_user.id)
    rows = storage.get_notes(user_id)
    if not rows:
        await message.answer("Заметок нет.")
        return

    lines = ["Заметки:"]
    for idx, (_item_id, title) in enumerate(rows, start=1):
        base = (title or "").strip() or "Заметка"
        lines.append(f"{idx}. {base}")
    await message.answer("\n".join(lines))


# === Callback-и по конфликту ===


@dp.callback_query(F.data == "conf_help_time")
async def cb_conf_help_time(query: CallbackQuery):
    user_id = str(query.from_user.id)
    if user_id not in PENDING_CONFLICTS:
        await query.answer()
        await query.message.answer("Нет ожидающей встречи. Создай новую фразой.")
        return
    await query.answer()
    await query.message.answer(
        "Введи только новое время в формате HH:MM.\n"
        "Описание и длительность возьму из последней конфликтной встречи."
    )


@dp.callback_query(F.data == "conf_help_day")
async def cb_conf_help_day(query: CallbackQuery):
    user_id = str(query.from_user.id)
    if user_id not in PENDING_CONFLICTS:
        await query.answer()
        await query.message.answer("Нет ожидающей встречи. Создай новую фразой.")
        return
    await query.answer()
    await query.message.answer(
        "Введи новую дату или дату и время, например:\n"
        "13.11\n"
        "или\n"
        "13.11 15:00\n"
        "Описание и длительность возьму из последней конфликтной встречи."
    )


@dp.callback_query(F.data == "conf_help_cancel")
async def cb_conf_help_cancel(query: CallbackQuery):
    user_id = str(query.from_user.id)
    PENDING_CONFLICTS.pop(user_id, None)
    await query.answer()
    await query.message.answer("Добавление встречи отменено.")


# === Обработка текста ===


@dp.message(F.text)
async def handle_text(message: Message):
    user_id = str(message.from_user.id)
    text = message.text.strip()

    # режим разрешения конфликта
    if user_id in PENDING_CONFLICTS:
        pending = PENDING_CONFLICTS[user_id]
        title = pending["title"]
        duration_min = pending["duration"]
        day_iso = pending["day"]

        # 1) Только время HH:MM -> тот же день
        if TIME_REGEX.fullmatch(text):
            h, m = map(int, text.split(":"))
            try:
                day = datetime.fromisoformat(day_iso).date()
            except Exception:
                day = datetime.now().date()
            start_dt = datetime(day.year, day.month, day.day, h, m)

            if duration_min and duration_min > 0:
                synth = f"{title} {start_dt.strftime('%d.%m.%Y %H:%M')} на {duration_min} минут"
            else:
                synth = f"{title} {start_dt.strftime('%d.%m.%Y %H:%M')}"

            PENDING_CONFLICTS.pop(user_id, None)
            reply, _item = service.handle_input(user_id, synth)
            await message.answer(reply)
            return

        # 2) Дата + время одновременно
        if DATE_REGEX.search(text) and TIME_ANY_REGEX.search(text):
            if duration_min and duration_min > 0:
                synth = f"{title} {text} на {duration_min} минут"
            else:
                synth = f"{title} {text}"
            PENDING_CONFLICTS.pop(user_id, None)
            reply, _item = service.handle_input(user_id, synth)
            await message.answer(reply)
            return

        # 3) Только дата -> обновляем день, просим время
        if DATE_REGEX.fullmatch(text):
            try:
                m = DATE_REGEX.fullmatch(text)
                d = int(m.group(1))
                mo = int(m.group(2))
                if m.group(3):
                    y_raw = int(m.group(3))
                    y = 2000 + y_raw if y_raw < 100 else y_raw
                else:
                    now = datetime.now()
                    y = now.year
                new_day = datetime(y, mo, d).date()
                pending["day"] = new_day.isoformat()
                PENDING_CONFLICTS[user_id] = pending
            except Exception:
                PENDING_CONFLICTS.pop(user_id, None)
                reply, _item = service.handle_input(user_id, text)
                await message.answer(reply)
                return

            await message.answer(
                "Дата принята. Теперь введи время в формате HH:MM для этой встречи."
            )
            return

        # 4) Любой другой ввод -> выходим из режима и обрабатываем как новый запрос
        PENDING_CONFLICTS.pop(user_id, None)
        reply, _item = service.handle_input(user_id, text)
        await message.answer(reply)
        return

    # обычный режим
    reply, _item = service.handle_input(user_id, text)

    if reply.startswith("__CONFLICT__|"):
        text_out, kb, pending = build_conflict_message(user_id, reply)
        if pending:
            PENDING_CONFLICTS[user_id] = pending
        await message.answer(text_out, reply_markup=kb)
    else:
        await message.answer(reply)


# === Напоминания + дайджест ===


async def reminder_loop():
    sent_digest = {}

    while True:
        try:
            now = datetime.now()

            # утренний дайджест
            if DAILY_DIGEST_ENABLED:
                digest_dt = now.replace(
                    hour=DAILY_DIGEST_HOUR,
                    minute=DAILY_DIGEST_MINUTE,
                    second=0,
                    microsecond=0,
                )
                if now >= digest_dt:
                    for user_id in storage.get_all_user_ids():
                        key = (user_id, now.date())
                        if key in sent_digest:
                            continue
                        text = build_today_summary_text(user_id)
                        if text:
                            try:
                                await bot.send_message(int(user_id), text)
                            except Exception as e:
                                logger.error(
                                    "Ошибка отправки утренней сводки пользователю %s: %s",
                                    user_id,
                                    e,
                                )
                        sent_digest[key] = True

            conn = get_conn()
            cur = conn.cursor()

            # события
            if EVENT_REMIND_BEFORE_MINUTES is not None:
                cur.execute(
                    """
                    SELECT id, user_id, title, start_at
                    FROM items
                    WHERE type = 'event'
                      AND status = 'active'
                      AND start_at IS NOT NULL
                      AND event_notified = 0
                    """
                )
                events = cur.fetchall()
                for item_id, uid, title, start_at in events:
                    try:
                        start_dt = datetime.fromisoformat(start_at)
                    except Exception:
                        continue
                    diff_min = (start_dt - now).total_seconds() / 60
                    if 0 <= diff_min <= EVENT_REMIND_BEFORE_MINUTES:
                        text = format_event_reminder(title, start_dt)
                        try:
                            await bot.send_message(int(uid), text)
                        except Exception as e:
                            logger.error("Ошибка отправки напоминания (событие): %s", e)
                        cur.execute(
                            "UPDATE items SET event_notified = 1 WHERE id = ?",
                            (item_id,),
                        )

            # задачи
            if TASK_REMIND_BEFORE_MINUTES is not None:
                cur.execute(
                    """
                    SELECT id, user_id, title, due_at
                    FROM items
                    WHERE type = 'task'
                      AND status = 'active'
                      AND due_at IS NOT NULL
                      AND due_notified = 0
                    """
                )
                tasks = cur.fetchall()
                for item_id, uid, title, due_at in tasks:
                    try:
                        due_dt = datetime.fromisoformat(due_at)
                    except Exception:
                        continue
                    diff_min = (due_dt - now).total_seconds() / 60
                    if 0 <= diff_min <= TASK_REMIND_BEFORE_MINUTES:
                        text = format_task_reminder(title, due_dt)
                        try:
                            await bot.send_message(int(uid), text)
                        except Exception as e:
                            logger.error("Ошибка отправки напоминания (задача): %s", e)
                        cur.execute(
                            "UPDATE items SET due_notified = 1 WHERE id = ?",
                            (item_id,),
                        )

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error("Ошибка в reminder_loop: %s", e)

        await asyncio.sleep(60)


# === Точка входа ===


async def main():
    service.init()
    asyncio.create_task(reminder_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
