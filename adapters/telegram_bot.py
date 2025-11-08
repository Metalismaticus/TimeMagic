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
from aiogram.types import Message
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

# aiogram 3.7+: parse_mode через DefaultBotProperties
bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
)
dp = Dispatcher()

# === Регекс и константы ===

TIME_REGEX = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
DATE_REGEX = re.compile(r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b")

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


# === Хелперы ===


def has_explicit_date_or_time(text: str) -> bool:
    t = text.lower()
    if TIME_REGEX.search(t):
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
    # Авто-время для "просто указан день"
    return dt.hour == 10 and dt.minute == 0 and dt.second == 0


def strip_weekday_phrase(text: str, default_label: str = "Задача") -> str:
    clean = WEEKDAY_IN_TEXT_PATTERN.sub("", text).strip(" ,.-")
    return clean or default_label


def replace_weekday_with_date(text: str, dt: datetime) -> str:
    return WEEKDAY_IN_TEXT_PATTERN.sub(format_date_ru(dt), text, count=1)


def format_timed_line(base: str, dt: datetime | None) -> str:
    """
    Слот со временем:
    - убираем 'в/во понедельник/...' из текста,
    - если есть HH:MM в тексте, используем его,
    - иначе используем dt,
    - формат: '📆 <b>HH:MM</b> текст'.
    """
    b = (base or "").strip() or "Без названия"
    b = strip_weekday_phrase(b, default_label="Без названия")

    # Явное время в тексте
    m = TIME_REGEX.search(b)
    if m:
        time_str = m.group(0)
        before = b[:m.start()]
        after = b[m.end():]
        text_clean = (before + after).strip(" ,.-")
        # убрать висящий предлог "в" в конце
        text_clean = re.sub(r"\bв$", "", text_clean).strip(" ,.-")
        if not text_clean:
            text_clean = "Без названия"
        return f"📆 <b>{time_str}</b> {text_clean}"

    # Время только из dt
    if dt is not None:
        time_str = dt.strftime("%H:%M")
        return f"📆 <b>{time_str}</b> {b}"

    return f"📆 {b}"


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
    На один день:
    - timed: всё с реальным временем,
    - day_tasks: всё "на день" без времени.
    """
    timed: list[str] = []
    day_tasks: list[str] = []

    # События
    for title, start_at in events:
        if not start_at:
            continue
        try:
            dt = datetime.fromisoformat(start_at)
        except Exception:
            continue
        if not (day_start <= dt < day_end):
            continue

        base = (title or "").strip() or "Без названия"

        # Авто 10:00 без явного времени → задача на день
        if is_default_morning(dt) and not has_explicit_date_or_time(base):
            day_tasks.append(strip_weekday_phrase(base, default_label="Запись"))
        else:
            timed.append(format_timed_line(base, dt))

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

        # Не конец дня → конкретное время
        if not is_end_of_day(dt):
            timed.append(format_timed_line(base, dt))
            continue

        # Конец дня:
        # если в тексте есть HH:MM → слот
        if TIME_REGEX.search(base):
            timed.append(format_timed_line(base, None))
        else:
            # Настоящая задача на день (из "в среду сделать ...")
            day_tasks.append(strip_weekday_phrase(base, default_label="Задача"))

    # сортировка timed по времени
    def extract_time_prefix(s: str):
        m = TIME_REGEX.search(s)
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


# === Сводка на сегодня ===


def build_today_summary_text(user_id: str) -> str | None:
    now = datetime.now()
    day_start = datetime(now.year, now.month, now.day, 0, 0, 0)
    day_end = day_start + timedelta(days=1)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT title, start_at FROM items
        WHERE user_id = ?
          AND type = 'event'
          AND start_at IS NOT NULL
          AND start_at >= ?
          AND start_at < ?
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
        """,
        (user_id, day_start.isoformat(), day_end.isoformat()),
    )
    tasks = cur.fetchall()

    conn.close()

    timed, day_tasks = split_items_for_day(events, tasks, day_start, day_end)

    if not timed and not day_tasks:
        return None

    lines = ["Сегодня:"]
    for item in timed:
        lines.append(f"- {item}")

    if day_tasks:
        lines.append("\n🧾 Задачи на день:")
        for item in day_tasks:
            lines.append(f"- {item}")

    return "\n".join(lines)


# === Расписание на период (week/month) ===


def get_period_items(user_id: str, days: int):
    now = datetime.now()
    start = datetime(now.year, now.month, now.day, 0, 0, 0)
    end = start + timedelta(days=days)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT title, start_at FROM items
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
            (t, s)
            for (t, s) in events
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
    """
    Показывает:
    - задачи без due_at;
    - задачи с due_at в 23:59, без HH:MM и с явным 'до' (крайний срок).
    Не показывает:
    - задачи на конкретный день ('в среду сделать ...');
    - задачи/события с конкретным временем.
    """
    user_id = str(message.from_user.id)
    rows = storage.get_tasks(user_id)
    if not rows:
        await message.answer("Активных задач нет.")
        return

    filtered: list[tuple[str, datetime | None]] = []

    for _item_id, title, due_at in rows:
        base = (title or "").strip() or "Задача"
        lower = base.lower()

        # Без дедлайна → оставить
        if not due_at:
            filtered.append((base, None))
            continue

        try:
            dt = datetime.fromisoformat(due_at)
        except Exception:
            # странная дата: если есть 'до' и нет времени, считаем дедлайном
            if "до " in lower and not TIME_REGEX.search(base):
                filtered.append((base, None))
            continue

        # Если есть точное время в due_at → это слотовое, не сюда
        if not is_end_of_day(dt):
            continue

        # Если в тексте есть HH:MM → тоже слотовое, не сюда
        if TIME_REGEX.search(base):
            continue

        # Если это формулировка "в/во понедельник/..." без 'до' → задача на день, не сюда
        if WEEKDAY_IN_TEXT_PATTERN.search(base) and "до " not in lower:
            continue

        # Остальное с 'до' считаем дедлайном
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


# === Обработка произвольного текста ===


@dp.message(F.text)
async def handle_text(message: Message):
    user_id = str(message.from_user.id)
    reply, _item = service.handle_input(user_id, message.text)
    await message.answer(reply)


# === Цикл напоминаний и утренний дайджест ===


async def reminder_loop():
    sent_digest = {}

    while True:
        try:
            now = datetime.now()

            # Утренний дайджест
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

            # Напоминания по событиям
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
                for item_id, user_id, title, start_at in events:
                    try:
                        start_dt = datetime.fromisoformat(start_at)
                    except Exception:
                        continue
                    diff_min = (start_dt - now).total_seconds() / 60
                    if 0 <= diff_min <= EVENT_REMIND_BEFORE_MINUTES:
                        text = format_event_reminder(title, start_dt)
                        try:
                            await bot.send_message(int(user_id), text)
                        except Exception as e:
                            logger.error("Ошибка отправки напоминания (событие): %s", e)
                        cur.execute(
                            "UPDATE items SET event_notified = 1 WHERE id = ?",
                            (item_id,),
                        )

            # Напоминания по задачам
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
                for item_id, user_id, title, due_at in tasks:
                    try:
                        due_dt = datetime.fromisoformat(due_at)
                    except Exception:
                        continue
                    diff_min = (due_dt - now).total_seconds() / 60
                    if 0 <= diff_min <= TASK_REMIND_BEFORE_MINUTES:
                        text = format_task_reminder(title, due_dt)
                        try:
                            await bot.send_message(int(user_id), text)
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
