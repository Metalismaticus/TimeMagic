import re
from datetime import datetime, timedelta, time
from typing import Optional, Tuple

ItemType = str

# --- Ключевые слова ---

EVENT_KEYWORDS = (
    "встреч",
    "созвон",
    "звонок",
    "бриф",
    "интервью",
    "демо",
)

TASK_KEYWORDS = (
    "сделать",
    "написать",
    "подготовить",
    "проверить",
    "купить",
    "отправить",
    "собрать",
    "позвонить",
    "настроить",
    "разобрать",
    "сдать",
    "закончить",
    "доделать",
    "проработать",
)

DOW_MAP_EVENT = {
    "понедельник": 0, "пн": 0,
    "вторник": 1, "вт": 1,
    "среда": 2, "среду": 2, "ср": 2,
    "четверг": 3, "чт": 3,
    "пятница": 4, "пятницу": 4, "пт": 4,
    "суббота": 5, "субботу": 5, "сб": 5,
    "воскресенье": 6, "вс": 6,
}

DOW_MAP_DUE = {
    "понедельника": 0, "пн": 0,
    "вторника": 1, "вт": 1,
    "среды": 2, "ср": 2,
    "четверга": 3, "чт": 3,
    "пятницы": 4, "пт": 4,
    "субботы": 5, "сб": 5,
    "воскресенья": 6, "вс": 6,
}

PART_OF_DAY = {
    "утром": time(10, 0),
    "днем": time(14, 0),
    "днём": time(14, 0),
    "вечером": time(19, 0),
    "ночью": time(23, 0),
}

# --- Регексы ---

TIME_REGEX = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
DATE_REGEX = re.compile(
    r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b",
    re.IGNORECASE,
)

# Длительность:
# "на 2 часа", "продлится 1 час 30 минут", "на 30 минут", "длительность 2 часа"
DURATION_PATTERN = re.compile(
    r"\b(на|продлится|продолжится|длительн\w*)\s+"
    r"(?P<hours>\d+)\s*(час(?:а|ов)?|ч)\b"
    r"(?:\s*(?P<mins>\d+)\s*(минут(?:ы|а)?|мин))?",
    re.IGNORECASE,
)

DURATION_MINS_ONLY_PATTERN = re.compile(
    r"\b(на|продлится|продолжится|длительн\w*)\s+"
    r"(?P<mins>\d+)\s*(минут(?:ы|а)?|мин)\b",
    re.IGNORECASE,
)


# --- Вспомогательные ---

def _next_weekday(now: datetime, target_weekday: int) -> datetime:
    days_ahead = (target_weekday - now.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return now + timedelta(days=days_ahead)


def _strip_spaces(s: str) -> str:
    return " ".join(s.split())


def _remove_trailing_v(s: str) -> str:
    return re.sub(r"\bв$", "", s).strip(" ,.-")


# --- Парсинг длительности ---

def parse_duration(text: str) -> Tuple[Optional[timedelta], str]:
    """
    Ищем длительность:
      - "на 2 часа"
      - "продлится 1 час 30 минут"
      - "на 30 минут"
      - "длительность 2 часа"
    Возвращаем (timedelta | None, текст без этого фрагмента).
    """
    s = text

    m = DURATION_PATTERN.search(s)
    if m:
        hours = int(m.group("hours"))
        mins = int(m.group("mins") or 0)
        dur = timedelta(hours=hours, minutes=mins)
        s = (s[:m.start()] + s[m.end():]).strip(" ,.-")
        return dur, _strip_spaces(s)

    m2 = DURATION_MINS_ONLY_PATTERN.search(s)
    if m2:
        mins = int(m2.group("mins"))
        dur = timedelta(minutes=mins)
        s = (s[:m2.start()] + s[m2.end():]).strip(" ,.-")
        return dur, _strip_spaces(s)

    return None, text


# --- Парсинг относительных интервалов ---

def parse_relative(text: str, now: datetime) -> Tuple[Optional[datetime], Optional[datetime]]:
    m = re.search(
        r"через\s+(\d+)\s*(минут[уы]?|мин|час[аов]?|ч|дн[еяь]|день|дней|недел[юи])",
        text.lower(),
    )
    if not m:
        return None, None

    n = int(m.group(1))
    unit = m.group(2)

    if unit.startswith("мин"):
        start = now + timedelta(minutes=n)
    elif unit.startswith("час") or unit == "ч":
        start = now + timedelta(hours=n)
    elif unit.startswith("дн"):
        start = now + timedelta(days=n)
    elif unit.startswith("недел"):
        start = now + timedelta(weeks=n)
    else:
        return None, None

    end = start + timedelta(minutes=30)
    return start, end


# --- Парсинг абсолютных дат/времени для событий ---

def parse_absolute(text: str, now: datetime) -> Tuple[Optional[datetime], Optional[datetime]]:
    t = text.lower()
    base_date = now.date()
    date = base_date
    date_set = False

    # сегодня / завтра / послезавтра
    if "послезавтра" in t:
        date = base_date + timedelta(days=2)
        date_set = True
    elif "завтра" in t:
        date = base_date + timedelta(days=1)
        date_set = True
    elif "сегодня" in t:
        date_set = True

    # "в/во понедельник ..."
    m_dow_in = re.search(
        r"\bво?\s+(понедельник|вторник|сред[ау]|четверг|пятниц[ау]|"
        r"суббот[уы]|воскресенье|пн|вт|ср|чт|пт|сб|вс)\b",
        t,
    )
    if m_dow_in:
        wd = DOW_MAP_EVENT.get(m_dow_in.group(1))
        if wd is not None:
            dt = _next_weekday(now, wd)
            date = dt.date()
            date_set = True

    # "Понедельник ..." в начале
    m_dow_start = re.match(
        r"^(понедельник|вторник|сред[ау]|четверг|пятниц[ау]|"
        r"суббот[уы]|воскресенье|пн|вт|ср|чт|пт|сб|вс)\b",
        t,
    )
    if m_dow_start:
        wd = DOW_MAP_EVENT.get(m_dow_start.group(1))
        if wd is not None:
            dt = _next_weekday(now, wd)
            date = dt.date()
            date_set = True

    # явная дата
    m_date = DATE_REGEX.search(t)
    if m_date:
        day = int(m_date.group(1))
        month = int(m_date.group(2))
        year = now.year
        if m_date.group(3):
            y_raw = int(m_date.group(3))
            year = 2000 + y_raw if y_raw < 100 else y_raw
        else:
            if (month, day) < (now.month, now.day):
                year += 1
        try:
            date = datetime(year, month, day).date()
            date_set = True
        except ValueError:
            pass

    # время
    m_time = TIME_REGEX.search(t)

    # часть дня
    part_time = None
    for key, pt in PART_OF_DAY.items():
        if key in t:
            part_time = pt
            break

    if m_time:
        hour = int(m_time.group(1))
        minute = int(m_time.group(2))
        start = datetime.combine(date, time(hour, minute))
        end = start + timedelta(minutes=30)
        return start, end

    if part_time:
        start = datetime.combine(date, part_time)
        end = start + timedelta(minutes=30)
        return start, end

    if date_set:
        start = datetime.combine(date, time(10, 0))
        end = start + timedelta(minutes=30)
        return start, end

    return None, None


def parse_datetime(text: str, now: Optional[datetime] = None) -> Tuple[Optional[datetime], Optional[datetime]]:
    if now is None:
        now = datetime.now()
    start, end = parse_relative(text, now)
    if start:
        return start, end
    return parse_absolute(text, now)


# --- Парсинг дедлайнов задач ---

def parse_due(text: str, now: Optional[datetime] = None) -> Optional[datetime]:
    if now is None:
        now = datetime.now()
    t = text.lower()

    m_dow = re.search(
        r"до\s+(понедельника|вторника|среды|четверга|пятницы|"
        r"субботы|воскресенья|пн|вт|ср|чт|пт|сб|вс)",
        t,
    )
    if m_dow:
        wd = DOW_MAP_DUE.get(m_dow.group(1))
        if wd is not None:
            days_ahead = (wd - now.weekday()) % 7
            dt = (now + timedelta(days=days_ahead)).replace(
                hour=23, minute=59, second=0, microsecond=0
            )
            return dt

    m_date = re.search(r"до\s+(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?", t)
    if m_date:
        day = int(m_date.group(1))
        month = int(m_date.group(2))
        year = now.year
        if m_date.group(3):
            y_raw = int(m_date.group(3))
            year = 2000 + y_raw if y_raw < 100 else y_raw
        else:
            if (month, day) < (now.month, now.day):
                year += 1
        try:
            return datetime(year, month, day, 23, 59)
        except ValueError:
            return None

    return None


# --- split_title_desc ---

def split_title_desc(text: str) -> Tuple[str, str]:
    """
    Как было:
    - первая строка -> title,
    - остальное -> desc.
    Дополнительно:
    - вырезаем фразу длительности,
    - чистим висящее 'в' в конце,
    - не трогаем даты/время/дни.
    """
    text = text.strip()
    if not text:
        return "Без названия", ""

    parts = text.split("\n", 1)
    title = parts[0].strip()
    desc = parts[1].strip() if len(parts) > 1 else ""

    _, title_clean = parse_duration(title)
    title_clean = _remove_trailing_v(_strip_spaces(title_clean))

    if not title_clean:
        title_clean = "Без названия"

    if len(title_clean) > 120:
        title_clean = title_clean[:117].rstrip() + "..."

    return title_clean, desc


# --- Доп. дедлайн по дню недели для задач ---

def _extract_task_weekday_due(text: str, now: datetime) -> Optional[datetime]:
    t = text.lower()
    m = re.search(
        r"\bво?\s+(понедельник|вторник|сред[ау]|четверг|пятниц[ау]|"
        r"суббот[уы]|воскресенье|пн|вт|ср|чт|пт|сб|вс)\b",
        t,
    )
    if not m:
        return None
    wd = DOW_MAP_EVENT.get(m.group(1))
    if wd is None:
        return None
    dt = _next_weekday(now, wd)
    return dt.replace(hour=23, minute=59, second=0, microsecond=0)


# --- Классификация ---

def classify(
    text: str,
    now: Optional[datetime] = None,
) -> Tuple[ItemType, Optional[datetime], Optional[datetime], Optional[datetime]]:
    """
    Возвращает (type, start_dt, end_dt, due_dt).

    Приоритет:
    1) task: задачный глагол или дедлайн.
    2) event: ключевые слова или распарсенный слот.
    3) note: остальное.

    Новое:
    - если event и есть длительность, end_dt = start_dt + duration.
    """
    if now is None:
        now = datetime.now()

    raw = text.strip()
    if not raw:
        return "note", None, None, None

    t = raw.lower()

    # базовый слот как раньше
    start_dt, end_dt = parse_datetime(raw, now)

    # ключевые слова
    is_event_kw = any(k in t for k in EVENT_KEYWORDS)
    is_task_kw = any(t.startswith(k) or f" {k}" in t for k in TASK_KEYWORDS)

    # дедлайны
    due_dt = parse_due(raw, now)

    # задачный глагол + день недели → дедлайн
    if is_task_kw and due_dt is None:
        extra_due = _extract_task_weekday_due(raw, now)
        if extra_due:
            due_dt = extra_due

    # тип
    if is_task_kw or due_dt:
        item_type: ItemType = "task"
    elif is_event_kw or start_dt:
        item_type = "event"
    else:
        item_type = "note"

    # длительность только для событий
    if item_type == "event" and start_dt:
        dur, _ = parse_duration(raw)
        if dur:
            end_dt = start_dt + dur

    return item_type, start_dt, end_dt, due_dt
