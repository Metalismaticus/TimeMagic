import re
from datetime import datetime, timedelta
from typing import Tuple, Optional

from .models import Item
from . import storage
from .parser import classify, split_title_desc


def init():
    storage.init_db()


def _load_user_events(user_id: str):
    conn = storage.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, title, start_at, end_at
        FROM items
        WHERE user_id = ?
          AND type = 'event'
          AND status = 'active'
          AND start_at IS NOT NULL
        """,
        (str(user_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _parse_interval(start_str: str, end_str: Optional[str]):
    """
    Превращаем (start_at, end_at) в:
    - ("point", S, S) если нет нормальной длительности;
    - ("interval", S, E) если E > S.
    """
    try:
        s = datetime.fromisoformat(start_str)
    except Exception:
        return None

    e = None
    if end_str:
        try:
            e = datetime.fromisoformat(end_str)
            if e <= s:
                e = None
        except Exception:
            e = None

    if e is None:
        return ("point", s, s)
    return ("interval", s, e)


def _find_event_conflict(user_id: str, new_start: datetime, new_end: Optional[datetime]) -> Optional[tuple]:
    """
    Проверяем пересечение нового события с существующими.

    Правила:
    - если у нового нет new_end или new_end <= new_start → считаем точкой;
    - точка конфликтует с:
        - точкой в то же время;
        - интервалом, который её содержит;
    - интервал конфликтует с:
        - точкой внутри него;
        - интервалом, который с ним пересекается;
    - [12:00-14:00] и [14:00-...] не конфликтуют.
    """
    rows = _load_user_events(user_id)

    if new_end and new_end > new_start:
        new_type = "interval"
    else:
        new_type = "point"
        new_end = new_start

    for item_id, title, s_str, e_str in rows:
        parsed = _parse_interval(s_str, e_str)
        if not parsed:
            continue
        ex_type, ex_start, ex_end = parsed

        if new_type == "point" and ex_type == "point":
            if new_start == ex_start:
                return item_id, title, s_str, e_str

        elif new_type == "point" and ex_type == "interval":
            if ex_start <= new_start < ex_end:
                return item_id, title, s_str, e_str

        elif new_type == "interval" and ex_type == "point":
            if new_start <= ex_start < new_end:
                return item_id, title, s_str, e_str

        else:  # оба интервалы
            if not (new_end <= ex_start or ex_end <= new_start):
                return item_id, title, s_str, e_str

    return None


def _extract_explicit_duration_minutes(text: str) -> int:
    """
    'на 2 часа', 'на 1 час', 'на 90 минут' -> минуты.
    Если не нашли — 0.
    """
    t = text.lower()
    m = re.search(r"\bна\s+(\d+)\s*(минут|мин|час|часа|часов)\b", t)
    if not m:
        return 0
    n = int(m.group(1))
    unit = m.group(2)
    if unit.startswith("мин"):
        return n
    return n * 60


def handle_input(user_id: str, text: str) -> Tuple[str, Optional[Item]]:
    """
    Вход: user_id, сырой текст.
    Выход:
      - нормальный текст + Item при успехе;
      - "__CONFLICT__|..." и None при конфликте слота.
    """
    item_type, start_dt, end_dt, due_dt = classify(text)
    title, desc = split_title_desc(text)
    uid = str(user_id)

    # --- Событие ---
    if item_type == "event":
        # Если парсер не поставил дату/время — ставим базовый слот (завтра 10:00).
        if not start_dt:
            base = datetime.now().date() + timedelta(days=1)
            start_dt = datetime.combine(base, datetime.min.time()).replace(hour=10, minute=0)

        # Явная длительность из текста (если есть).
        explicit_dur = _extract_explicit_duration_minutes(text)

        # Если end_dt пришёл из парсера и валиден — используем его.
        if end_dt and end_dt > start_dt:
            duration_min = int((end_dt - start_dt).total_seconds() // 60)
        else:
            # Если есть явная длительность — считаем end_dt.
            if explicit_dur > 0:
                duration_min = explicit_dur
                end_dt = start_dt + timedelta(minutes=duration_min)
            else:
                # Нет явной длительности → событие-точка (duration_min = 0, end_dt = None).
                duration_min = 0
                end_dt = None

        # Проверка конфликта
        conflict = _find_event_conflict(uid, start_dt, end_dt)
        if conflict:
            _cid, ctitle, cstart, cend = conflict
            try:
                cs = datetime.fromisoformat(cstart)
                # для вывода конца берём как есть, если был
                ce = datetime.fromisoformat(cend) if cend else cs
                # __CONFLICT__|day|conf_title|conf_start|conf_end|new_title|duration_min
                payload = (
                    "__CONFLICT__|"
                    f"{start_dt.date().isoformat()}|"
                    f"{ctitle}|"
                    f"{cs.isoformat()}|"
                    f"{ce.isoformat()}|"
                    f"{title}|"
                    f"{duration_min}"
                )
            except Exception:
                payload = (
                    "__CONFLICT__|"
                    f"{start_dt.date().isoformat()}|||"
                    f"|{title}|{duration_min}"
                )
            return payload, None

        # Создаём событие
        item = Item(
            user_id=uid,
            type="event",
            title=title,
            description=desc,
            start_at=start_dt,
            end_at=end_dt,
        )
        item = storage.insert_item(item)

        if end_dt:
            times = f"{start_dt.strftime('%d.%m.%Y %H:%M')} - {end_dt.strftime('%H:%M')}"
        else:
            times = f"{start_dt.strftime('%d.%m.%Y %H:%M')}"
        return f"Добавил событие: {title}\n{times}", item

    # --- Задача ---
    if item_type == "task":
        item = Item(
            user_id=uid,
            type="task",
            title=title,
            description=desc,
            due_at=due_dt,
        )
        item = storage.insert_item(item)

        if due_dt:
            return f"Добавил задачу: {title} (к {due_dt.strftime('%d.%m.%Y')})", item
        return f"Добавил задачу: {title}", item

    # --- Заметка ---
    item = Item(
        user_id=uid,
        type="note",
        title=title,
        description=desc,
    )
    item = storage.insert_item(item)
    return "Добавил заметку.", item
