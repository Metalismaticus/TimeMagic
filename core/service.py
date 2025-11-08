from datetime import datetime, timedelta
from typing import Tuple, Optional

from .models import Item
from . import storage
from .parser import classify, split_title_desc


def init():
    storage.init_db()


def _find_event_conflict(user_id: str, start_dt: datetime, end_dt: datetime) -> Optional[tuple]:
    """
    Ищем активные события, которые пересекаются с [start_dt, end_dt).
    """
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
          AND start_at < ?
          AND COALESCE(end_at, start_at) > ?
        LIMIT 1
        """,
        (str(user_id), end_dt.isoformat(), start_dt.isoformat()),
    )
    row = cur.fetchone()
    conn.close()
    return row


def handle_input(user_id: str, text: str) -> Tuple[str, Optional[Item]]:
    """
    Вход: user_id, сырой текст.
    Выход: (ответ, Item или None если не сохранили).
    """
    item_type, start_dt, end_dt, due_dt = classify(text)
    title, desc = split_title_desc(text)

    # --- Событие ---
    if item_type == "event":
        # если нет времени вообще, ставим дефолт
        if not start_dt:
            base = datetime.now().date() + timedelta(days=1)
            start_dt = datetime.combine(base, datetime.min.time()).replace(hour=10, minute=0)
        # если нет конца, по умолчанию 30 минут
        if not end_dt:
            end_dt = start_dt + timedelta(minutes=30)

        # защита от пересечений
        conflict = _find_event_conflict(user_id, start_dt, end_dt)
        if conflict:
            _cid, ctitle, cstart, cend = conflict
            try:
                cs = datetime.fromisoformat(cstart)
                ce = datetime.fromisoformat(cend) if cend else cs + timedelta(minutes=30)
                return (
                    f"Не могу добавить: в это время уже есть '{ctitle}' "
                    f"({cs.strftime('%d.%m %H:%M')}-{ce.strftime('%H:%M')}).",
                    None,
                )
            except Exception:
                return (
                    f"Не могу добавить: в это время уже есть другое событие.",
                    None,
                )

        item = Item(
            user_id=str(user_id),
            type="event",
            title=title,
            description=desc,
            start_at=start_dt,
            end_at=end_dt,
        )
        item = storage.insert_item(item)

        return (
            f"Добавил событие: {title}\n"
            f"{start_dt.strftime('%d.%m.%Y %H:%M')} - {end_dt.strftime('%H:%M')}",
            item,
        )

    # --- Задача ---
    if item_type == "task":
        item = Item(
            user_id=str(user_id),
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
        user_id=str(user_id),
        type="note",
        title=title,
        description=desc,
    )
    item = storage.insert_item(item)
    return "Добавил заметку.", item
