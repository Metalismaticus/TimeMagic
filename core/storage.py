import os
import sqlite3
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from pathlib import Path

from .models import Item

# --- Путь к БД всегда в корне проекта ---
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("DB_PATH", "data.db"))
if not DB_PATH.is_absolute():
    DB_PATH = ROOT_DIR / DB_PATH


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            type TEXT NOT NULL,
            start_at TEXT,
            end_at TEXT,
            due_at TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            event_notified INTEGER NOT NULL DEFAULT 0,
            due_notified INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    conn.commit()
    conn.close()


def insert_item(item: Item) -> Item:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO items (user_id, title, description, type, start_at, end_at, due_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item.user_id,
            item.title,
            item.description,
            item.type,
            item.start_at.isoformat() if item.start_at else None,
            item.end_at.isoformat() if item.end_at else None,
            item.due_at.isoformat() if item.due_at else None,
            item.status,
        ),
    )
    item.id = cur.lastrowid
    conn.commit()
    conn.close()
    return item


def get_today_items(user_id: str):
    now = datetime.now()
    today = now.date()
    tomorrow = today + timedelta(days=1)

    conn = get_conn()
    cur = conn.cursor()

    # события
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
        (user_id, today.isoformat(), tomorrow.isoformat()),
    )
    events = cur.fetchall()

    # задачи
    cur.execute(
        """
        SELECT title, due_at FROM items
        WHERE user_id = ?
          AND type = 'task'
          AND status = 'active'
          AND (due_at IS NULL OR due_at <= ?)
        ORDER BY COALESCE(due_at, ?)
        """,
        (user_id, tomorrow.isoformat(), tomorrow.isoformat()),
    )
    tasks = cur.fetchall()

    conn.close()
    return events, tasks


def get_tasks(user_id: str, limit: int = 30):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, title, due_at FROM items
        WHERE user_id = ?
          AND type = 'task'
          AND status = 'active'
        ORDER BY COALESCE(due_at, '9999-12-31') ASC, id ASC
        LIMIT ?
        """,
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_notes(user_id: str, limit: int = 30):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, title FROM items
        WHERE user_id = ?
          AND type = 'note'
        ORDER BY id DESC
        LIMIT ?
        """,
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_due_events_and_tasks(now: datetime, event_window_min: int, task_window_min: int):
    """
    Старый вспомогательный метод.
    Сейчас reminder_loop в telegram_bot.py делает выборки напрямую.
    Оставлен для совместимости, если где-то ещё используется.
    """
    conn = get_conn()
    cur = conn.cursor()

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

    conn.close()
    return events, tasks


def get_all_user_ids():
    """
    Используется для утреннего дайджеста:
    получить список всех пользователей, у кого есть записи.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT user_id FROM items")
    rows = [row[0] for row in cur.fetchall()]
    conn.close()
    return rows
