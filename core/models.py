from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Literal

ItemType = Literal["event", "task", "note"]


@dataclass
class Item:
    user_id: str
    type: ItemType
    title: str
    description: str = ""
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    due_at: Optional[datetime] = None
    status: str = "active"
    id: Optional[int] = None
