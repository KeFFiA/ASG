import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

load_dotenv()

SECONDS_BETWEEN_REQUESTS = 60 / 90
RANGE_DAYS = 14
MAX_REG_PER_BATCH = 15

HEADERS = {
    "Authorization": f"Bearer {os.getenv('FR_API_KEY')}",
    "Accept-Version": "v1",
    "Accept": "application/json"
}

BASE_URL = "https://fr24api.flightradar24.com/api"

engine = create_async_engine(os.getenv("DATABASE_URL_FR"), echo=False)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


def write_csv(rows: List[dict], path: str):
    file_exists = Path(path).exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc)


def parse_date_or_datetime(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def ensure_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def get_today_range_utc() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    return (
        start_of_day.strftime("%Y-%m-%d %H:%M:%S"),
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )


def get_earliest_time(flights: List[dict]) -> Optional[str]:
    times = []
    for flight in flights:
        if flight["first_seen"]:
            try:
                dt = datetime.strptime(flight["first_seen"], "%Y-%m-%d %H:%M:%S")
                times.append(dt)
            except ValueError:
                pass
    if times:
        return min(times).strftime("%Y-%m-%d %H:%M:%S")
    return None