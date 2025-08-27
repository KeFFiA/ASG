import asyncio
import os
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp

from API.FR.FR_API import fetch_all_ranges
from DATABASE.FR.FR import LivePositions
from Utils import AsyncSessionLocal, BASE_URL, get_today_range_utc, get_earliest_time, MAX_REG_PER_BATCH, HEADERS, \
    write_csv

_last_flights: Optional[List[dict]] = None


async def dashboard_loop(regs: List[str], http: aiohttp.ClientSession, first_run: bool, storage_mode: str = "db",
        csv_path: Optional[str] = None):
    global _last_flights

    if first_run:
        start_date, end_date = get_today_range_utc()
    else:
        earliest = get_earliest_time(_last_flights)
        if earliest:
            start_date = earliest
        else:
            # Fallback
            start_date, end_date = get_today_range_utc()
        _, end_date = get_today_range_utc()

    flights = await fetch_all_ranges(registrations=regs, start_date=start_date, end_date=end_date)
    _last_flights = flights

    flight_ids = [flight["flight"] for flight in flights]

    flight_id_batches = (
        [flight_ids[i:i + MAX_REG_PER_BATCH] for i in range(0, len(flight_ids), MAX_REG_PER_BATCH)]
        if flight_ids else [None]
    )

    for batch_index, flight_batch in enumerate(flight_id_batches):
        if flight_batch:
            print(f"\n✈️ Processing a batch of flights {batch_index + 1} out of {len(flight_id_batches)}")
        params = {
            "flights": ",".join(flight_batch),
            "limit": 20000
        }
        async with AsyncSessionLocal() as session:
            async with http.get(f"{BASE_URL}/api/live/flight-positions/full", headers=HEADERS, params=params) as resp:
                try:
                    if resp.status != 200:
                        print(f"❌ Error {resp.status}: {await resp.text()}")
                        break

                    flights = await resp.json()
                    if not flights or not flights.get("data"):
                        print("✅ No data for the current interval.")
                        break

                    flights_data = flights["data"]
                    if not flights_data:
                        break

                    flights = []
                    csv_rows = []

                    for flight in flights_data:
                        row_data = {
                            "fr24_id": flight.get("fr24_id"),
                            "flight": flight.get("flight"),
                            "callsign": flight.get("callsign"),
                            "lat": flight.get("lat"),
                            "lon": flight.get("lon"),
                            "track": flight.get("track"),
                            "alt": flight.get("alt"),
                            "gspeed": flight.get("gspeed"),
                            "vspeed": flight.get("vspeed"),
                            "squawk": flight.get("squawk"),
                            "timestamp": flight.get("timestamp"),
                            "source": flight.get("source"),
                            "hex": flight.get("hex"),
                            "type": flight.get("type"),
                            "reg": flight.get("reg"),
                            "painted_as": flight.get("painted_as"),
                            "operating_as": flight.get("operating_as"),
                            "orig_iata": flight.get("orig_iata"),
                            "orig_icao": flight.get("orig_icao"),
                            "dest_iata": flight.get("dest_iata"),
                            "dest_icao": flight.get("dest_icao"),
                            "eta": flight.get("eta")
                        }

                        if storage_mode in ("db", "both"):
                            flights.append(LivePositions(**row_data))
                        if storage_mode in ("csv", "both"):
                            csv_rows.append(row_data)

                except Exception as e:
                    print(f"⚠️ Record processing error: {e}")

                if flights and storage_mode in ("db", "both"):
                    session.add_all(flights)
                    await session.commit()
                    print(f"💾 Saved {len(flights)} new records to DB.")

                if csv_rows and storage_mode in ("csv", "both") and csv_path:
                    write_csv(csv_rows, csv_path)
                    print(f"📄 Appended {len(csv_rows)} records to CSV.")


async def main_loop(regs: List[str], hours: int = 2):
    last_run_date: datetime | None = None

    while True:
        now = datetime.now(timezone.utc)

        first_run = (last_run_date != now)
        last_run_date = now

        async with aiohttp.ClientSession() as http:
            await dashboard_loop(first_run=first_run, regs=regs, http=http)

        await asyncio.sleep(hours * 3600)
