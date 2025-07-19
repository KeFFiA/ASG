import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import aiohttp
import dotenv
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from DATABASE import FlightSummary

dotenv.load_dotenv()

HEADERS = {
    "Authorization": f"Bearer {os.getenv('FR_API_KEY')}",
    "Accept-Version": "v1",
    "Accept": "application/json"
}

BASE_URL = "https://fr24api.flightradar24.com/api"
engine = create_async_engine(os.getenv("DATABASE_URL_FR"), echo=False)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

SECONDS_BETWEEN_REQUESTS = 60 / 90
RANGE_DAYS = 14
MAX_REG_PER_BATCH = 15


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc)


def ensure_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


async def fetch_date_range(
        icao: Optional[str],
        regs: Optional[List[str]],
        range_from: datetime,
        range_to: datetime,
        http: aiohttp.ClientSession
):
    async with AsyncSessionLocal() as session:
        print(f"📆 Range Processing: {range_from.date()} - {range_to.date()} | ICAO={icao} | REGS={regs}")
        next_from = range_from

        while True:
            params = {
                "flight_datetime_from": next_from.strftime("%Y-%m-%d %H:%M:%S"),
                "flight_datetime_to": range_to.strftime("%Y-%m-%d %H:%M:%S"),
                "limit": 20000
            }
            if icao:
                params["painted_as"] = icao
                # params["operating_as"] = icao
            if regs:
                params["registrations"] = ",".join(regs)

            await asyncio.sleep(SECONDS_BETWEEN_REQUESTS)

            async with http.get(f"{BASE_URL}/flight-summary/full", headers=HEADERS, params=params) as resp:
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

                fr24_ids = [f.get("fr24_id") for f in flights_data if f.get("fr24_id")]
                existing_ids = set(
                    row[0] for row in (await session.execute(
                        select(FlightSummary.fr24_id).where(FlightSummary.fr24_id.in_(fr24_ids))
                    )).all()
                )

                new_flights = []
                max_takeoff = next_from

                for flight in flights_data:
                    try:
                        fr24_id = flight.get("fr24_id")
                        if not fr24_id or fr24_id in existing_ids:
                            print(f"🔁 Skipping a duplicate: {flight.get('flight')}")
                            continue

                        takeoff = parse_dt(flight.get("datetime_takeoff"))
                        max_takeoff = max(max_takeoff, takeoff) if takeoff else max_takeoff

                        flight_obj = FlightSummary(
                            fr24_id=fr24_id,
                            flight=flight.get("flight"),
                            callsign=flight.get("callsign"),
                            operating_as=flight.get("operating_as"),
                            painted_as=flight.get("painted_as"),
                            type=flight.get("type"),
                            reg=flight.get("reg"),
                            orig_icao=flight.get("orig_icao"),
                            orig_iata=flight.get("orig_iata"),
                            datetime_takeoff=ensure_naive_utc(takeoff),
                            runway_takeoff=flight.get("runway_takeoff"),
                            dest_icao=flight.get("dest_icao"),
                            dest_iata=flight.get("dest_iata"),
                            dest_icao_actual=flight.get("dest_icao_actual"),
                            dest_iata_actual=flight.get("dest_iata_actual"),
                            datetime_landed=ensure_naive_utc(parse_dt(flight.get("datetime_landed"))),
                            runway_landed=flight.get("runway_landed"),
                            flight_time=flight.get("flight_time"),
                            actual_distance=flight.get("actual_distance"),
                            circle_distance=flight.get("circle_distance"),
                            category=flight.get("category"),
                            hex=flight.get("hex"),
                            first_seen=ensure_naive_utc(parse_dt(flight.get("first_seen"))),
                            last_seen=ensure_naive_utc(parse_dt(flight.get("last_seen"))),
                            flight_ended=flight.get("flight_ended"),
                        )

                        new_flights.append(flight_obj)

                    except Exception as e:
                        print(f"⚠️ Record processing error: {e}")

                session.add_all(new_flights)
                await session.commit()
                print(f"💾 Saved {len(new_flights)} new records out of {len(flights_data)}.")

                if max_takeoff == next_from or max_takeoff >= range_to:
                    break

                next_from = max_takeoff + timedelta(seconds=1)


async def fetch_all_ranges(
        start_date: str,
        end_date: str,
        icao: Optional[str] = None,
        registrations: Optional[List[str]] = None
):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    date_ranges = []
    current = start_dt
    while current <= end_dt:
        range_end = min(current + timedelta(days=RANGE_DAYS) - timedelta(seconds=1), end_dt)
        date_ranges.append((current, range_end))
        current = range_end + timedelta(seconds=1)

    registration_batches = (
        [registrations[i:i + MAX_REG_PER_BATCH] for i in range(0, len(registrations), MAX_REG_PER_BATCH)]
        if registrations else [None]
    )

    async with aiohttp.ClientSession() as http:
        for batch_index, reg_batch in enumerate(registration_batches):
            if reg_batch:
                print(f"\n📦 Processing a batch of registrations {batch_index + 1} out of {len(registration_batches)}")
            for i, (range_start, range_end) in enumerate(date_ranges):
                print(f"🚀 Range {i + 1} out of {len(date_ranges)}")
                await fetch_date_range(icao=icao, regs=reg_batch, range_from=range_start, range_to=range_end, http=http)


if __name__ == "__main__":
    ICAO = ["UAE", "FDB"]
    START_DATE = "2024-04-07"
    END_DATE = "2025-07-14"
    # REGISTRATIONS = ['9H-SWN', '9H-CGC ', 'YL-LDN', '9H-SWM', '9H-AMU', '9H-MLQ', 'YL-LDE', 'UP-B3739', 'YL-LDK',
    #                  '9H-SWJ', '9H-SLE', '9H-AMJ', '9H-SLF', '9H-SMD', '9H-SLI', 'HS-SXB', 'OM-EDH', '9H-SLC', 'LV-NVI',
    #                  'LY-MLI', '9H-CGN', 'LY-MLJ', '9H-PTP', 'ES-SAA', '9H-SLG', 'UP-B3727', '9H-AMV', 'UP-B3732',
    #                  'VH-TBA', 'LY-NVF', 'OM-LEX', 'YL-LDQ', 'UP-CJ004', 'LY-FLT ', 'UP-B3735', 'LY-NVL', 'UP-B3722',
    #                  '9A-ZAG', '9A-BTN', '9H-MLR', 'YL-LDL', '9H-SLJ', '9H-ETA', '9H-SWB', 'YL-LDZ', 'ES-SAZ', '9H-SWG',
    #                  'OM-NEX', '9H-MLL', '9H-AML', 'UP-B5702', 'UP-B3720', '9H-SWK', 'UP-B3733', '9H-SWA', '9H-MLC',
    #                  'OM-FEX', '9H-MLO', 'G-HAGI', 'UP-B3729', '9H-AMH', '9A-BER', '9H-SWF', 'HS-SXE', 'OM-OEX',
    #                  '9H-MLE', '9H-MLX', '9H-ORN', 'UP-B3731', 'LY-MLG', 'YL-LDW', 'G-LESO', '9H-SLH', '9H-SLL',
    #                  '9H-DRA', 'HS-SXA', 'HS-SXD', 'OM-EDI', 'OM-EDA', 'YL-LDS', '9H-CEN', '9H-SLD', 'HS-SXC',
    #                  'UP-B3730', 'YL-LDX', 'YL-LCV', '9H-SWE', 'YL-LDR', 'UP-B3736', 'YL-LDP', '9H-CGG', 'UP-B3740',
    #                  'UP-B5703', '9H-CHA', 'UP-B5705', '9H-SWI', '9H-MLD', 'UP-B3741', '9H-MLU', 'OM-IEX', 'LY-NVG',
    #                  '9H-AMM', 'ES-SAW', '9H-CGD', '9H-GKK', '9H-SWD', '9H-CGI', 'OM-EDG', '9A-BTL', '9H-CGE', 'D-ANNA',
    #                  'UP-CJ008', 'VH-L7A', 'G-HODL', 'UP-B3721', 'D-ASMR', 'UP-B5704', '9H-SLK', 'LY-MLN', '9H-AMP',
    #                  'UP-B3726', '9H-CGA', '9H-DOR', '9H-MLS', 'OM-JEX', 'UP-CJ005', 'YL-LDI', 'OM-EDE', '9H-HYA',
    #                  'OM-EDC', '9H-AMK', '9A-SHO', '9H-SMG', '9H-SMH', '9A-BTK', '9H-SZF', 'UP-B6703', '9H-AME',
    #                  '9A-MUC', 'ES-SAD', 'OM-MEX', '9A-BWK', 'G-WEAH', 'YL-LDV', '9H-SLM', 'LY-NVE', 'UP-B3725',
    #                  '9H-MLV', 'LV-NVJ', 'YL-LCQ', 'YL-LDU', 'UP-B3734', '9H-GKJ', 'YL-LDD', '9H-ARI', '9H-TAU',
    #                  'UP-B3737', 'LY-VEL', 'YL-LDF', '9H-SWC', 'UP-B3738', '9H-MLZ', '9H-CGR', 'LY-NVH', '9H-CGJ',
    #                  'YL-LDM', '2-VSLP', 'ES-SAF', 'LY-MLK', '9H-CHI', 'OM-HEX', 'UP-B3742', 'UP-CJ011', 'OM-KEX',
    #                  '9H-MLB', 'RP-TBA', '9H-AMI', '9H-MLW', '9H-LYR', '9H-MLP', 'LY-MLF', 'YL-LDJ', 'UP-B3723',
    #                  '9H-MLY', '9H-CGB', 'ES-SAB', '9H-GEM', 'D-ASGK', 'ES-SAG', 'ES-SAX', 'LY-NVM', 'YL-LDO', 'YL-LCT',
    #                  'OM-EDF', 'UP-B3724', 'LY-NVN', '9H-CGK', '9A-IRM', '9H-ERI', 'OM-EDD', 'ES-SAY', 'G-CRUX',
    #                  '9A-BTI', 'ES-SAM', 'OM-EDB']
    REGISTRATIONS = None

    asyncio.run(fetch_all_ranges(
        start_date=START_DATE,
        end_date=END_DATE,
        icao=ICAO if ICAO else None,
        registrations=REGISTRATIONS if REGISTRATIONS else None
    ))
