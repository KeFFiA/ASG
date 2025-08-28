import asyncio
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp

from API.FR.FR_API import fetch_all_ranges
from DATABASE.FR.FR import LivePositions
from Utils import AsyncSessionLocal, BASE_URL, get_today_range_utc, get_earliest_time, MAX_REG_PER_BATCH, HEADERS, \
    write_csv, parse_dt, ensure_naive_utc

_last_flights: Optional[List[dict]] = None


async def dashboard_loop(regs: List[str], http: aiohttp.ClientSession, first_run: bool, storage_mode: str,
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

    flights_nested = await fetch_all_ranges(registrations=regs, start_date=start_date, end_date=end_date, storage_mode="db")
    _last_flights = flights_nested
    print(flights_nested)

    flight_ids = list({
        flight.get("flight")
        for group in flights_nested if group is not None
        for flight in group if flight.get("flight")
    })

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
            async with http.get(f"{BASE_URL}/live/flight-positions/full", headers=HEADERS, params=params) as resp:
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
                            "timestamp": ensure_naive_utc(parse_dt(flight.get("datetime_landed"))),
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
                            "eta": ensure_naive_utc(parse_dt(flight.get("datetime_landed")))
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


async def main_loop(regs: List[str], hours: int = 2, storage_mode: str = "db",
                    csv_path: Optional[str] = f"output/live_{datetime.strftime(datetime.now(), '%Y%m%d_%H%M')}.csv"):
    last_run_date: datetime | None = None

    while True:
        now = datetime.now(timezone.utc)

        first_run = (last_run_date != now)
        last_run_date = now

        async with aiohttp.ClientSession() as http:
            await dashboard_loop(first_run=first_run, regs=regs, http=http, storage_mode=storage_mode, csv_path=csv_path)

        await asyncio.sleep(hours * 3600)



if __name__ == "__main__":

    regs = ['9H-SWN', '9H-CGC ', 'YL-LDN', '9H-SWM', '9H-AMU', '9H-MLQ', 'YL-LDE', 'UP-B3739', 'YL-LDK',
                     '9H-SWJ', '9H-SLE', '9H-AMJ', '9H-SLF', '9H-SMD', '9H-SLI', 'HS-SXB', 'OM-EDH', '9H-SLC', 'LV-NVI',
                     'LY-MLI', '9H-CGN', 'LY-MLJ', '9H-PTP', 'ES-SAA', '9H-SLG', 'UP-B3727', '9H-AMV', 'UP-B3732',
                     'VH-TBA', 'LY-NVF', 'OM-LEX', 'YL-LDQ', 'UP-CJ004', 'LY-FLT ', 'UP-B3735', 'LY-NVL', 'UP-B3722',
                     '9A-ZAG', '9A-BTN', '9H-MLR', 'YL-LDL', '9H-SLJ', '9H-ETA', '9H-SWB', 'YL-LDZ', 'ES-SAZ', '9H-SWG',
                     'OM-NEX', '9H-MLL', '9H-AML', 'UP-B5702', 'UP-B3720', '9H-SWK', 'UP-B3733', '9H-SWA', '9H-MLC',
                     'OM-FEX', '9H-MLO', 'G-HAGI', 'UP-B3729', '9H-AMH', '9A-BER', '9H-SWF', 'HS-SXE', 'OM-OEX',
                     '9H-MLE', '9H-MLX', '9H-ORN', 'UP-B3731', 'LY-MLG', 'YL-LDW', 'G-LESO', '9H-SLH', '9H-SLL',
                     '9H-DRA', 'HS-SXA', 'HS-SXD', 'OM-EDI', 'OM-EDA', 'YL-LDS', '9H-CEN', '9H-SLD', 'HS-SXC',
                     'UP-B3730', 'YL-LDX', 'YL-LCV', '9H-SWE', 'YL-LDR', 'UP-B3736', 'YL-LDP', '9H-CGG', 'UP-B3740',
                     'UP-B5703', '9H-CHA', 'UP-B5705', '9H-SWI', '9H-MLD', 'UP-B3741', '9H-MLU', 'OM-IEX', 'LY-NVG',
                     '9H-AMM', 'ES-SAW', '9H-CGD', '9H-GKK', '9H-SWD', '9H-CGI', 'OM-EDG', '9A-BTL', '9H-CGE', 'D-ANNA',
                     'UP-CJ008', 'VH-L7A', 'G-HODL', 'UP-B3721', 'D-ASMR', 'UP-B5704', '9H-SLK', 'LY-MLN', '9H-AMP',
                     'UP-B3726', '9H-CGA', '9H-DOR', '9H-MLS', 'OM-JEX', 'UP-CJ005', 'YL-LDI', 'OM-EDE', '9H-HYA',
                     'OM-EDC', '9H-AMK', '9A-SHO', '9H-SMG', '9H-SMH', '9A-BTK', '9H-SZF', 'UP-B6703', '9H-AME',
                     '9A-MUC', 'ES-SAD', 'OM-MEX', '9A-BWK', 'G-WEAH', 'YL-LDV', '9H-SLM', 'LY-NVE', 'UP-B3725',
                     '9H-MLV', 'LV-NVJ', 'YL-LCQ', 'YL-LDU', 'UP-B3734', '9H-GKJ', 'YL-LDD', '9H-ARI', '9H-TAU',
                     'UP-B3737', 'LY-VEL', 'YL-LDF', '9H-SWC', 'UP-B3738', '9H-MLZ', '9H-CGR', 'LY-NVH', '9H-CGJ',
                     'YL-LDM', '2-VSLP', 'ES-SAF', 'LY-MLK', '9H-CHI', 'OM-HEX', 'UP-B3742', 'UP-CJ011', 'OM-KEX',
                     '9H-MLB', 'RP-TBA', '9H-AMI', '9H-MLW', '9H-LYR', '9H-MLP', 'LY-MLF', 'YL-LDJ', 'UP-B3723',
                     '9H-MLY', '9H-CGB', 'ES-SAB', '9H-GEM', 'D-ASGK', 'ES-SAG', 'ES-SAX', 'LY-NVM', 'YL-LDO', 'YL-LCT',
                     'OM-EDF', 'UP-B3724', 'LY-NVN', '9H-CGK', '9A-IRM', '9H-ERI', 'OM-EDD', 'ES-SAY', 'G-CRUX',
                     '9A-BTI', 'ES-SAM', 'OM-EDB']


    asyncio.run(main_loop(
        regs=regs,
        hours=2,
        storage_mode="both"
    ))
