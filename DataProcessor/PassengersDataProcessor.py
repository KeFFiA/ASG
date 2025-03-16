import asyncio
import math

import numpy as np
import psutil
from pathlib import Path
import warnings
from openpyxl.styles.stylesheet import Stylesheet
import pandas as pd
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from tqdm import tqdm

from Utills.Logger import logger
from Utills import StateManager as state
from DATABASE import ASGPassengersTable

warnings.filterwarnings(
    'ignore',
    category=UserWarning,
    module=Stylesheet.__module__
)


class DataProcessor:
    def __init__(self, db_url: str, max_workers: int = 4, chunk_size: int = 500):
        self.db_url = db_url
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
        self.progress = None
        self.chunk_size = chunk_size
        self.errors: dict = {'AC_PASSED': [], "FAILED": [], "FAILED_DATA": []}
        self.additional_fields = {
            'from_state', 'to_state', 'from_territory', 'to_territory',
            'number_of_flights', 'average_seats_available', 'average_payload_capacity'
        }

    async def process_files(self, file_paths: list[str]):
        """Basic file processing method"""

        engine = create_async_engine(self.db_url)
        async_session = async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession
        )

        with tqdm(total=len(file_paths), desc="[PassengersFlow]File processing") as self.progress:
            tasks = [self._process_file(async_session, file_path) for file_path in file_paths]
            await asyncio.gather(*tasks)

        await engine.dispose()

    async def _process_file(self, async_session, file_path: str):
        """Processing a single file"""
        async with self.semaphore:
            try:
                # 1. Reading a file with memory control
                if psutil.virtual_memory().percent > 80:
                    await asyncio.sleep(1)  # Artificial slowdown

                # 2. Asynchronous Excel Reading with Thread Pool
                loop = asyncio.get_running_loop()

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=UserWarning)
                    df = await loop.run_in_executor(
                        None,
                        lambda: pd.read_excel(
                            file_path,
                            engine='openpyxl'
                        )
                    )

                required_columns = ['air carrier']
                missing_columns = [col for col in required_columns if col not in df.columns.str.strip().str.lower()]

                if missing_columns:
                    logger.debug(f"File {file_path} passed. Missing columns: {', '.join(missing_columns)}")
                    self.errors['AC_PASSED'].append(file_path)
                    self.progress.update(1)
                    return

                # 3. Data Conversion
                processed_df = await self._transform_data(df)
                records = processed_df.to_dict('records')

                # 4. Asynchronous writing to the DataBase
                async with async_session() as session:
                    for i in range(0, len(records), self.chunk_size):
                        chunk = records[i:i + self.chunk_size]
                        await self._insert_to_db(session, chunk)
                        await session.commit()

                self.progress.update(1)
                self.progress.set_postfix_str(f"Processed: {Path(file_path).name}")

            except Exception as e:
                logger.warning(f"File error {file_path}: {str(e)}", exc_info=True)
                self.errors['FAILED'].append(file_path)
                self.progress.update(1)

    async def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data transformation from Excel to PassengersFlow table format"""

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False)

        expected_columns = {
            'air_carrier', 'from_city', 'to_city', 'year', 'aircraft_type',
            'prt', 'seats_available', 'passenger_occupancy_factor', 'from_state', 'to_state',
            'from_territory', 'to_territory', 'number_of_flights',
            'average_seats_available', 'average_payload_capacity'
        }

        for col in expected_columns:
            if col not in df.columns:
                df[col] = np.nan

        df = df.replace([np.nan, pd.NA, '', ' '], None)
        int_columns = ['year', 'prt', 'seats_available', 'number_of_flights', 'average_seats_available']
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        float_columns = ['passenger_occupancy_factor', 'average_payload_capacity']
        for col in float_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            df[col] = df[col].replace(np.nan, None)

        ordered_columns = [
            'from_city', 'to_city', 'year', 'air_carrier', 'aircraft_type',
            'prt', 'seats_available', 'passenger_occupancy_factor', 'from_state', 'to_state',
            'from_territory', 'to_territory', 'number_of_flights',
            'average_seats_available', 'average_payload_capacity'
        ]

        return df[ordered_columns]

    async def _insert_to_db(self, session, records: list[dict]):
        """Batch insert/update into DataBase"""
        if not records:
            return

        try:
            for record in records:
                try:
                    numeric_fields = {
                        'year', 'prt', 'seats_available', 'number_of_flights',
                        'average_seats_available', 'pof', 'apof'
                    }

                    for field in numeric_fields:
                        value = record.get(field)
                        if isinstance(value, float) and math.isnan(value):
                            record[field] = None
                        elif isinstance(value, (int, float)) and pd.isna(value):
                            record[field] = None
                    filtered_record = {
                        'from_city': record.get('from_city'),
                        'to_city': record.get('to_city'),
                        'year': record.get('year'),
                        'air_carrier': record.get('air_carrier'),
                        'aircraft_type': record.get('aircraft_type'),
                        'prt': record.get('prt'),
                        'seats_available': record.get('seats_available'),
                        'passenger_occupancy_factor': record.get('passenger_occupancy_factor'),
                        'from_state': record.get('from_state'),
                        'to_state': record.get('to_state'),
                        'from_territory': record.get('from_territory'),
                        'to_territory': record.get('to_territory'),
                        'number_of_flights': record.get('number_of_flights'),
                        'average_seats_available': record.get('average_seats_available'),
                        'average_payload_capacity': record.get('average_payload_capacity')
                    }

                    required_fields = {'from_city', 'to_city', 'year', 'air_carrier', 'aircraft_type'}
                    missing_fields = [field for field in required_fields if field not in filtered_record]

                    if missing_fields:
                        raise KeyError(f"Missing fields: {', '.join(missing_fields)}")

                    conditions = [
                        ASGPassengersTable.from_city == filtered_record['from_city'],
                        ASGPassengersTable.to_city == filtered_record['to_city'],
                        ASGPassengersTable.year == filtered_record['year'],
                        ASGPassengersTable.air_carrier == filtered_record['air_carrier'],
                        ASGPassengersTable.aircraft_type == filtered_record['aircraft_type']
                    ]

                    existing = await session.execute(
                        select(ASGPassengersTable).where(and_(*conditions)))
                    existing = existing.scalars().first()

                    if existing:
                        update_data = {
                            k: v for k, v in filtered_record.items()
                            if v is not None and k not in required_fields
                        }

                        numeric_fields = ['number_of_flights', 'average_seats_available', 'average_payload_capacity']
                        for field in numeric_fields:
                            if field in filtered_record and filtered_record[field] is not None:
                                update_data[field] = filtered_record[field]

                        if update_data:
                            await session.execute(
                                update(ASGPassengersTable)
                                .where(and_(*conditions))
                                .values(**update_data)
                            )
                    else:
                        new_record_data = {
                            **filtered_record,
                            **{field: None for field in self.additional_fields if field not in filtered_record}
                        }
                        session.add(ASGPassengersTable(**new_record_data))

                except KeyError as e:
                    logger.warning(f"Skipping record due to missing data: {e}. Data: {record}")
                    self.errors["FAILED_DATA"].append(record)

            await session.commit()

        except Exception as e:
            await session.rollback()
            logger.critical(f"Critical DB error: {str(e)}", exc_info=True)
            state.update_error(f"Critical DB error: {str(e)}")

    async def retry_failed_insertions(self):
        """Reprocessing files and data not inserted into the database"""

        if not self.errors["FAILED"] and not self.errors["FAILED_DATA"]:
            logger.info("No failed records to reprocess.")
            return

        failed_files = self.errors["FAILED"][:]
        self.errors["FAILED"].clear()

        engine = create_async_engine(self.db_url)
        async_session = async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession
        )

        for file_path in failed_files:
            try:
                await self._process_file(async_session, file_path)
            except Exception as e:
                logger.warning(f"Repeated error while processing file {file_path}: {e}")
                self.errors["FAILED"].append(file_path)

        failed_data = self.errors["FAILED_DATA"][:]
        self.errors["FAILED_DATA"].clear()
        async with async_session() as session:
            for entry in failed_data:
                try:
                    data = entry["data"]
                    await self._insert_to_db(session, [data])
                except Exception as e:
                    logger.warning(f"Error while re-inserting data: {e}. Data: {data}")
                    self.errors["FAILED_DATA"].append(data)

        await engine.dispose()

        logger.info(
            f"Reprocessing completed. Remaining {len(self.errors['FAILED'])} files and {len(self.errors['FAILED_DATA'])} records")
