import asyncio
import math
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
from openpyxl.styles.stylesheet import Stylesheet
from sqlalchemy import and_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from tqdm import tqdm

from DATABASE import ASGPassengersTable
from Utills import StateManager as state
from Utills.Logger import logger

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
            'nb._of_flights', 'average_seats_available', 'average_payload_capacity'
        }

    async def process_files(self, file_paths: list[str]):
        """Basic file processing method"""

        engine = create_async_engine(self.db_url, pool_size=20,
                                     max_overflow=50,
                                     pool_timeout=60,
                                     pool_recycle=1800,
                                     pool_pre_ping=True)
        async_session = async_sessionmaker(
            engine,
            expire_on_commit=False,
            autoflush=False,
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
                            engine='openpyxl',
                        )
                    )

                missing_columns = [col for col in ['air carrier'] if col not in df.columns.str.strip().str.lower()]

                if missing_columns:
                    self.errors['AC_PASSED'].append(file_path)
                    self.progress.update(1)
                    return

                # 3. Data Conversion
                processed_df = await self._transform_data(df)
                records = processed_df.to_dict('records')

                # 4. Asynchronous writing to the DataBase
                for i in range(0, len(records), self.chunk_size):
                    chunk = records[i:i + self.chunk_size]
                    async with async_session() as session:
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
            'passengers_revenue_traffic', 'seats_available', 'passenger_occupancy_factor', 'from_state', 'to_state',
            'from_territory', 'to_territory', 'nb._of_flights',
            'average_seats_available', 'average_payload_capacity'
        }

        for col in expected_columns:
            if col not in df.columns:
                df[col] = np.nan

        df = df.replace([np.nan, pd.NA, '', ' '], None)
        int_columns = ['year', 'passengers_revenue_traffic', 'seats_available', 'nb._of_flights',
                       'average_seats_available']
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        float_columns = ['passenger_occupancy_factor', 'average_payload_capacity']
        for col in float_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            df[col] = df[col].replace(np.nan, None)

        ordered_columns = [
            'from_city', 'to_city', 'year', 'air_carrier', 'aircraft_type',
            'passengers_revenue_traffic', 'seats_available', 'passenger_occupancy_factor', 'from_state', 'to_state',
            'from_territory', 'to_territory', 'nb._of_flights',
            'average_seats_available', 'average_payload_capacity'
        ]

        return df[ordered_columns]

    async def _insert_to_db(self, session, records: list[dict]):
        """Batch insert/update into DataBase"""
        if not records:
            return
        try:
            max_params_per_chunk = 30000
            fields_per_record = 15
            safe_chunk_size = max_params_per_chunk // fields_per_record

            for i in range(0, len(records), safe_chunk_size):
                chunk = records[i:i + safe_chunk_size]

                filtered_chunk = []
                for record in chunk:
                    filtered_record = {
                        'from_city': record.get('from_city'),
                        'to_city': record.get('to_city'),
                        'year': record.get('year'),
                        'air_carrier': record.get('air_carrier'),
                        'aircraft_type': record.get('aircraft_type'),
                        'prt': record.get('passengers_revenue_traffic'),
                        'seats_available': record.get('seats_available'),
                        'passenger_occupancy_factor': record.get('passenger_occupancy_factor'),
                        'from_state': record.get('from_state'),
                        'to_state': record.get('to_state'),
                        'from_territory': record.get('from_territory'),
                        'to_territory': record.get('to_territory'),
                        'number_of_flights': record.get('nb._of_flights'),
                        'average_seats_available': record.get('average_seats_available'),
                        'average_payload_capacity': record.get('average_payload_capacity')
                    }
                    filtered_chunk.append(filtered_record)

                stmt = insert(ASGPassengersTable).values(filtered_chunk)
                stmt = stmt.on_conflict_do_update(
                    constraint='unique_passengers_record',
                    set_={c.name: c for c in stmt.excluded if c.name not in ['id']}
                )

                try:
                    await session.execute(stmt)
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Error inserting chunk {i}-{i + len(chunk)}: {str(e)}")
                    self.errors["FAILED_DATA"].extend(chunk)

        except Exception as e:
            logger.critical(f"Critical DB error: {str(e)}", exc_info=True)
            state.update_error(f"Critical DB error: {str(e)}")
            raise

        finally:
            await session.close()

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
                finally:
                    await session.close()

        await engine.dispose()

        logger.info(
            f"Reprocessing completed. Remaining {len(self.errors['FAILED'])} files and {len(self.errors['FAILED_DATA'])} records")
