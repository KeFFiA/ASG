import asyncio

import numpy as np
import psutil
from pathlib import Path
import warnings
from openpyxl.styles.stylesheet import Stylesheet
import pandas as pd
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from tqdm import tqdm

from FindPath import sync_async_method
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

    @sync_async_method
    async def process_files(self, file_paths: list[str]):
        """Basic file processing method"""

        engine = create_async_engine(self.db_url)
        async_session = async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession
        )

        with tqdm(total=len(file_paths), desc="File processing") as self.progress:
            tasks = [self._process_file(async_session, file_path) for file_path in file_paths]
            await asyncio.gather(*tasks)

        await engine.dispose()

    @sync_async_method
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

    @sync_async_method
    async def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data transformation from Excel to PassengersFlow table format"""

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False)

        df = df.replace([np.nan, '', ' ', 'nan'], None)

        df.columns = df.columns.str.strip().str.lower()

        column_mapping = {
            'air_carrier': 'air_carrier',
            'from_city': 'from_city',
            'to_city': 'to_city',
            'year': 'year',
            'aircraft_type': 'aircraft_type',
            'passengers_revenue_traffic': 'prt',
            'seats_available': 'seats_available',
            'passenger_occupancy_factor': 'pof'
        }

        df = df.rename(columns=column_mapping)
        valid_columns = [col for col in df.columns if col in column_mapping.values()]

        return df[valid_columns]

    @sync_async_method
    async def _insert_to_db(self, session, records: list[dict]):
        """Batch insert/update into DataBase"""
        if not records:
            return

        try:
            for record in records:
                try:
                    valid_fields = {
                        'from_city', 'to_city', 'year',
                        'air_carrier', 'aircraft_type',
                        'prt', 'seats_available', 'pof'
                    }
                    filtered_record = {k: v for k, v in record.items() if k in valid_fields}

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
                        select(ASGPassengersTable).where(and_(*conditions))
                    )
                    existing = existing.scalars().first()

                    if existing:
                        await session.execute(
                            update(ASGPassengersTable)
                            .where(and_(*conditions))
                            .values(**filtered_record)
                        )
                    else:
                        session.add(ASGPassengersTable(**filtered_record))

                except KeyError as e:
                    logger.warning(f"Skipping record due to missing data: {e}. Data: {record}")
                    self.errors["FAILED_DATA"].append(record)

            await session.commit()

        except Exception as e:
            await session.rollback()
            logger.critical(f"Critical DB error: {str(e)}", exc_info=True)
            state.update_error(f"Critical DB error: {str(e)}")

    @sync_async_method
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
