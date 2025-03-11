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
from Logger import logger
from DATABASE import MyTable

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
                    logger.info(f"File {file_path} passed. Missing columns: {', '.join(missing_columns)}")
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
                self.progress.update(1)

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

    async def _insert_to_db(self, session, records: list[dict]):
        """Batch insert/update into DataBase"""
        if not records:
            return

        try:
            for record in records:
                valid_fields = {
                    'from_city', 'to_city', 'year',
                    'air_carrier', 'aircraft_type',
                    'prt', 'seats_available', 'pof'
                }
                filtered_record = {k: v for k, v in record.items() if k in valid_fields}

                conditions = [
                    MyTable.from_city == filtered_record['from_city'],
                    MyTable.to_city == filtered_record['to_city'],
                    MyTable.year == filtered_record['year'],
                    MyTable.air_carrier == filtered_record['air_carrier'],
                    MyTable.aircraft_type == filtered_record['aircraft_type']
                ]

                existing = await session.execute(
                    select(MyTable).where(and_(*conditions))
                )
                existing = existing.scalar_one_or_none()

                if existing:
                    await session.execute(
                        update(MyTable)
                        .where(and_(*conditions))
                        .values(**filtered_record)
                    )
                else:
                    session.add(MyTable(**filtered_record))

            await session.commit()

        except Exception as e:
            await session.rollback()
            logger.critical(f"Critical DB error: {str(e)}", exc_info=True)
