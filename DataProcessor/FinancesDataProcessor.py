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

from Utills.Logger import logger
from Utills import StateManager as state
from DATABASE import ASGFinancesTable

warnings.filterwarnings(
    'ignore',
    category=UserWarning,
    module=Stylesheet.__module__
)


class FinancialDataProcessor:
    def __init__(self, db_url: str, max_workers: int = 4, chunk_size: int = 500):
        self.db_url = db_url
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
        self.progress = None
        self.chunk_size = chunk_size
        self.errors: dict = {"FAILED": [], "FAILED_DATA": []}

    async def process_files(self, file_paths: list[str]):
        engine = create_async_engine(self.db_url)
        async_session = async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession
        )

        with tqdm(total=len(file_paths), desc="[Finances]File processing") as self.progress:
            tasks = [self._process_file(async_session, file_path) for file_path in file_paths]
            await asyncio.gather(*tasks)

        await engine.dispose()

    async def _process_file(self, async_session, file_path: str):
        async with self.semaphore:
            try:
                if psutil.virtual_memory().percent > 80:
                    await asyncio.sleep(1)

                loop = asyncio.get_running_loop()

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=UserWarning)
                    df = await loop.run_in_executor(
                        None,
                        lambda: pd.read_excel(
                            file_path,
                            header=[0, 1],  # Read two lines of headings
                            engine='openpyxl'
                        )
                    )

                processed_df = await self._transform_data(df)
                records = processed_df.to_dict('records')

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
        # Multi-column index processing
        columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                if pd.isna(col[1]):
                    columns.append(col[0].strip().lower().replace(' ', '_'))
                else:
                    columns.append(col)
            else:
                columns.append(col)

        df.columns = columns

        # Converting wide format to long format
        id_vars = [
            'financial category',
            'main account',
            'sub-account'
        ]

        value_vars = [col for col in df.columns if col not in id_vars]

        melted_df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='year_airline',
            value_name='value'
        )

        # Splitting the year and airline
        melted_df[['year', 'air_carrier']] = melted_df['year_airline'].apply(
            lambda x: pd.Series([x[0], x[1]])
        )
        melted_df.drop('year_airline', axis=1, inplace=True)

        # Data cleaning
        melted_df = melted_df.rename(columns={
            'financial category': 'financial_category',
            'main account': 'main_account',
            'sub-account': 'sub_account'
        })

        melted_df['year'] = pd.to_numeric(melted_df['year'], errors='coerce')
        melted_df['value'] = pd.to_numeric(melted_df['value'], errors='coerce').fillna(0)
        melted_df['air_carrier'] = melted_df['air_carrier'].str.strip()

        return melted_df[['year', 'air_carrier', 'financial_category','main_account', 'sub_account', 'value']]

    async def _insert_to_db(self, session, records: list[dict]):
        if not records:
            return

        try:
            for record in records:
                try:
                    valid_fields = {
                        'year', 'air_carrier', 'financial_category',
                        'main_account', 'sub_account', 'value'
                    }
                    filtered_record = {k: v for k, v in record.items() if k in valid_fields}

                    required_fields = valid_fields
                    missing_fields = [field for field in required_fields if field not in filtered_record]

                    if missing_fields:
                        raise KeyError(f"Missing fields: {', '.join(missing_fields)}")

                    conditions = [
                        ASGFinancesTable.year == filtered_record['year'],
                        ASGFinancesTable.air_carrier == filtered_record['air_carrier'],
                        ASGFinancesTable.financial_category == filtered_record['financial_category'],
                        ASGFinancesTable.main_account == filtered_record['main_account'],
                        ASGFinancesTable.sub_account == filtered_record['sub_account']
                    ]

                    existing = await session.execute(
                        select(ASGFinancesTable).where(and_(*conditions))
                    )
                    existing = existing.scalars().first()

                    if existing:
                        await session.execute(
                            update(ASGFinancesTable)
                            .where(and_(*conditions))
                            .values(value=filtered_record['value'])
                        )
                    else:
                        session.add(ASGFinancesTable(**filtered_record))

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
