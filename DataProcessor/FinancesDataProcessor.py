import asyncio
from typing import List, Dict
import re

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from tqdm import tqdm
import logging

from DATABASE import ASGFinancesTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FinancialDataProcessor:
    def __init__(self, db_url: str, max_workers: int = 4):
        self.engine = create_async_engine(db_url)
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        self.semaphore = asyncio.Semaphore(max_workers)
        self.errors = {
            'failed_files': [],
            'failed_records': []
        }

    async def process_files(self, file_paths: List[str]):
        with tqdm(total=len(file_paths), desc="[Financial]Processing files") as pbar:
            tasks = [self._process_file_wrapper(fp, pbar) for fp in file_paths]
            await asyncio.gather(*tasks)

        await self.engine.dispose()
        logger.info("Processing completed. Errors: %s", self.errors)

    async def _process_file_wrapper(self, file_path: str, pbar: tqdm):
        async with self.semaphore:
            await self._process_file(file_path)
            pbar.update(1)

    async def _process_file(self, file_path: str):
        async with self.async_session() as session:
            try:
                df = await self._read_excel(file_path)
                records = await self._transform_data(df)
                await self._bulk_upsert(session, records)

            except Exception as e:
                await session.rollback()
                logger.error(f"Error processing {file_path}: {str(e)}")
                self.errors['failed_files'].append(file_path)

    async def _read_excel(self, file_path: str) -> pd.DataFrame:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: pd.read_excel(file_path, header=[0, 1], engine='openpyxl')
        )

    async def _transform_data(self, df: pd.DataFrame) -> List[Dict]:
        """Data Transformation with Improved Column Handling"""
        # Normalize column names
        df.columns = [self._normalize_column_name(col) for col in df.columns]

        # Search for required columns by patterns
        column_map = {
            'financial_category': r'financial.*category',
            'main_account': r'main.*account',
            'sub_account': r'sub.*account'
        }

        found_columns = {}
        for col in df.columns:
            for key, pattern in column_map.items():
                if re.search(pattern, col, re.IGNORECASE):
                    found_columns[key] = col
                    break

        # Checking mandatory columns
        required = ['financial_category', 'main_account']
        missing = [col for col in required if col not in found_columns]
        if missing:
            raise ValueError(
                f"Missing required columns: {missing}. "
                f"Found columns: {df.columns}"
            )

            # Add sub_account if it is missing
        if 'sub_account' not in found_columns:
            df['sub_account'] = 'Not Specified'
            found_columns['sub_account'] = 'sub_account'

        # Data processing
        return self._process_rows(df, found_columns)

    def _normalize_column_name(self, col: str) -> str:
        """Normalize column name"""
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col if str(c).strip())
        return re.sub(r'[^\w]', '', col.lower().replace(' ', '_'))

    def _process_rows(self, df: pd.DataFrame, column_map: dict) -> List[Dict]:
        """String processing with improved parsing"""
        records = []
        financial_col = column_map['financial_category']
        main_account_col = column_map['main_account']
        sub_account_col = column_map['sub_account']

        for _, row in df.iterrows():
            try:
                base_record = {
                    'financial_category': str(row[financial_col]).strip(),
                    'main_account': str(row[main_account_col]).strip(),
                    'sub_account': str(row.get(sub_account_col, 'Not Specified')).strip()
                }

                # Processing Numeric Columns
                for col in df.columns:
                    if col in column_map.values():
                        continue

                    year, airline = self._parse_column_name(col)
                    if not year or not airline:
                        continue

                    value = pd.to_numeric(row[col], errors='coerce')
                    if pd.isna(value):
                        continue

                    records.append({
                        **base_record,
                        'year': int(year),
                        'air_carrier': airline.strip(),
                        'value': float(value)
                    })

            except Exception as e:
                self.errors['failed_records'].append((dict(row), str(e)))

        return records

    def _parse_column_name(self, col: str) -> tuple:
        """Improved parsing of column names"""
        # Remove special characters
        clean_col = re.sub(r'[^\w\s]', '', str(col))
        parts = re.split(r'_|\s+', clean_col.strip())

        # Find the year in different formats
        year = next((p for p in parts if p.isdigit() and len(p) == 4), None)
        if year:
            airline = ' '.join(p for p in parts if p != year)
            return year, airline

        return None, None

    async def _bulk_upsert(self, session: AsyncSession, records: List[Dict]):
        if not records:
            return

        try:
            max_params_per_chunk = 30000
            fields_per_record = 6
            safe_chunk_size = max_params_per_chunk // fields_per_record

            for i in range(0, len(records), safe_chunk_size):
                chunk = records[i:i + safe_chunk_size]

                stmt = insert(ASGFinancesTable).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    constraint='unique_finance_record',
                    set_={"value": stmt.excluded.value}
                )

                await session.execute(stmt)
                await session.commit()

        except Exception as e:
            await session.rollback()
            logger.error(f"Bulk upsert error: {str(e)}")
