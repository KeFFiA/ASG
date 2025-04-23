import json
import os
import ssl
from enum import Enum
from typing import Type, Any

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from Utills.Logger import logger
from enums import ICAOEndpoints

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

load_dotenv()


class ApiClient:
    def __init__(self, session: AsyncSession, headers: dict, base_url: str, api_key: str = None):
        self.session = session
        self.headers = headers
        self.API_KEY = api_key
        self.BASE_URL = base_url

    async def _fetch(self, endpoint: str, params: dict) -> list[dict]:
        if self.API_KEY is not None:
            params["api_key"] = self.API_KEY
            params["format"] = "json"
            params.pop("callback", None)

        url = f"{self.BASE_URL}/{endpoint}"
        logger.info(f"Sending request to: {url} with params: {params}")

        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as client:
                async with client.get(url, params=params, headers=self.headers) as response:
                    response.raise_for_status()
                    text = await response.text()
                    json_data = json.loads(text)
                    logger.info(f"✅ Received response from {endpoint}: {len(json_data)} records")
                    return json_data
        except aiohttp.ClientResponseError as e:
            logger.error(f"❌ HTTP error [{e.status}] for {url}: {e.message}")
            raise
        except Exception as e:
            logger.exception(f"❌ Unexpected error while fetching data from {url}: {e}")
            raise

    def _chunked(self, iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    async def _save_to_db(self, data: list[dict], model: Type[Any], conflict_enums: Enum):
        if not data:
            logger.warning("⚠️ No data to save to database.")
            return
        try:
            corrected_data = []
            for item in data:
                corrected_item = {}
                for key, value in item.items():
                    corrected_key = key.strip().replace(" ", "").replace(",", "").replace("-", "")
                    if value == "":
                        value = None
                    if value == "TRUE":
                        value = True
                    if isinstance(value, (dict, list)):
                        try:
                            value = json.dumps(value, ensure_ascii=False)
                        except Exception as json_error:
                            logger.warning(f"⚠️ Failed to convert {corrected_key} to JSON: {json_error}")
                    corrected_item[corrected_key] = value
                corrected_data.append(corrected_item)

            all_keys = set()
            for item in corrected_data:
                all_keys.update(item.keys())
            for item in corrected_data:
                for key in all_keys:
                    item.setdefault(key, None)

            conflict_columns = conflict_enums.value
            valid_columns = set(c.name for c in model.__table__.columns)
            update_columns = [col for col in corrected_data[0].keys() if
                              col in valid_columns and col not in conflict_columns]

            max_params = 32767
            num_columns = len(corrected_data[0])
            chunk_size = max(1, max_params // num_columns)

            total_inserted = 0

            for chunk in self._chunked(corrected_data, chunk_size):
                stmt = insert(model).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_={col: stmt.excluded[col] for col in update_columns}
                )

                if not self.session.in_transaction():
                    async with self.session.begin():
                        await self.session.execute(stmt)
                        await self.session.commit()
                else:
                    await self.session.execute(stmt)
                    await self.session.commit()

                total_inserted += len(chunk)

            logger.info(f"✅ Successfully saved {total_inserted} records to {model.__name__}")
        except Exception as e:
            logger.exception(f"❌ Error while saving to {model.__name__}: {e}")
            raise

    async def fetch_and_store(self, endpoint: Enum, model: Type[Any], conflict_enums: Enum, **kwargs):
        logger.debug(f"Fetching and storing data for endpoint: {endpoint}")
        data = await self._fetch(endpoint.value, kwargs)
        await self._save_to_db(data, model, conflict_enums)
