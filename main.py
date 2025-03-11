import asyncio
import os

from DATABASE import check_and_create_table
from DataProcessor import DataProcessor
from FindPath import Finder
from dotenv import load_dotenv
from Logger import logger

load_dotenv()


async def main():
    logger.info("Starting database initialization")
    await check_and_create_table()

    logger.info("Starting finder initialization")
    finder = Finder()
    files_list = await finder.all_data()
    logger.info(f"Found {len(files_list)} files")

    logger.info("Starting data processor initialization")
    processor = DataProcessor(db_url=os.getenv("DATABASE_URL"), max_workers=os.cpu_count() * 2, chunk_size=5000)
    logger.info("Data processor initialized")
    logger.info("Starting data processor loop")
    await processor.process_files(file_paths=files_list)
    logger.info("Data processor loop completed")


if __name__ == "__main__":
    asyncio.run(main())
