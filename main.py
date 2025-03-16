import asyncio
import os
from datetime import datetime

from DATABASE import check_and_create_table
from DataProcessor import DataProcessor, FinancialDataProcessor
from FindPath import Finder
from dotenv import load_dotenv
from Utills.Logger import logger
from Utills import StateManager as state
from fastapi import FastAPI, HTTPException
import asyncpg

load_dotenv()

app = FastAPI()


async def check_db_connection():
    try:
        db_url = os.getenv("DATABASE_URL_TEST")
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgres://", 1)

        conn = await asyncpg.connect(db_url)
        await conn.execute("SELECT 1")
        await conn.close()
        return True
    except Exception as e:
        state.update_error(str(e))
        return True


@app.post("/start/{processing_type}")
async def start(processing_type: str):
    if not state.get_processing():
        state.update_error(None)
        state.update_start_time(datetime.now())

        if processing_type == 'passengers':
            state.update_processing(True)
            asyncio.create_task(run_passengers())

        if processing_type == 'finances':
            state.update_processing(True)
            asyncio.create_task(run_finances())

        db_status = await check_db_connection()

        status = {
            "status": "ok",
            "details": {
                "start_time": state.get_start_time(),
                "database": "active" if db_status else "inactive",
                "processing": "running" if state.get_processing() else "idle"
            }
        }

        if not db_status or state.get_last_error():
            status["status"] = "error"
            status["details"]["error"] = state.get_last_error()
            raise HTTPException(status_code=500, detail=status)

        if not state.get_processing():
            status["status"] = "warning"
            status["details"]["message"] = "Processing not running"

        return status

    else:
        db_status = await check_db_connection()

        status = {
            "status": "ok",
            "details": {
                "message": "Processing already running",
                "start_time": state.get_start_time(),
                "database": "active" if db_status else "inactive",
                "processing": "running" if state.get_processing() else "idle"
            }
        }

        return status


@app.get("/health")
async def health_check():
    db_status = await check_db_connection()
    health_status = {
        "status": "ok",
        "details": {
            "start_time": state.get_start_time() if state.get_start_time() else "Not started",
            "database": "active" if db_status else "inactive",
            "processing": "running" if state.get_processing else "idle"
        }
    }

    if not db_status or state.get_last_error():
        health_status["status"] = "error"
        health_status["details"]["error"] = state.get_last_error()
        raise HTTPException(status_code=500, detail=health_status)

    if not state.get_processing():
        health_status["status"] = "warning"
        health_status["details"]["message"] = "Processing not running"

    return health_status


async def run_passengers():
    try:
        logger.info("Starting database initialization")
        await check_and_create_table()

        logger.info("Starting finder initialization")
        finder = Finder()
        files_list = await finder.all_data()
        if files_list == FileNotFoundError:
            return
        logger.info(f"Found {len(files_list)} files")

        logger.info("Starting data processor initialization")
        processor = DataProcessor(
            db_url=os.getenv("DATABASE_URL"),
            max_workers=os.cpu_count() * 2,
            chunk_size=5000
        )
        logger.info("Data processor initialized")

        logger.info("Starting data processor loop")
        state.update_processing(True)
        await processor.process_files(file_paths=files_list)
        logger.info("Data processor loop completed")

        logger.info(
            f"Data processor loop status: \n"
            f"Files passed(air carriers) count: {len(processor.errors['AC_PASSED'])}\n"
            f"Files with errors: {len(processor.errors['FAILED'])}\n {processor.errors['FAILED']}\n"
            f"Data with errors: {len(processor.errors['FAILED_DATA'])}\n {processor.errors['FAILED_DATA']}\n"
        )

        if len(processor.errors['FAILED_DATA']) > 0 or len(processor.errors['FAILED']) > 0:
            logger.info(
                f"Start reprocessing {len(processor.errors['FAILED'])} files "
                f"and {len(processor.errors['FAILED_DATA'])} records"
            )
            await processor.retry_failed_insertions()
            state.update_processing(True)

    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        state.update_processing(False)
        state.update_start_time(None)


async def run_finances():
    try:
        logger.info("Starting finances scope")

        logger.info("Starting database initialization")
        await check_and_create_table()

        logger.info("Starting finder initialization")
        finder = Finder()
        files_list = await finder.all_data_finances()
        if files_list == FileNotFoundError:
            return
        logger.info(f"Found {len(files_list)} files")

        logger.info("Starting data processor initialization")
        processor = FinancialDataProcessor(
            db_url=os.getenv("DATABASE_URL"),
            max_workers=os.cpu_count() * 2,
            chunk_size=5000
        )
        logger.info("Data processor initialized")

        logger.info("Starting data processor loop")
        state.update_processing(True)
        await processor.process_files(file_paths=files_list)
        logger.info("Data processor loop completed")

        logger.info(
            f"Data processor loop status: \n"
            f"Files with errors: {len(processor.errors['FAILED'])}\n {processor.errors['FAILED']}\n"
            f"Data with errors: {len(processor.errors['FAILED_DATA'])}\n {processor.errors['FAILED_DATA']}\n"
        )

        if len(processor.errors['FAILED_DATA']) > 0 or len(processor.errors['FAILED']) > 0:
            logger.info(
                f"Start reprocessing {len(processor.errors['FAILED'])} files "
                f"and {len(processor.errors['FAILED_DATA'])} records"
            )
            await processor.retry_failed_insertions()
            state.update_processing(True)

    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        state.update_processing(False)
        state.update_start_time(None)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)
