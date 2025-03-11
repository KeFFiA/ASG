import asyncio
import os
from contextlib import asynccontextmanager

from DATABASE import check_and_create_table
from DataProcessor import DataProcessor
from FindPath import Finder
from dotenv import load_dotenv
from Logger import logger

load_dotenv()

from fastapi import FastAPI, HTTPException
import asyncpg


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(run_main())
    yield


app = FastAPI(lifespan=lifespan)
app.state.processing = False
app.state.last_error = None
app.state.db_connected = False


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
        app.state.last_error = str(e)
        return False


@app.get("/health")
async def health_check():
    db_status = await check_db_connection()
    health_status = {
        "status": "ok",
        "details": {
            "database": "active" if db_status else "inactive",
            "processing": "running" if app.state.processing else "idle"
        }
    }

    if not db_status or app.state.last_error:
        health_status["status"] = "error"
        health_status["details"]["error"] = app.state.last_error
        raise HTTPException(status_code=500, detail=health_status)

    if not app.state.processing:
        health_status["status"] = "warning"
        health_status["details"]["message"] = "Processing not running"

    return health_status


async def run_main():
    try:
        logger.info("Starting database initialization")
        await check_and_create_table()

        logger.info("Starting finder initialization")
        finder = Finder()
        files_list = await finder.all_data()
        logger.info(f"Found {len(files_list)} files")

        logger.info("Starting data processor initialization")
        processor = DataProcessor(
            db_url=os.getenv("DATABASE_URL"),
            max_workers=os.cpu_count() * 2,
            chunk_size=5000
        )
        logger.info("Data processor initialized")

        logger.info("Starting data processor loop")
        app.state.processing = True
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
            app.state.processing = True

    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        app.state.processing = False


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
