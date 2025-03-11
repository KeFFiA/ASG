import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Column, Integer, String, Float, text
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from Logger import logger

load_dotenv()

engine = create_async_engine(url=os.getenv("DATABASE_URL"), echo=False)
metadata = MetaData()
Base = declarative_base()


class MyTable(Base):
    __tablename__ = "PassengersFlow"
    id = Column(Integer, primary_key=True)
    from_city = Column(String, nullable=False)
    to_city = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    air_carrier = Column(String, nullable=False)
    aircraft_type = Column(String, nullable=False)
    from_state = Column(String, nullable=True, default=None)
    to_state = Column(String, nullable=True, default=None)
    from_territory = Column(String, nullable=True, default=None)
    to_territory = Column(String, nullable=True, default=None)
    prt = Column(Integer, nullable=False)
    number_of_flights = Column(Integer, nullable=True, default=None)
    seats_available = Column(Integer, nullable=False)
    average_seats_available = Column(Integer, nullable=True, default=None)
    pof = Column(Float, nullable=False)
    apof = Column(Float, nullable=True, default=None)


async def check_and_create_table():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

        try:
            await conn.execute(text("""
                        CREATE UNIQUE INDEX IF NOT EXISTS passengers_flow_unique_idx 
                        ON "PassengersFlow" 
                        USING btree (
                            (COALESCE(from_city, 'NULL')),
                            (COALESCE(to_city, 'NULL')),
                            year,
                            (COALESCE(air_carrier, 'NULL')),
                            (COALESCE(aircraft_type, 'NULL')),
                            prt
                        )
                    """))
        except Exception as e:
            logger.critical(f"Index creation warning: {str(e)}", exc_info=True)

    logger.info("Database initialization complete")
