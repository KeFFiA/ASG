import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from Utills.Logger import logger

load_dotenv()

engine = create_async_engine(url=os.getenv("DATABASE_URL_FR"), echo=False)
metadata = MetaData()
Base = declarative_base()


class FlightSummary(Base):
    __tablename__ = 'flight_summary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fr24_id = Column(String)
    flight = Column(String)
    callsign = Column(String)
    operating_as = Column(String)
    painted_as = Column(String)
    type = Column(String)
    reg = Column(String)
    orig_icao = Column(String)
    orig_iata = Column(String)
    datetime_takeoff = Column(DateTime)
    runway_takeoff = Column(String)
    dest_icao = Column(String)
    dest_iata = Column(String)
    dest_icao_actual = Column(String)
    dest_iata_actual = Column(String)
    datetime_landed = Column(DateTime)
    runway_landed = Column(String)
    flight_time = Column(Integer)
    actual_distance = Column(Float)
    circle_distance = Column(Float)
    category = Column(String)
    hex = Column(String)
    first_seen = Column(DateTime)
    last_seen = Column(DateTime)
    flight_ended = Column(Boolean)


class LivePositions(Base):
    __tablename__ = 'live_positions'
    fr24_id = Column(String, primary_key=True)
    flight = Column(String)
    callsign = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    track = Column(Integer)
    alt = Column(Float)
    gspeed = Column(Float)
    vspeed = Column(Float)
    squawk = Column(Integer)
    timestamp = Column(DateTime)
    source = Column(String)
    hex = Column(String)
    type = Column(String)
    reg = Column(String)
    orig_icao = Column(String)
    orig_iata = Column(String)
    dest_icao = Column(String)
    dest_iata = Column(String)
    operating_as = Column(String)
    painted_as = Column(String)
    eta = Column(DateTime)



async def check_and_create_table():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
        await conn.close()

    logger.info("Database initialization complete")


if __name__ == '__main__':
    asyncio.run(check_and_create_table())


