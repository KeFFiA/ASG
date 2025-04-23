import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Column, Integer, String, Float, text, Numeric, UniqueConstraint
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from Utills.Logger import logger

load_dotenv()

engine = create_async_engine(url=os.getenv("DATABASE_URL"), echo=False)
metadata = MetaData()
Base = declarative_base()


class ASGPassengersTable(Base):
    __tablename__ = "passengersflow"

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_city = Column(String, nullable=False, default=None)
    to_city = Column(String, nullable=False, default=None)
    year = Column(Integer, nullable=False, default=None)
    air_carrier = Column(String, nullable=False, default=None)
    aircraft_type = Column(String, nullable=False, default=None)
    from_state = Column(String, nullable=True, default=None)
    to_state = Column(String, nullable=True, default=None)
    from_territory = Column(String, nullable=True, default=None)
    to_territory = Column(String, nullable=True, default=None)
    prt = Column(Integer, nullable=True, default=None)
    number_of_flights = Column(Integer, nullable=True, default=None)
    seats_available = Column(Integer, nullable=True, default=None)
    average_seats_available = Column(Integer, nullable=True, default=None)
    passenger_occupancy_factor = Column(Float, nullable=True, default=None)
    average_payload_capacity = Column(Float, nullable=True, default=None)

    __table_args__ = (
        UniqueConstraint(
            'from_city',
            'to_city',
            'year',
            'air_carrier',
            'aircraft_type',
            name='unique_passengers_record'
        ),
    )


class ASGFinancesTable(Base):
    __tablename__ = 'finances'

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    air_carrier = Column(String(255), nullable=False)
    financial_category = Column(String(255), nullable=False)
    main_account = Column(String(255), nullable=False)
    sub_account = Column(String(255), nullable=False)
    value = Column(Numeric(32, 2), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'year',
            'air_carrier',
            'financial_category',
            'main_account',
            'sub_account',
            name='unique_finance_record'
        ),
    )



async def check_and_create_table():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
        await conn.close()

    logger.info("Database initialization complete")
