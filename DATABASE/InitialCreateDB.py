import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Column, Integer, String, Float, text, Numeric
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from Utills.Logger import logger
from Utills import StateManager as state

load_dotenv()

engine = create_async_engine(url=os.getenv("DATABASE_URL"), echo=False)
metadata = MetaData()
Base = declarative_base()


class ASGPassengersTable(Base):
    __tablename__ = "PassengersFlow"

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


class ASGFinancesTable(Base):
    __tablename__ = 'Finances'

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    air_carrier = Column(String(255), nullable=False)
    financial_category = Column(String(255), nullable=False)
    main_account = Column(String(255), nullable=False)
    sub_account = Column(String(255), nullable=False)
    value = Column(Numeric(32, 2), nullable=False)

    def __repr__(self):
        return f"<FinancialRecord({self.year}, {self.air_carrier}, {self.financial_category}, {self.value})>"

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
            state.update_error(str(e))
            logger.warning(f"Index creation warning: {str(e)}")

    logger.info("Database initialization complete")
