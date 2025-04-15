import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Column, Integer, String, Float, text, Numeric, UniqueConstraint, Boolean
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from Utills.Logger import logger

load_dotenv()

engine = create_async_engine(url=os.getenv("DATABASE_URL_API"), echo=False)
metadata = MetaData()
Base = declarative_base()

class CountriesISO(Base):
    __tablename__ = "countries_iso"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    alpha3_code = Column(String(3), unique=True, nullable=False)

    def __repr__(self):
        return f"<Country(name='{self.name}', code='{self.alpha3_code}')>"

class Manufacturer(Base):
    __tablename__ = 'manufacturers'
    manufacturer_code = Column(String, primary_key=True)
    types = Column(Integer)

class AircraftType(Base):
    __tablename__ = 'aircraft_types'
    id = Column(Integer, primary_key=True, autoincrement=True)
    manufacturer_code = Column(String)
    model_no = Column(String)
    model_name = Column(String)
    model_version = Column(String)
    engine_count = Column(String)
    engine_type = Column(String)
    aircraft_desc = Column(String)
    description = Column(String)
    wtc = Column(String)
    wtg = Column(String)
    tdesig = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'manufacturer_code', 'model_no', 'model_name', 'model_version',
            name='uq_aircraft_type_unique'
        ),
    )

class Operator(Base):
    __tablename__ = 'operators'
    id = Column(Integer, primary_key=True, autoincrement=True)
    countryName = Column(String)
    countryCode = Column(String)
    operatorName = Column(String)
    operatorCode = Column(String, unique=True)
    telephonyName = Column(String)
    LastModified = Column(String)
    AIRAC = Column(String)

class OperatorRiskProfile(Base):
    __tablename__ = 'operator_risk_profiles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    countryName = Column(String)
    countryCode = Column(String)
    operatorName = Column(String)
    operatorCode = Column(String)
    av_fleet_age = Column(Float)
    aircraft = Column(Integer)
    models = Column(Integer)
    aircraft_over_25y = Column(Integer)
    routes = Column(Integer)
    annual_flights = Column(Integer)
    annual_international_flights = Column(Integer)
    is_iosa_certified = Column(Boolean)
    is_international = Column(Boolean)
    accidents_5y = Column(Integer)
    fatalaccidents_5y = Column(Integer)
    connections = Column(Integer)
    destinations = Column(Integer)


    __table_args__ = (
        UniqueConstraint(
            'operatorCode', 'aircraft', 'models',
            name='uq_risk_profile_unique'
        ),
    )

class LocationIndicator(Base):
    __tablename__ = 'location_indicators'
    id = Column(Integer, primary_key=True, autoincrement=True)
    Terr_code = Column(String)
    State_Name = Column(String)
    ICAO_Code = Column(String)
    AFTN = Column(String)
    Location_Name = Column(String)
    Lat = Column(String)
    Long = Column(String)
    Latitude = Column(Float)
    Longitude = Column(Float)
    codcoun = Column(String)
    IATA_Code = Column(String)

    __table_args__ = (
        UniqueConstraint(

        )
    )

class AerodromeLocation(Base):
    __tablename__ = 'aerodrome_locations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String)
    country_code = Column(String)
    airport_name = Column(String)
    city_name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    airport_code = Column(String)
    geometry = Column(String)

class InternationalAerodrome(Base):
    __tablename__ = 'international_aerodromes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String)
    country_code = Column(String)
    airport_name = Column(String)
    city_name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    airport_code = Column(String)
    geometry = Column(String)

class OperationalAerodromeInfo(Base):
    __tablename__ = 'operational_aerodrome_information'
    id = Column(Integer, primary_key=True)
    FIRname = Column(String)
    FIRcode = Column(String)
    region = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)
    proc_runways = Column(Integer)
    countryCode = Column(String)
    iatacode = Column(String)
    is_international = Column(String)
    countryName = Column(String)

class AirportPBNImplementation(Base):
    __tablename__ = 'airport_pbn_implementation'
    id = Column(Integer, primary_key=True)
    countryName = Column(String)
    countryCode = Column(String)
    airportName = Column(String)
    cityName = Column(String)
    airportCode = Column(String)
    nb_instr_vg_runways = Column(Integer)
    nb_instr_runways = Column(Integer)
    pbn_implementation = Column(Integer)
    pc_pbn_lnav = Column(Integer)
    pc_pbn_lnavvnav = Column(Integer)
    pc_pbn_lpv = Column(Integer)
    pc_pbn_rnpar = Column(Integer)
    pc_pbn_unknown = Column(Integer)
    Year = Column(Integer)
    State = Column(String)
    IsInternational = Column(String)

class InternationalAirportSafety(Base):
    __tablename__ = 'international_airport_safety'
    id = Column(Integer, primary_key=True)
    countryName = Column(String)
    countryCode = Column(String)
    airportName = Column(String)
    cityName = Column(String)
    airportCode = Column(String)
    airnavigation_ei = Column(Float)
    airnavigation_margin = Column(Float)
    hasFullInstrumentVG = Column(String)
    hasInstrumentVG = Column(String)
    hasInstrument = Column(String)
    IMC = Column(Float)
    elevation = Column(Float)
    TerrainAbove300m = Column(String)
    TerrainAbove600m = Column(String)
    TerrainAbove900m = Column(String)
    hasIntersectingRWYs = Column(String)

class METARProviderLocation(Base):
    __tablename__ = 'metar_provider_location'
    id = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    countryCode = Column(String)
    is_international = Column(String)
    countryName = Column(String)

class Accident(Base):
    __tablename__ = 'accidents'
    id = Column(Integer, primary_key=True)
    Date = Column(String)
    StateOfOccurrence = Column(String)
    Location = Column(String)
    Model = Column(String)
    Registration = Column(String)
    Operator = Column(String)
    StateOfOperator = Column(String)
    StateOfRegistry = Column(String)
    FlightPhase = Column(String)
    Class = Column(String)
    Fatalities = Column(Integer)
    Over2250 = Column(Boolean)
    Over5700 = Column(Boolean)
    ScheduledCommercial = Column(Boolean)
    InjuryLevel = Column(String)
    TypeDesignator = Column(String)
    Helicopter = Column(Boolean)
    Airplane = Column(Boolean)
    Engines = Column(Integer)
    EngineType = Column(Integer)
    Official = Column(String)
    OccCats = Column(String)
    Year = Column(Integer)

class SafetyRelatedOccurrence(Base):
    __tablename__ = 'safety_related_occurrences'
    id = Column(Integer, primary_key=True)
    Date = Column(String)
    StateOfOccurrence = Column(String)
    Location = Column(String)
    Model = Column(String)
    Registration = Column(String)
    Operator = Column(String)
    StateOfOperator = Column(String)
    StateOfRegistry = Column(String)
    FlightPhase = Column(String)
    Class = Column(String)
    Fatalities = Column(Integer)
    Over2250 = Column(Boolean)
    Over5700 = Column(Boolean)
    ScheduledCommercial = Column(Boolean)
    InjuryLevel = Column(String)
    TypeDesignator = Column(String)
    Helicopter = Column(Boolean)
    Airplane = Column(Boolean)
    Engines = Column(Integer)
    EngineType = Column(Integer)
    Official = Column(String)
    OccCats = Column(String)
    Year = Column(Integer)

class Incident(Base):
    __tablename__ = 'incidents'
    id = Column(Integer, primary_key=True)
    Date = Column(String)
    StateOfOccurrence = Column(String)
    Location = Column(String)
    Model = Column(String)
    Registration = Column(String)
    Operator = Column(String)
    StateOfOperator = Column(String)
    StateOfRegistry = Column(String)
    FlightPhase = Column(String)
    Class = Column(String)
    Fatalities = Column(Integer)
    Over2250 = Column(Boolean)
    Over5700 = Column(Boolean)
    ScheduledCommercial = Column(Boolean)
    InjuryLevel = Column(String)
    TypeDesignator = Column(String)
    Helicopter = Column(Boolean)
    Airplane = Column(Boolean)
    Engines = Column(Integer)
    EngineType = Column(Integer)
    Official = Column(String)
    Year = Column(Integer)


async def check_and_create_table_api():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
        await conn.close()

    logger.info("Database API initialization complete")


if __name__ == "__main__":
    asyncio.run(check_and_create_table_api())
