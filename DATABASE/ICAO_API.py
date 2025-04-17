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


class AerodromeLocation(Base):
    __tablename__ = 'aerodrome_locations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    countryName = Column(String)
    countryCode = Column(String)
    airportName = Column(String)
    cityName = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    airportCode = Column(String)
    geometry = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'airportCode', 'countryCode',
            name='uq_aerodrome_location_unique'
        ),
    )


class InternationalAerodrome(Base):
    __tablename__ = 'international_aerodromes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    countryName = Column(String)
    countryCode = Column(String)
    airportName = Column(String)
    cityName = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    airportCode = Column(String)
    geometry = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'airportCode', 'countryCode',
            name='uq_international_aerodrome_unique'
        ),
    )


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
    is_international = Column(Boolean)
    countryName = Column(String)
    airportCode = Column(String)
    airportName = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'airportCode',
            name='uq_operational_aerodrome_info_unique'
        ),
    )


class AirportPBNImplementation(Base):
    __tablename__ = 'airport_pbn_implementation'
    id = Column(Integer, primary_key=True)
    countryName = Column(String)
    countryCode = Column(String)
    airportName = Column(String)
    cityName = Column(String, default=None)
    airportCode = Column(String)
    nb_instr_vg_runways = Column(Integer, default=None)
    nb_instr_runways = Column(Integer, default=None)
    pbn_implementation = Column(Integer, default=None)
    pc_pbn_lnav = Column(Integer, default=None)
    pc_pbn_lnavvnav = Column(Integer, default=None)
    pc_pbn_lpv = Column(Integer, default=None)
    pc_pbn_rnpar = Column(Integer, default=None)
    pc_pbn_unknown = Column(Integer, default=None)
    Year = Column(Integer)
    IsInternational = Column(Boolean)

    __table_args__ = (
        UniqueConstraint(
            'airportCode',
            name='uq_airport_pbn_imp_unique'
        ),
    )


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
    hasFullInstrumentVG = Column(Boolean)
    hasInstrumentVG = Column(Boolean)
    hasInstrument = Column(Boolean)
    IMC = Column(Float)
    elevation = Column(Float)
    TerrainAbove300m = Column(Float)
    TerrainAbove600m = Column(Float)
    TerrainAbove900m = Column(Float)
    hasIntersectingRWYs = Column(Boolean)

    __table_args__ = (
        UniqueConstraint(
            'airportCode',
            name='uq_int_airport_safety_unique'
        ),
    )


class METARProviderLocation(Base):
    __tablename__ = 'metar_provider_location'
    id = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    countryCode = Column(String)
    is_international = Column(Boolean)
    countryName = Column(String)
    airportName = Column(String)
    airportCode = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'airportCode',
            name='uq_metar_provider_unique'
        ),
    )


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
    EngineType = Column(String)
    Official = Column(String)
    Risk = Column(String)
    OccCats = Column(String)
    Year = Column(Integer)

    __table_args__ = (
        UniqueConstraint(
            'Date', 'Location', 'Model', 'Registration', 'StateOfOccurrence',
            name='uq_accident_unique'
        ),
    )


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
    EngineType = Column(String)
    Official = Column(String)
    OccCats = Column(String)
    Risk = Column(String)
    Year = Column(Integer)

    __table_args__ = (
        UniqueConstraint(
            'Date', 'Location', 'Model', 'Registration', 'StateOfOccurrence',
            name='uq_safety_related_occ_unique'
        ),
    )


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
    EngineType = Column(String)
    Official = Column(String)
    OccCats = Column(String)
    Risk = Column(String)
    Year = Column(Integer)

    __table_args__ = (
        UniqueConstraint(
            'Date', 'Location', 'Model', 'Registration', 'StateOfOccurrence',
            name='uq_incidents_unique'
        ),
    )


class ICAOMemberState(Base):
    __tablename__ = "icao_member_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    RASG = Column(String, index=True)
    iso_2_code = Column(String(2))
    iso_3_code = Column(String(3), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    UN_numerical_code = Column(String)
    UN_region = Column(String)
    UN_state_name = Column(String)
    UN_state_name_html = Column(String)
    ICAO_regional_office = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'UN_numerical_code',
            name='uq_members_unique'
        ),
    )

class StateOfRegistry(Base):
    __tablename__ = "state_of_registries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    RASG = Column(String, index=True)
    iso_2_code = Column(String(2))
    iso_3_code = Column(String(3), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    UN_numerical_code = Column(String)
    UN_region = Column(String)
    UN_state_name = Column(String)
    UN_state_name_html = Column(String)
    ICAO_regional_office = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'UN_numerical_code',
            name='uq_state_of_registry_unique'
        ),
    )


class ASIAPPrioritization(Base):
    __tablename__ = "asiap_prioritization"

    id = Column(Integer, primary_key=True, autoincrement=True)
    iso_2_code = Column(String(2))
    iso_3_code = Column(String(3), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    UN_numerical_code = Column(String)
    UN_region = Column(String)
    UN_state_name = Column(String)
    UN_state_name_html = Column(String)
    ro = Column(String, index=True)
    wgi_year = Column(Integer)
    gdp = Column(Float)
    gdp_pcapita = Column(Float)
    corruption = Column(Float)
    stability = Column(Float)
    operations_ei = Column(Float)
    support_ei = Column(Float)
    airnavigation_ei = Column(Float)
    operations_margin = Column(Float)
    support_margin = Column(Float)
    airnavigation_margin = Column(Float)
    isSSC = Column(Boolean)
    SSC_area = Column(String)

    __table_args__ = (
        UniqueConstraint(
            'UN_numerical_code',
            name='uq_asiap_unique'
        ),
    )


class StateSafetyMargin(Base):
    __tablename__ = "state_safety_margins"

    id = Column(Integer, primary_key=True)
    State = Column(String(3), index=True)
    Name = Column(String)
    operations_ei = Column(Float)
    support_ei = Column(Float)
    airnavigation_ei = Column(Float)
    departures = Column(Integer)
    flagcarrier_flights = Column(Integer)
    operations_margin = Column(Float)
    support_margin = Column(Float)
    airnavigation_margin = Column(Float)
    operations_index = Column(Float)
    support_index = Column(Float)
    airnavigation_index = Column(Float)

    __table_args__ = (
        UniqueConstraint(
            'State',
            name='uq_state_safety_unique'
        ),
    )


class SSPFoundation(Base):
    __tablename__ = "ssp_foundation"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    State = Column(String(3), index=True)
    OverallSSPFoundation = Column(Float)
    OverallCapCompleted = Column(Float)
    OverallValidated = Column(Float)
    Accidentandincidentinvestigation = Column(Float)
    Delegation = Column(Float)
    Enforcement = Column(Float)
    Exemptions = Column(Float)
    Hazardidentificationandsafetyriskassessment = Column(Float)
    Licensingcertificationauthorizationandapprovalobligations = Column(Float)
    Managementofsafetyrisks = Column(Float)
    Primaryaviationlegislation = Column(Float)
    Qualifiedtechnicalpersonnel = Column(Float)
    Resources = Column(Float)
    Specificoperatingregulations = Column(Float)
    StateAuthorities = Column(Float)
    StateOrganizationalStructure = Column(Float)
    Statefunctions = Column(Float)
    Statesafetypromotion = Column(Float)
    Surveillanceobligations = Column(Float)
    Technicalguidancetoolsandprovisionofsafetycriticalinformation = Column(Float)

    __table_args__ = (
        UniqueConstraint(
            'State',
            name='uq_ssp_foundation_unique'
        ),
    )


class AerodromeStatistic(Base):
    __tablename__ = "aerodrome_statistics"

    id = Column(Integer, primary_key=True)
    State = Column(String(3), index=True)
    Name = Column(String)
    Year = Column(Integer)
    Departures = Column(Integer)
    Int_departures = Column(Integer)
    All_active_aerodromes = Column(Integer)
    Int_active_aerodromes = Column(Integer)

    __table_args__ = (
        UniqueConstraint(
            'State',
            name="uq_aerodrome_statistic_unique"
        )
    )


class OperatorStatistic(Base):
    __tablename__ = "operator_statistics"

    id = Column(Integer, primary_key=True)
    State = Column(String(3), index=True)
    Name = Column(String)
    Year = Column(Integer)
    Flights = Column(Integer)
    Int_flights = Column(Integer)
    All_active_operators = Column(Integer)
    Int_active_operators = Column(Integer)

    __table_args__ = (
        UniqueConstraint(
            'State',
            name="uq_operator_statistic_unique"
        )
    )


class Connection(Base):
    __tablename__ = "connections"

    id = Column(Integer, primary_key=True)
    state_a = Column(String(3), index=True)
    name_a = Column(String)
    state_b = Column(String(3))
    name_b = Column(String)
    year = Column(Integer)
    flights = Column(Integer)
    state_a_carrier_flights = Column(Integer)
    state_b_carrier_flights = Column(Integer)
    other_state_carrier_flights = Column(Integer)


class StateTrafficStatistic(Base):
    __tablename__ = "state_traffic_statistics"

    id = Column(Integer, primary_key=True)
    state = Column(String(3), index=True)
    name = Column(String)
    year = Column(Integer)
    departures = Column(Integer)
    domestic = Column(Integer)
    international = Column(Integer)
    flagcarrier_flights = Column(Integer)


class CAAHR(Base):
    __tablename__ = "caahr"

    id = Column(Integer, primary_key=True)
    iso_2_code = Column(String(2), index=True)
    iso_3_code = Column(String(3), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    un_numerical_code = Column(String)
    un_region = Column(String)
    un_state_name = Column(String)
    un_state_name_html = Column(String)
    ro = Column(String)
    aeroplane_cat_ops = Column(Integer)
    aeroplane_used_cat = Column(Integer)
    approved_maintenance = Column(Integer)
    ifr_aerodromes = Column(Integer)
    atc_training_org = Column(Integer)
    atc_licenses = Column(Integer)
    fto = Column(Integer)
    mto = Column(Integer)
    private_licences = Column(Integer)
    professional_licences = Column(Integer)
    maintenance_licences = Column(Integer)
    total_air = Column(Integer)
    total_aga = Column(Integer)
    total_ans = Column(Integer)
    total_pel = Column(Integer)
    total_ops = Column(Integer)
    is_original_survey = Column(Boolean)


class SafetyPartnerProgram(Base):
    __tablename__ = "safety_partner_programs"

    id = Column(Integer, primary_key=True)
    state = Column(String(3), index=True)
    name = Column(String)
    iosa_operators = Column(Integer)
    is_faa_cat2 = Column(Boolean)
    has_eu_restrictions = Column(Boolean)
    faa_update = Column(String)
    eu_update = Column(String)


class SignificantSafetyConcern(Base):
    __tablename__ = "significant_safety_concerns"

    id = Column(Integer, primary_key=True)
    state = Column(String(3), index=True)
    name = Column(String)
    year = Column(Integer)
    area = Column(String)


async def check_and_create_table_api():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
        await conn.close()

    logger.info("Database API initialization complete")


if __name__ == "__main__":
    asyncio.run(check_and_create_table_api())
