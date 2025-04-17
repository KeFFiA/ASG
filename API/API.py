import json

from sqlalchemy import select

from client import ICAOApiClient
from enums import ICAOEndpoints, DatabaseUniqueColumns, manufacturer_list, states_ISO
from DATABASE import *
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

engine = create_async_engine(os.getenv("DATABASE_URL_API"), echo=False)

async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

year_list = [2021, 2022, 2023, 2024, 2025]

async def countries_codes():
    async with async_session() as session:
        stmt = select(CountriesISO.alpha3_code)
        result = await session.execute(stmt)
        codes = result.scalars().all()
        return codes

async def manufacturer():
    for manufacture in manufacturer_list:
        async with async_session() as session:
            client = ICAOApiClient(session)
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.MANUFACTURER_LIST,
                model=Manufacturer,
                conflict_enums=DatabaseUniqueColumns.MANUFACTURER_CODE,
                manufacturer=manufacture
            )


async def type_designators():
    async with async_session() as session:
        client = ICAOApiClient(session)
        for manufacture in manufacturer_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.AIRCRAFT_TYPE_DESIGNATORS,
                model=AircraftType,
                conflict_enums=DatabaseUniqueColumns.AIRCRAFT_TYPE_DESIGNATORS,
                manufacturer=manufacture
            )

async def operators(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.OPERATOR_3_LETTER_CODES,
                model=Operator,
                conflict_enums=DatabaseUniqueColumns.OPERATOR_3_LETTERS,
                states=code,
            )


async def risk_profile(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.OPERATOR_RISK_PROFILE,
                model=OperatorRiskProfile,
                conflict_enums=DatabaseUniqueColumns.OPERATOR_RISK_PROFILE,
                states=code,
            )


async def aerodrome_location(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.AERODROME_LOCATION_INDICATORS,
                model=AerodromeLocation,
                conflict_enums=DatabaseUniqueColumns.AERODROME_LOCATION_INDICATORS,
                state=code,
            )


async def international_aerodromes(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.INTERNATIONAL_AERODROMES,
                model=InternationalAerodrome,
                conflict_enums=DatabaseUniqueColumns.INTERNATIONAL_AERODROMES,
                states=code,
            )


async def operational_aerodrome_info(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.OPERATIONAL_AERODROME_INFORMATION,
                model=OperationalAerodromeInfo,
                conflict_enums=DatabaseUniqueColumns.OPERATIONAL_AERODROME_INFORMATION,
                states=code,
            )


async def airport_pbn_impl(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.AIRPORT_PBN_IMPLEMENTATION,
                model=AirportPBNImplementation,
                conflict_enums=DatabaseUniqueColumns.AIRPORT_PBN_IMPLEMENTATION,
                states=code,
            )


async def international_airport_safety(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.INTERNATIONAL_AIRPORT_SAFETY_CHARACTERISTICS,
                model=InternationalAirportSafety,
                conflict_enums=DatabaseUniqueColumns.INTERNATIONAL_AIRPORT_SAFETY_CHARACTERISTICS,
                states=code,
            )


async def metar_provider(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.METAR_PROVIDER_LOCATIONS,
                model=METARProviderLocation,
                conflict_enums=DatabaseUniqueColumns.INTERNATIONAL_AIRPORT_SAFETY_CHARACTERISTICS,
                states=code,
            )


async def accident(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            for year in year_list:
                await client.fetch_and_store(
                    endpoint=ICAOEndpoints.ACCIDENTS,
                    model=Accident,
                    conflict_enums=DatabaseUniqueColumns.ACCIDENTS,
                    StateOfOccurrence=code,
                    Year=year
                )


async def safety_related_occurrence(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        for code in await codes:
            for year in year_list:
                await client.fetch_and_store(
                    endpoint=ICAOEndpoints.SAFETY_RELATED_OCCURRENCES,
                    model=SafetyRelatedOccurrence,
                    conflict_enums=DatabaseUniqueColumns.SAFETY_RELATED_OCCURRENCES,
                    StateOfOccurrence=code,
                    Year=year
                )


async def incident(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        count = 0
        for code in await codes:
            for year in year_list:
                if count < 1:
                    await client.fetch_and_store(
                        endpoint=ICAOEndpoints.INCIDENTS,
                        model=Incident,
                        conflict_enums=DatabaseUniqueColumns.INCIDENTS,
                        StateOfOccurrence=code,
                        Year=year
                    )
                    count += 1


async def current_fleet(codes = countries_codes()):
    async with async_session() as session:
        client = ICAOApiClient(session)
        count = 0
        for code in await codes:
            for year in year_list:
                if count < 1:
                    await client.fetch_and_store(
                        endpoint=ICAOEndpoints.,
                        model=Incident,
                        conflict_enums=DatabaseUniqueColumns.INCIDENTS,
                        StateOfOccurrence=code,
                        Year=year
                    )
                    count += 1


if __name__ == "__main__":
    asyncio.run(incident())
