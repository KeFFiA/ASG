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

async def operators():
    async with async_session() as session:
        client = ICAOApiClient(session)
        stmt = select(CountriesISO.alpha3_code)
        result = await session.execute(stmt)
        codes = result.scalars().all()
        for code in codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.OPERATOR_3_LETTER_CODES,
                model=Operator,
                conflict_enums=DatabaseUniqueColumns.OPERATOR_3_LETTERS,
                states=code,
            )


async def risk_profile():
    async with async_session() as session:
        client = ICAOApiClient(session)
        stmt = select(CountriesISO.alpha3_code)
        result = await session.execute(stmt)
        codes = result.scalars().all()
        for code in codes:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.OPERATOR_RISK_PROFILE,
                model=OperatorRiskProfile,
                conflict_enums=DatabaseUniqueColumns.OPERATOR_RISK_PROFILE,
                states=code,
            )


async def aerodrome_location():
    async with async_session() as session:
        client = ICAOApiClient(session)
        stmt = select(CountriesISO.alpha3_code)
        result = await session.execute(stmt)
        codes = result.scalars().all()
        count = 0
        for code in codes:
            if count < 1:
                await client.fetch_and_store(
                    endpoint=ICAOEndpoints.AERODROME_LOCATION_INDICATORS,
                    model=AerodromeLocation,
                    conflict_enums=DatabaseUniqueColumns.AERODROME_LOCATION_INDICATORS,
                    state=code,
                )
                count += 1


if __name__ == "__main__":
    asyncio.run(aerodrome_location())
