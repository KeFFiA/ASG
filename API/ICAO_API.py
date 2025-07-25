from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from DATABASE.ICAO import *
from client import ApiClient
from enums import ICAOEndpoints, DatabaseUniqueColumns, manufacturer_list

engine = create_async_engine(os.getenv("DATABASE_URL_API"), echo=False)

async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


year_list = [2021, 2022, 2023, 2024, 2025]
RO = ["APAC", "MID", "EUR/NAT", "NACC", "SAM", "WACAF", "ESAF"]
HEADERS = {"Accept": "application/json"}
API_KEY = os.getenv("ICAO_API_KEY")
BASE_URL = "https://applications.icao.int/dataservices/api"


async def db_session(_async_session: async_session, **kwargs):
    async with async_session() as session:
        if "stmt" in kwargs:
            return await session.execute(kwargs["stmt"])
        return ApiClient(session, **kwargs)


async def countries_codes(code: str = None):
    stmt = select(CountriesISO.alpha3_code)
    result = await db_session(_async_session=async_session, stmt=stmt)
    codes = result.scalars().all()
    code = "AFG" if code is None else code.upper()
    del codes[0:codes.index(code)]

    return codes


async def manufacturer():
    for manufacture in manufacturer_list:
        client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.MANUFACTURER_LIST,
            model=Manufacturer,
            conflict_enums=DatabaseUniqueColumns.MANUFACTURER_CODE,
            manufacturer=manufacture
        )


async def type_designators():
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for manufacture in manufacturer_list:
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.AIRCRAFT_TYPE_DESIGNATORS,
            model=AircraftType,
            conflict_enums=DatabaseUniqueColumns.AIRCRAFT_TYPE_DESIGNATORS,
            manufacturer=manufacture
        )


async def operators(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.OPERATOR_3_LETTER_CODES,
            model=Operator,
            conflict_enums=DatabaseUniqueColumns.OPERATOR_3_LETTERS,
            states=_code,
        )


async def risk_profile(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.OPERATOR_RISK_PROFILE,
            model=OperatorRiskProfile,
            conflict_enums=DatabaseUniqueColumns.OPERATOR_RISK_PROFILE,
            states=_code,
        )


async def aerodrome_location(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.AERODROME_LOCATION_INDICATORS,
            model=AerodromeLocation,
            conflict_enums=DatabaseUniqueColumns.AERODROME_LOCATION_INDICATORS,
            state=_code,
        )


async def international_aerodromes(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.INTERNATIONAL_AERODROMES,
            model=InternationalAerodrome,
            conflict_enums=DatabaseUniqueColumns.INTERNATIONAL_AERODROMES,
            states=_code,
        )


async def operational_aerodrome_info(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.OPERATIONAL_AERODROME_INFORMATION,
            model=OperationalAerodromeInfo,
            conflict_enums=DatabaseUniqueColumns.OPERATIONAL_AERODROME_INFORMATION,
            states=_code,
        )


async def airport_pbn_impl(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.AIRPORT_PBN_IMPLEMENTATION,
            model=AirportPBNImplementation,
            conflict_enums=DatabaseUniqueColumns.AIRPORT_PBN_IMPLEMENTATION,
            states=_code,
        )


async def international_airport_safety(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.INTERNATIONAL_AIRPORT_SAFETY_CHARACTERISTICS,
            model=InternationalAirportSafety,
            conflict_enums=DatabaseUniqueColumns.INTERNATIONAL_AIRPORT_SAFETY_CHARACTERISTICS,
            states=_code,
        )


async def metar_provider(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.METAR_PROVIDER_LOCATIONS,
            model=METARProviderLocation,
            conflict_enums=DatabaseUniqueColumns.INTERNATIONAL_AIRPORT_SAFETY_CHARACTERISTICS,
            states=_code,
        )


async def accident(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.ACCIDENTS,
                model=Accident,
                conflict_enums=DatabaseUniqueColumns.ACCIDENTS,
                StateOfOccurrence=_code,
                Year=year
            )


async def safety_related_occurrence(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.SAFETY_RELATED_OCCURRENCES,
                model=SafetyRelatedOccurrence,
                conflict_enums=DatabaseUniqueColumns.SAFETY_RELATED_OCCURRENCES,
                StateOfOccurrence=_code,
                Year=year
            )


async def incident(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.INCIDENTS,
                model=Incident,
                conflict_enums=DatabaseUniqueColumns.INCIDENTS,
                StateOfOccurrence=_code,
                Year=year
            )


async def member_state():
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for ro in RO:
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.ICAO_MEMBER_STATE,
            model=ICAOMemberState,
            conflict_enums=DatabaseUniqueColumns.ICAO_MEMBER_STATE,
            RO=ro
        )


async def state_of_registry():
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for ro in RO:
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.STATE_OF_REGISTRY,
            model=StateOfRegistry,
            conflict_enums=DatabaseUniqueColumns.STATE_OF_REGISTRY,
            RO=ro
        )


async def asiap():
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for ro in RO:
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.ASIAPPRIORITIZATION,
            model=ASIAPPrioritization,
            conflict_enums=DatabaseUniqueColumns.ASIAPPRIORITIZATION,
            region=ro
        )


async def safety_margin_stats(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.STATE_SAFETY_MARGINS,
            model=StateSafetyMargin,
            conflict_enums=DatabaseUniqueColumns.STATE_SAFETY_MARGINS,
            states=_code,
        )


async def ssp_foundation(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        await client.fetch_and_store(
            endpoint=ICAOEndpoints.SSP_FOUNDATION_STATISTICS,
            model=SSPFoundation,
            conflict_enums=DatabaseUniqueColumns.SSP_FOUNDATION_STATISTICS,
            States=_code,
        )


async def aerodrome_stats(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.AERODROME_STATISTICS,
                model=AerodromeStatistic,
                conflict_enums=DatabaseUniqueColumns.AERODROME_STATISTICS,
                states=_code,
                Year=year
            )


async def operator_stats(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.OPERATOR_STATISTICS,
                model=OperatorStatistic,
                conflict_enums=DatabaseUniqueColumns.OPERATOR_STATISTICS,
                states=_code,
                Year=year
            )


async def connections(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.CONNECTIONS,
                model=Connection,
                conflict_enums=DatabaseUniqueColumns.CONNECTIONS,
                states=_code,
                Year=year
            )


async def state_traffic_stats(code: str = None):
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    for _code in await countries_codes(code):
        for year in year_list:
            await client.fetch_and_store(
                endpoint=ICAOEndpoints.STATE_TRAFFIC_STATISTICS,
                model=StateTrafficStatistic,
                conflict_enums=DatabaseUniqueColumns.STATE_TRAFFIC_STATISTICS,
                states=_code,
                Year=year
            )


async def caahr():
    client = await db_session(_async_session=async_session, headers=HEADERS, api_key=API_KEY, base_url=BASE_URL)
    await client.fetch_and_store(
        endpoint=ICAOEndpoints.CAAHR,
        model=CAAHR,
        conflict_enums=DatabaseUniqueColumns.CAAHR,
    )


if __name__ == "__main__":
    asyncio.run(aerodrome_location(code="VNM"))
