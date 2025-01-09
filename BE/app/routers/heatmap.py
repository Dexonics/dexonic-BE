from fastapi import APIRouter
from typing import List
from sqlalchemy import text

from app.database import async_session
from app.schemas.heatmap import HeatMap, ChartData

router = APIRouter()


@router.get("/top-over-sold", response_model=List[HeatMap])
async def get_tos(heatMapType: str, timeType: str):
    if timeType == "FOUR_HOUR":
        table_name = "f_coin_signal_4h"
    elif timeType == "ONE_HOUR":
        table_name = "f_coin_signal_1h"
    elif timeType == "THIRTY_MINUTE":
        table_name = "f_coin_signal_30m"
    else:
        table_name = "f_coin_signal_1d"
    rsi_period = heatMapType.lower()
    
    async with async_session() as session:
        async with session.begin():
            sql = text(f"""
                select symbol, {rsi_period} as rsi, close, low, high, open_time as date_created
                from {table_name}
                where {rsi_period} is not null
                and {rsi_period} < 30
                and symbol like '%USDT'
                and open_time = (
                    select max(open_time) as open_time
                    from (
                        select open_time, count(symbol) as num
                        from {table_name}
                        where open_time >= now() - interval 7 day
                        group by open_time
                    ) d
                    where d.num > 0
                )
                order by {rsi_period} asc
                limit 100;
            """)

            result = await session.execute(sql)

            res = [HeatMap(symbol=r[0], rsi=r[1], close=r[2], high=r[3], low=r[4], dateCreated=str(r[5])) for r in result]

            return res



@router.get("/top-over-bought", response_model=List[HeatMap])
async def get_tob(heatMapType: str, timeType: str):
    if timeType == "FOUR_HOUR":
        table_name = "f_coin_signal_4h"
    elif timeType == "ONE_HOUR":
        table_name = "f_coin_signal_1h"
    elif timeType == "THIRTY_MINUTE":
        table_name = "f_coin_signal_30m"
    else:
        table_name = "f_coin_signal_1d"
    rsi_period = heatMapType.lower()

    async with async_session() as session:
        async with session.begin():
            sql = text(f"""
                select symbol, {rsi_period} as rsi, close, low, high, open_time as date_created
                from {table_name}
                where {rsi_period} is not null
                and {rsi_period} > 70
                and symbol like '%USDT'
                and open_time = (
                    select max(open_time) as open_time
                    from (
                        select open_time, count(symbol) as num
                        from {table_name}
                        where open_time >= now() - interval 7 day
                        group by open_time
                    ) d
                    where d.num > 0
                )
                order by {rsi_period} desc
                limit 100;
            """)

            result = await session.execute(sql)
            res = [HeatMap(symbol=r[0], rsi=r[1], close=r[2], high=r[3], low=r[4], dateCreated=str(r[5])) for r in result]

            return res
            

@router.get("/chart-data", response_model=List[ChartData])
async def get_chart_data(heatMapType: str, timeType: str):
    if timeType == "FOUR_HOUR":
        table_name = "f_coin_signal_4h"
    elif timeType == "ONE_HOUR":
        table_name = "f_coin_signal_1h"
    elif timeType == "THIRTY_MINUTE":
        table_name = "f_coin_signal_30m"
    else:
        table_name = "f_coin_signal_1d"
    rsi_period = heatMapType.lower()
    
    async with async_session() as session:
        async with session.begin():
            sql = text(f"""

                SELECT latest.symbol, latest.{rsi_period} AS rsi, 
                ((latest.{rsi_period} - previous.{rsi_period}) / previous.{rsi_period}) * 100 AS percentage_change
                FROM {table_name} latest
                JOIN (
                    SELECT symbol, MAX(open_time) AS previous_open_time
                    FROM {table_name}
                    WHERE open_time < (SELECT MAX(open_time) FROM {table_name})
                    GROUP BY symbol
                ) prev ON latest.symbol = prev.symbol
                JOIN {table_name} previous ON prev.symbol = previous.symbol AND prev.previous_open_time = previous.open_time
                WHERE latest.open_time = (SELECT MAX(open_time) FROM {table_name})
                AND latest.symbol LIKE '%USDT'
                ORDER BY latest.symbol;
            """)

            result = await session.execute(sql)
            res = [ChartData(symbol=r[0], rsi=r[1], percentage_change=r[2]) for r in result]

            return res