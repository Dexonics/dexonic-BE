from fastapi import APIRouter
from typing import List
from sqlalchemy import text

from app.database import async_session
from app.schemas.fibo import OriSymbol, RefSymbol

from app.utils import str_to_list, str_to_list2d

router = APIRouter()


@router.get("/original-pair-list", response_model=List[OriSymbol])
async def get_original_pair_list(timeType: str):
    if timeType == "FOUR_HOUR":
        table_name = "pattern_matching_4h"
    elif timeType == "ONE_HOUR":
        table_name = "pattern_matching_1h"
    elif timeType == "THIRTY_MINUTE":
        table_name = "pattern_matching_30m"
    else:
        table_name = "pattern_matching_1d"
    
    async with async_session() as session:
        async with session.begin():
            sql = text(f"""
                SELECT  symbol2 as symbol, MAX(start_date2) AS discovered_symbol_time
                FROM  {table_name}
                GROUP BY symbol2
                order by symbol2
            """)
            result = await session.execute(sql)
            res = [OriSymbol(symbol=r[0], discoveredOn=str(r[1])) for r in result]

            return res
    

@router.get("/fibonacci-info") #, response_model=List[RefSymbol])
async def get_fibo_info(originalPair, timeType):
    if timeType == "FOUR_HOUR":
        table_name = "pattern_matching_4h"
    elif timeType == "ONE_HOUR":
        table_name = "pattern_matching_1h"
    elif timeType == "THIRTY_MINUTE":
        table_name = "pattern_matching_30m"
    else:
        table_name = "pattern_matching_1d"
    
    async with async_session() as session:
        async with session.begin():
            """ 
                String sql =
        "SELECT " +
        "pm.symbol2 as original_symbol, pm.start_date2 as original_start_date, pm.end_date as original_end_date, pm.prices2 as original_prices, pm.s2_norm as original_fibonacci, " +
        "pm.symbol1 as similar_symbol, pm.start_date1 as similar_start_date, pm.end_date as similar_end_date, pm.prices1 as similar_prices, pm.s1_norm as similar_fibonacci " +
        "FROM " + tableName + " pm " +
        "WHERE pm.symbol2 = ? " +
        "AND pm.start_date2 = ( " +
        "    SELECT MAX(start_date2) " +
        "    FROM " + tableName +
        "    WHERE symbol2 = ? " +
        ")";
            """

            sql = text(f"""
                SELECT 
                    pm.symbol2 as original_symbol, 
                    pm.start_date2 as original_start_date, 
                    pm.end_date as original_end_date, 
                    pm.prices2 as original_prices, 
                    pm.s2_norm as original_fibonacci, 
                    pm.symbol1 as similar_symbol, 
                    pm.start_date1 as similar_start_date, 
                    pm.end_date as similar_end_date, 
                    pm.prices1 as similar_prices, 
                    pm.s1_norm as similar_fibonacci 
                FROM {table_name} pm 
                WHERE pm.symbol2 = :originalPair
                AND pm.start_date2 = ( 
                    SELECT MAX(start_date2) 
                    FROM {table_name} 
                    WHERE symbol2 = :originalPair
                )
            """)
            
            result = await session.execute(sql, {"originalPair": originalPair})
            res = [RefSymbol(
                originalSymbol=r[0],
                originalStartDate=str(r[1]), 
                originalEndDate=str(r[2]),
                originalPrices=str_to_list(r[3]), 
                originalFibonacci=str_to_list(r[4]),
                similarSymbols=[str(r[5])], 
                similarStartDates=[str(r[6])],
                similarEndDates=[str(r[7])],
                similarPrices=str_to_list2d(r[8]),
                similarFibonacci=str_to_list2d(r[9]),
            ) for r in result]

            print(res)

            return res[0]