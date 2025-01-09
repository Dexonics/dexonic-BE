from fastapi import APIRouter
from sqlalchemy import text
from typing import List

from app.database import async_session
from app.schemas.coin_prices import CoinPrice

router = APIRouter()

@router.get("/coin-prices", response_model=List[CoinPrice])
async def get_coin_price():
    async with async_session() as session:
        async with session.begin():
            query = text("""
                select left(t1.symbol, char_length(t1.symbol) - 4) as coin, t1.close as price, round(((t1.close - t3.close) * 100 / t3.close ), 2) as price_change
                from devdb.f_coin_signal_5m t1
                join (
                    select symbol, max(open_time) as latest_open_time
                    from devdb.f_coin_signal_5m
                    where open_time >= now() - interval 100 hour
                    group by symbol
                ) t2 on t1.symbol = t2.symbol and t1.open_time = t2.latest_open_time
                join (
                    select symbol, close, open_time
                    from devdb.f_coin_signal_1d
                    where (symbol, open_time) in (
                        select symbol, max(open_time)
                        from devdb.f_coin_signal_1d
                        where open_time >= now() - interval 20 day
                        group by symbol
                    )
                ) t3 on t1.symbol = t3.symbol;
            """)
            
            result = await session.execute(query)
            coin_prices = []
            for r in result:
                coin_prices.append(CoinPrice(coin=r[0], price=r[1], priceChange=r[2]))

            return coin_prices