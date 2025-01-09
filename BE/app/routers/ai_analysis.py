from app.database import async_session
from fastapi import APIRouter
from sqlalchemy.sql import text
from schemas.ai_analysis import Prediction

router = APIRouter()

@router.get("/get-predictions", response_model=list[Prediction])
async def get_predictions():
    async with async_session() as session:
        async with session.begin():
            query = text("""
                SELECT 
                    symbol, 
                    open_time as date, 
                    last_price as price, 
                    next_pred as prediction,
                    ((next_pred - last_price) / last_price) * 100 AS change_percentage
                FROM 
                    coin_predictions cp 
                WHERE 
                    open_time = (SELECT MAX(open_time) FROM coin_predictions)
            """)
            
            result = await session.execute(query)
            predictions = []
            for r in result:
                predictions.append(Prediction(symbol=r[0], date=str(r[1]), price=r[2], prediction=r[3], priceChange=r[4]))

            print(predictions)
            
            return predictions
        
@router.get("/test")
def test():
    