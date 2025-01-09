from pydantic import BaseModel, field_validator

class Prediction(BaseModel):
    symbol: str
    date: str
    price: float
    prediction: float
    priceChange: float

    @field_validator("price")
    def round_price(cls, v:float) -> float:
        return round(v, 6)
    
    @field_validator("prediction")
    def round_prediction(cls, v:float) -> float:
        return round(v, 6)

    @field_validator("priceChange")
    def round_pc(cls, v: float) -> float:
        return round(v, 6)