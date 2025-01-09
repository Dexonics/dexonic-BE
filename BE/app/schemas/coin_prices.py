from pydantic import BaseModel, field_validator

class CoinPrice(BaseModel):
    coin: str
    price: float
    priceChange: float

    @field_validator("price")
    def round_price(cls, v:float) -> float:
        return round(v, 6)

    @field_validator("priceChange")
    def round_pc(cls, v: float) -> float:
        return round(v, 2)