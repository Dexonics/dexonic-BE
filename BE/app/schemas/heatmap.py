from pydantic import BaseModel, field_validator

class HeatMap(BaseModel):
    symbol: str
    rsi: float
    close: float
    high: float
    low: float
    dateCreated: str


class ChartData(BaseModel):
    symbol: str
    rsi: float 
    percentage_change: float

    @field_validator("rsi", "percentage_change")
    def round_value(cls, v: float) -> float:
        return round(v, 2)
    