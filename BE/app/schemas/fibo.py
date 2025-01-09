from pydantic import BaseModel, field_validator
from typing import List


class OriSymbol(BaseModel):
    symbol: str
    discoveredOn: str


class RefSymbol(BaseModel):
    originalSymbol: str
    originalStartDate: str
    originalEndDate: str
    originalPrices: List[float]
    originalFibonacci: List[float]
    similarSymbols: List[str]
    similarStartDates: List[str]
    similarEndDates: List[str]
    similarPrices: List[List[float]]
    similarFibonacci: List[List[float]]