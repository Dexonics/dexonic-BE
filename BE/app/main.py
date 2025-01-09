# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import coin_prices, health, heatmap, fibo, ai_analysis
from app.middleware import IPWhitelistMiddleware

app = FastAPI()

# CORS settings
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# IP Whitelist settings
allowed_ips = [ "127.0.0.1"]  # Replace with your front-end IP
app.add_middleware(IPWhitelistMiddleware, allowed_ips=allowed_ips)

# Include routers
app.include_router(health.router)
app.include_router(coin_prices.router)
app.include_router(heatmap.router, prefix="/heatmap")
app.include_router(fibo.router, prefix="/fibonacci")
app.include_router(ai_analysis.router, prefix="/ai-analysis")

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        ssl_keyfile="",
        ssl_certfile="",
        reload=True
    )