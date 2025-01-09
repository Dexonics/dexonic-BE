import sys
import pandas as pd
from config.db import DB, XRP_DB
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sqlalchemy import create_engine, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kw)

engine = create_engine(f"mysql+pymysql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['database']}")
SCHEMA = DB['database'] + '.'
PRICE_1 = SCHEMA+'coin_prices' 
SIGNAL_1 = SCHEMA+'f_coin_signal_1h'
SIGNAL_4 = SCHEMA+'f_coin_signal_4h'
SIGNAL_24 = SCHEMA+'f_coin_signal_1d'


def get_prediction(data):
    arima = ARIMA(data, order=(1, 2, 1))
    arima_res = arima.fit()
    train_pred = arima_res.fittedvalues

    prediction_res = arima_res.get_forecast(1)
    conf_int = prediction_res.conf_int()
   
    lower, upper = conf_int[conf_int.columns[0]], conf_int[conf_int.columns[1]]
    forecast = prediction_res.predicted_mean

    mse = mean_squared_error(train_pred.values, data.values)
    mae = mean_absolute_error(train_pred.values, data.values)

    amse = mse / data.mean()
    amae = mae / data.mean()


    return forecast.values[0], lower.values[0], upper.values[0], amse, amae 

def main():
    df = pd.read_sql(f"select * from {PRICE_1} where open_time >= UNIX_TIMESTAMP(now()) - 3600*24*90", con=engine)
    symbols = []
    open_times = []
    preds = []
    last_prices = []
    lowers = []
    uppers = []
    mses = []
    maes = []

    for symbol, coin_df in df.groupby("symbol"):
        coin_df = coin_df.sort_values("open_time").reset_index(drop=True)
        next_pred, lower, upper, mse, mae = get_prediction(coin_df.close)

        symbols.append(symbol)
        open_times.append(coin_df.open_time.values.tolist()[-1])
        preds.append(next_pred)
        last_prices.append(coin_df.close.values.tolist()[-1])
        lowers.append(lower)
        uppers.append(upper)
        mses.append(mse)
        maes.append(mae)

    out_data = {"symbol": symbols, "open_time": open_times, "next_pred": preds, "last_price": last_prices, "lower": lowers, "upper": uppers, "mse": mses, "mae": maes}
    out_df = pd.DataFrame(data=out_data)
    out_df.to_sql("coin_predictions", con=engine, if_exists="append", index=False)

def run_xrp():
    engine = create_engine(f"mysql+pymysql://{XRP_DB['user']}:{XRP_DB['password']}@{XRP_DB['host']}:{XRP_DB['port']}/{XRP_DB['database']}")
    df = pd.read_sql_table("coin_prices", con=engine)
    symbols = []
    open_times = []
    preds = []
    last_prices = []
    lowers = []
    uppers = []
    mses = []
    maes = []

    for symbol, coin_df in df.groupby("symbol"):
        coin_df = coin_df.sort_values("open_time").reset_index(drop=True)
        next_pred, lower, upper, mse, mae = get_prediction(coin_df.close)
        symbols.append(symbol)
        open_times.append(coin_df.open_time.values.tolist()[-1])
        preds.append(next_pred)
        last_prices.append(coin_df.close.values.tolist()[-1])
        lowers.append(lower)
        uppers.append(upper)
        mses.append(mse)
        maes.append(mae)

    out_data = {"symbol": symbols, "open_time": open_times, "next_pred": preds, "last_price": last_prices, "lower": lowers, "upper": uppers, "mse": mses, "mae": maes}
    out_df = pd.DataFrame(data=out_data)
    out_df.to_sql("coin_predictions", con=engine, if_exists="append", index=False)

def run():
    print("running")
    tmp_insert_sql = """
    INSERT IGNORE into proddb.coin_predictions 
    SELECT 
        symbol, 
        UNIX_TIMESTAMP(open_time) - 7*3600 as open_time, 
        next_pred, last_price, lower, upper, mse, mae 
    FROM devdb.coin_predictions
    where last_price >0 and open_time > now() - interval 2 hour
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(tmp_insert_sql))
            # conn.commit()  # commit the transaction
    except Exception as e:
        print(e)
        print("insert coin_predictions")

if __name__ == "__main__":
    try:
        globals()[sys.argv[1]]()
    except Exception as e:
        # run main on default
        print(e)
        main()
        run_xrp()
