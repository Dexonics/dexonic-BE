import os, sys, requests
os.environ['TZ'] = 'UTC'
import pandas as pd
from config.db import DB
from config.crypto import ls_symbols
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kw)

engine = create_engine(f"mysql+pymysql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['database']}")

SCHEMA = DB['database'] + '.'

PRICE_5M = SCHEMA+'coin_prices_5m' 
SIGNAL_5M = SCHEMA+'f_coin_signal_5m'
SIGNAL_10M = SCHEMA+'f_coin_signal_10m'
SIGNAL_15M = SCHEMA+'f_coin_signal_15m'
SIGNAL_30M = SCHEMA+'f_coin_signal_30m'

sql_create_signal = """
CREATE TABLE IF NOT EXISTS {db_name} (
  `update_time` int unsigned DEFAULT 0,
  `open_time` int unsigned DEFAULT 0,
  `symbol` varchar(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `quote_asset` double DEFAULT NULL,
  `num_trades` bigint DEFAULT NULL,
  `buy_base` double DEFAULT NULL,
  `buy_quote` double DEFAULT NULL,
  `ph` double DEFAULT NULL,
  `pl` double DEFAULT NULL,
  `pc` double DEFAULT NULL,
  `tr` double DEFAULT NULL,
  `c_diff_p` double DEFAULT NULL,
  `c_diff_n` double DEFAULT NULL,
  `dm_p` double DEFAULT NULL,
  `dm_n` double DEFAULT NULL,
  `ep14_h` double DEFAULT NULL,
  `ep14_l` double DEFAULT NULL,
  `ep28_h` double DEFAULT NULL,
  `ep28_l` double DEFAULT NULL,
  `atr14` double DEFAULT NULL,
  `atr28` double DEFAULT NULL,
  `ag7` double DEFAULT NULL,
  `ag14` double DEFAULT NULL,
  `al7` double DEFAULT NULL,
  `al14` double DEFAULT NULL,
  `dm14_p` double DEFAULT NULL,
  `dm14_n` double DEFAULT NULL,
  `di14_diff` double DEFAULT NULL,
  `di14_sum` double DEFAULT NULL,
  `dx14` double DEFAULT NULL,
  `rsi7` double DEFAULT NULL,
  `rsi14` double DEFAULT NULL,
  `di14_p` double DEFAULT NULL,
  `di14_n` double DEFAULT NULL,
  `di14_line_cross` int NOT NULL DEFAULT '0',
  `adx` double DEFAULT NULL,
  `af` decimal(3,2) NOT NULL DEFAULT '0.00',
  `psar_type` varchar(4) NOT NULL DEFAULT '',
  `psar` double DEFAULT NULL,
  `psar_up` double DEFAULT NULL,
  `psar_down` double DEFAULT NULL,
  `ep` double DEFAULT 0,
  UNIQUE KEY `{db_name}_open_time_IDX` (`open_time`,`symbol`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

sql_table_wrap = """ with a as(
select *
from(
	SELECT
		symbol
	    , open_time
	    , close_time
		, `open`
	    , ROW_NUMBER() over (PARTITION by symbol, close_time order by open_time asc) as r
	from (
		SELECT 
			 symbol
		    , open_time
            , (open_time div ({min}*60) + 1) * ({min}*60) as close_time
		    , `open`
		from {from_table}
		where open_time >= {start_time} and open_time < {end_time} {additional_conditions}
	) o
) o
WHERE r = 1
),
b as(
select *
from(
SELECT symbol
	, open_time
	, close_time
	, max(high) over (PARTITION by symbol, close_time) as high
	, min(low) over (PARTITION by symbol, close_time) as low
    , `close`
	, sum(volume) over (PARTITION by symbol, close_time) as volume
	, sum(quote_asset) over (PARTITION by symbol, close_time) as quote_asset
	, sum(num_trades) over (PARTITION by symbol, close_time) as num_trades
	, sum(buy_base) over (PARTITION by symbol, close_time) as buy_base
	, sum(buy_quote) over (PARTITION by symbol, close_time) as buy_quote
    , ROW_NUMBER() over (PARTITION by symbol, close_time order by open_time desc) as r
	from (
		select
			 symbol
		    , open_time
            , (open_time div ({min}*60) + 1) * ({min}*60) as close_time
		    , high 
		    , low 
			,`close`
		    , volume 
		    , quote_asset 
		    , num_trades
		    ,buy_base 
		    ,buy_quote 
		from {from_table}
		where open_time >= {start_time} and open_time < {end_time} {additional_conditions}
	) c
) c
where r=1
),
{to_table} as(
SELECT a.symbol
	, a.open_time
	, b.close_time
	, a.`open`
	, b.high
	, b.low 
	, b.`close`
    , b.volume 
    , b.quote_asset 
    , b.num_trades
    , b.buy_base 
    , b.buy_quote 
from a inner join b on a.symbol=b.symbol and a.close_time=b.close_time
) """

sql_signal_0 = """
INSERT ignore INTO {to_table} 
    (update_time, open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote, ph, pl, pc, tr
    , c_diff_p, c_diff_n, dm_p, dm_n, ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28, ag7, ag14, al7, al14, dm14_p, dm14_n
    , di14_diff, di14_sum, dx14, rsi7, rsi14, di14_p, di14_n, di14_line_cross, adx, af, ep, psar_type, psar)
{pre_sql}
SELECT UNIX_TIMESTAMP(now()) as update_time, open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote
	, ph, pl, pc, tr, c_diff_p, c_diff_n, dm_p, dm_n
	, ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28, ag7, ag14, al7, al14, dm14_p, dm14_n, di14_diff, di14_sum, dx14
	, 100*ag7/(ag7+al7) as rsi7
	, 100*ag14/(ag14+al14) as rsi14
	, di14_p, di14_n
	, if((pdm14_p - pdm14_n)*(dm14_p - dm14_n) <= 0, TRUE, FALSE) as di14_line_cross -- 2 duong +DI va -DI cat nhau
	, adx, af, ep, psar_type
	, ppsar+af*(ep-ppsar) as psar -- psar khoi diem SARn = SARn-1 + AF * (EP - SARn-1)
FROM (
	SELECT open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote
        , ph, pl, pc, tr, c_diff_p, c_diff_n, dm_p, dm_n, di14_p, di14_n, af, r
        , ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28, ag7, ag14, al7, al14, dm14_p, dm14_n, di14_diff, di14_sum, dx14
        , pdm14_p, pdm14_n
		, avg(dx14) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as adx -- adx 14
		, if(c_diff_n>0, 'DOWN', 'UP') as psar_type
		, if(c_diff_n>0, low, high) as ep 
		, if(c_diff_n>0, ep14_h, ep14_l) as ppsar
	from (
        select open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote
            , ph, pl, pc, tr, c_diff_p, c_diff_n, dm_p, dm_n, af, r
            , ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28, ag7, ag14, al7, al14, dm14_p, dm14_n
            , 100*dm14_p / atr14 as di14_p -- +DI cho ADX
            , 100*dm14_n / atr14 as di14_n -- -DI cho ADX
            , 100*(dm14_p - dm14_n) / atr14 as di14_diff -- DI minus
            , 100*(dm14_p + dm14_n) / atr14 as di14_sum -- DI sum
            , LEAD(dm14_p,1,dm14_p) over (PARTITION by symbol order by r ASC) as pdm14_p -- previos smoothed +DM cho xac dinh DI cat nhau
            , LEAD(dm14_n,1,dm14_n) over (PARTITION by symbol order by r ASC) as pdm14_n -- previos smoothed -DM cho xac dinh DI cat nhau
            , 100*abs((dm14_p-dm14_n)/(dm14_p+dm14_n)) as dx14 -- DX cho ADX
        from (
            SELECT open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote
                , ph, pl, pc, tr, c_diff_p, c_diff_n, dm_p, dm_n, af, r
                , ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28
                , avg(c_diff_p) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING) as ag7 -- gain cho RSI7
                , avg(c_diff_p) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as ag14 -- gain cho RSI14
                , avg(c_diff_n) over (PARTITION by symbol order by r ASC ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING) as al7 -- gain cho RSI7
                , avg(c_diff_n) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as al14 -- gain cho RSI14
                , avg(dm_p) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as dm14_p -- smoothed +DM cho ADX
                , avg(dm_n) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as dm14_n -- smoothed -DM cho ADX
            from(
                SELECT *
                , if(`close`-pc>=0, `close` - pc, 0) as c_diff_p -- gain cho RSI
                , if(`close`-pc<0, pc - `close`, 0) as c_diff_n -- loss cho RSI
                , if(high - ph >= pl - low, high - ph, 0) as dm_p -- +DM cho ADX
                , if(pl - low > high - ph, pl - low, 0) as dm_n -- -DM cho ADX
                , 0.02 as af -- thong so Acceleration Factor cho psar
                , avg(tr) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as atr14 -- average true range cho ADX
                , avg(tr) over (PARTITION by symbol order by r asc ROWS BETWEEN CURRENT ROW AND 27 FOLLOWING) as atr28 -- average true range cho ADX
                from (
                    SELECT open_time
                        ,open
                        ,high 
                        ,low 
                        ,close
                        ,volume 
                        , quote_asset 
                        , num_trades
                        ,buy_base 
                        ,buy_quote 
                        ,symbol
                        , LEAD(high,1,high) over (PARTITION by symbol order by open_time desc) as ph -- high phien truoc
                        , LEAD(low,1,low) over (PARTITION by symbol order by open_time desc) as pl -- low phien truoc
                        , LEAD(`close`,1,`close`) over (PARTITION by symbol order by open_time desc) as pc -- close phien truoc
                        , high -low as tr -- true range 
                        , max(high) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 14 FOLLOWING) as ep14_h -- cuc tri cao 14 ngay
                        , min(low) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 14 FOLLOWING) as ep14_l -- cuc tri thap 14 ngay
                        , max(high) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 27 FOLLOWING) as ep28_h -- cuc tri cao 28 ngay
                        , min(low) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 27 FOLLOWING) as ep28_l -- cuc tri thap 28 ngay
                        , ROW_NUMBER() over (PARTITION by symbol order by open_time desc) as r
                    from {from_table}
                    WHERE open_time >= {time_27} and open_time <= {time} {additional_conditions}
                ) ts
            ) t1
        ) t2
    ) t3
) t
WHERE r = 1
"""

sql_insert_signal = """
insert ignore into {to_table}
    (update_time, open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote, ph, pl, pc, tr
    , c_diff_p, c_diff_n, dm_p, dm_n, ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28, ag7, ag14, al7, al14, dm14_p, dm14_n
    , di14_diff, di14_sum, dx14, rsi7, rsi14, di14_p, di14_n, di14_line_cross, adx, af, ep, psar_type, psar)
{pre_sql}
SELECT UNIX_TIMESTAMP(now()) as update_time, open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote, ph, pl, pc, tr
	, c_diff_p, c_diff_n, dm_p, dm_n, ep14_h, ep14_l, ep28_h, ep28_l, atr14, atr28, ag7, ag14, al7, al14, dm14_p, dm14_n
	, 100*(dm14_p - dm14_n) / atr14 as di14_diff -- DI minus
	, 100*(dm14_p + dm14_n) / atr14 as di14_sum -- DI sum
	, 100*abs((dm14_p-dm14_n)/(dm14_p + dm14_n)) as dx14 -- DX cho ADX
	, 100*ag7/(ag7+al7) as rsi7
	, 100*ag14/(ag14+al14) as rsi14
	, 100*dm14_p / atr14 as di14_p -- +DI cho ADX
	, 100*dm14_n / atr14 as di14_n -- -DI cho ADX
	, if((pdm14_p - pdm14_n)*(dm14_p - dm14_n) <= 0, TRUE, FALSE) as di14_line_cross -- 2 duong +DI va -DI cat nhau
	, (100*abs((dm14_p-dm14_n)/(dm14_p + dm14_n))+padx*13)/14 as adx -- adx 14
    , if (psar_turn=1, 0.02, af_) as af -- reset af if change psar_type
    , if (psar_turn=1, if(psar_type_='DOWN', greatest(ep_, high), least(ep_, low)), ep_) as ep -- get opposite ep if change psar_type
    , if (psar_turn=1, if(psar_type_='DOWN', 'UP','DOWN'), psar_type_) as psar_type -- change psar_type if psar_turn
    , if (psar_turn=1, _ppsar+0.02*(_ep-_ppsar), ppsar+af_*(ep_-ppsar)) as psar

from (
	SELECT open_time, symbol, `open`, high, low, `close`, volume, quote_asset, num_trades, buy_base, buy_quote, ph, pl, pc, tr
        , c_diff_p, c_diff_n, dm_p, dm_n, ep14_h, ep14_l, ep28_h, ep28_l
        , pdm14_p, pdm14_n, padx
		, (pag7*6 + c_diff_p)/7 as ag7 -- gain cho RSI7
		, (pag14*13 + c_diff_p)/14 as ag14 -- gain cho RSI14
		, (pal7*6 + c_diff_n)/7 as al7 -- gain cho RSI7
		, (pal14*13 + c_diff_n)/14 as al14 -- gain cho RSI14
		, (pdm14_p*13 + dm_p)/14 as dm14_p -- smoothed +DM cho ADX
		, (pdm14_n*13 + dm_n)/14 as dm14_n -- smoothed -DM cho ADX
		, (patr14*13 + tr)/14  as atr14 -- average true range cho ADX
		, (patr28*27 + tr)/28 as atr28 -- average true range cho ADX
        , if((psar_type_='DOWN' and ppsar+af_*(ep_-ppsar)<=high) or (psar_type_='UP' and ppsar+af_*(ep_-ppsar)>=low), 1, 0) as psar_turn
        , ppsar,af_,ep_, psar_type_
        , if(psar_type_='DOWN', greatest(ep_, high), least(ep_, low)) as _ep -- get other ep
        , if(psar_type_='DOWN', least(ep_, low-0.1*tr), greatest(ep_, high+0.1*tr)) as _ppsar -- get other ppsar

	from (
		SELECT n.open_time, n.symbol, n.open, n.high, n.low, n.close
            , n.volume, n.quote_asset, n.num_trades, n.buy_base, n.buy_quote
            , ep14_h, n.ep14_l, n.ep28_h, n.ep28_l, n.tr 
            -- , n.r
			, o.high as ph
			, o.low as pl
			, o.`close` as pc
			, if(n.`close`-o.`close`>=0, n.`close` - o.`close`, 0) as c_diff_p -- gain cho RSI
			, if(n.`close`-o.`close`<0, o.`close` - n.`close`, 0) as c_diff_n -- loss cho RSI
			, if(n.high - o.high >= o.low - n.low, n.high - o.high, 0) as dm_p -- +DM cho ADX
			, if(o.low - n.low > n.high - o.high, o.low - n.low, 0) as dm_n -- -DM cho ADX
		    , o.adx as padx
            , if(o.psar_type='UP', greatest(o.ep, n.high), least(o.ep, n.low)) as ep_
            , LEAST(o.af+0.02, 0.2) as af_ 
            , o.psar_type as psar_type_
            , o.psar as ppsar
			, o.ag7 as pag7 -- gain cho RSI7
			, o.ag14 as pag14 -- gain cho RSI14
			, o.al7 as pal7 -- gain cho RSI7
			, o.al14 as pal14 -- gain cho RSI14
			, o.dm14_p as pdm14_p -- smoothed +DM cho ADX
			, o.dm14_n as pdm14_n -- smoothed -DM cho ADX
			, o.atr14 as patr14 -- average true range cho ADX
			, o.atr28 as patr28 -- average true range cho ADX
		from (
			SELECT open_time, symbol
				, `open`, high, low, `close`
				, volume, quote_asset, num_trades, buy_base, buy_quote
				, high -low as tr -- true range 
				, max(high) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as ep14_h -- cuc tri cao 14 ngay
				, min(low) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) as ep14_l -- cuc tri thap 14 ngay
				, max(high) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 27 FOLLOWING) as ep28_h -- cuc tri cao 28 ngay
				, min(low) over (PARTITION by symbol order by open_time desc ROWS BETWEEN CURRENT ROW AND 27 FOLLOWING) as ep28_l -- cuc tri thap 28 ngay
				, ROW_NUMBER() over (PARTITION by symbol order by open_time desc) as r
				from {from_table}
			WHERE open_time >= {time_27} and open_time <= {time} {additional_conditions}
		) n inner join (
			SELECT symbol
				, high -- high phien truoc
				, low -- low phien truoc
				, `close` -- close phien truoc
				, ag7, ag14
				, al7, al14
				, dm14_p, dm14_n
				, atr14, atr28
				, af, psar_type, ep, psar
				, adx
			from {to_table}
			where open_time = {time_1} {additional_conditions}
		) o on n.r=1 and o.symbol = n.symbol
	) n
) t
"""

def get_data(symbol, start_time:str, end_time:str):
    start_time=str(start_time)
    end_time=str(end_time)
    klines = []
    url = "https://api.mobula.io/api/1/market/history/pair"
    querystring = {"to":start_time+"000","from":end_time+"000","usd":"true","period":"1m","asset":symbol}

    response = requests.request("GET", url, params=querystring)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return None
    else:
        coin_df = pd.DataFrame.from_records(klines,
            columns=["open_time", "open", "high", "low", "close", "volume", "close_time", "time", "quote_asset", "num_trades", "buy_base", "buy_quote"], 
        )
        coin_df['close_time'] = coin_df['time'].astype(int) // 1000 - 60
        coin_df['open_time'] = coin_df['close_time'] 
        coin_df.drop(columns=["time"], inplace=True)
        coin_df = coin_df.apply(pd.to_numeric, errors='coerce')
        coin_df["symbol"] = symbol

        return coin_df.fillna(0)

def write_data_db(db_name:str, start_time:str, end_time:str, symbols:list = ls_symbols, row_limit:int=500, catchup=True):
    # use table name with out schema cause insert use pandas
    if catchup:
        latest_time = pd.read_sql(f"""
            SELECT max(open_time)+1 as start_time from {db_name} where open_time > (UNIX_TIMESTAMP(now())-3600)
        """, engine)
        start_time = latest_time['start_time'][0]
        end_time = int(datetime.now().timestamp()) - 300

    data = pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume", "quote_asset", "num_trades", "buy_base", "buy_quote", "symbol"])
    for symbol in symbols:
        try:
            print(f"Writing {symbol} to db...")
            data = pd.concat([data, get_data(symbol, str(start_time), str(end_time))], ignore_index=True)
        except Exception as e:
            print(e)
            continue
        if len(data) >= row_limit:
            # insert use pandas
            data.to_sql(name=db_name, index=False, con=engine, if_exists="append")
            data = pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume", "quote_asset", "num_trades", "buy_base", "buy_quote", "symbol"])
    if len(data) >= 0:
        # insert use pandas
        data.to_sql(name=db_name, index=False, con=engine, if_exists="append")
    print("Done!!!")

def init_signal_m(end_time, time, time_27, min, from_table=PRICE_5M, to_table=SIGNAL_5M, cond=""):
    print(f"init_signal_m {min}M: at {time} to {to_table}")
    table_name = "tmp_table"
    source_table = sql_table_wrap.format(
        from_table=from_table,
        to_table=table_name,
        min=min,
        start_time=time_27,
        end_time=end_time,
        additional_conditions=cond
        )
    try:
        with engine.begin() as conn:
            conn.execute(text(sql_create_signal.format(db_name=to_table)))
            sql = sql_signal_0.format(
                pre_sql=source_table,
                from_table=table_name,
                to_table=to_table,
                time_27=time_27,
                time=time,
                additional_conditions=cond
                )
            # print(sql)
            conn.execute(text(sql))
            # conn.commit()  # commit the transaction
    except Exception as e:
        print(e)
        print("init_signal_m error")

def insert_signal_m(end_time, time, time_1, time_27, min, from_table=PRICE_5M, to_table=SIGNAL_5M, cond="", check_init=False):
    print(f"insert_signal_m {min}M: at {time} to {to_table}")
    table_name = "tmp_table"
    insert_cond = "" 
    if check_init:
        # list symbol from root table
        check1 = set(pd.read_sql(f"SELECT symbol FROM {from_table} where open_time = {time} {cond}",engine)["symbol"].values)
        # list symbol from f table - also check adx, psar, rsi14 not null
        check2 = set(pd.read_sql(f"""
            SELECT symbol 
            FROM {to_table} 
            where open_time = {time_1}
                and rsi7 is not NULL 
                and rsi14 is not NULL 
                and adx is not NULL 
                and psar is not NULL 
                {cond}
        """,engine)["symbol"].values)
        init_list = list(check1 - check2)
        if len(init_list) > 0:
            init_cond = f"""and symbol in ('{"', '".join(init_list)}')"""
            # init first row for symbols if it on first run
            init_signal_m(end_time, time, time_27, min, from_table=from_table, to_table=to_table, cond=init_cond)
            insert_cond = f"""and symbol in ('{"', '".join(check1 & check2)}')""" 
    try:
        with engine.begin() as conn:
            source_table = sql_table_wrap.format(from_table=from_table,to_table=table_name, min=min, start_time=time_27, end_time=end_time, additional_conditions=insert_cond) 
            sql = sql_insert_signal.format(pre_sql=source_table, from_table=table_name, to_table=to_table, time_27=time_27, time=time, time_1=time_1, additional_conditions=insert_cond)
            conn.execute(text(sql))
            # conn.commit()  # commit the transaction
    except Exception as e:
        print(e)

def calculate_time(end_time:int, min:int=5, hour:int=0):
    td = min * 60 + hour * 3600
    time = end_time-td
    time_1 = end_time - 2*td
    time_27 = end_time - 28*td
    return time, time_1, time_27

def loop_insert_signal(t:int,ti:int,m:int, end_time,from_table, to_table, cond, check_init):
    # print(t,ti,m, end_time)
    while t <= end_time:
        time, time_1, time_27 = calculate_time(t,min=m)
        insert_signal_m(t, time, time_1, time_27, min=m, from_table=from_table, to_table=to_table, cond=cond, check_init=check_init)
        t += ti

def get_signal_m(end_time: int, cond="", check_init=False, intervals=['5m', '10m', '15m', '30m']):
    t_5m = 300
    end_time = end_time // t_5m * t_5m

    t_5m = 300
    t_10m = t_5m*2
    t_15m = t_5m*3
    t_30m = t_5m*6
    # limit_5m = end_time - 100*t_5m
    # limit_10m = end_time - 100*t_10m
    # limit_15m = end_time - 100*t_15m
    # limit_30m = end_time - 100*t_30m

    latest_time = pd.read_sql(f"""
        SELECT a.end_time, b.max_time as time_5m, 
            c.max_time as time_10m,
            d.max_time as time_15m,
            e.max_time as time_30m
        from (SELECT max(open_time + {t_5m}) as end_time from {PRICE_5M} where open_time < {end_time} {cond}) a
        join (select min(max_time) max_time from (SELECT max(open_time + 2*{t_5m}) as max_time from {SIGNAL_5M} where open_time < {end_time} {cond} group by symbol) t) b
        join (select min(max_time) max_time from (SELECT max(open_time + 2*{t_10m}) as max_time from {SIGNAL_10M} where open_time < {end_time} {cond} group by symbol) t) c
        join (select min(max_time) max_time from (SELECT max(open_time + 2*{t_15m}) as max_time from {SIGNAL_15M} where open_time < {end_time} {cond} group by symbol) t) d
        join (select min(max_time) max_time from (SELECT max(open_time + 2*{t_30m}) as max_time from {SIGNAL_30M} where open_time < {end_time} {cond} group by symbol) t) e
    """, engine)

    end_time = latest_time['end_time'][0] or end_time
    end_time_5m = latest_time['time_5m'][0] or end_time
    end_time_10m = latest_time['time_10m'][0] or end_time
    end_time_15m = latest_time['time_15m'][0] or end_time
    end_time_30m = latest_time['time_30m'][0] or end_time
    print(end_time, latest_time)
    del latest_time
    if '5m' in intervals:
        loop_insert_signal(end_time_5m,t_5m,5,end_time,from_table=PRICE_5M,to_table=SIGNAL_5M,cond=cond,check_init=check_init)
    if '10m' in intervals:
        loop_insert_signal(end_time_10m,t_10m,10,end_time,from_table=PRICE_5M,to_table=SIGNAL_10M,cond=cond,check_init=check_init)
    if '15m' in intervals:
        loop_insert_signal(end_time_15m,t_15m,15,end_time,from_table=PRICE_5M,to_table=SIGNAL_15M,cond=cond,check_init=check_init)
    if '30m' in intervals:
        loop_insert_signal(end_time_30m,t_30m,30,end_time,from_table=PRICE_5M,to_table=SIGNAL_30M,cond=cond,check_init=check_init)

def main():
    end_time = int(datetime.now().timestamp())
    write_data_db(
        db_name='coin_prices_5m',  # use table name with out schema cause insert use pandas
        start_time=str(end_time - 600),
        end_time=str(end_time - 300)
        )
    get_signal_m(end_time)

if __name__ == "__main__":
    try:  # run function in commamnd
        globals()[sys.argv[1]]()
    except Exception as e:
        # run main on default
        print(e)
        main()
        # print(False)
