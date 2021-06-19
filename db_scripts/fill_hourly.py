import sqlite3
from sqlite3 import Error
import tinvest as ti
import os
import datetime as dt
from tinvest import CandleResolution
import time
import pandas as pd
from tinvest import TooManyRequestsError, UnexpectedError


TOKEN = os.getenv("TINVEST_TOKEN")


def create_connection(db_file):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        c.execute("""PRAGMA synchronous = EXTRA""")
        c.execute("""PRAGMA journal_mode = WAL""")
    except Error as e:
        print(e)


def select_stocks_rows(conn, column=None):
    cur = conn.cursor()
    if column is None:
        sql = """ SELECT * FROM stocks """
    else:
        sql = """ SELECT {} FROM stocks """.format(",".join(column))
    cur.execute(sql)
    rows = cur.fetchall()
    return rows


def main():
    database = r"pythonsqlite.db"

    conn = create_connection(database)
    if conn is not None:
        sql_create_stocks_daily_table = """ 
            CREATE TABLE IF NOT EXISTS stocks_hourly(
                UPDATE_DATE date,
                PRICE_DATE date,
                FIGI text,
                CLOSE_PRICE DECIMAL, 
                OPEN_PRICE DECIMAL, 
                HIGH_PRICE DECIMAL, 
                LOW_PRICE DECIMAL, 
                TYPE text , 
                PRIMARY KEY (PRICE_DATE, FIGI)
        )"""
        create_table(conn, sql_create_stocks_daily_table)
    else:
        print("Error with connection, captain!")

    figi_list = pd.read_sql("SELECT FIGI FROM stocks", conn)["FIGI"].tolist()
    client = ti.SyncClient(TOKEN)

    errors_figi = []

    with conn:
        figi_in_db = pd.read_sql("""SELECT DISTINCT FIGI FROM stocks_hourly""", conn)[
            "FIGI"
        ].tolist()

        column_order = [
            "UPDATE_DATE",
            "PRICE_DATE",
            "FIGI",
            "CLOSE_PRICE",
            "OPEN_PRICE",
            "HIGH_PRICE",
            "LOW_PRICE",
            "TYPE",
        ]

        hourly_ranges = []
        for h in range(52):
            hourly_ranges.append(
                [
                    (
                        dt.datetime.strptime("2020-06-01", "%Y-%m-%d").date()
                        + dt.timedelta(7 * h)
                    ).strftime("%Y-%m-%d")
                    + "T00:00:00+03:00",
                    (
                        dt.datetime.strptime("2020-06-01", "%Y-%m-%d").date()
                        + dt.timedelta(7 * (h + 1) - 1)
                    ).strftime("%Y-%m-%d")
                    + "T23:59:00+03:00",
                ]
            )

        i = 0

        while i < len(figi_list):
            print(i, figi_list[i])
            if figi_list[i] in figi_in_db:
                i += 1
                continue
            j = 0
            while j < len(hourly_ranges):
                try:
                    response = client.get_market_candles(
                        figi=figi_list[i],
                        from_=hourly_ranges[j][0],
                        to=hourly_ranges[j][1],
                        interval=CandleResolution("hour"),
                    )
                    candles = pd.DataFrame(response.dict()["payload"]["candles"])
                    if len(candles) == 0:
                        j += 1
                        continue
                    candles.rename(
                        columns={
                            "c": "CLOSE_PRICE",
                            "o": "OPEN_PRICE",
                            "h": "HIGH_PRICE",
                            "l": "LOW_PRICE",
                            "figi": "FIGI",
                            "time": "PRICE_DATE",
                        },
                        inplace=True,
                    )
                    candles["UPDATE_DATE"] = dt.datetime.now()
                    candles["TYPE"] = "hour"
                    candles["CLOSE_PRICE"] = candles["CLOSE_PRICE"].astype(float)
                    candles["OPEN_PRICE"] = candles["OPEN_PRICE"].astype(float)
                    candles["HIGH_PRICE"] = candles["HIGH_PRICE"].astype(float)
                    candles["LOW_PRICE"] = candles["LOW_PRICE"].astype(float)
                    candles[column_order].to_sql(
                        name="stocks_hourly",
                        con=conn,
                        method="multi",
                        if_exists="append",
                        index=False,
                    )
                    j += 1
                except TooManyRequestsError:
                    print("sleep")
                    time.sleep(30)
                except UnexpectedError:
                    errors_figi.append(
                        [figi_list[i], hourly_ranges[0], hourly_ranges[1]]
                    )
                    j += 1

            i += 1


if __name__ == "__main__":
    main()
