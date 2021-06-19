import sqlite3
from sqlite3 import Error
import tinvest as ti
import os
import datetime as dt
from tinvest import CandleResolution
import time
import pandas as pd

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
    except Error as e:
        print(e)


def create_stock(conn, stock):
    sql = """ 
    INSERT INTO stocks(
        UPDATE_DATE, 
        CURRENCY, 
        FIGI, 
        ISIN,
        LOT,
        MIN_PRICE_INCREMENT,
        NAME,
        TICKER,
        TYPE,
        MIN_QUANTITY) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, stock)
    conn.commit()
    return cur.lastrowid


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
        sql_create_stocks_table = """ 
            CREATE TABLE IF NOT EXISTS stocks(
                UPDATE_DATE DATE,
                CURRENCY text, 
                FIGI text PRIMARY KEY,
                ISIN text, 
                LOT	integer,
                MIN_PRICE_INCREMENT float,
                NAME text,
                TICKER text,
                TYPE text,
                MIN_QUANTITY integer

        )"""
        create_table(conn, sql_create_stocks_table)

    else:
        print("Error with connection, captain!")

    client = ti.SyncClient(TOKEN)
    response = client.get_market_stocks()
    result = response.dict()["payload"]["instruments"]

    with conn:
        stocks_df = pd.read_sql("SELECT * FROM stocks", conn)
        figi_in_db = set(stocks_df["FIGI"])

        for r in result:
            if r["figi"] in figi_in_db:
                continue

            increament = (
                None
                if r["min_price_increment"] is None
                else float(r["min_price_increment"])
            )
            stock = (
                dt.datetime.now(),
                r["currency"].value,
                r["figi"],
                r["isin"],
                r["lot"],
                increament,
                r["name"],
                r["ticker"],
                r["type"].value,
                r["min_quantity"],
            )
            stock_id = create_stock(conn, stock)


if __name__ == "__main__":
    main()
