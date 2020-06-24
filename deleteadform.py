import config.db as db
import dbconnect as sql
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import configparser
import pandas as pd
import numpy as np

now = datetime.now()
CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")


def deleteadform(conn):
    cur = conn.cursor()
    query = """Select a.CampaingID,b.Media from Accounts b, Campaings a where a.AccountsID=b.AccountsID and Media='AF'"""
    query2 = """Select a.CampaingID,b.Media from Accounts b, Campaings a where a.AccountsID=b.AccountsID and Media='AF'"""
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        Errores = []
        for row in resultscon:
            print(row[0])

    except Exception as e:
        print(e)


if __name__ == '__main__':

    # Iniciamos la conexion
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])

    deleteadform(conn)
