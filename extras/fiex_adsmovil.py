# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta

host= '3.95.117.169'
# host= 'localhost'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit= 'True'
def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def CampAM(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    slqCampana = """ select b.Account,b.Media,a.CampaingID,a.Campaingname,a.Campaignstatus,a.EndDate from Campaings a
    inner join Accounts b on a.AccountsID=b.AccountsID where b.Media='AM'"""
    sqlInserErrors = "UPDATE Campaings set Campaingname=%s where CampaingID=%s"

    try:
        cur.execute(slqCampana,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
          searchObj = re.search(r'([\S\w\d_.&/-]+)(\s\([0-9]+\))', result[3], re.M | re.I)
          if searchObj:
            print(searchObj.group(1))
            print(searchObj.group(2))
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            valores=(searchObj.group(1),result[2])
            cur.execute(sqlInserErrors,valores)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
    except Exception as e:
        print(e)

    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    CampAM(conn)
    conn.close()