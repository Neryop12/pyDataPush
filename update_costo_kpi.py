import mysql.connector as mysql
from mysql.connector import Error
import config.db as db
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath_semanal as mm
import datos_adform_semanal as adf
import sys


def openConnection():
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',
                             user='root', password='AnnalectDB2019', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


if __name__ == '__main__':
    conn = openConnection()
    cur = conn.cursor()
    query = """Select id,Cost, Result from CampaingMetrics where KPICost = 0 and Result > 0  and Objetive = 'CPMA' AND Week >0 """
    query2 = """UPDATE `MediaPlatformsReports`.`CampaingMetrics` SET `KPICost` = %s where id=%s """
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        campanas = []
        for row in resultscon:
            kpi = int(row[1])/int(row[2])
            campana = [kpi, row[0]]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()

        cur.close()
        print('FIN')
    except Exception as e:
        print(e)