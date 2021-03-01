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
        conn = mysql.connect(host='127.0.0.1', database='MediaPlatformsReports',
                             user='root', password='', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


if __name__ == '__main__':
    conn = openConnection() 
    # Facebook
    try:
        
        dfcampanas = medios.Spreadsheet(
            db.FBA['key'], db.FBA['media'], db.FBA['campanas'])

        #medios.cuentas(dfcampanas, db.FBA['media'], conn)
        #medios.campanas(dfcampanas, db.FBA['media'], conn)
        #medios.metricas_campanas(dfcampanas, db.FBA['media'], conn)

    except Exception as e:
        print(e)
    # GOSogle
    try:
        dfcampanas = medios.Spreadsheet(
            db.GOA['key'], db.GOA['media'], db.GOA['campanas'])

        #medios.cuentas(dfcampanas, db.GOA['media'], conn)
        #medios.campanas(dfcampanas, db.GOA['media'], conn)
        #medios.metricas_campanas(dfcampanas, db.GOA['media'], conn)

    except Exception as e:
        print(e)

    # TWSitter
    try:
        dfcampanas = medios.Spreadsheet(
            db.TWA['key'], db.TWA['media'], db.TWA['campanas'])

        #medios.cuentas(dfcampanas, db.TWA['media'], conn)
        #medios.campanas(dfcampanas, db.TWA['media'], conn)
        medios.metricas_campanas(dfcampanas, db.TWA['media'], conn)

    except Exception as e:
        print(e)
     # MediaMath
    
    # Cerramos la conexion
    sql.connect.close(conn)
