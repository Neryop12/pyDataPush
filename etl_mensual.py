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
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


if __name__ == '__main__':
    conn = openConnection()
    # Facebook
    try:
        dfcampanas = medios.Spreadsheet(
            db.FBM['key'], db.FBM['media'], db.FBM['campanas'])

        medios.cuentas(dfcampanas, db.FBS['media'], conn)
        medios.campanas(dfcampanas, db.FBS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.FBS['media'], conn)

    except Exception as e:
        print(e)
    # GOSogle
    try:
        dfcampanas = medios.Spreadsheet(
            db.GOM['key'], db.GOM['media'], db.GOM['campanas'])

        medios.cuentas(dfcampanas, db.GOS['media'], conn)
        medios.campanas(dfcampanas, db.GOS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.GOS['media'], conn)

    except Exception as e:
        print(e)

    # TWSitter
    try:
        dfcampanas = medios.Spreadsheet(
            db.TWM['key'], db.TWM['media'], db.TWM['campanas'])

        medios.cuentas(dfcampanas, db.TWS['media'], conn)
        medios.campanas(dfcampanas, db.TWS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.TWS['media'], conn)

    except Exception as e:
        print(e)
     # MediaMath
    try:
        mm.GetToken()
        mm.GetSession()
        mm.CuentasCampanas(conn)
    except Exception as e:
        print(e)

    try:
        dfcampanas = medios.Spreadsheet(
        db.AFM['key'], db.AFM['media'], db.AFM['campanas'])

        medios.cuentas(dfcampanas, db.AFS['media'], conn)
        medios.campanas(dfcampanas, db.AFS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.AFS['media'], conn)
        
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


    # Cerramos la conexion
    sql.connect.close(conn)
