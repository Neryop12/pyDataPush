import mysql.connector as mysql
from mysql.connector import Error
import config.db as db
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath_semanal as mm
import datos_adform_semanal as adf


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
            db.FBS['key'], db.FBS['media'], db.FBS['campanas'])

        medios.cuentas(dfcampanas, db.FBS['media'], conn)
        medios.campanas(dfcampanas, db.FBS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.FBS['media'], conn)

    except Exception as e:
        print(e)
    # GOSogle
    try:
        dfcampanas = medios.Spreadsheet(
            db.GOS['key'], db.GOS['media'], db.GOS['campanas'])

        medios.cuentas(dfcampanas, db.GOS['media'], conn)
        medios.campanas(dfcampanas, db.GOS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.GOS['media'], conn)

    except Exception as e:
        print(e)

    # TWSitter
    try:
        dfcampanas = medios.Spreadsheet(
            db.TWS['key'], db.TWS['media'], db.TWS['campanas'])

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

    # Cerramos la conexion
    sql.connect.close(conn)
