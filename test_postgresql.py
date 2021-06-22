import psycopg2
import config.db as db
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath_semanal as mm
import datos_adform_semanal as adf
import sys


def openConnection():
    try:
        conn = psycopg2.connect(
            host='visor-rds.cebdvwvqle9k.us-west-1.rds.amazonaws.com',
            database='MediaPlatformsReports',
            user='postgres',
            password='Postgres21'
        )
        #conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',user='omgdev', password='Sdev@2002!', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


if __name__ == '__main__':
    conn = openConnection()
    # Facebook
    try:
        dfcampanas = medios.Spreadsheet(
            db.FBS['key'], db.FBS['media'], db.FBS['campanas'])

        #medios.cuentas(dfcampanas, db.FBS['media'], conn)
        #medios.campanas(dfcampanas, db.FBS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.FBS['media'], conn)

    except Exception as e:
        print(e)


    # Cerramos la conexion
    conn.close()
    print('Database connection closed.')
