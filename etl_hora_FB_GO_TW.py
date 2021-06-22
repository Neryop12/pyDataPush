import sys
import psycopg2
import dbconnect as sql
import datos.datos_fbgotw as medios
import datos.datos_mediamath_semanal as mm
import datos.datos_adform_semanal as adf
import config.db as db
conn = None
def openConnection():
    try:
        conn = psycopg2.connect(
            host='visor-rds.cebdvwvqle9k.us-west-1.rds.amazonaws.com',
            database='MediaPlatformsReports',
            user='postgres',
            password='Postgres21')
        #conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',user='omgdev', password='Sdev@2002!', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")

def semanal_FB():
    try:
        dfcampanas = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['campanas'])
        dfadsets = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['adsets'])
        dfads = medios.Spreadsheet(db.FB['key'], db.FB['media'], db.FB['ads'])

        medios.cuentas(dfcampanas, db.FB['media'], conn)
        medios.campanas(dfcampanas, db.FB['media'], conn)
        medios.metricas_campanas(dfcampanas, db.FB['media'], conn,'hour')
        medios.metricas_adsets(dfadsets, db.FB['media'], conn, 'hour')
        medios.metricas_ads(dfads, db.FB['media'], conn, 'hour')

    except Exception as e:
        print(e)

def semanal_GO():
    try:
        dfcampanas = medios.Spreadsheet(
            db.GO['key'], db.GO['media'], db.GO['campanas'])

        medios.cuentas(dfcampanas, db.GO['media'], conn)
        medios.campanas(dfcampanas, db.GO['media'], conn)
        medios.metricas_campanas(dfcampanas, db.GO['media'], conn,'hour')

    except Exception as e:
        print(e)

def semanal_TW():
    try:
        dfcampanas = medios.Spreadsheet(
            db.TW['key'], db.TW['media'], db.TW['campanas'])

        medios.cuentas(dfcampanas, db.TW['media'], conn)
        medios.campanas(dfcampanas, db.TW['media'], conn)
        medios.metricas_campanas(dfcampanas, db.TW['media'], conn,'hour')

    except Exception as e:
        print(e)

def semanal_MM():
    try:
        mm.GetToken()
        #mm.GetSession()
        #mm.CuentasCampanas(conn)
    except Exception as e:
        print(e)

def semanal_ADF():
    try:
        dfcampanas = medios.Spreadsheet(
        db.AF['key'], db.AF['media'], db.AF['campanas'])

        medios.cuentas(dfcampanas, db.AF['media'], conn)
        #medios.campanas(dfcampanas, db.AF['media'], conn)
        #medios.metricas_campanas(dfcampanas, db.AF['media'], conn,'hour')
        
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


if __name__ == '__main__':
    conn = openConnection()
    semanal_FB()
    semanal_GO()
    semanal_TW()
    semanal_ADF()
    sql.connect.close(conn)
