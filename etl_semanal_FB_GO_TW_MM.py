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
            db.FBS['key'], db.FBS['media'], db.FBS['campanas'])
        dfadsets = medios.Spreadsheet(
            db.FBS['key'], db.FBS['media'], db.FBS['adsets'])
        dfads = medios.Spreadsheet(db.FBS['key'], db.FBS['media'], db.FBS['ads'])

        medios.cuentas(dfcampanas, db.FBS['media'], conn)
        medios.campanas(dfcampanas, db.FBS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.FBS['media'], conn,'week')
        medios.metricas_adsets(dfadsets, db.GO['media'], conn, 'week')
        medios.metricas_ads(dfads, db.GO['media'], conn, 'week')

    except Exception as e:
        print(e)

def semanal_GO():
    try:
        dfcampanas = medios.Spreadsheet(
            db.GOS['key'], db.GOS['media'], db.GOS['campanas'])

        medios.cuentas(dfcampanas, db.GOS['media'], conn)
        medios.campanas(dfcampanas, db.GOS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.GOS['media'], conn,'week')

    except Exception as e:
        print(e)

def semanal_TW():
    try:
        dfcampanas = medios.Spreadsheet(
            db.TWS['key'], db.TWS['media'], db.TWS['campanas'])

        medios.cuentas(dfcampanas, db.TWS['media'], conn)
        medios.campanas(dfcampanas, db.TWS['media'], conn)
        medios.metricas_campanas(dfcampanas, db.TWS['media'], conn,'week')

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
        db.AFS['key'], db.AFS['media'], db.AFS['campanas'])

        medios.cuentas(dfcampanas, db.AFS['media'], conn)
        #medios.campanas(dfcampanas, db.AFS['media'], conn)
        #medios.metricas_campanas(dfcampanas, db.AFS['media'], conn,'week')
        
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


if __name__ == '__main__':
    conn = openConnection()
    semanal_FB()
    semanal_GO()
    semanal_TW()
    semanal_MM()
    semanal_ADF()
    sql.connect.close(conn)
