import mysql.connector as mysql
from mysql.connector import Error
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
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',
                             user='root', password='AnnalectDB2019', autocommit=True)
        #conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',user='omgdev', password='Sdev@2002!', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")

def semanal_FB():
    try:
        dfcampanas = medios.Spreadsheet(
            db.FBD['key'], db.FBD['media'], db.FBD['campanas'])
        dfadsets = medios.Spreadsheet(
            db.FBD['key'], db.FBD['media'], db.FBD['adsets'])
        dfads = medios.Spreadsheet(db.FBD['key'], db.FBD['media'], db.FBD['ads'])

        medios.cuentas(dfcampanas, db.FBD['media'], conn)
        medios.campanas(dfcampanas, db.FBD['media'], conn)
        medios.diario_campanas(dfcampanas, db.FBD['media'], conn,)
        medios.metricas_adsets_daily(dfadsets, db.FBD['media'], conn,)
        #medios.metricas_ads(dfads, db.FBD['media'], conn, 'daily')

    except Exception as e:
        print(e)

def semanal_GO():
    try:
        dfcampanas = medios.Spreadsheet(
            db.GOD['key'], db.GOD['media'], db.GOD['campanas'])
        dfadsets = medios.Spreadsheet(
            db.GOD['key'], db.GOD['media'], db.GOD['adsets'])
        medios.cuentas(dfcampanas, db.GOD['media'], conn)
        medios.campanas(dfcampanas, db.GOD['media'], conn)
        medios.diario_campanas(dfcampanas, db.GOD['media'], conn)
        medios.metricas_adsets_daily(dfadsets, db.GOD['media'], conn)

    except Exception as e:
        print(e)

def semanal_TW():
    try:
        dfcampanas = medios.Spreadsheet(
            db.TWD['key'], db.TWD['media'], db.TWD['campanas'])

        medios.cuentas(dfcampanas, db.TWD['media'], conn)
        medios.campanas(dfcampanas, db.TWD['media'], conn)
        medios.metricas_campanas(dfcampanas, db.TWD['media'], conn,'daily')

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
        #medios.metricas_campanas(dfcampanas, db.AFS['media'], conn,'daily')
        
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


if __name__ == '__main__':
    conn = openConnection()
    #semanal_FB()
    semanal_GO()
    #semanal_TW()
    #semanal_ADF()
    sql.connect.close(conn)
