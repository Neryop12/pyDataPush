import config.db as DB
import dbconnect as sql
import datos_fbgotw as medios
import mysql.connector as mysql
from mysql.connector import Error
import sys


def openConnection():
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='root', password='AnnalectDB2019', autocommit=True)
        return conn
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


if __name__ == '__main__':
    conn = openConnection()
    # Facebook
    try:
        dfcampanas = medios.Spreadsheet(
            DB.FB['key'], DB.FB['media'], DB.FB['campanas'])
        dfadsets = medios.Spreadsheet(
            DB.FB['key'], DB.FB['media'], DB.FB['adsets'])
        dfads = medios.Spreadsheet(
            DB.FB['key'], DB.FB['media'], DB.FB['ads'])

        medios.cuentas(dfcampanas, DB.FB['media'], conn)
        medios.campanas(dfcampanas, DB.FB['media'], conn)
        medios.adsets(dfadsets, DB.FB['media'], conn)
        medios.ads(dfads, DB.FB['media'], conn)
        medios.creative_ads(dfads, DB.FB['media'], conn)
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

        # MediaMath

    # Google
    try:
        dfcampanas = medios.Spreadsheet(
            DB.GO['key'], DB.GO['media'], DB.GO['campanas'])
        dfadsets = medios.Spreadsheet(
            DB.GO['key'], DB.GO['media'], DB.GO['adsets'])
        dfads = medios.Spreadsheet(
            DB.GO['key'], DB.GO['media'], DB.GO['ads'])

        medios.cuentas(dfcampanas, DB.GO['media'], conn)
        medios.campanas(dfcampanas, DB.GO['media'], conn)
        medios.adsets(dfadsets, DB.GO['media'], conn)
        medios.ads(dfads, DB.GO['media'], conn)
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    # Twitter
    try:
        dfcampanas = medios.Spreadsheet(
            DB.TW['key'], DB.TW['media'], DB.TW['campanas'])
        dfadsets = medios.Spreadsheet(
            DB.TW['key'], DB.TW['media'], DB.TW['adsets'])
        dfads = medios.Spreadsheet(
            DB.TW['key'], DB.TW['media'], DB.TW['ads'])

        medios.cuentas(dfcampanas, DB.TW['media'], conn)
        medios.campanas(dfcampanas, DB.TW['media'], conn)
        medios.adsets(dfadsets, DB.TW['media'], conn)
        medios.ads(dfads, DB.TW['media'], conn)
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

      # Adform
    try:
        dfcampanas = medios.Spreadsheet(
            DB.AF['key'], DB.AF['media'], DB.AF['campanas'])
        medios.cuentas(dfcampanas, DB.AF['media'], conn)
        medios.campanas(dfcampanas, DB.AF['media'], conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    # Cerramos la conexion
    conn.close()
