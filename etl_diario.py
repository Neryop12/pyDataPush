import mysql.connector as mysql
from mysql.connector import Error
import config.db as DB
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath as mm
import datos_adform as adf
import datos_mediosextras as mediosextras
import datos_adsmovil as am
import sys


def openConnection():
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='root', password='AnnalectDB2019', autocommit=True)
        return conn
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


if __name__ == '__main__':
    conn = openConnection()
    # Facebook
    # Facebook
    try:
        dfdiarios = medios.Spreadsheet(
            DB.FB['key'], DB.FB['media'], DB.DAY['FB'])

        medios.diario_campanas(dfdiarios, 'FB', conn)

        dextras = medios.Spreadsheet(DB.EXTRA['key'], 'FB', 0)
        medios.extrametrics(dextras, 'FB', conn)

        dactions = medios.Spreadsheet(
            DB.EXTRA['key'], 'FB', DB.EXTRA['CONV'])
        medios.actions(dactions, 'FB', conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    # Google
    try:

        dfdiarios = medios.Spreadsheet(DB.DAY['key'], 'GO', DB.DAY['GO'])
        medios.actualizarestado(dfdiarios, 'GO', conn)
        medios.diario_campanas(dfdiarios, 'GO', conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    # Twitter
    try:

        dfdiarios = medios.Spreadsheet(DB.DAY['key'], 'TW', DB.DAY['TW'])
        medios.actualizarestado(dfdiarios, 'TW', conn)
        medios.diario_campanas(dfdiarios, 'TW', conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        # Twitter
    try:

        dfdiarios = medios.Spreadsheet(DB.DAY['key'], 'AF', DB.DAY['AF'])
        medios.actualizarestado(dfdiarios, 'AF', conn)
        medios.diario_campanas(dfdiarios, 'AF', conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
     # MediaMath
    try:
        mm.GetToken()
        mm.GetSession()
        mm.CuentasCampanas(conn)
        mm.Adsets(conn)
        mm.Ads(conn)
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


    # Cerramos la conexion
    conn.close()
