import mysql.connector as mysql
from mysql.connector import Error
import config.db as db
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath as mm
import datos_adform as adf
import datos_mediosextras as mediosextras
import datos_adsmovil as am
import sys

if __name__ == '__main__':

    
    # Iniciamos la conexion
    conn = mysql.connect(host='127.0.0.1', database='MediaPlatformsReports',
                             user='root', password='AnnalectDB2019', autocommit=True)
    ###MEDIOS EXTERNOS                             
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['CR'])

        
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn,'CR')
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['CR'])

        
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn,'CR')
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['CR'])

        
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn,'CR')
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)