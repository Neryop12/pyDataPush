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
    conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',
                             user='root', password='AnnalectDB2019', autocommit=True)
    ###MEDIOS EXTERNOS                             
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['CR'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'CR')
        ###mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['CR'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'CR - AM')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['CR'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'CR - GOS')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###
    ## GT
    ###
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['GT'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'GT')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['GT'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'GT -AM')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['GT'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'GT- GOS')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    
    ###
    ## sv
    ###
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['SV'])

        
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn,'SV')
        ###mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['SV'])

        
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn,'SV - AM')
        #mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['SV'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'SV - GOS')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    ###
    ## HN
    ###
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['HN'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'HN')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['HN'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'HN - AM')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['HN'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'HN - GOS')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    
    ###
    ## NI
    ###
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['NI'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'NI')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['NI'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'NI - AM')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['NI'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'NI - GOS')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    
    ###
    ## PA
    ###
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_Medios_Externos['key'],  db.CLARO_Medios_Externos['PA'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'PA')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###ADSMOVIL
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_ADSMOVIL['key'],  db.CLARO_ADSMOVIL['PA'])

        
        #mediosextras.campanas(dfni, conn)
        #mediosextras.metricas_campanas(dfni, conn,'PA - AM')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    ###Search
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO_GO_SEARCH['key'],  db.CLARO_GO_SEARCH['PA'])

        
        ##mediosextras.campanas(dfni, conn)
        ##mediosextras.metricas_campanas(dfni, conn,'PA - GOS')
        ##mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)