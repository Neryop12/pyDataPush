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
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])

    try:
        dfni = mediosextras.Spreadsheet(db.CLARO['key'],  db.CLARO['NI'])

        mediosextras.cuentas(dfni, conn)
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn)
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    try:
        dfni = mediosextras.Spreadsheet(db.CLARO['key'],  db.CLARO['GT'])

        mediosextras.cuentas(dfni, conn)
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn)
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    try:
        dfni = mediosextras.Spreadsheet(db.CLARO['key'],  db.CLARO['HN'])

        mediosextras.cuentas(dfni, conn)
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn)
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO['key'],  db.CLARO['CR'])

        mediosextras.cuentas(dfni, conn)
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn)
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    try:
        dfni = mediosextras.Spreadsheet(db.CLARO['key'],  db.CLARO['PA'])

        mediosextras.cuentas(dfni, conn)
        mediosextras.campanas(dfni, conn)
        mediosextras.metricas_campanas(dfni, conn)
        # mediosextras.diario_campanas(dfni, conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    # Cerramos la conexion
    sql.connect.close(conn)
