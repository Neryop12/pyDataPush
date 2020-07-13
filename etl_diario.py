import config.db as db
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath as mm
import datos_adform as adf
import datos_mediosextras as mediosextras
import datos_adsmovil as am

if __name__ == '__main__':

    # Iniciamos la conexion
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])
    # Facebook
    try:
        dfdiarios = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.DAY['FB'])

        medios.diario_campanas(dfdiarios, 'FB', conn)

        dextras = medios.Spreadsheet(db.EXTRA['key'], 'FB', 0)
        medios.extrametrics(dextras, 'FB', conn)

        dactions = medios.Spreadsheet(db.EXTRA['key'], 'FB', db.EXTRA['CONV'])
        medios.actions(dactions, 'FB', conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    # Google
    try:

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'GO', db.DAY['GO'])
        medios.actualizarestado(dfdiarios, 'GO', conn)
        medios.diario_campanas(dfdiarios, 'GO', conn)

    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    # Twitter
    try:

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'TW', db.DAY['TW'])
        medios.actualizarestado(dfdiarios, 'TW', conn)
        medios.diario_campanas(dfdiarios, 'TW', conn)

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
    sql.connect.close(conn)
