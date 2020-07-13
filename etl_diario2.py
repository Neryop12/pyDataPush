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
        dfcampanas = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['campanas'])

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'FB', db.DAY['FB'])
        medios.actualizarestado(dfdiarios, 'FB', conn)
        medios.diario_campanas(dfdiarios, 'FB', conn)

        dextras = medios.Spreadsheet(db.EXTRA['key'], 'FB', 0)
        medios.extrametrics(dextras, 'FB', conn)

        dactions = medios.Spreadsheet(db.EXTRA['key'], 'FB', db.EXTRA['CONV'])
        medios.actions(dactions, 'FB', conn)

    except Exception as e:
        print(e)
    # Google
    try:
        dfcampanas = medios.Spreadsheet(
            db.GO['key'], db.GO['media'], db.GO['campanas'])

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'GO', db.DAY['GO'])
        medios.actualizarestado(dfdiarios, 'GO', conn)
        medios.diario_campanas(dfdiarios, 'GO', conn)

    except Exception as e:
        print(e)

    # Twitter
    try:
        dfcampanas = medios.Spreadsheet(
            db.TW['key'], db.TW['media'], db.TW['campanas'])

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'TW', db.DAY['TW'])
        medios.actualizarestado(dfdiarios, 'TW', conn)
        medios.diario_campanas(dfdiarios, 'TW', conn)

    except Exception as e:
        print(e)
     # MediaMath
    try:
        mm.GetToken()
        mm.GetSession()
        mm.CuentasCampanas(conn)
        mm.Adsets(conn)
        mm.Ads(conn)
    except Exception as e:
        print(e)

    # Cerramos la conexion
    sql.connect.close(conn)
