import config.db as db
import dbconnect as sql 
import datos_fbgotw as medios

if __name__ == '__main__':

    # Iniciamos la conexion
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])

    # Facebook
    try:
        dfcampanas = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['campanas'])
        dfadsets = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['adsets'])
        dfads = medios.Spreadsheet(db.FB['key'], db.FB['media'], db.FB['ads'])

        medios.cuentas(dfcampanas, db.FB['media'], conn)
        medios.campanas(dfcampanas, db.FB['media'], conn)
        medios.metricas_campanas(dfcampanas, db.FB['media'], conn)
        medios.adsets(dfadsets, db.FB['media'], conn)
    except Exception as e:
        print(e)

        # MediaMath

    # Google
    try:
        dfcampanas = medios.Spreadsheet(
            db.GO['key'], db.GO['media'], db.GO['campanas'])
        dfadsets = medios.Spreadsheet(
            db.GO['key'], db.GO['media'], db.GO['adsets'])
        dfads = medios.Spreadsheet(db.GO['key'], db.GO['media'], db.GO['ads'])

        medios.cuentas(dfcampanas, db.GO['media'], conn)
        medios.campanas(dfcampanas, db.GO['media'], conn)
        medios.metricas_campanas(dfcampanas, db.GO['media'], conn)
        medios.adsets(dfadsets, db.GO['media'], conn)
        medios.ads(dfads, db.GO['media'], conn)
    except Exception as e:
        print(e)

    # Twitter
    try:
        dfcampanas = medios.Spreadsheet(
            db.TW['key'], db.TW['media'], db.TW['campanas'])
        dfadsets = medios.Spreadsheet(
            db.TW['key'], db.TW['media'], db.TW['adsets'])
        dfads = medios.Spreadsheet(db.TW['key'], db.TW['media'], db.TW['ads'])

        medios.cuentas(dfcampanas, db.TW['media'], conn)
        medios.campanas(dfcampanas, db.TW['media'], conn)
        medios.metricas_campanas(dfcampanas, db.TW['media'], conn)
        medios.adsets(dfadsets, db.TW['media'], conn)
        medios.ads(dfads, db.TW['media'], conn)
    except Exception as e:
        print(e)

    # Cerramos la conexion
    sql.connect.close(conn)
