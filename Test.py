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

        #medios.cuentas(dfcampanas, db.FB['media'], conn)
        medios.campanas(dfcampanas, db.FB['media'], conn)
        #medios.adsets(dfadsets, db.FB['media'], conn)
        #medios.ads(dfads, db.FB['media'], conn)
        #medios.creative_ads(dfads, db.FB['media'], conn)
    except Exception as e:
        print(e)

    # Cerramos la conexion
    sql.connect.close(conn)
