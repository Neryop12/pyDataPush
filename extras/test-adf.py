from .. import dbconnect as sql
import config.db as db
import datos_fbgotw as medios
import datos_mediamath as mm
import datos_adform as adf
import datos_mediosextras as mediosextras
import datos_adsmovil as am

if __name__ == '__main__':

    # Iniciamos la conexion
    conn = sql.connect.open(db.DBA['host'], db.DBA['user'], db.DBA['password'],
                            db.DBA['dbname'], db.DBA['port'], db.DBA['autocommit'])
    # Facebook
    
    try:
        adf.GetToken()
        adf.CuentasCampanas(conn)
        adf.Ads(conn)
        adf.Adsets(conn)
        adf.CreativesAds(conn)
    except Exception as e:
        print(e)

    # Adsmovil
    # Cerramos la conexion
    sql.connect.close(conn)
