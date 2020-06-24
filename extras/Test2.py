import config.db as db
import dbconnect as sql
import datos_adsmovil as am

if __name__ == '__main__':

    # Iniciamos la conexion
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])

# Adsmovil
    try:
        am.GetToken()
        am.Camapanas('AM', conn)
    except Exception as e:
        print(e)

    except Exception as e:
        print(e)
    # Cerramos la conexion
    sql.connect.close(conn)
