import mysql.connector as mysql
from mysql.connector import Error
import sys
from datetime import datetime, timedelta
import psycopg2

def openConnection():
    global conn 
    try:
        conn = mysql.connect(host='3.95.117.169', database='mfcgt',
                             user='root', password='AnnalectDB2019', autocommit=True)
        return conn
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")

def openConnection_postgres():
    global conn_pos
    try:
        conn_pos = psycopg2.connect(
            host='visor-rds.cebdvwvqle9k.us-west-1.rds.amazonaws.com',
            database='MediaPlatformsReports',
            user='postgres',
            password='Postgres21')
        return conn_pos
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


def reviewErroresNomen(conn, conn_pos):
    global cur
    cur = conn.cursor()
    cur_pos = conn_pos.cursor()
    Comentario = ''
    Estatus = ''
    
    try:
        print(datetime.now())
        sqlConjuntosFB = """
        select Marca.id,Marca.nombre, Cliente.nombre from mfcgt.dmarca Marca
        inner join mfcgt.dcliente Cliente on Marca.idcliente = Cliente.id
        ;
        """
        slqUpdate =  """
        INSERT INTO "mfcgt"."dmarca" VALUES ( %s, %s, %s)
        ON CONFLICT (id) 
        DO NOTHING;
        """

        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        cur_pos.executemany(slqUpdate, resultscon)
        conn_pos.commit()
        cur.close()
        cur_pos.close()
    # ANALISIS IMPRESIONES Y
        # print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("put_estado_mfc", "{}", "pushReviewErrors.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print(datetime.now())

if __name__ == '__main__':
    openConnection()
    openConnection_postgres()
    reviewErroresNomen(conn, conn_pos)
    conn.close()
    conn_pos.close()