import mysql.connector as mysql
from mysql.connector import Error
import sys


def openConnection():
    try:
        conn = mysql.connect(host='wordpress.ctoj0d7vtn5z.us-east-1.rds.amazonaws.com', database='LaChalupa',
                             user='prodwol', password=r'+8<j%Ejb\7BE\5du', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")


if __name__ == '__main__':
    conn = openConnection()
    cur = conn.cursor()
    query = """UPDATE `LaChalupa`.`users_profile` SET `count` = '4' WHERE (`id` > '0' and count < 4);"""
    
    try:
        cur.execute(query)
        print('FIN')
    except Exception as e:
        print(e)


