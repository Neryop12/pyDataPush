import config.db as db
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time
import configparser
import pandas as pd


# Conexion de base de datos

def openConnection():
    global conn
    try:
        conn = mysql.connect(host=db.DATABASE_CONFIG['host'], database=db.DATABASE_CONFIG['dbname'],
                             user=db.DATABASE_CONFIG['user'], password=db.DATABASE_CONFIG['password'], autocommit=db.DATABASE_CONFIG['autocommit'],port=db.DATABASE_CONFIG['port'])
    except Exception as e:
        raise ValueError ('No se pudo establecer conexión con la base de datos!')


def Spreadsheet(key,tipo,medio):
  try:
    Spreadsheet=key
    url='https://docs.google.com/spreadsheet/ccc?key='+Spreadsheet+'&output=csv'
    df=pd.read_csv(url)
    df=df.replace(np.nan,"0")
    print (df)
  except Exception as e:
    raise ValueError ('Ocurrio un error al conectarse con la hoja de SpreadSheets!')


if __name__ == '__main__':
    # openConnection()
    Spreadsheet('1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4','Campanas','FB')
