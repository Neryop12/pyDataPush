# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time
import logger
conn = None

ACCESS_TOKEN_URL = "https://auth.mediamath.com/oauth/token"

def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def GetToken():
    url='https://auth.mediamath.com/oauth/token'
    Token=requests.post(
                ACCESS_TOKEN_URL,
                data={
                    "grant_type": "password",
                    "username": "sfranco@omg.com.gt",
                    "password": "SFomg2019",
                    "audience": "https://api.mediamath.com/",
                    "scope": "",
                    "client_id": "7Geve1fUt8luTYXCuB1KiVNjIDAcsGxl",
                    "client_secret": "gKDRia_oS-ChUinxxFNXou09DKLOFSaPTeaxQFfWhnA105NwK6BOXnoGgBh4FTfx"
                    }
                )
    Token=Token.json()

    print  Token['access_token']

if __name__ == '__main__':
    GetToken()