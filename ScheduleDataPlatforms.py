import pushDataMedia as DM
from pushDataMedia import conn
import threading
import schedule
import time
import mysql.connector as mysql
from datetime import datetime
import time
import logger

if __name__ == '__main__':
   DM.openConnection()
   DM.push_camps(conn)
   conn.close()