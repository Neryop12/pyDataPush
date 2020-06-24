import config.db as db
import dbconnect as sql
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import configparser
import pandas as pd
import numpy as np

now = datetime.now()
CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")
