#from urllib.parse import quote
#import time
#import requests
#import json
import pymysql.cursors
#import re
from datetime import datetime
import pytz

# Global variable declaration
global connectioncp
global cursor

# Database connectioncp parameters
global strdbhost
global lngdbport
global strdbuser
global strdbpassword
global strdbname

import citizenphilsecrets as cps
strdbhost = cps.strdbhost
lngdbport = cps.lngdbport
strdbuser = cps.strdbuser
strdbpassword = cps.strdbpassword
strdbname = cps.strdbname
strsqlns = cps.strsqlns
strtmdbapidomainurl = cps.strtmdbapidomainurl
strtmdbapikey = cps.strtmdbapikey
strtmdbapitoken = cps.strtmdbapitoken

lnguseridsession = 1
strlanguagecountry = "en-US"
strlanguage = "en"

connectioncp = pymysql.connect(host=strdbhost, port=lngdbport, user=strdbuser, password=strdbpassword, database=strdbname, cursorclass=pymysql.cursors.DictCursor)

paris_tz = pytz.timezone(cps.strusertimezone)

def f_sqlupdatearray(strsqltablename, arrpersoncouples, strsqlupdatecondition, intaddstdfields):
    """
    Insert or update a record in a SQL table based on whether it already exists.
    
    Parameters:
    -----------
    strsqltablename : str
        The name of the SQL table to insert/update records in
    arrpersoncouples : dict
        Dictionary containing column names as keys and their corresponding values
        to be inserted or updated in the database table
    strsqlupdatecondition : str
        SQL WHERE condition string used to check if record exists and for updates
        (e.g., "id = 123" or "name = 'John' AND age = 30")
    intaddstdfields : int
        Flag to determine if standard fields should be automatically added:
        - 1: Add standard fields (TIM_UPDATED, DELETED, DAT_CREAT, ID_CREATOR, ID_OWNER, ID_USER_UPDATED)
        - 0: Do not add standard fields
    
    Returns:
    --------
    int or None
        - If inserting a new record: returns the auto-generated ID (lastrowid) of the inserted record
        - If updating an existing record: returns None (no explicit return value)
    
    Behavior:
    ---------
    - Checks if a record exists using the provided condition
    - If record doesn't exist: performs INSERT with optional standard fields
    - If record exists: performs UPDATE with proper value escaping for strings
    - Handles different data types (int, float, None/NULL, strings) appropriately
    - Commits transaction on success, rolls back on MySQL errors
    """
    global connectioncp
    global paris_tz
    
    cursor2 = connectioncp.cursor()
    if intaddstdfields == 1:
        if "TIM_UPDATED" not in arrpersoncouples:
            arrpersoncouples["TIM_UPDATED"] = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    strsqlexists = f"SELECT * FROM {strsqltablename} WHERE {strsqlupdatecondition}"
    # print(strsqlexists)
    cursor2.execute(strsqlexists)
    lngrowcount = cursor2.rowcount
    if lngrowcount == 0:
        # Record does not exist
        if intaddstdfields == 1:
            if "DELETED" not in arrpersoncouples:
                arrpersoncouples["DELETED"] = 0
            if "DAT_CREAT" not in arrpersoncouples:
                arrpersoncouples["DAT_CREAT"] = datetime.now(paris_tz).strftime("%Y-%m-%d")
            if "ID_CREATOR" not in arrpersoncouples:
                arrpersoncouples["ID_CREATOR"] = lnguseridsession
            if "ID_OWNER" not in arrpersoncouples:
                arrpersoncouples["ID_OWNER"] = lnguseridsession
            if "ID_USER_UPDATED" not in arrpersoncouples:
                arrpersoncouples["ID_USER_UPDATED"] = lnguseridsession
        
        # print(arrpersoncouples)
        # ("INSERT INTO")
        strsqlinsertcolumns = ', '.join(arrpersoncouples.keys())
        strsqlinsertplaceholders = ', '.join(['%s'] * len(arrpersoncouples))
        strsqlinsert = f"INSERT INTO {strsqltablename} ({strsqlinsertcolumns}) VALUES ({strsqlinsertplaceholders})"
        # print(strsqlinsert)
        cursor2.execute(strsqlinsert, list(arrpersoncouples.values()))
        lngnewid = cursor2.lastrowid
        connectioncp.commit()
        return lngnewid
    else:
        # Record already exist
        # print(arrpersoncouples)
        # generate key/pair array
        arrvalues = []
        for key,value in arrpersoncouples.items():
            # print(f"{key} = {value}")
            if isinstance(value, bool):
                arrvalues.append(f"{key} = {1 if value else 0}")
            elif isinstance(value, int): # Handle Integers
                arrvalues.append("{key} = {value}".format(key=key, value=value))
            elif isinstance(value, float): # Handle floats
                arrvalues.append("{key} = {value}".format(key=key, value=value))
            elif value is None: # Handle NULL
                arrvalues.append("{key} = NULL".format(key=key))
            else: # Default Handler
                # Fixing the value when it contains a \' element (espaped as \\\')
                value=value.replace("\\\'", "'")
                # Fixing the value when it contains a \" element (espaped as \\\")
                value=value.replace('\\\"', '"')
                # value=value.replace("\\'", "'")
                value=value.replace("'", "\\'")
                arrvalues.append("{key} = '{value}'".format(key=key, value=value))
        # generate string from key/pair array
        strsqlupdatesetclause = ", ".join(arrvalues)
        # Define the condition for the update
        # strsqlupdatecondition = f"{strsqlkeyfield} = {strsqlkeyvalue}"
        # format SQL string
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        # print(strsqlupdate)
        try:
            cursor2.execute(strsqlupdate)
            # print("UPDATE")
            connectioncp.commit()
        except pymysql.MySQLError as e:
            print(f"❌ MySQL Error: {e}")
            connectioncp.rollback()

# Server variables functions

def f_getservervariable(strvarname,lnglang=0):
    global strsqlns
    global connectioncp
    
    cursor2 = connectioncp.cursor()
    strresult = ""
    strsqlselect = "SELECT VAR_VALUE FROM " + strsqlns + "SERVER_VARIABLE WHERE DELETED = 0 AND VAR_NAME = " + f_stringtosql(strvarname)
    if lnglang > 0:
        # Language is managed for server variables
        strsqlselect += " AND ID_LANG = " + str(lnglang)
    cursor2.execute(strsqlselect)
    results = cursor2.fetchall()
    for row in results:
        strresult = row['VAR_VALUE']
        break
    return strresult
    
def f_setservervariable(strvarname,strvarvalue,strvardesc="",lnglang=0):
    global strsqlns
    
    arrcouples = {}
    arrcouples["VAR_NAME"] = strvarname
    arrcouples["VAR_VALUE"] = strvarvalue
    arrcouples["DESCRIPTION"] = strvarname
    arrcouples["LONG_DESC"] = strvardesc
    arrcouples["ID_LANG"] = lnglang
    # print(arrcouples)
    strsqltablename = strsqlns + "SERVER_VARIABLE"
    strsqlupdatecondition = f"DELETED = 0 AND VAR_NAME = '{strvarname}'"
    f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)

def convert_seconds_to_duration(seconds):
    """Convert seconds to a readable format: days, hours, minutes, seconds"""
    if seconds < 0:
        return "Invalid duration (negative seconds)"
    
    days = seconds // 86400  # 86400 seconds in a day
    hours = (seconds % 86400) // 3600  # 3600 seconds in an hour
    minutes = (seconds % 3600) // 60
    remaining_seconds = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if remaining_seconds > 0:
        parts.append(f"{remaining_seconds} second{'s' if remaining_seconds != 1 else ''}")
    
    return ", ".join(parts)

def f_stringtosql(strtext):
    return "'" + strtext.replace("'","\\'") + "'"

