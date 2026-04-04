#from urllib.parse import quote
#import time
#import requests
#import json
import pymysql.cursors
#import re
from datetime import datetime
import pytz
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().with_name(".env"))

strdbhost = os.environ.get("DB_HOST", "")
lngdbport = int(os.environ.get("DB_PORT", "3306"))
strdbuser = os.environ.get("DB_USER", "")
strdbpassword = os.environ.get("DB_PASSWORD", "")
strdbname = os.environ.get("DB_NAME", "")
strsqlns = os.environ.get("DB_NAMESPACE", "")
strtmdbapidomainurl = os.environ.get("TMDB_API_DOMAIN_URL", "")
strtmdbapikey = os.environ.get("TMDB_API_KEY", "")
strtmdbapitoken = os.environ.get("TMDB_API_TOKEN", "")

headers = {
    "accept": "application/json",
    "Authorization": "Bearer " + strtmdbapitoken
}

lnguseridsession = 1
strlanguagecountry = "en-US"
strlanguage = "en"

connectioncp = None

paris_tz = pytz.timezone(os.environ.get("USER_TIMEZONE", "Europe/Paris"))

def f_getconnection():
    """
    Get the active MariaDB connection, creating it lazily if needed.

    Returns:
    --------
    pymysql.connections.Connection
        An open PyMySQL connection configured with the database settings
        loaded from the environment.

    Behavior:
    ---------
    - Reuses the module-level `connectioncp` if it already exists and is open.
    - Opens a new connection only when no connection exists or the current one
      is closed.
    """
    global connectioncp
    
    if connectioncp is None or not getattr(connectioncp, "open", False):
        connectioncp = pymysql.connect(
            host=strdbhost,
            port=lngdbport,
            user=strdbuser,
            password=strdbpassword,
            database=strdbname,
            cursorclass=pymysql.cursors.DictCursor,
        )
    return connectioncp

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
    global paris_tz
    
    connectioncp = f_getconnection()
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
    """
    Retrieve the value of a server variable from the database.

    Parameters:
    -----------
    strvarname : str
        The name of the server variable to retrieve
    lnglang : int, optional
        Language ID filter. If > 0, only retrieves the variable for that specific language.
        Default is 0 (no language filter).

    Returns:
    --------
    str
        The value of the server variable, or empty string if not found.
    """
    global strsqlns
    
    connectioncp = f_getconnection()
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
    """
    Set or update a server variable in the database.

    Parameters:
    -----------
    strvarname : str
        The name of the server variable to set
    strvarvalue : str
        The value to assign to the server variable
    strvardesc : str, optional
        A long description of the variable's purpose. Default is empty string.
    lnglang : int, optional
        Language ID for the variable. Default is 0 (no specific language).

    Returns:
    --------
    None
    """
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
    """
    Convert seconds to a human-readable duration format.

    Parameters:
    -----------
    seconds : int
        The number of seconds to convert

    Returns:
    --------
    str
        A formatted string like "2 days, 3 hours, 15 minutes, 30 seconds".
        Returns "Invalid duration (negative seconds)" if input is negative.
    """
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
    """
    Escape a string for safe use in SQL queries.

    Parameters:
    -----------
    strtext : str
        The text string to escape

    Returns:
    --------
    str
        The escaped string wrapped in single quotes, with internal single quotes escaped.
        Example: "John's" becomes "'John\\'s'"
    """
    return "'" + strtext.replace("'","\\'") + "'"

def f_string(value):
    if value is None:
        return ""
    return str(value)

def f_fieldstringtoarray(strfields):
    if strfields is None:
        return []
    strfields = str(strfields).strip()
    if strfields == "":
        return []
    if "," in strfields:
        parts = strfields.split(",")
    else:
        parts = strfields.split("|")
    return [p.strip() for p in parts if p.strip() != ""]

def f_descfromcode(strtable, strfieldcode, strfielddesc, intcode, strwhere="", strassoctable=""):
    strresult = ""
    if (
        strtable
        and strfieldcode
        and strfielddesc
        and intcode is not None
        and str(intcode) != ""
    ):
        arrfields = f_fieldstringtoarray(strfielddesc)
        strsql = "SELECT *"
        strsql += f" FROM {strtable}"
        if strassoctable != "":
            strsql += f", {strassoctable}"
        strsql += " WHERE "
        if strassoctable != "":
            strsql += f"{strtable}.{strfieldcode}"
        else:
            strsql += f"{strfieldcode}"
        strsql += " = %s"
        if strwhere != "":
            strsql += f" AND {strwhere}"

        connectioncp = f_getconnection()
        cursor2 = connectioncp.cursor()
        cursor2.execute(strsql, (intcode,))
        rstemp = cursor2.fetchone()
        if rstemp and arrfields:
            strtemp = ""
            for field in arrfields:
                if field in rstemp:
                    strtemp += f_string(rstemp[field]) + " "
            strresult = strtemp.strip()
    return strresult

def f_fieldfromquery(strsql, strfield="", params=None, execute=True):
    if not strsql:
        return None
    if not execute:
        return None

    connectioncp = f_getconnection()
    cursor2 = connectioncp.cursor()
    if params is None:
        cursor2.execute(strsql)
    else:
        cursor2.execute(strsql, params)
    rstemp = cursor2.fetchone()
    if not rstemp:
        return None

    if strfield == "":
        for _, value in rstemp.items():
            return f_string(value)
        return ""

    return f_string(rstemp.get(strfield))

def f_fieldsfromquery(strsql, strvars, strfields, params=None, execute=True, target_dict=None):
    if not strsql or not strvars or not strfields:
        return {}
    if not execute:
        return {}

    connectioncp = f_getconnection()
    cursor2 = connectioncp.cursor()
    if params is None:
        cursor2.execute(strsql)
    else:
        cursor2.execute(strsql, params)
    rstemp = cursor2.fetchone()
    if not rstemp:
        return {}

    arrvars = [v.strip() for v in str(strvars).split("|")]
    arrfields = f_fieldstringtoarray(strfields)

    result = {}
    for var_name, field_name in zip(arrvars, arrfields):
        if var_name and field_name:
            value = f_string(rstemp.get(field_name))
            result[var_name] = value

    if target_dict is None:
        target_dict = globals()
    if target_dict is not None:
        for k, v in result.items():
            target_dict[k] = v

    return result

