"""
citizenphil -- shared MariaDB/MySQL access layer (PyMySQL).

This module is **copied verbatim into ~20 sibling repositories** (imdb-crawler,
tmdb-crawler, wikidata-crawler, movieparadise, ...). Any change here must stay
**backward compatible**: never alter the signature or behavior of an existing
function -- only add new ones. A regression here is a regression in every repo.

Core invariants
---------------
1. ONE shared connection. ``f_getconnection`` returns a single module-level
   PyMySQL connection (``connectioncp``). PyMySQL connections are **NOT
   thread-safe**: every DB call must run on the **same thread**. Code may
   parallelize network/parse work, but all DB writes stay on one thread.
2. MANY cursors, one session. Opening several cursors on that one connection is
   supported and relied upon (e.g. movieparadise; the wikipedia crawler's
   DELETE-after-write). Because the cursors share one session, a row written
   through one cursor is visible to another cursor **as soon as the statement is
   executed** -- this is read-your-writes, and several repos depend on it.
3. Visibility depends on WHEN the write reaches the DB:
   - ``f_sqlupdatearray``  -> commits per row        -> immediately visible.
   - ``f_sqlbulkupsert``   -> one statement per chunk -> visible only after the
     call returns. Do NOT use it for rows another cursor must read mid-batch.

Which function for which use case
---------------------------------
| Use case                                             | Function            | Visibility          |
|------------------------------------------------------|---------------------|---------------------|
| Single row that must be readable right away by       | f_sqlupdatearray    | Immediate (per-row  |
|   another cursor, or re-SELECTed later in the run    |                     | commit)             |
| Many rows, same table, NOT re-read within the call   | f_sqlbulkupsert     | Deferred until the  |
|   (bulk derived data: sections, images, ...)         |                     | call returns        |
| Resume checkpoints / progress counters               | f_setservervariable | Immediate; NEVER    |
|                                                      |                     | batch               |
| Read one field / one row                             | f_fieldfromquery /  | --                  |
|                                                      | f_descfromcode      |                     |
| Lazy connection handle                               | f_getconnection     | --                  |

Do NOT use f_sqlbulkupsert when:
  * a row is re-SELECTed (by any cursor) before the call returns,
  * you need the generated AUTO_INCREMENT id of an inserted row,
  * the rows are a crash-recovery checkpoint that must be durable in order.

f_sqlbulkupsert and unique keys
-------------------------------
f_sqlbulkupsert emits ``INSERT ... ON DUPLICATE KEY UPDATE``. The UPDATE branch
only fires when the target table has a UNIQUE/PRIMARY key on the conflict
columns. On a table WITHOUT such a key it degrades to a plain multi-row INSERT
(every row is inserted) -- correct only when the caller has already removed the
rows being replaced (the delete-then-bulk-insert / replace-set pattern).
"""
#from urllib.parse import quote
#import time
#import requests
#import json
import pymysql.cursors
#import re
from datetime import datetime
import time
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

def f_ismysqllocktimeout(err):
    errcode = None
    if hasattr(err, "args") and len(err.args) > 0:
        errcode = err.args[0]
    return errcode == 1205

def f_handlemysqlerror(err, context="", rollback=True):
    connectioncp = globals().get("connectioncp")
    if rollback and connectioncp is not None and getattr(connectioncp, "open", False):
        try:
            connectioncp.rollback()
        except Exception:
            pass
    if f_ismysqllocktimeout(err):
        if context:
            print(f"⚠️ MySQL lock wait timeout skipped in {context}: {err}")
        else:
            print(f"⚠️ MySQL lock wait timeout skipped: {err}")
        return True
    print(f"❌ MySQL Error: {err}")
    return False

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
            local_infile=True,
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
    intattemptsremaining = 3
    while intattemptsremaining > 0:
        cursor2 = connectioncp.cursor()
        try:
            if intaddstdfields == 1:
                if "TIM_UPDATED" not in arrpersoncouples:
                    arrpersoncouples["TIM_UPDATED"] = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            strsqlexists = f"SELECT * FROM {strsqltablename} WHERE {strsqlupdatecondition}"
            cursor2.execute(strsqlexists)
            lngrowcount = cursor2.rowcount
            if lngrowcount == 0:
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
                strsqlinsertcolumns = ', '.join(arrpersoncouples.keys())
                strsqlinsertplaceholders = ', '.join(['%s'] * len(arrpersoncouples))
                strsqlinsert = f"INSERT INTO {strsqltablename} ({strsqlinsertcolumns}) VALUES ({strsqlinsertplaceholders})"
                cursor2.execute(strsqlinsert, list(arrpersoncouples.values()))
                lngnewid = cursor2.lastrowid
                connectioncp.commit()
                return lngnewid
            arrsetclauses = []
            arrupdatevalues = []
            for key, value in arrpersoncouples.items():
                arrsetclauses.append(f"{key} = %s")
                if isinstance(value, bool):
                    arrupdatevalues.append(1 if value else 0)
                else:
                    arrupdatevalues.append(value)
            strsqlupdatesetclause = ", ".join(arrsetclauses)
            strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
            cursor2.execute(strsqlupdate, arrupdatevalues)
            connectioncp.commit()
            return None
        except pymysql.MySQLError as e:
            intattemptsremaining -= 1
            if f_ismysqllocktimeout(e) and intattemptsremaining > 0:
                f_handlemysqlerror(e, f"f_sqlupdatearray({strsqltablename})")
                time.sleep(1)
                continue
            f_handlemysqlerror(e, f"f_sqlupdatearray({strsqltablename})")
            return None

def f_sqlbulkupsert(strsqltablename, arrrows, arrkeycolumns, intaddstdfields=1, intchunksize=500):
    """
    Insert many rows in a single statement (bulk upsert).

    Collapses N per-row SELECT+INSERT/UPDATE round-trips (see ``f_sqlupdatearray``)
    into one multi-row ``INSERT ... ON DUPLICATE KEY UPDATE`` per chunk. Intended
    for high-volume derived rows that are NOT re-read inside the same call.

    Parameters
    ----------
    strsqltablename : str
        Target table.
    arrrows : list[dict]
        One dict per row (column name -> value). Rows may carry different keys;
        the ordered union of all keys is used and any missing value is sent as
        NULL so every row in a chunk shares one column list.
    arrkeycolumns : list[str]
        Columns that form the logical/unique key. They are written on INSERT but
        excluded from the ON DUPLICATE KEY UPDATE clause (a row never overwrites
        its own key). If the table has a matching UNIQUE/PRIMARY key the call is a
        true upsert; otherwise it is a plain multi-row INSERT -- correct only when
        the caller has already deleted the rows being replaced (see module docs).
    intaddstdfields : int
        1 -> add standard fields. Creation fields (DELETED, DAT_CREAT, ID_CREATOR,
        ID_OWNER, ID_USER_UPDATED) are written on INSERT only and preserved on
        update; TIM_UPDATED is always (re)written. 0 -> add nothing.
    intchunksize : int
        Maximum rows per INSERT statement (guards max_allowed_packet). Default 500.

    Returns
    -------
    int
        Number of input rows processed (0 for an empty list).

    Notes
    -----
    Same single-connection rules as the rest of the module. Rows become visible
    to other cursors only AFTER this call returns -- never use it for resume
    checkpoints, lastrowid needs, or read-your-writes inside the batch.
    """
    global paris_tz

    if not arrrows:
        return 0

    arrkeycolumns = list(arrkeycolumns or [])
    insertonlystd = {"DELETED", "DAT_CREAT", "ID_CREATOR", "ID_OWNER", "ID_USER_UPDATED"}

    # Normalize rows (add standard fields) and build the ordered column union.
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    strtoday = datetime.now(paris_tz).strftime("%Y-%m-%d")
    arrnormalized = []
    arrcolumns = []
    setcolumns = set()
    for row in arrrows:
        rowcopy = dict(row)
        if intaddstdfields == 1:
            rowcopy.setdefault("TIM_UPDATED", strnow)
            rowcopy.setdefault("DELETED", 0)
            rowcopy.setdefault("DAT_CREAT", strtoday)
            rowcopy.setdefault("ID_CREATOR", lnguseridsession)
            rowcopy.setdefault("ID_OWNER", lnguseridsession)
            rowcopy.setdefault("ID_USER_UPDATED", lnguseridsession)
        for col in rowcopy.keys():
            if col not in setcolumns:
                setcolumns.add(col)
                arrcolumns.append(col)
        arrnormalized.append(rowcopy)

    # On duplicate, refresh the data columns + TIM_UPDATED, but never the key
    # columns or the insert-only creation metadata (mirrors f_sqlupdatearray).
    arrupdatecolumns = [
        col for col in arrcolumns
        if col not in arrkeycolumns and col not in insertonlystd
    ]

    strcolumnlist = ", ".join(arrcolumns)
    strrowplaceholder = "(" + ", ".join(["%s"] * len(arrcolumns)) + ")"
    if arrupdatecolumns:
        strupdateclause = " ON DUPLICATE KEY UPDATE " + ", ".join(
            f"{col} = VALUES({col})" for col in arrupdatecolumns
        )
    else:
        strupdateclause = ""

    def _rowvalues(rowcopy):
        values = []
        for col in arrcolumns:
            value = rowcopy.get(col)
            if isinstance(value, bool):
                values.append(1 if value else 0)
            else:
                values.append(value)
        return values

    connectioncp = f_getconnection()
    lngtotal = 0
    for lngstart in range(0, len(arrnormalized), intchunksize):
        arrchunk = arrnormalized[lngstart:lngstart + intchunksize]
        strvalues = ", ".join([strrowplaceholder] * len(arrchunk))
        strsql = f"INSERT INTO {strsqltablename} ({strcolumnlist}) VALUES {strvalues}{strupdateclause}"
        arrparams = []
        for rowcopy in arrchunk:
            arrparams.extend(_rowvalues(rowcopy))

        intattemptsremaining = 3
        while intattemptsremaining > 0:
            cursor2 = connectioncp.cursor()
            try:
                cursor2.execute(strsql, arrparams)
                connectioncp.commit()
                lngtotal += len(arrchunk)
                break
            except pymysql.MySQLError as e:
                intattemptsremaining -= 1
                if f_ismysqllocktimeout(e) and intattemptsremaining > 0:
                    f_handlemysqlerror(e, f"f_sqlbulkupsert({strsqltablename})")
                    time.sleep(1)
                    continue
                f_handlemysqlerror(e, f"f_sqlbulkupsert({strsqltablename})")
                break
    return lngtotal

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

    # VAR_VALUE is varchar(255); guard every write so an over-long value
    # (e.g. a long SQL query persisted for monitoring) can never trigger
    # MySQL error 1406 "Data too long for column 'VAR_VALUE'".
    if isinstance(strvarvalue, str) and len(strvarvalue) > 255:
        strvarvalue = strvarvalue[:252] + "..."

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

