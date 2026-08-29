import time
import requests
import pymysql.cursors
#from pymysql import Error
import json
import citizenphil as cp
from datetime import datetime, timedelta
import gzip
import shutil
import numpy as np
import pandas as pd
import psutil
import re
import sys
import os
from typing import Dict, Optional, List, Set
from language_family import guess_language_family
from person_names import build_person_names, split_also_known_as, f_aliaskey

def check_memory():
    """Check and display system memory information"""
    memory_info = psutil.virtual_memory()
    print(f"Total Memory: {memory_info.total / (1024 ** 3):.2f} GB")
    print(f"Available Memory: {memory_info.available / (1024 ** 3):.2f} GB")
    print(f"Used Memory: {memory_info.used / (1024 ** 3):.2f} GB")
    print(f"Free Memory: {memory_info.free / (1024 ** 3):.2f} GB")
    print(f"Memory Usage: {memory_info.percent}%")
    return memory_info.available / (1024 ** 3)

# Global lookup dictionary that will be populated once and used for all lookups
country_lookup_dict: Dict[str, str] = {}
is_initialized: bool = False
#intaskllm: bool = True
intaskllm: bool = False
# Per-row tracing. Left off in production: the loops below run over millions of
# persons and, under Docker's json-file log driver, one print per row is a
# blocking write that dominates the runtime and fills the disk with logs.
intverbose: bool = False

# Read page sizes. Both loops paginate on the primary key (keyset pagination)
# instead of buffering the whole table client-side.
lngcobreadchunksize: int = 200000   # COUNTRY_OF_BIRTH: 4 narrow columns per row
lngakapersonchunksize: int = 1000   # ALSO_KNOWN_AS: ALSO_KNOWN_AS is a mediumtext

# --- Incremental reading ----------------------------------------------------
#
# Both passes only ever WRITE real changes, but until now they still READ every
# person on every run: 5M rows, the ALSO_KNOWN_AS mediumtext included. That read was
# essentially the whole runtime.
#
# T_WC_TMDB_PERSON.TIM_UPDATED became a trustworthy change signal the day this
# preprocess stopped stamping it, so a run can now skip persons whose source data has
# not moved. The watermark W means "everything with TIM_UPDATED < W has been
# processed". A run reads the window [W, its own start time) and, only if it finishes,
# sets W to that start time. Rows updated while the run is in flight sit outside the
# window on purpose and are picked up by the next one.
#
# The watermark is stored per pass, and written only after that pass completes, so a
# crash or a database error costs a repeat rather than a silent hole.

# Bump this whenever the derivation logic changes: clean_place_of_birth,
# f_countrylookup, guess_language_family, f_aliaskey, build_person_names. A mismatch
# with the stored value forces one full pass, so new logic reaches the rows a watermark
# would otherwise skip forever. Forgetting to bump it is caught by the periodic full
# pass below, just later.
lngderivationversion: int = 1

# A full pass is forced when the last one is this old. It is the safety net for
# everything TIM_UPDATED cannot see: a T_WC_COUNTRY row edited by hand, an alias
# changed outside this pipeline, a chunk of writes an error handler swallowed. Set it
# to 0 to make every run a full pass.
lngfullpassmaxagedays: int = 7

# What a watermark reads as when nothing has been stored yet.
STRWATERMARKFLOOR: str = "1970-01-01 00:00:00"


def f_readwatermark(strvarname, intfullpass):
    """Return the watermark a pass should start from.

    A full pass ignores whatever is stored and starts from the floor.
    """
    if intfullpass:
        return STRWATERMARKFLOOR
    strvalue = cp.f_getservervariable(strvarname, 0)
    return strvalue if strvalue else STRWATERMARKFLOOR


def f_decidefullpass():
    """Say whether this run must read every person instead of following the watermarks.

    Returns:
        (intfullpass, strreason) with strreason worth printing either way.
    """
    if os.environ.get("PREPROCESS_FULL_PASS", "0") == "1":
        return 1, "PREPROCESS_FULL_PASS is set in the environment"

    strstoredversion = cp.f_getservervariable("strtmdbpersonpreprocessderivationversion", 0)
    if strstoredversion != str(lngderivationversion):
        return 1, "derivation version " + (strstoredversion or "unset") + " -> " + str(lngderivationversion)

    strlastfull = cp.f_getservervariable("strtmdbpersonpreprocesslastfullpass", 0)
    if not strlastfull:
        return 1, "no full pass recorded yet"
    try:
        datlastfull = datetime.strptime(strlastfull, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return 1, "unreadable last full pass timestamp '" + strlastfull + "'"

    lngdays = (datetime.now(cp.paris_tz).replace(tzinfo=None) - datlastfull).days
    if lngdays >= lngfullpassmaxagedays:
        return 1, "last full pass was " + str(lngdays) + " days ago"
    return 0, "last full pass was " + str(lngdays) + " day(s) ago"


def f_advancekeyset(row, intfullpass):
    """Return the (last TIM_UPDATED, last ID_PERSON) cursor after a page.

    A full pass paginates on the primary key alone and has no timestamp to carry.
    """
    if intfullpass:
        return None, row['ID_PERSON']
    timupdated = row['TIM_UPDATED']
    if hasattr(timupdated, "strftime"):
        timupdated = timupdated.strftime("%Y-%m-%d %H:%M:%S")
    return str(timupdated), row['ID_PERSON']


def normalize_string(s: str) -> str:
    """
    Normalize a string for consistent lookup by converting to lowercase and stripping whitespace.
    
    Args:
        s: The string to normalize
        
    Returns:
        The normalized string
    """
    return s.lower().strip()

def initialize_country_lookup():
    """
    Initialize the country lookup dictionary from the database.
    This should be called once before using f_countrylookup.
    """
    global country_lookup_dict, is_initialized
    
    if is_initialized:
        return
    
    try:
        # Execute the query to get all country data
        query = """
            SELECT COUNTRY_CODE, COUNTRY_NAME_FR, COUNTRY_NAME_EN, COUNTRY_ALIASES 
            FROM T_WC_COUNTRY 
            WHERE DELETED = 0 
            ORDER BY COUNTRY_CODE ASC
        """
        cursor2 = cp.connectioncp.cursor()
        cursor2.execute(query)
        results2 = cursor2.fetchall()
        for row2 in results2:
            country_code = row2['COUNTRY_CODE']
            name_fr = row2['COUNTRY_NAME_FR']
            name_en = row2['COUNTRY_NAME_EN']
            aliases = row2['COUNTRY_ALIASES']
            
            # Add the French name
            if name_fr:
                if name_fr != "":
                    normalized_fr = normalize_string(name_fr)
                    if normalized_fr not in country_lookup_dict:
                        country_lookup_dict[normalized_fr] = country_code
                        if intverbose:
                            print(normalized_fr,'->',country_code)
            
            # Add the English name
            if name_en:
                if name_en != "":
                    normalized_en = normalize_string(name_en)
                    if normalized_en not in country_lookup_dict:
                        country_lookup_dict[normalized_en] = country_code
                        if intverbose:
                            print(normalized_en,'->',country_code)
            
            # Process aliases if they exist
            if aliases and len(aliases) > 1:  # Check if it's not empty or just a single pipe
                # Split by pipe and process each alias
                for alias in aliases.split('|'):
                    if alias.strip() != "":  # Skip empty strings
                        normalized_alias = normalize_string(alias)
                        if normalized_alias not in country_lookup_dict:
                            country_lookup_dict[normalized_alias] = country_code
                            if intverbose:
                                print(normalized_alias,'->',country_code)
        
        cursor2.close()
        print(f"Country lookup initialized with {len(country_lookup_dict)} entries")
        is_initialized = True
        
    except Exception as e:
        print(f"Error initializing country lookup: {e}")
        raise

def add_country_alias(country_code: str, alias: str) -> None:
    """
    Add a new country alias to the lookup dictionary and store it in the database.
    This can be used to enrich the dictionary when a string is not found.
    
    Args:
        country_code: The ISO 3166-1 alpha-2 country code
        alias: The new alias to associate with the country code
    """
    if not country_code or not alias:
        return
    
    normalized_alias = normalize_string(alias)
    
    # Add to the in-memory dictionary
    country_lookup_dict[normalized_alias] = country_code
    
    try:
        # Get the current aliases for this country code from the database
        cursor = cp.connectioncp.cursor()
        query = """
            SELECT COUNTRY_ALIASES, ID_COUNTRY 
            FROM T_WC_COUNTRY 
            WHERE COUNTRY_CODE = %s AND DELETED = 0
        """
        cursor.execute(query, (country_code,))
        result = cursor.fetchone()
        
        if result:
            country_id = result['ID_COUNTRY']
            current_aliases = result['COUNTRY_ALIASES'] or ''
            
            # Check if the alias already exists in the database
            aliases_list = current_aliases.split('|') if current_aliases else []
            normalized_aliases = [normalize_string(a) for a in aliases_list if a.strip()]
            
            # Only add if it's not already in the list
            if normalized_alias not in normalized_aliases:
                # Add the new alias to the list
                if current_aliases:
                    updated_aliases = current_aliases + '|' + alias.strip()
                else:
                    updated_aliases = alias.strip()
                
                # Update the database
                strsqltablename = "T_WC_COUNTRY"
                strsqlupdatecondition = f"ID_COUNTRY = {country_id}"
                
                # Prepare the data for update
                update_data = {
                    "COUNTRY_ALIASES": updated_aliases
                }
                
                # Use the f_sqlupdatearray function to update the database
                cp.f_sqlupdatearray(strsqltablename, update_data, strsqlupdatecondition, 1)
                print(f"Added new alias '{alias}' for country code '{country_code}' to database")
        else:
            print(f"Warning: Country code '{country_code}' not found in database")
            
        cursor.close()
        
    except Exception as e:
        print(f"Error updating country alias in database: {e}")
        # Still keep the alias in memory even if database update fails
        # This ensures the current run can still use the alias

def f_countrylookup(input_str: str) -> str:
    """
    Look up a country code based on a country name or alias.
    If not found in the lookup dictionary, ask an OpenAI LLM.
    
    Args:
        input_str: The country name or alias to look up
        
    Returns:
        The ISO 3166-1 alpha-2 country code if found, or an empty string if not found
    """
    if not is_initialized:
        raise RuntimeError("Country lookup not initialized. Call initialize_country_lookup first.")
    
    normalized_input = normalize_string(input_str)
    strcountrycode = country_lookup_dict.get(normalized_input, "")
    
    # If not found in the dictionary, ask the LLM
    if not strcountrycode and input_str.strip() and intaskllm:
        strcountrycode = ask_llm_for_country_code(input_str)
        
        # If the LLM found a valid country code, add it to our lookup dictionary for future use
        if strcountrycode:
            add_country_alias(strcountrycode, input_str)
    
    if intverbose:
        print(input_str,'->',strcountrycode)
    return strcountrycode


# Global variables for rate limiting
from collections import deque

# Rate limiting configuration
_api_call_timestamps = deque(maxlen=50)  # Store timestamps of recent API calls
_max_calls_per_minute = 40  # Adjust based on your OpenAI tier
_backoff_time = 5  # Initial backoff time in seconds
_max_backoff_time = 60  # Maximum backoff time in seconds
_llm_cache = {}  # Simple cache to avoid repeated identical queries

def ask_llm_for_country_code(input_str: str) -> str:
    """
    Ask an OpenAI LLM to identify a country code from the input string.
    Includes rate limiting protection to avoid 429 errors.
    
    Args:
        input_str: The string containing geographical information
        
    Returns:
        The ISO 3166-1 alpha-2 country code if identified, or an empty string if not
    """
    import os
    from dotenv import load_dotenv
    import openai
    import re
    
    # Check cache first to avoid unnecessary API calls
    normalized_input = normalize_string(input_str)
    if normalized_input in _llm_cache:
        return _llm_cache[normalized_input]
    
    # Load environment variables (OPENAI_API_KEY)
    load_dotenv()
    
    # Check if API key is available
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Warning: OPENAI_API_KEY not found in environment variables")
        return ""
    
    # Apply rate limiting
    current_time = time.time()
    
    # Clean up old timestamps (older than 60 seconds)
    while _api_call_timestamps and current_time - _api_call_timestamps[0] > 60:
        _api_call_timestamps.popleft()
    
    # Check if we're exceeding rate limits
    if len(_api_call_timestamps) >= _max_calls_per_minute:
        # Calculate time to wait before next request
        wait_time = 60 - (current_time - _api_call_timestamps[0])
        if wait_time > 0:
            print(f"Rate limit approaching. Waiting {wait_time:.2f} seconds before next API call...")
            time.sleep(wait_time)
    
    # Attempt API call with exponential backoff for rate limit errors
    backoff = _backoff_time
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Set up the OpenAI client
            client = openai.OpenAI(api_key=api_key)
            
            # Create the prompt for the LLM
            prompt = f"""You are an expert at political geography.
I will provide geographical information that may contain a city, a province, a state or a country.
If this geographical information matches an ISO 3166-1 alpha-2 country code, you will return only the matching 2-digits country code and nothing else.
If this geographical information matches a country that do not exist anymore and is now split in several countries, like USSR or Czechoslovakia, return nothing.
If this geographical information matches a country that do not exist anymore and is now part of a country, return that country alpha-2 code.
If this geographical information matches something bigger like a group of countries, like Europa ou EU or a continent, return nothing.
If you are not sure, return nothing.
Input: {input_str}"""
            
            # Make a direct call to OpenAI API
            response = client.chat.completions.create(
                model="gpt-4o",  # Using GPT-4o for better accuracy
                temperature=0,  # Use deterministic output
                messages=[
                    {"role": "system", "content": "You are a country code identifier. Respond only with the ISO 3166-1 alpha-2 country code, no explanations."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Record this successful API call for rate limiting
            _api_call_timestamps.append(time.time())
            
            # Extract the country code from the response
            country_code = response.choices[0].message.content.strip()
            
            # Validate that the response is a valid 2-letter country code (ISO 3166-1 alpha-2)
            if re.match(r'^[A-Z]{2}$', country_code):
                # Cache the result for future use
                _llm_cache[normalized_input] = country_code
                return country_code
            else:
                # Cache empty result
                _llm_cache[normalized_input] = ""
                return ""
                
        except openai.RateLimitError as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                print(f"Rate limit exceeded. Backing off for {backoff} seconds...")
                time.sleep(backoff)
                backoff = min(backoff * 2, _max_backoff_time)  # Exponential backoff
            else:
                print(f"Rate limit error after {max_retries} attempts: {str(e)}")
                return ""
                
        except Exception as e:
            print(f"Error in LLM country code lookup: {str(e)}")
            return ""
    
    # If we get here, all retries failed
    return ""

def safe_transform(text, transform_func):
    """Safely apply a transformation function with error handling"""
    try:
        return transform_func(text)
    except Exception as e:
        print(f"Error processing text: {text}")
        print(f"Error: {str(e)}")
        return text

def clean_basic_format(text):
    """Clean basic formatting while preserving parentheses content"""
    replacements = {
        '—': ' ',
        '|': ' ',
        '/': ' ',
        '–': ' ',
        '•': ' ',
        '"': ' ',
        ' : ': ' ',
        ' , ': ' '
    }
    
    # Apply all replacements
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Remove extra spaces
    return re.sub(r'\s+', ' ', text).strip()

def extract_country_of_birth(text):
    """Extract country of birth from place of birth."""
    components = {
        'COUNTRY_OF_BIRTH': None
    }
    
    if not isinstance(text, str):
        return components
    
    text = text.lower()
    
    # Check for country of birth
    components['COUNTRY_OF_BIRTH'] = f_countrylookup(text)
    
    return components

def clean_place_of_birth(text):
    """Clean place of birth according to specific rules"""
    text = text.replace('[now ',', ')
    text = text.replace('(now ',', ')
    
    text = text.replace('"','')
    text = text.replace('[','')
    text = text.replace(']','')
    text = text.replace('(','')
    text = text.replace(')','')
    text = text.replace(' - ',', ')
    text = text.replace('，',', ')
    
    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    textorg = text
    
    # Keep only the text after the last comma
    if ',' in text:
        text = textorg.split(',')[-1].strip()

        # Handling special cases where the last country is not actually a country
        if text == "eu" or text == "ussr" or text == "yugoslavia" or text == "československo" or text == "czechoslovakia" or text == "british india" or text == "europe" or text == "korea" or text == "soviet union" or text == "russian empire" or text =="austria-hungary" or text == "cccp" or text == "africa" or text == "asia" or text == "austro-hungarian empire" or text == "caribbean" or text == "central america" or text == "holy roman empire" or text == "latin america" or text == "middle east" or text == "jugoslávie" or text == "rakousko-uhersko" or text == "rossiyskaya imperiya" or text == "u.s.s.r." or text =="urss" or text == "ussr." or text == "west indies" or text == "cccp":
            text = textorg.split(',')[-2].strip()
    
    # Remove trailing period if present
    if text.endswith('.'):
        text = text[:-1]
    
    return text

# Helper function to convert NaN to None and handle lists
def process_value(val, is_integer=False):
    # Handle None and NaN values
    if pd.isna(val) or val is None:
        return 0 if is_integer else ""
    
    # Convert to string if not already
    val_str = str(val)
    
    # For integer columns, handle immediately
    if is_integer:
        # Handle empty strings and empty lists
        if not val_str or val_str == '':
            return 0
        try:
            # Try to convert to integer
            return int(float(val_str)) if '.' in val_str else int(val_str)
        except (ValueError, TypeError):
            return 0
    
    # Handle empty strings and empty lists for non-integer columns
    if not val_str or val_str == '[]':
        return ""
    
    # Handle string representation of lists
    if val_str.startswith('[') and val_str.endswith(']'):
        # Remove brackets and split by comma
        val_str = val_str[1:-1]
        if not val_str:
            return ""
        # Clean up each element
        elements = [x.strip() for x in val_str.split(',') if x.strip()]
        if not elements:
            return ""
        return ', '.join(elements)
    
    return val_str

def f_samestoredvalue(dbvalue, newvalue):
    """Tell whether a stored column already holds the freshly computed value.

    The column is NULL for a person never processed and '' for one processed to an
    empty result; both mean "nothing stored", so neither must be reported as a
    change or the loop would rewrite the whole table on every run.
    """
    if dbvalue is None or (isinstance(dbvalue, float) and pd.isna(dbvalue)):
        dbvalue = ""
    return str(dbvalue) == str(newvalue)

def batch_update_data_country_of_birth(connection, df, batch_size=500):
    """Write back only the persons whose derived country of birth actually changed.

    The previous version sent, per person, a `SELECT *` (biography mediumtext
    included) plus an UPDATE plus a COMMIT through f_sqlupdatearray, and because it
    was called with batch_size=1 it also upserted two server variables per row:
    roughly thirteen round-trips and four commits for each of the 4.3M rows, whether
    or not the value had moved. PLACE_OF_BIRTH is near-immutable, so the computed
    values are compared against what the table already holds and only the
    differences are shipped, in multi-row statements.

    TIM_UPDATED is deliberately NOT touched (intaddstdfields=0). COUNTRY_OF_BIRTH is
    derived, not source data, and tmdb-crawler fills its person refresh queue from
    `WHERE T_WC_TMDB_PERSON.TIM_UPDATED < <J-30>`; stamping the column here marks
    every person as freshly crawled and starves that queue.

    Args:
        connection: open MariaDB connection (kept for signature compatibility).
        df: DataFrame carrying ID_PERSON, the computed COUNTRY_OF_BIRTH /
            COUNTRY_OF_BIRTH_LONG, and their stored counterparts suffixed _DB.
        batch_size: rows per INSERT ... ON DUPLICATE KEY UPDATE statement.

    Returns:
        A (rows_examined, rows_updated) tuple.
    """
    arrchanged = []
    for row in df.itertuples(index=False):
        strlong = process_value(row.COUNTRY_OF_BIRTH_LONG)[:200]
        strcode = process_value(row.COUNTRY_OF_BIRTH)[:10]
        if (f_samestoredvalue(row.COUNTRY_OF_BIRTH_LONG_DB, strlong)
                and f_samestoredvalue(row.COUNTRY_OF_BIRTH_DB, strcode)):
            continue
        arrchanged.append({
            "ID_PERSON": int(row.ID_PERSON),
            "COUNTRY_OF_BIRTH_LONG": strlong,
            "COUNTRY_OF_BIRTH": strcode,
        })

    if not arrchanged:
        return len(df), 0

    # ID_PERSON is the primary key, so ON DUPLICATE KEY UPDATE is a true in-place
    # update; the INSERT branch is never taken for persons read from this table.
    lngupdated = cp.f_sqlbulkupsert(
        "T_WC_TMDB_PERSON", arrchanged, ["ID_PERSON"], 0, batch_size
    )
    return len(df), lngupdated

strdattoday = datetime.now(cp.paris_tz).strftime("%Y-%m-%d")

try:
    conn = cp.f_getconnection()
    with conn:
        with conn.cursor() as cursor:
            cursor2 = conn.cursor()
            cursor3 = conn.cursor()
            start_time = time.time()
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strtmdbpersonpreprocessstartdatetime",strnow,"Date and time of the last start of the TMDb database preprocess",0)
            strprocessesexecutedprevious = cp.f_getservervariable("strtmdbpersonpreprocessprocessesexecuted",0)
            strprocessesexecuteddesc = "List of processes executed in the TMDb person preprocess"
            cp.f_setservervariable("strtmdbpersonpreprocessprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
            strprocessesexecuted = ""
            cp.f_setservervariable("strtmdbpersonpreprocessprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
            strtotalruntimedesc = "Total runtime of the TMDb person preprocess"
            strtotalruntimeprevious = cp.f_getservervariable("strtmdbpersonpreprocesstotalruntime",0)
            cp.f_setservervariable("strtmdbpersonpreprocesstotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = "RUNNING"
            cp.f_setservervariable("strtmdbpersonpreprocesstotalruntime",strtotalruntime,strtotalruntimedesc,0)
            
            # One decision for the whole run, so the two passes cannot disagree about
            # which window they are reading, and so a single line in the log explains
            # why a run read 5M persons or 2000.
            strrunstart = strnow
            intfullpass, strfullpassreason = f_decidefullpass()
            strpassmode = ("full pass (" + strfullpassreason + ")") if intfullpass else ("incremental (" + strfullpassreason + ")")
            print(strpassmode)
            cp.f_setservervariable("strtmdbpersonpreprocesspassmode",strpassmode,"Whether this run read every person or only those updated since the watermark",0)
            setprocessesrun = set()
            setprocessesok = set()

            arrprocessscope = {1: 'COUNTRY_OF_BIRTH', 2: 'ALSO_KNOWN_AS'}
            arrprocessscope = {2: 'ALSO_KNOWN_AS', 1: 'COUNTRY_OF_BIRTH'}
            #arrprocessscope = {2: 'ALSO_KNOWN_AS'}
            for intindex, strdesc in arrprocessscope.items():
                setprocessesrun.add(intindex)
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strtmdbpersonpreprocessprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                cp.f_setservervariable("strtmdbpersonpreprocesscurrentprocess",strdesc,"Current process in the TMDb database preprocess",0)
                cp.f_setservervariable("strtmdbpersonpreprocesscurrentsubprocess","","Current sub process in the TMDb database preprocess",0)
                if intindex == 1:
                    #----------------------------------------------------
                    print("COUNTRY_OF_BIRTH processing")
                    # Check memory
                    dblavailableram = check_memory()

                    # Initialize the lookup dictionary
                    initialize_country_lookup()

                    try:
                        # clean_place_of_birth() and f_countrylookup() are pure functions
                        # of PLACE_OF_BIRTH, and millions of persons share a few tens of
                        # thousands of distinct birthplaces. Memoizing across pages turns
                        # 4.3M parses + lookups into one per distinct value per run.
                        arrcleancache = {}
                        arrcountrycache = {}

                        strlasttim = f_readwatermark("strtmdbpersonpreprocesscobwatermark", intfullpass)
                        lnglastid = 0
                        lngexamined = 0
                        lngupdated = 0
                        lngfailed = 0
                        while True:
                            # Two query shapes, one per mode, so each is index-optimal.
                            #
                            # A full pass paginates on the primary key: it visits every
                            # person, TIM_UPDATED NULL included, which no timestamp window
                            # would reach. An incremental pass paginates on
                            # (TIM_UPDATED, ID_PERSON), which the TIM_UPDATED index already
                            # provides since InnoDB appends the primary key to every
                            # secondary index.
                            #
                            # The <> 'None' guard is not paranoia: tmdb-crawler used to store
                            # str(None), the 4-character text "None", when TMDb returned a
                            # null place of birth. That is neither NULL nor empty, so it
                            # slipped through the two filters above and made this pass read
                            # and parse ~4.5M junk rows out of 5M. The crawler now writes NULL
                            # and migrations/clear_none_place_of_birth.py cleaned the backlog;
                            # the guard stays as cheap insurance against a regression upstream.
                            if intfullpass:
                                cursor2.execute(
                                    "SELECT ID_PERSON, PLACE_OF_BIRTH, "
                                    "COUNTRY_OF_BIRTH_LONG AS COUNTRY_OF_BIRTH_LONG_DB, "
                                    "COUNTRY_OF_BIRTH AS COUNTRY_OF_BIRTH_DB "
                                    "FROM T_WC_TMDB_PERSON "
                                    "WHERE ID_PERSON > %s "
                                    "AND PLACE_OF_BIRTH IS NOT NULL AND PLACE_OF_BIRTH <> '' "
                                    "AND PLACE_OF_BIRTH <> 'None' "
                                    "ORDER BY ID_PERSON ASC LIMIT %s",
                                    (lnglastid, lngcobreadchunksize)
                                )
                            else:
                                cursor2.execute(
                                    "SELECT ID_PERSON, PLACE_OF_BIRTH, TIM_UPDATED, "
                                    "COUNTRY_OF_BIRTH_LONG AS COUNTRY_OF_BIRTH_LONG_DB, "
                                    "COUNTRY_OF_BIRTH AS COUNTRY_OF_BIRTH_DB "
                                    "FROM T_WC_TMDB_PERSON "
                                    "WHERE (TIM_UPDATED > %s OR (TIM_UPDATED = %s AND ID_PERSON > %s)) "
                                    "AND TIM_UPDATED < %s "
                                    "AND PLACE_OF_BIRTH IS NOT NULL AND PLACE_OF_BIRTH <> '' "
                                    "AND PLACE_OF_BIRTH <> 'None' "
                                    "ORDER BY TIM_UPDATED ASC, ID_PERSON ASC LIMIT %s",
                                    (strlasttim, strlasttim, lnglastid, strrunstart, lngcobreadchunksize)
                                )
                            result = cursor2.fetchall()
                            if not result:
                                break
                            strlasttim, lnglastid = f_advancekeyset(result[-1], intfullpass)

                            data = pd.DataFrame(result)
                            data['PLACE_OF_BIRTH'] = data['PLACE_OF_BIRTH'].astype(str).str.lower()

                            for strplace in data['PLACE_OF_BIRTH'].unique():
                                if strplace in arrcleancache:
                                    continue
                                strclean = clean_place_of_birth(strplace)
                                arrcleancache[strplace] = strclean
                                if strclean not in arrcountrycache:
                                    arrcountrycache[strclean] = f_countrylookup(strclean)

                            data['PLACE_OF_BIRTH'] = data['PLACE_OF_BIRTH'].map(arrcleancache)
                            data['COUNTRY_OF_BIRTH_LONG'] = data['PLACE_OF_BIRTH']
                            data['COUNTRY_OF_BIRTH'] = data['PLACE_OF_BIRTH'].map(arrcountrycache)

                            lngpageexamined, lngpageupdated = batch_update_data_country_of_birth(cp.connectioncp, data)
                            lngexamined += lngpageexamined
                            lngupdated += lngpageupdated

                            cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthparsedcount",str(lngexamined),"Count of PLACE_OF_BIRTH row parsed",0)
                            cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthupdatedcount",str(lngupdated),"Count of PLACE_OF_BIRTH row actually updated",0)
                            print(f"COUNTRY_OF_BIRTH: {lngexamined} examined, {lngupdated} updated, {len(arrcleancache)} distinct places (up to ID_PERSON {lnglastid})", flush=True)

                        # Republished outside the page loop, not only inside it. An
                        # incremental run with nothing to read never enters that loop, and
                        # the counters would otherwise still show the previous run's totals.
                        cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthparsedcount",str(lngexamined),"Count of PLACE_OF_BIRTH row parsed",0)
                        cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthupdatedcount",str(lngupdated),"Count of PLACE_OF_BIRTH row actually updated",0)
                        cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthfailedcount",str(lngfailed),"Count of PLACE_OF_BIRTH row failed",0)
                        # Written here and nowhere else: reaching this line means the whole
                        # window was read. An exception below leaves the watermark where it
                        # was, so the next run repeats the work instead of stepping over it.
                        cp.f_setservervariable("strtmdbpersonpreprocesscobwatermark",strrunstart,"Persons with TIM_UPDATED below this have been parsed for COUNTRY_OF_BIRTH",0)
                        setprocessesok.add(1)
                        print("")
                        print("COUNTRY_OF_BIRTH done: " + str(lngexamined) + " rows examined, " + str(lngupdated) + " rows updated, " + str(lngfailed) + " failed")

                        # Calculate and display execution time
                        end_time = time.time()
                        execution_time = end_time - start_time
                        print(f"Execution time: {execution_time:.2f} seconds")
                    except pymysql.MySQLError as e:
                        print(f"Database error: {e}")
                    except Exception as e:
                        print(f"Error processing data: {e}")
                    finally:
                        print("PLACE_OF_BIRTH parsing done")
                if intindex == 2:
                    #----------------------------------------------------
                    print("ALSO_KNOWN_AS processing")
                    try:
                        lng_persons_processed = 0
                        lng_aliases_upserted = 0
                        lng_aliases_inserted = 0
                        lng_aliases_updated = 0
                        lng_aliases_deleted = 0
                        lng_duplicates_removed = 0
                        lng_pages = 0

                        strlasttim = f_readwatermark("strtmdbpersonpreprocessakawatermark", intfullpass)
                        lnglastid = 0
                        while True:
                            # Two query shapes, one per mode. A full pass paginates on the
                            # primary key and so visits every person, TIM_UPDATED NULL
                            # included; an incremental pass paginates on
                            # (TIM_UPDATED, ID_PERSON), which the TIM_UPDATED index already
                            # provides since InnoDB appends the primary key to it.
                            #
                            # Either way the read is paginated. An earlier version ran one
                            # unbounded SELECT and walked it with fetchmany(), so pymysql
                            # buffered 5M rows, the ALSO_KNOWN_AS mediumtext included, before
                            # the first person was processed.
                            if intfullpass:
                                cursor2.execute(
                                    "SELECT ID_PERSON, NAME, ALSO_KNOWN_AS FROM T_WC_TMDB_PERSON "
                                    "WHERE ID_PERSON > %s ORDER BY ID_PERSON ASC LIMIT %s",
                                    (lnglastid, lngakapersonchunksize)
                                )
                            else:
                                cursor2.execute(
                                    "SELECT ID_PERSON, NAME, ALSO_KNOWN_AS, TIM_UPDATED "
                                    "FROM T_WC_TMDB_PERSON "
                                    "WHERE (TIM_UPDATED > %s OR (TIM_UPDATED = %s AND ID_PERSON > %s)) "
                                    "AND TIM_UPDATED < %s "
                                    "ORDER BY TIM_UPDATED ASC, ID_PERSON ASC LIMIT %s",
                                    (strlasttim, strlasttim, lnglastid, strrunstart, lngakapersonchunksize)
                                )
                            rows = cursor2.fetchall()
                            if not rows:
                                break
                            strlasttim, lnglastid = f_advancekeyset(rows[-1], intfullpass)
                            lng_pages += 1

                            arrdesired = {}
                            for row in rows:
                                arrdesired[row['ID_PERSON']] = build_person_names(row.get('NAME'), row.get('ALSO_KNOWN_AS'))
                            lng_persons_processed += len(rows)

                            # One round-trip for the whole page, instead of one SELECT per
                            # person as before.
                            arrids = list(arrdesired.keys())
                            strplaceholders = ", ".join(["%s"] * len(arrids))
                            cursor3.execute(
                                "SELECT ID_ROW, ID_PERSON, PERSON_NAME, LANGUAGE_FAMILY, DISPLAY_ORDER "
                                "FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS "
                                "WHERE ID_PERSON IN (" + strplaceholders + ") ORDER BY ID_ROW ASC",
                                arrids
                            )
                            # Existing rows are indexed twice: by their exact PERSON_NAME,
                            # and by f_aliaskey. The exact index is authoritative and the
                            # folded one is the fallback, because PERSON_NAME is
                            # utf8mb4_unicode_ci under a UNIQUE key: the server keeps ONE row
                            # for "Jean Reno" and "JEAN RENO". Matching on the exact string
                            # alone made the pass insert the variant it believed missing, the
                            # upsert landed on the existing row and imposed its DISPLAY_ORDER,
                            # and the next run put the other one back. That was ~26k writes
                            # per run, forever, with no deletion to show for it.
                            #
                            # The order matters in the other direction too. The collation folds
                            # more than case (accents among others) and f_aliaskey cannot claim
                            # to reproduce it exactly. Consulting the exact index first means a
                            # fold that is too aggressive can at worst skip an insert; it can
                            # never make the pass delete an alias the server was happy to keep.
                            arrbyexact = {}    # ID_PERSON -> {PERSON_NAME: row}
                            arrbyfold = {}     # ID_PERSON -> {alias key: first row}
                            arrallrows = {}    # ID_PERSON -> [row, ...], nameless rows included
                            setdelete = set()
                            for rowexisting in cursor3.fetchall():
                                arrallrows.setdefault(rowexisting['ID_PERSON'], []).append(rowexisting)
                                if not rowexisting.get('PERSON_NAME'):
                                    continue
                                arrexact = arrbyexact.setdefault(rowexisting['ID_PERSON'], {})
                                if rowexisting['PERSON_NAME'] in arrexact:
                                    # Two rows with the very same name. The UNIQUE key added by
                                    # migrations/add_unique_person_alias_key.py makes this
                                    # impossible going forward, so this only catches leftovers.
                                    setdelete.add(rowexisting['ID_ROW'])
                                    lng_duplicates_removed += 1
                                    continue
                                arrexact[rowexisting['PERSON_NAME']] = rowexisting
                                arrbyfold.setdefault(rowexisting['ID_PERSON'], {}).setdefault(
                                    f_aliaskey(rowexisting['PERSON_NAME']), rowexisting)

                            arrinsert = []
                            arrupdate = []
                            for lngidperson, arraliases in arrdesired.items():
                                arrexact = arrbyexact.get(lngidperson, {})
                                arrfold = arrbyfold.get(lngidperson, {})
                                if not arraliases:
                                    setdelete.update(r['ID_ROW'] for r in arrallrows.get(lngidperson, []))
                                    continue
                                setclaimed = set()
                                setseen = set()
                                for lngdisplayorder, stralias in enumerate(arraliases, start=1):
                                    stralias = stralias[:200]
                                    strkey = f_aliaskey(stralias)
                                    rowexisting = arrexact.get(stralias)
                                    if rowexisting is None:
                                        if strkey in setseen:
                                            # Another alias already occupies the only row the
                                            # server will hold for this spelling.
                                            continue
                                        rowexisting = arrfold.get(strkey)
                                    setseen.add(strkey)
                                    strlanguagefamily = guess_language_family(stralias)
                                    if rowexisting is None:
                                        arrinsert.append({
                                            "ID_PERSON": lngidperson,
                                            "PERSON_NAME": stralias,
                                            "LANGUAGE_FAMILY": strlanguagefamily,
                                            "DISPLAY_ORDER": lngdisplayorder,
                                        })
                                        continue
                                    setclaimed.add(rowexisting['ID_ROW'])
                                    if (rowexisting.get('LANGUAGE_FAMILY') != strlanguagefamily
                                            or rowexisting.get('DISPLAY_ORDER') != lngdisplayorder):
                                        # The row is there and already correct in the vast
                                        # majority of cases: only real drift is written.
                                        # PERSON_NAME goes back as the server stores it, never
                                        # as computed here, so a row reached through the folded
                                        # index cannot start the flip-flop over again.
                                        arrupdate.append({
                                            "ID_ROW": rowexisting['ID_ROW'],
                                            "ID_PERSON": lngidperson,
                                            "PERSON_NAME": rowexisting['PERSON_NAME'],
                                            "LANGUAGE_FAMILY": strlanguagefamily,
                                            "DISPLAY_ORDER": lngdisplayorder,
                                        })
                                for rowexisting in arrallrows.get(lngidperson, []):
                                    if (rowexisting.get('PERSON_NAME')
                                            and rowexisting['ID_ROW'] not in setclaimed):
                                        setdelete.add(rowexisting['ID_ROW'])

                            if setdelete:
                                arrdelete = sorted(setdelete)
                                for lngstart in range(0, len(arrdelete), 1000):
                                    arrchunk = arrdelete[lngstart:lngstart + 1000]
                                    strplaceholders = ", ".join(["%s"] * len(arrchunk))
                                    cursor3.execute(
                                        "DELETE FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS WHERE ID_ROW IN (" + strplaceholders + ")",
                                        arrchunk
                                    )
                                    lng_aliases_deleted += cursor3.rowcount
                                cp.connectioncp.commit()

                            if arrinsert:
                                # No-clobber, and that is the whole point. f_aliaskey folds
                                # case and Latin diacritics, but the collation also folds
                                # hiragana against katakana, full-width against half-width,
                                # and more; no Python normalization reproduces it exactly. So
                                # some of these "missing" aliases are in fact the server's
                                # view of a row another alias already owns. An upsert would
                                # hand them that row's DISPLAY_ORDER, the owner would take it
                                # back next run, and the pass would write forever. Leaving the
                                # stored row alone removes the possibility rather than trying
                                # to predict it. The count returned is rows actually inserted,
                                # so the server variable stays a true convergence signal.
                                lng_aliases_inserted += cp.f_sqlbulkinsertnoclobber(
                                    "T_WC_TMDB_PERSON_ALSO_KNOWN_AS", arrinsert, 1, 500
                                )
                            if arrupdate:
                                # Keyed on ID_ROW (the primary key), so this is a true in-place
                                # update and DAT_CREAT / ID_CREATOR are preserved.
                                lng_aliases_updated += cp.f_sqlbulkupsert(
                                    "T_WC_TMDB_PERSON_ALSO_KNOWN_AS", arrupdate,
                                    ["ID_ROW"], 1, 500
                                )

                            lng_aliases_upserted = lng_aliases_inserted + lng_aliases_updated
                            if lng_pages % 10 == 0:
                                cp.f_setservervariable("strtmdbpersonpreprocessalsoknownaspersons",str(lng_persons_processed),"Count of persons processed for ALSO_KNOWN_AS",0)
                                cp.f_setservervariable("strtmdbpersonpreprocessalsoknownasupserted",str(lng_aliases_upserted),"Count of ALSO_KNOWN_AS aliases written (inserted or corrected)",0)
                                cp.f_setservervariable("strtmdbpersonpreprocessalsoknownasdeleted",str(lng_aliases_deleted),"Count of ALSO_KNOWN_AS aliases deleted",0)
                                print("ALSO_KNOWN_AS: " + str(lng_persons_processed) + " persons, " + str(lng_aliases_inserted) + " inserted, " + str(lng_aliases_updated) + " updated, " + str(lng_aliases_deleted) + " deleted (up to ID_PERSON " + str(lnglastid) + ")", flush=True)

                        cp.f_setservervariable(
                            "strtmdbpersonpreprocessalsoknownaspersons",
                            str(lng_persons_processed),
                            "Count of persons processed for ALSO_KNOWN_AS",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbpersonpreprocessalsoknownasupserted",
                            str(lng_aliases_upserted),
                            "Count of ALSO_KNOWN_AS aliases written (inserted or corrected)",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbpersonpreprocessalsoknownasdeleted",
                            str(lng_aliases_deleted),
                            "Count of ALSO_KNOWN_AS aliases deleted",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbpersonpreprocessalsoknownasinserted",
                            str(lng_aliases_inserted),
                            "Count of ALSO_KNOWN_AS aliases inserted",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbpersonpreprocessalsoknownasupdated",
                            str(lng_aliases_updated),
                            "Count of ALSO_KNOWN_AS aliases corrected in place",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbpersonpreprocessalsoknownasduplicates",
                            str(lng_duplicates_removed),
                            "Count of duplicate (ID_PERSON, PERSON_NAME) alias rows removed",
                            0
                        )
                        # Same rule as the other pass: the watermark moves only once the
                        # window has been read in full.
                        cp.f_setservervariable("strtmdbpersonpreprocessakawatermark",strrunstart,"Persons with TIM_UPDATED below this have been processed for ALSO_KNOWN_AS",0)
                        setprocessesok.add(2)
                        print("\nALSO_KNOWN_AS done: " + str(lng_persons_processed) + " persons, " + str(lng_aliases_inserted) + " inserted, " + str(lng_aliases_updated) + " updated, " + str(lng_aliases_deleted) + " deleted (" + str(lng_duplicates_removed) + " of them duplicates)")
                    except pymysql.MySQLError as e:
                        print(f"Database error: {e}")
                    except Exception as e:
                        print(f"Error processing ALSO_KNOWN_AS: {e}")
                    finally:
                        print("ALSO_KNOWN_AS processing done")
            if intfullpass and setprocessesrun and setprocessesrun == setprocessesok:
                # Both the clock and the version are stamped here, never earlier: a full
                # pass that half finished must not stop the next run from redoing it.
                cp.f_setservervariable("strtmdbpersonpreprocesslastfullpass",strrunstart,"Start of the last full pass that completed",0)
                cp.f_setservervariable("strtmdbpersonpreprocessderivationversion",str(lngderivationversion),"Derivation logic version the stored data was produced with",0)
            elif intfullpass:
                print("Full pass incomplete: not recording it, the next run will start over")
            print("------------------------------------------")
            strcurrentprocess = ""
            cp.f_setservervariable("strtmdbpersonpreprocesscurrentprocess",strcurrentprocess,"Current process in the TMDb database preprocess",0)
            strsql = ""
            cp.f_setservervariable("strtmdbpersonpreprocesscurrentsql",strsql,"Current SQL query in the TMDb database preprocess",0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strtmdbpersonpreprocessenddatetime",strnow,"Date and time of the TMDb database preprocess ending",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strtmdbpersonpreprocesstotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
            
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()
