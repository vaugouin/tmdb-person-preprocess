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
from typing import Dict, Optional

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
        print(query)
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
                        print(normalized_fr,'->',country_code)
            
            # Add the English name
            if name_en:
                if name_en != "":
                    normalized_en = normalize_string(name_en)
                    if normalized_en not in country_lookup_dict:
                        country_lookup_dict[normalized_en] = country_code
                        print(normalized_en,'->',country_code)
            
            # Process aliases if they exist
            if aliases and len(aliases) > 1:  # Check if it's not empty or just a single pipe
                # Split by pipe and process each alias
                for alias in aliases.split('|'):
                    if alias.strip() != "":  # Skip empty strings
                        normalized_alias = normalize_string(alias)
                        if normalized_alias not in country_lookup_dict:
                            country_lookup_dict[normalized_alias] = country_code
                            print(normalized_alias,'->',country_code)
        
        cursor2.close()
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

def batch_update_data_country_of_birth(connection, df, batch_size=1000):
    """Update data in batches to improve performance"""
    try:
        cursor = connection.cursor()
        
        # Create a copy of the dataframe to avoid modifying the original
        processed_df = df.copy()
        
        print("\nSample of processed data:")
        print(processed_df.head())
        
        # Total number of rows
        total_rows = len(processed_df)
        print("\nTotal number of rows to update: ",total_rows)
        rows_updated = 0
        rows_failed = 0
        
        # Process in batches
        for i in range(0, total_rows, batch_size):
            batch_df = processed_df.iloc[i:i+batch_size]
            
            # Process each row in the batch
            for _, row in batch_df.iterrows():
                # Prepare data for update, ensuring all values are properly processed
                update_data = (
                    #process_value(row['PLACE_OF_BIRTH']),
                    #process_value(row['PLACE_OF_BIRTH_ORIGINAL']),
                    process_value(row['COUNTRY_OF_BIRTH_LONG']),
                    process_value(row['COUNTRY_OF_BIRTH']),
                    row['ID_PERSON']  # WHERE clause
                )
                
                # Display the produced UPDATE SQL query with parameter values
                #print("\nExecuting SQL query with parameters:")
                print(update_data)
                arrpersoncouples = {}
                #arrpersoncouples["PLACE_OF_BIRTH"] = row['PLACE_OF_BIRTH']
                #arrpersoncouples["PLACE_OF_BIRTH_ORIGINAL"] = row['PLACE_OF_BIRTH_ORIGINAL']
                arrpersoncouples["COUNTRY_OF_BIRTH_LONG"] = row['COUNTRY_OF_BIRTH_LONG']
                arrpersoncouples["COUNTRY_OF_BIRTH"] = row['COUNTRY_OF_BIRTH']
                strsqltablename = "T_WC_TMDB_PERSON"
                strsqlupdatecondition = f"ID_PERSON = {row['ID_PERSON']}"
                cp.f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
                rows_updated += 1
                
            cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthparsedcount",str(rows_updated),"Count of PLACE_OF_BIRTH row parsed",0)
            cp.f_setservervariable("strtmdbpersonpreprocesscountryofbirthfailedcount",str(rows_failed),"Count of PLACE_OF_BIRTH row failed",0)
            # Commit batch
            connection.commit()
            
            # Update progress
            progress = ((i + len(batch_df)) / total_rows) * 100
            print(f"Progress: {progress:.2f}% - Updated {rows_updated} rows, Failed {rows_failed}", end='\r')
        
        print(f"\nData update completed: {rows_updated} rows updated successfully, {rows_failed} rows failed")
        
    except Error as e:
        print(f"\nError updating data: {e}")
        print("\nTerminating script due to SQL error.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        print("\nTerminating script due to unexpected error.")
        sys.exit(1)

strdattoday = datetime.now(cp.paris_tz).strftime("%Y-%m-%d")

# Connect to the database
#connection = pymysql.connect(host=cp.strdbhost, user=cp.strdbuser, password=cp.strdbpassword, database=cp.strdbname, cursorclass=pymysql.cursors.DictCursor)

try:
    with cp.connectioncp:
        with cp.connectioncp.cursor() as cursor:
            cursor2 = cp.connectioncp.cursor()
            cursor3 = cp.connectioncp.cursor()
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
            strtotalruntime = ""
            cp.f_setservervariable("strtmdbpersonpreprocesstotalruntime",strtotalruntime,strtotalruntimedesc,0)
            
            arrprocessscope = {1: 'COUNTRY_OF_BIRTH'}
            for intindex, strdesc in arrprocessscope.items():
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
                        # Read data from database using fetchall()
                        query = """
SELECT ID_PERSON, PLACE_OF_BIRTH 
FROM T_WC_TMDB_PERSON 
WHERE PLACE_OF_BIRTH IS NOT NULL 
AND PLACE_OF_BIRTH <> '' 
ORDER BY ID_PERSON ASC 
                        """
                        print(query)
                        cursor2.execute(query)
                        result = cursor2.fetchall()
                        # Convert the result to a pandas DataFrame
                        data = pd.DataFrame(result)
                        print(f"Loaded {len(data)} rows of data")
                        print(data.head(20))
                        #time.sleep(5)

                        # Create backup of original data
                        data['PLACE_OF_BIRTH'] = data['PLACE_OF_BIRTH'].astype(str)
                        data['PLACE_OF_BIRTH_ORIGINAL'] = data['PLACE_OF_BIRTH']
                        
                        # Convert to lowercase and apply cleaning
                        data['PLACE_OF_BIRTH'] = data['PLACE_OF_BIRTH'].str.lower()
                        data['PLACE_OF_BIRTH'] = data['PLACE_OF_BIRTH'].apply(clean_place_of_birth)
                        data['COUNTRY_OF_BIRTH_LONG'] = data['PLACE_OF_BIRTH']

                        print("\nAfter clean_place_of_birth()")
                        print(data['PLACE_OF_BIRTH'].head(20))
                        time.sleep(5)
                        #sys.exit(1)
                        
                        #  Apply transformations and extract components
                        country_of_birth = data['PLACE_OF_BIRTH'].apply(extract_country_of_birth)

                        print("\nAfter extract_country_of_birth()")
                        print(country_of_birth.head(20))
                        time.sleep(5)

                        # Convert the list of dictionaries to a DataFrame
                        format_df = pd.DataFrame(country_of_birth.tolist())
                        
                        # Add the extracted components to the main DataFrame
                        data['COUNTRY_OF_BIRTH'] = format_df['COUNTRY_OF_BIRTH']
                        
                        # Display sample of processed data
                        print("\nSample of processed data:")
                        print(data.head(20))
                        #time.sleep(5)
                        
                        # Update data in batches
                        print("Updating data in MariaDB...")
                        batch_update_data_country_of_birth(cp.connectioncp, data, 1)
                        
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
    cp.connectioncp.rollback()

