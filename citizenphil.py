from urllib.parse import quote
import time
import requests
import json
import pymysql.cursors
import re
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

headers = {
    "accept": "application/json",
    "Authorization": "Bearer " + strtmdbapitoken
}

connectioncp = pymysql.connect(host=strdbhost, port=lngdbport, user=strdbuser, password=strdbpassword, database=strdbname, cursorclass=pymysql.cursors.DictCursor)

paris_tz = pytz.timezone(cps.strusertimezone)

strdatepattern = r"^\d{4}-\d{2}-\d{2}$"

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
            if type(value) is int: # Handle Integers
                arrvalues.append("{key} = {value}".format(key=key, value=value))
            elif type(value) is float: # Handle floats
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

def f_tmdbjsonremovekeys(strjson,strbegin,strend,strreplace):
    # strbegin = ', "overview":'
    lnglenbegin = len(strbegin)
    # strend = ', "popularity":'
    lnglenend = len(strend)
    # strreplace = ', "popularity":'
    while True:
        lngposbegin = strjson.find(strbegin)
        if lngposbegin == -1:
            # Begin string not found
            break
        else:
            lngposend = strjson.find(strend, lngposbegin+lnglenbegin)
            if lngposend == -1:
                # End string not found
                break
            else:
                strjson = strjson[0:lngposbegin] + strreplace + strjson[lngposend + lnglenend:]
    return strjson

def f_tmdbcontentimagesstosql(lngcontentid, strcontenttype, strsqlmastertable, strsqltablename, strkeyfieldname):
    """
    Fetch images for a content from TMDb API and store them in the T_WC_TMDB_*_IMAGE table.
    
    Args:
        lngcontentid (int): TMDb ID of the content
        
    Returns:
        bool: True if successful, False if failed
    """
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global strsqlns
    global paris_tz
    
    if lngcontentid <= 0:
        print(f"Error: Invalid {strcontenttype} ID {lngcontentid}")
        return False
    
    strtmdbapiimagesurl = f"3/{strcontenttype}/{lngcontentid}/images"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiimagesurl
    
    # Add retry logic with error handling
    intencore = True
    intattemptsremaining = 5
    intsuccess = False
    
    while intencore:
        try:
            response = requests.get(strtmdbapifullurl, headers=headers)
            intencore = False
            intsuccess = True
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except requests.exceptions.ConnectionError as conn_err:
            print(f'Connection error occurred: {conn_err}')
        except requests.exceptions.Timeout as timeout_err:
            print(f'Timeout error occurred: {timeout_err}')
        except requests.exceptions.RequestException as req_err:
            print(f'Request error occurred: {req_err}')
        except Exception as err:
            print(f'An error occurred: {err}')
        
        if intencore:
            intattemptsremaining = intattemptsremaining - 1
            if intattemptsremaining >= 0:
                time.sleep(1)  # Wait for 1 second before next request
            else:
                intencore = False
    
    if not intsuccess:
        print(f"f_tmdbcontentimagesstosql({lngcontentid}) failed!")
        return False
    
    data = response.json()
    if 'status_code' in data and data['status_code'] > 1:
        print(f"Error: API returned status code {data['status_code']}")
        if 'status_message' in data:
            print(f"Status message: {data['status_message']}")
        return False
    
    # Get current timestamp for database records
    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")
    
    # Track all image paths to clean up obsolete ones later
    all_image_paths = []
    
    # Function to process image arrays (both backdrops and posters)
    def process_image_array(image_array, image_type):
        lngdisplayorder = 0
        for image in image_array:
            lngdisplayorder += 1
            
            # Extract image data
            image_path = image.get('file_path', '')
            if not image_path:
                continue
                
            all_image_paths.append(image_path)
            
            # Prepare data for database
            arrimagedata = {
                strkeyfieldname: lngcontentid,
                "DISPLAY_ORDER": lngdisplayorder,
                "DAT_CREAT": current_date,
                "TIM_UPDATED": current_time,
                "TYPE_IMAGE": image_type,
                "LANG": image.get('iso_639_1', ''),
                "IMAGE_PATH": image_path,
                "ASPECT_RATIO": image.get('aspect_ratio', 0),
                "WIDTH": image.get('width', 0),
                "HEIGHT": image.get('height', 0),
                "VOTE_AVERAGE": image.get('vote_average', 0),
                "VOTE_COUNT": image.get('vote_count', 0)
            }
            
            # Update or insert into database
            strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid} AND TYPE_IMAGE = '{image_type}' AND IMAGE_PATH = '{image_path}'"
            f_sqlupdatearray(strsqltablename, arrimagedata, strsqlupdatecondition, 1)
    
    # Process backdrops
    if 'backdrops' in data and data['backdrops']:
        process_image_array(data['backdrops'], 'backdrop')
    
    # Process posters
    if 'posters' in data and data['posters']:
        process_image_array(data['posters'], 'poster')
    
    # Process logos
    if 'logos' in data and data['logos']:
        process_image_array(data['logos'], 'logo')
    
    # Process profiles
    if 'profiles' in data and data['profiles']:
        process_image_array(data['profiles'], 'profile')
    
    # Clean up obsolete images
    if all_image_paths:
        # Create a comma-separated list of image paths with quotes
        image_paths_list = "'" + "', '".join(all_image_paths) + "'"
        
        # Delete images that are no longer present in the API response
        strsqldelete = f"DELETE FROM {strsqltablename} WHERE {strkeyfieldname} = {lngcontentid} AND IMAGE_PATH NOT IN ({image_paths_list})"
        cursor = connectioncp.cursor()
        cursor.execute(strsqldelete)
        connectioncp.commit()
    else:
        # If no images were found, delete all images for this contents
        strsqldelete = f"DELETE FROM {strsqltablename} WHERE {strkeyfieldname} = {lngcontentid}"
        cursor = connectioncp.cursor()
        cursor.execute(strsqldelete)
        connectioncp.commit()
    
    # Update the content record to mark images as completed
    strtimimagescompleted = current_time
    strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid}"
    strsqlupdatesetclause = f"TIM_IMAGES_COMPLETED = '{strtimimagescompleted}'"
    strsqlupdate = f"UPDATE {strsqlmastertable} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
    cursor = connectioncp.cursor()
    cursor.execute(strsqlupdate)
    connectioncp.commit()
    return True
    
# https://developer.themoviedb.org/reference/person-details

def f_tmdbpersontosql(lngpersonid):
    global strtmdbapidomainurl
    global headers
    global strdatepattern
    
    if lngpersonid > 0:
        strtmdbapipersonurl = "3/person/" + str(lngpersonid) + "?append_to_response=combined_credits,external_ids"
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapipersonurl
        # print(strtmdbapifullurl)
        intencore = True
        intattemptsremaining = 5
        intsuccess = False
        while intencore:
            try:
                response = requests.get(strtmdbapifullurl, headers=headers)
                intencore = False
                intsuccess = True
            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
            except requests.exceptions.ConnectionError as conn_err:
                print(f'Connection error occurred: {conn_err}')  # Handle connection errors
            except requests.exceptions.Timeout as timeout_err:
                print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
            except requests.exceptions.RequestException as req_err:
                print(f'Request error occurred: {req_err}')  # Handle other request-related errors
            except Exception as err:
                print(f'An error occurred: {err}')  # Handle any other exceptions
            if intencore:
                intattemptsremaining = intattemptsremaining - 1
                if intattemptsremaining >= 0:
                    time.sleep(1)  # Wait for 1 second before next request
                else:
                    intencore = False
        if not intsuccess:
            print(f"f_tmdbpersontosql({lngpersonid}) failed!")
        else:
            # strapiperson = response.text
            # Parse the JSON data into a dictionary
            data = response.json()
            
            lngpersonstatuscode = 0
            if 'status_code' in data:
                lngpersonstatuscode = data['status_code']
            if lngpersonstatuscode <= 1:
                # API request result is not an error
                # Extract data using the keys
                strpersonidimdb = ""
                if 'imdb_id' in data:
                    strpersonidimdb = data['imdb_id']
                strpersonbiography = data['biography']
                strpersonbirthday = data['birthday']
                lngpersonbirthyear = 0
                lngpersonbirthmonth = 0
                lngpersonbirthday = 0
                if strpersonbirthday:
                    if not re.match(strdatepattern, strpersonbirthday):
                        # Date is invalid
                        strpersonbirthday = None
                    else:
                        lngpersonbirthyear, lngpersonbirthmonth, lngpersonbirthday = map(int, strpersonbirthday.split('-'))
                strpersondeathday = data['deathday']
                lngpersondeathyear = 0
                lngpersondeathmonth = 0
                lngpersondeathday = 0
                if strpersondeathday:
                    if not re.match(strdatepattern, strpersondeathday):
                        # Date is invalid
                        strpersondeathday = None
                    else:
                        lngpersondeathyear, lngpersondeathmonth, lngpersondeathday = map(int, strpersondeathday.split('-'))
                intpersongender = data['gender']
                strpersonprofilepath = data['profile_path']
                strpersonhomepage = data['homepage']
                strpersonname = data['name']
                strpersonplaceofbirth = str(data['place_of_birth'])
                strpersonplaceofbirth = strpersonplaceofbirth.strip()
                dblpersonpopularity = data['popularity']
                strpersonknownfordepartment = data['known_for_department']
                boopersonadult = data['adult']
                intpersonadult = 0
                if boopersonadult:
                    intpersonadult = 1
                arrpersonalsoknownas = data['also_known_as']
                strpersonalsoknownas = "|"
                for strvalue in arrpersonalsoknownas:
                    strpersonalsoknownas += strvalue + "|"
                
                strpersonidwikidata = ""
                if 'external_ids' in data:
                    if 'wikidata_id' in data['external_ids']:
                        strpersonidwikidata = data['external_ids']['wikidata_id']
                
                # print(f"{strpersonname} {strpersonidimdb}")
                #"print(f"{strpersonknownfordepartment}")
                # if strpersonbirthday:
                #     print(f"Birth {strpersonbirthday}")
                # if strpersondeathday: 
                #     print(f"Death {strpersondeathday}")
                # print(f"Biography: {strpersonbiography}")
                # print(f"Adult: {intpersonadult}")
                # print(f"Also know as: {strpersonalsoknownas}")
                # print(f"Popularity: {dblpersonpopularity}")
                
                if strpersonhomepage: 
                    if len(strpersonhomepage) > 250:
                        # If homepage URL is too long, we chop it
                        strpersonhomepage = strpersonhomepage[:250]
                
                arrpersoncouples = {}
                #arrpersoncouples["API_URL"] = strtmdbapipersonurl
                # arrpersoncouples["API_RESULT"] = strapipersonfordb
                if strpersonidimdb:
                    arrpersoncouples["ID_IMDB"] = strpersonidimdb
                else:
                    arrpersoncouples["ID_IMDB"] = ""
                if strpersonidwikidata:
                    arrpersoncouples["ID_WIKIDATA"] = strpersonidwikidata
                else:
                    arrpersoncouples["ID_WIKIDATA"] = ""
                arrpersoncouples["BIOGRAPHY"] = strpersonbiography
                if strpersonbirthday:
                    arrpersoncouples["BIRTHDAY"] = strpersonbirthday
                arrpersoncouples["BIRTH_YEAR"] = lngpersonbirthyear
                arrpersoncouples["BIRTH_MONTH"] = lngpersonbirthmonth
                arrpersoncouples["BIRTH_DAY"] = lngpersonbirthday
                if strpersondeathday: 
                    arrpersoncouples["DEATHDAY"] = strpersondeathday
                arrpersoncouples["DEATH_YEAR"] = lngpersondeathyear
                arrpersoncouples["DEATH_MONTH"] = lngpersondeathmonth
                arrpersoncouples["DEATH_DAY"] = lngpersondeathday
                arrpersoncouples["GENDER"] = intpersongender
                arrpersoncouples["ID_PERSON"] = lngpersonid
                arrpersoncouples["PROFILE_PATH"] = strpersonprofilepath
                arrpersoncouples["HOMEPAGE_URL"] = strpersonhomepage
                arrpersoncouples["NAME"] = strpersonname
                arrpersoncouples["PLACE_OF_BIRTH"] = strpersonplaceofbirth
                arrpersoncouples["POPULARITY"] = dblpersonpopularity
                arrpersoncouples["KNOWN_FOR_DEPARTMENT"] = strpersonknownfordepartment
                arrpersoncouples["ADULT"] = intpersonadult
                arrpersoncouples["ALSO_KNOWN_AS"] = strpersonalsoknownas
                
                strmoviecredits = ""
                strseriecredits = ""
                arrcredittype = {1: 'cast', 2:'crew'}
                for intcredittype,strpersoncreditcredittype in arrcredittype.items():
                    if intcredittype == 1:
                        strtitle = "Cast"
                    else:
                        strtitle = "Crew"
                    # print(strtitle)
                    if "combined_credits" in data:
                        if strpersoncreditcredittype in data['combined_credits']:
                            arrpersoncredits = data['combined_credits'][strpersoncreditcredittype]
                            # print(arrpersoncredits)
                            for onecontent in arrpersoncredits:
                                strpersoncreditmediatype = onecontent['media_type']
                                # print(strpersoncreditmediatype)
                                if strpersoncreditmediatype == "movie":
                                    # This is a movie
                                    strpersoncredittitle = onecontent['title']
                                    strpersoncreditreleaseyear = ""
                                    if 'release_date' in onecontent:
                                        strpersoncreditreleasedate = onecontent['release_date']
                                        strpersoncreditreleaseyear = strpersoncreditreleasedate[:4]
                                    strpersoncreditcreditid = onecontent['credit_id']
                                    lngmovieid = onecontent['id']
                                    arrpersonmoviecouples = {}
                                    if intcredittype == 1:
                                        strpersoncreditcharacter = onecontent['character']
                                        strpersoncreditdepartment = ""
                                        strpersoncreditjob = ""
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditcharacter}")
                                    else:
                                        strpersoncreditcharacter = ""
                                        strpersoncreditdepartment = onecontent['department']
                                        strpersoncreditjob = onecontent['job']
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditdepartment} {strpersoncreditjob}")
                                    if strmoviecredits != "":
                                        strmoviecredits += ","
                                    strmoviecredits += "'" + strpersoncreditcreditid + "'"
                                    arrpersonmoviecouples["ID_PERSON"] = lngpersonid
                                    arrpersonmoviecouples["ID_MOVIE"] = lngmovieid
                                    arrpersonmoviecouples["ID_CREDIT"] = strpersoncreditcreditid
                                    arrpersonmoviecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                    arrpersonmoviecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                    arrpersonmoviecouples["CREW_JOB"] = strpersoncreditjob
                                    arrpersonmoviecouples["CREDIT_TYPE"] = strpersoncreditcredittype
                                    # print(arrpersonmoviecouples)
                                    
                                    strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
                                    strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                    f_sqlupdatearray(strsqltablename,arrpersonmoviecouples,strsqlupdatecondition,1)
                                    
                                elif strpersoncreditmediatype == "tv":
                                    # This is a TV show
                                    strpersoncredittitle = onecontent['name']
                                    strpersoncreditreleaseyear = ""
                                    if 'first_air_date' in onecontent:
                                        strpersoncreditreleasedate = onecontent['first_air_date']
                                        strpersoncreditreleaseyear = strpersoncreditreleasedate[:4]
                                    strpersoncreditcreditid = onecontent['credit_id']
                                    lngserieid = onecontent['id']
                                    arrpersonseriecouples = {}
                                    if intcredittype == 1:
                                        strpersoncreditcharacter = onecontent['character']
                                        strpersoncreditdepartment = ""
                                        strpersoncreditjob = ""
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditcharacter}")
                                    else:
                                        strpersoncreditcharacter = ""
                                        strpersoncreditdepartment = onecontent['department']
                                        strpersoncreditjob = onecontent['job']
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditdepartment} {strpersoncreditjob}")
                                    if strseriecredits != "":
                                        strseriecredits += ","
                                    strseriecredits += "'" + strpersoncreditcreditid + "'"
                                    arrpersonseriecouples["ID_PERSON"] = lngpersonid
                                    arrpersonseriecouples["ID_SERIE"] = lngserieid
                                    arrpersonseriecouples["ID_CREDIT"] = strpersoncreditcreditid
                                    arrpersonseriecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                    arrpersonseriecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                    arrpersonseriecouples["CREW_JOB"] = strpersoncreditjob
                                    arrpersonseriecouples["CREDIT_TYPE"] = strpersoncreditcredittype
                                    # print(arrpersonseriecouples)
                                    
                                    strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                                    strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                    f_sqlupdatearray(strsqltablename,arrpersonseriecouples,strsqlupdatecondition,1)
                
                encoded_biography = data['biography'].replace('\n', '\\n').replace('"', '\\"')
                data['biography'] = encoded_biography
                strapipersonfordb = json.dumps(data, ensure_ascii=False)
                strapipersonfordb = f_tmdbjsonremovekeys(strapipersonfordb,', "overview":',', "popularity":',', "popularity":')
                strapipersonfordb = f_tmdbjsonremovekeys(strapipersonfordb,', "original_title":',', "popularity":',', "popularity":')
                strapipersonfordb = f_tmdbjsonremovekeys(strapipersonfordb,', "title":',', "video":',', "video":')
                #arrpersoncouples["API_RESULT"] = strapipersonfordb
                #arrpersoncouples["CRAWLER_VERSION"] = 3
                
                strsqltablename = "T_WC_TMDB_PERSON"
                strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
                f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
                
                # Now delete credits that are not for this person
                if strmoviecredits == "":
                    strmoviecredits = "'0'"
                strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_MOVIE WHERE ID_PERSON = " + str(lngpersonid) + " AND ID_CREDIT NOT IN (" + strmoviecredits + ")"
                # print(f"{strsqldelete}")
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
                if strseriecredits == "":
                    strseriecredits = "'0'"
                strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_SERIE WHERE ID_PERSON = " + str(lngpersonid) + " AND ID_CREDIT NOT IN (" + strseriecredits + ")"
                # print(f"{strsqldelete}")
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()

def f_tmdbpersonexist(lngpersonid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    # By default, we assume that this person exists
    intresult = True
    if lngpersonid > 0:
        strtmdbapipersonurl = "3/person/" + str(lngpersonid) + "?language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapipersonurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        lngpersonstatuscode = 0
        if 'status_code' in data:
            lngpersonstatuscode = data['status_code']
        if lngpersonstatuscode == 34:
            # API request result is an error:
            # The resource you requested could not be found
            intresult = False
    return intresult

def f_tmdbpersondelete(lngpersonid):
    global connectioncp
    
    if lngpersonid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_PERSON"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_IMAGE"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
def f_tmdbpersonsetcreditscompleted(lngpersonid):
    global paris_tz
    global connectioncp
    
    if lngpersonid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_PERSON"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbpersonimagestosql(lngpersonid):
    f_tmdbcontentimagesstosql(lngpersonid, "person", "T_WC_TMDB_PERSON", "T_WC_TMDB_PERSON_IMAGE", "ID_PERSON")
    
def f_tmdbpersontosqleverything(lngpersonid):
    f_tmdbpersontosql(lngpersonid)
    f_tmdbpersonsetcreditscompleted(lngpersonid)
    f_tmdbpersonimagestosql(lngpersonid)

# https://developer.themoviedb.org/reference/movie-details

def f_tmdbmovietosql(lngmovieid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    global strsqlns
    
    if lngmovieid > 0:
        # New TMDb API call with append_to_response since 2024-05-24 10:00
        strtmdbapimovieurl = "3/movie/" + str(lngmovieid) + "?append_to_response=credits,alternative_titles,external_ids&language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimovieurl
        # print(strtmdbapifullurl)
        # Add retry logic with error handling
        intencore = True
        intattemptsremaining = 5
        intsuccess = False
        while intencore:
            try:
                response = requests.get(strtmdbapifullurl, headers=headers)
                intencore = False
                intsuccess = True
            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
            except requests.exceptions.ConnectionError as conn_err:
                print(f'Connection error occurred: {conn_err}')  # Handle connection errors
            except requests.exceptions.Timeout as timeout_err:
                print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
            except requests.exceptions.RequestException as req_err:
                print(f'Request error occurred: {req_err}')  # Handle other request-related errors
            except Exception as err:
                print(f'An error occurred: {err}')  # Handle any other exceptions
            if intencore:
                intattemptsremaining = intattemptsremaining - 1
                if intattemptsremaining >= 0:
                    time.sleep(1)  # Wait for 1 second before next request
                else:
                    intencore = False
        
        if not intsuccess:
            print(f"f_tmdbmovietosql({lngmovieid}) failed!")
            return
        
        data = response.json()
        lngmoviestatuscode = 0
        if 'status_code' in data:
            lngmoviestatuscode = data['status_code']
        if lngmoviestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strmovieidimdb = ""
            if 'imdb_id' in data:
                strmovieidimdb = data['imdb_id']
            strmovieoverview = ""
            if 'overview' in data:
                strmovieoverview = data['overview']
            strmoviereleasedate = ""
            #strmoviereleaseyear = ""
            lngmoviereleaseyear = 0
            lngmoviereleasemonth = 0
            lngmoviereleaseday = 0
            if 'release_date' in data:
                strmoviereleasedate = data['release_date']
                if not re.match(strdatepattern, strmoviereleasedate):
                    # Date is invalid
                    strmoviereleasedate = None
                else:
                    lngmoviereleaseyear, lngmoviereleasemonth, lngmoviereleaseday = map(int, strmoviereleasedate.split('-'))
            intmovievideo = 0
            if 'video' in data:
                boomovievideo = data['video']
                if boomovievideo:
                    intmovievideo = 1
            strmovieposterpath = ""
            if 'poster_path' in data:
                strmovieposterpath = data['poster_path']
            strmoviehomepage = ""
            if 'homepage' in data:
                strmoviehomepage = data['homepage']
            strmovietitle = ""
            if 'title' in data:
                strmovietitle = data['title']
            strmovieoriginallanguage = ""
            if 'original_language' in data:
                strmovieoriginallanguage = data['original_language']
            dblmoviepopularity = 0
            if 'popularity' in data:
                dblmoviepopularity = data['popularity']
            strmoviebackdroppath = ""
            if 'backdrop_path' in data:
                strmoviebackdroppath = data['backdrop_path']
            intmovieadult = 0
            if 'adult' in data:
                boomovieadult = data['adult']
                if boomovieadult:
                    intmovieadult = 1
            strmovieoriginaltitle = ""
            if 'original_title' in data:
                strmovieoriginaltitle = data['original_title']
                if strmovieoriginaltitle: 
                    if len(strmovieoriginaltitle) > 250:
                        strmovieoriginaltitle = strmovieoriginaltitle[:250]
            strmoviestatus = ""
            if 'status' in data:
                strmoviestatus = data['status']
            strmoviegenres = ""
            intdocumentary = False
            if 'genres' in data:
                arrmoviegenres = data['genres']
                strmoviegenres = "|"
                arrgenrenames = [genre['name'] for genre in arrmoviegenres]
                for strvalue in arrgenrenames:
                    strmoviegenres += strvalue + "|"
                    if strvalue == "Documentary":
                        # This is a documentary
                        intdocumentary = True
            lngcollectionid = 0
            if 'belongs_to_collection' in data:
                if data['belongs_to_collection']:
                    if 'id' in data['belongs_to_collection']:
                        lngcollectionid = data['belongs_to_collection']['id']
            dblmoviebudget = 0
            if 'budget' in data:
                dblmoviebudget = data['budget']
            lngmovieruntime = 0
            intmovieisshortfilm = 0
            if 'runtime' in data:
                lngmovieruntime = data['runtime']
                if lngmovieruntime > 58:
                    intmovieisshortfilm = 0
                elif lngmovieruntime > 0:
                    intmovieisshortfilm = 1
            dblmovierevenue = 0
            if 'revenue' in data:
                dblmovierevenue = data['revenue']
            strmovietagline = ""
            if 'tagline' in data:
                strmovietagline = data['tagline']
            dblmovievoteaverage = 0
            if 'vote_average' in data:
                dblmovievoteaverage = data['vote_average']
            lngmovievotecount = 0
            if 'vote_count' in data:
                lngmovievotecount = data['vote_count']
            strmovieidwikidata = ""
            if 'external_ids' in data:
                if 'wikidata_id' in data['external_ids']:
                    strmovieidwikidata = data['external_ids']['wikidata_id']
            strmoviecountries = ""
            strcountryidlist = ""
            lngcountrydisplayorder = 0
            if 'production_countries' in data:
                arrmoviecountries = data['production_countries']
                strmoviecountries = "|"
                arrcountrynames = [country['iso_3166_1'] for country in arrmoviecountries]
                for strvalue in arrcountrynames:
                    strmoviecountries += strvalue + "|"
                    lngcountrydisplayorder = lngcountrydisplayorder + 1
                    if strcountryidlist != "":
                        strcountryidlist += ","
                    strcountryidlist += "'" + strvalue + "'"
                    arrmoviecountrycouples = {}
                    arrmoviecountrycouples["ID_MOVIE"] = lngmovieid
                    arrmoviecountrycouples["COUNTRY_CODE"] = strvalue
                    arrmoviecountrycouples["DISPLAY_ORDER"] = lngcountrydisplayorder
                    # print(arrmoviecountrycouples)
                    strsqltablename = "T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND COUNTRY_CODE = '{strvalue}'"
                    f_sqlupdatearray(strsqltablename,arrmoviecountrycouples,strsqlupdatecondition,1)
            if strcountryidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_PRODUCTION_COUNTRY WHERE ID_MOVIE = " + str(lngmovieid) + " AND COUNTRY_CODE NOT IN (" + strcountryidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            strmoviespokenlanguages = ""
            strspokenlanguageidlist = ""
            lngspokenlanguagedisplayorder = 0
            if 'spoken_languages' in data:
                arrmoviespokenlanguages = data['spoken_languages']
                strmoviespokenlanguages = "|"
                arrspokenlanguagenames = [spokenlanguage['iso_639_1'] for spokenlanguage in arrmoviespokenlanguages]
                for strvalue in arrspokenlanguagenames:
                    strmoviespokenlanguages += strvalue + "|"
                    lngspokenlanguagedisplayorder = lngspokenlanguagedisplayorder + 1
                    if strspokenlanguageidlist != "":
                        strspokenlanguageidlist += ","
                    strspokenlanguageidlist += "'" + strvalue + "'"
                    arrmoviespokenlanguagecouples = {}
                    arrmoviespokenlanguagecouples["ID_MOVIE"] = lngmovieid
                    arrmoviespokenlanguagecouples["SPOKEN_LANGUAGE"] = strvalue
                    arrmoviespokenlanguagecouples["DISPLAY_ORDER"] = lngspokenlanguagedisplayorder
                    # print(arrmoviespokenlanguagecouples)
                    strsqltablename = "T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND SPOKEN_LANGUAGE = '{strvalue}'"
                    f_sqlupdatearray(strsqltablename,arrmoviespokenlanguagecouples,strsqlupdatecondition,1)
            if strspokenlanguageidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_SPOKEN_LANGUAGE WHERE ID_MOVIE = " + str(lngmovieid) + " AND SPOKEN_LANGUAGE NOT IN (" + strspokenlanguageidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            strcompanyidlist = ""
            lngcompanydisplayorder = 0
            if data['production_companies']:
                # Array is not empty
                for onecontent in data['production_companies']:
                    lngcompanyid = onecontent['id']
                    lngcompanydisplayorder = lngcompanydisplayorder + 1
                    if strcompanyidlist != "":
                        strcompanyidlist += ","
                    strcompanyidlist += str(lngcompanyid)
                    arrmoviecompanycouples = {}
                    arrmoviecompanycouples["ID_MOVIE"] = lngmovieid
                    arrmoviecompanycouples["ID_COMPANY"] = lngcompanyid
                    arrmoviecompanycouples["DISPLAY_ORDER"] = lngcompanydisplayorder
                    # print(arrmoviecompanycouples)
                    strsqltablename = "T_WC_TMDB_MOVIE_COMPANY"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_COMPANY = {lngcompanyid}"
                    f_sqlupdatearray(strsqltablename,arrmoviecompanycouples,strsqlupdatecondition,1)
            if strcompanyidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_COMPANY WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_COMPANY NOT IN (" + strcompanyidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            strgenreidlist = ""
            lnggenredisplayorder = 0
            if data['genres']:
                # Array is not empty
                for onecontent in data['genres']:
                    lnggenreid = onecontent['id']
                    lnggenredisplayorder = lnggenredisplayorder + 1
                    if strgenreidlist != "":
                        strgenreidlist += ","
                    strgenreidlist += str(lnggenreid)
                    arrmoviegenrecouples = {}
                    arrmoviegenrecouples["ID_MOVIE"] = lngmovieid
                    arrmoviegenrecouples["ID_GENRE"] = lnggenreid
                    arrmoviegenrecouples["DISPLAY_ORDER"] = lnggenredisplayorder
                    # print(arrmoviegenrecouples)
                    
                    strsqltablename = "T_WC_TMDB_MOVIE_GENRE"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_GENRE = {lnggenreid}"
                    f_sqlupdatearray(strsqltablename,arrmoviegenrecouples,strsqlupdatecondition,1)
            if strgenreidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_GENRE WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_GENRE NOT IN (" + strgenreidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # print(f"{strmovietitle} ({strmoviereleaseyear}) {strmovieidimdb}")
            # print(f"{strmoviebackdroppath}")
            # if strmoviereleasedate:
            #     print(f"Release {strmoviereleasedate}")
            # print(f"Overview: {strmovieoverview}")
            # print(f"Adult: {intmovieadult}")
            # print(f"Original title: {strmovieoriginaltitle}")
            # print(f"Popularity: {dblmoviepopularity}")
            
            if strmovietitle:
                if len(strmovietitle) > 250:
                    # If title is too long, we chop it
                    strmovietitle = strmovietitle[:250]
            if strmoviehomepage:
                if len(strmoviehomepage) > 500:
                    # If homepage URL is too long, we chop it
                    strmoviehomepage = strmoviehomepage[:500]
            if intdocumentary:
                intisdocumentary = 1
                intismovie = 0
            else:
                intisdocumentary = 0
                intismovie = 1
            
            arrmoviecouples = {}
            #arrmoviecouples["API_URL"] = strtmdbapimovieurl
            # arrmoviecouples["API_RESULT"] = strapimoviefordb
            if strmovieidimdb:
                arrmoviecouples["ID_IMDB"] = strmovieidimdb
            else:
                arrmoviecouples["ID_IMDB"] = ""
            if strmovieidwikidata:
                arrmoviecouples["ID_WIKIDATA"] = strmovieidwikidata
            else:
                arrmoviecouples["ID_WIKIDATA"] = ""
            arrmoviecouples["OVERVIEW"] = strmovieoverview
            if strmoviereleasedate:
                if strmoviereleasedate != "":
                    arrmoviecouples["DAT_RELEASE"] = strmoviereleasedate
                    arrmoviecouples["RELEASE_YEAR"] = lngmoviereleaseyear
                    arrmoviecouples["RELEASE_MONTH"] = lngmoviereleasemonth
                    arrmoviecouples["RELEASE_DAY"] = lngmoviereleaseday
            arrmoviecouples["VIDEO"] = intmovievideo
            arrmoviecouples["ID_MOVIE"] = lngmovieid
            arrmoviecouples["POSTER_PATH"] = strmovieposterpath
            arrmoviecouples["HOMEPAGE_URL"] = strmoviehomepage
            if strmovietitle != "":
                arrmoviecouples["TITLE"] = strmovietitle
            else:
                arrmoviecouples["TITLE"] = ""
            arrmoviecouples["ORIGINAL_LANGUAGE"] = strmovieoriginallanguage
            arrmoviecouples["POPULARITY"] = dblmoviepopularity
            arrmoviecouples["BACKDROP_PATH"] = strmoviebackdroppath
            arrmoviecouples["ORIGINAL_TITLE"] = strmovieoriginaltitle
            arrmoviecouples["STATUS"] = strmoviestatus
            arrmoviecouples["GENRES"] = strmoviegenres
            arrmoviecouples["ID_COLLECTION"] = lngcollectionid
            arrmoviecouples["ADULT"] = intmovieadult
            arrmoviecouples["BUDGET"] = dblmoviebudget
            arrmoviecouples["RUNTIME"] = lngmovieruntime
            arrmoviecouples["IS_SHORT_FILM"] = intmovieisshortfilm
            arrmoviecouples["REVENUE"] = dblmovierevenue
            arrmoviecouples["TAGLINE"] = strmovietagline
            arrmoviecouples["VOTE_AVERAGE"] = dblmovievoteaverage
            arrmoviecouples["VOTE_COUNT"] = lngmovievotecount
            arrmoviecouples["COUNTRIES"] = strmoviecountries
            arrmoviecouples["SPOKEN_LANGUAGES"] = strmoviespokenlanguages
            arrmoviecouples["IS_DOCUMENTARY"] = intisdocumentary
            arrmoviecouples["IS_MOVIE"] = intismovie
            
            strpersoncredits = ""
            arrcredittype = {1: 'cast', 2:'crew'}
            for intcredittype,strmoviecreditcredittype in arrcredittype.items():
                if intcredittype == 1:
                    strtitle = "Cast"
                else:
                    strtitle = "Crew"
                # print(strtitle)
                if 'credits' in data:
                    if strmoviecreditcredittype in data['credits']:
                        arrmoviecredits = data['credits'][strmoviecreditcredittype]
                        lngdisplayorder = 0
                        arrcredits = {}
                        if arrmoviecredits:
                            # print(arrmoviecredits)
                            for onecontent in arrmoviecredits:
                                lngdisplayorder += 1
                                strpersoncreditname = onecontent['name']
                                strpersoncreditcreditid = onecontent['credit_id']
                                lngpersonid = onecontent['id']
                                if lngpersonid in arrcredits:
                                    lngcreditdisplayorder = arrcredits[lngpersonid]
                                else:
                                    lngcreditdisplayorder = lngdisplayorder
                                    arrcredits[lngpersonid] = lngdisplayorder
                                
                                if strpersoncredits != "":
                                    strpersoncredits += ","
                                strpersoncredits += "'" + strpersoncreditcreditid + "'"
                                
                                arrpersonmoviecouples = {}
                                if intcredittype == 1:
                                    strpersoncreditcharacter = onecontent['character']
                                    strpersoncreditdepartment = ""
                                    strpersoncreditjob = ""
                                    # print(f"{strpersoncreditname}: {strpersoncreditcharacter}")
                                else:
                                    strpersoncreditcharacter = ""
                                    strpersoncreditdepartment = onecontent['department']
                                    strpersoncreditjob = onecontent['job']
                                    # print(f"{strpersoncreditname}: {strpersoncreditdepartment} {strpersoncreditjob}")
                                arrpersonmoviecouples["ID_PERSON"] = lngpersonid
                                arrpersonmoviecouples["ID_MOVIE"] = lngmovieid
                                arrpersonmoviecouples["ID_CREDIT"] = strpersoncreditcreditid
                                arrpersonmoviecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                arrpersonmoviecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                arrpersonmoviecouples["CREW_JOB"] = strpersoncreditjob
                                arrpersonmoviecouples["CREDIT_TYPE"] = strmoviecreditcredittype
                                arrpersonmoviecouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                                # print(arrpersonmoviecouples)
                                
                                strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
                                strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                f_sqlupdatearray(strsqltablename,arrpersonmoviecouples,strsqlupdatecondition,1)
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            strapimoviefordb = json.dumps(data, ensure_ascii=False)
            # strapimoviefordb = f_tmdbjsonremovekeys(strapimoviefordb,', "original_name":',', "popularity":',', "popularity":')
            # strapimoviefordb = f_tmdbjsonremovekeys(strapimoviefordb,', "name":',', "popularity":',', "popularity":')
            # strapimoviefordb = f_tmdbjsonremovekeys(strapimoviefordb,', "original_name":',', "popularity":',', "popularity":')
            #arrmoviecouples["API_RESULT"] = strapimoviefordb
            #arrmoviecouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_MOVIE"
            strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
            f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
            
            # Now delete credits that are not for this movie
            if strpersoncredits == "":
                strpersoncredits = "'0'"
            strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_MOVIE WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_CREDIT NOT IN (" + strpersoncredits + ")"
            # print(f"{strsqldelete}")
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()

def f_tmdbmovielangtosql(lngmovieid, strlang):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    
    if lngmovieid > 0:
        strtmdbapimovieurl = "3/movie/" + str(lngmovieid) + "?language=" + strlang
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimovieurl
        # print(strtmdbapifullurl)
        # Add retry logic with error handling
        intencore = True
        intattemptsremaining = 5
        intsuccess = False
        while intencore:
            try:
                response = requests.get(strtmdbapifullurl, headers=headers)
                intencore = False
                intsuccess = True
            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
            except requests.exceptions.ConnectionError as conn_err:
                print(f'Connection error occurred: {conn_err}')  # Handle connection errors
            except requests.exceptions.Timeout as timeout_err:
                print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
            except requests.exceptions.RequestException as req_err:
                print(f'Request error occurred: {req_err}')  # Handle other request-related errors
            except Exception as err:
                print(f'An error occurred: {err}')  # Handle any other exceptions
            if intencore:
                intattemptsremaining = intattemptsremaining - 1
                if intattemptsremaining >= 0:
                    time.sleep(1)  # Wait for 1 second before next request
                else:
                    intencore = False
        
        if not intsuccess:
            print(f"f_tmdbmovietosql({lngmovieid}) failed!")
            return
        
        data = response.json()
        lngmoviestatuscode = 0
        if 'status_code' in data:
            lngmoviestatuscode = data['status_code']
        if lngmoviestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strmovieoverview = ""
            if 'overview' in data:
                strmovieoverview = data['overview']
            strmovieposterpath = ""
            if 'poster_path' in data:
                strmovieposterpath = data['poster_path']
            strmovietitle = ""
            if 'title' in data:
                strmovietitle = data['title']
            strmoviebackdroppath = ""
            if 'backdrop_path' in data:
                strmoviebackdroppath = data['backdrop_path']
            strmovietagline = ""
            if 'tagline' in data:
                strmovietagline = data['tagline']
            
            if strmovietitle:
                if len(strmovietitle) > 250:
                    # If title is too long, we chop it
                    strmovietitle = strmovietitle[:250]
            
            if strmovietitle:
                if len(strmovietitle) > 250:
                    # If title is too long, we chop it
                    strmovietitle = strmovietitle[:250]
            
            arrmoviecouples = {}
            #arrmoviecouples["API_URL"] = strtmdbapimovieurl
            arrmoviecouples["OVERVIEW"] = strmovieoverview
            arrmoviecouples["ID_MOVIE"] = lngmovieid
            arrmoviecouples["LANG"] = strlang
            arrmoviecouples["POSTER_PATH"] = strmovieposterpath
            if strmovietitle != "":
                arrmoviecouples["TITLE"] = strmovietitle
            arrmoviecouples["BACKDROP_PATH"] = strmoviebackdroppath
            arrmoviecouples["TAGLINE"] = strmovietagline
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            #strapimoviefordb = json.dumps(data, ensure_ascii=False)
            #arrmoviecouples["API_RESULT"] = strapimoviefordb
            #arrmoviecouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_MOVIE_LANG"
            strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND LANG = '{strlang}'"
            f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)

def f_tmdbmoviekeywordstosql(lngmovieid):
    global strtmdbapidomainurl
    global headers
    
    if lngmovieid > 0:
        strtmdbapimoviekeywordsurl = "3/movie/" + str(lngmovieid) + "/keywords"
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimoviekeywordsurl
        # print(strtmdbapifullurl)
        intencore = True
        intattemptsremaining = 5
        intsuccess = False
        while intencore:
            try:
                response = requests.get(strtmdbapifullurl, headers=headers)
                intencore = False
                intsuccess = True
            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
            except requests.exceptions.ConnectionError as conn_err:
                print(f'Connection error occurred: {conn_err}')  # Handle connection errors
            except requests.exceptions.Timeout as timeout_err:
                print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
            except requests.exceptions.RequestException as req_err:
                print(f'Request error occurred: {req_err}')  # Handle other request-related errors
            except Exception as err:
                print(f'An error occurred: {err}')  # Handle any other exceptions
            if intencore:
                intattemptsremaining = intattemptsremaining - 1
                if intattemptsremaining >= 0:
                    time.sleep(1)  # Wait for 1 second before next request
                else:
                    intencore = False
        if not intsuccess:
            print(f"f_tmdbmoviekeywordstosql({lngkeywordid}) failed!")
        else:
            response = requests.get(strtmdbapifullurl, headers=headers)
            strapimoviekeywords = response.text
            jsonmoviekeywords = response.json()
            lngmoviekeywordsstatuscode = 0
            if 'status_code' in jsonmoviekeywords:
                lngmoviekeywordsstatuscode = jsonmoviekeywords['status_code']
            if lngmoviekeywordsstatuscode <= 1:
                # API request result is not an error
                lngkeyworddisplayorder = 0
                if jsonmoviekeywords['keywords']:
                    # Array is not empty
                    for onecontent in jsonmoviekeywords['keywords']:
                        strkeywordname = onecontent['name']
                        lngkeywordid = onecontent['id']
                        lngkeyworddisplayorder = lngkeyworddisplayorder + 1
                        arrmoviekeywordcouples = {}
                        arrmoviekeywordcouples["ID_MOVIE"] = lngmovieid
                        arrmoviekeywordcouples["ID_KEYWORD"] = lngkeywordid
                        arrmoviekeywordcouples["DISPLAY_ORDER"] = lngkeyworddisplayorder
                        # print(arrmoviekeywordcouples)
                        
                        strsqltablename = "T_WC_TMDB_MOVIE_KEYWORD"
                        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_KEYWORD = {lngkeywordid}"
                        f_sqlupdatearray(strsqltablename,arrmoviekeywordcouples,strsqlupdatecondition,1)

def f_tmdbmovieexist(lngmovieid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    # By default, we assume that this movie exists
    intresult = True
    if lngmovieid > 0:
        strtmdbapimovieurl = "3/movie/" + str(lngmovieid) + "?language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimovieurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        lngmoviestatuscode = 0
        if 'status_code' in data:
            lngmoviestatuscode = data['status_code']
        if lngmoviestatuscode == 34:
            # API request result is an error:
            # The resource you requested could not be found
            intresult = False
    return intresult

def f_tmdbmoviedelete(lngmovieid):
    global connectioncp
    
    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LANG"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LANG_META"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LIST"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_GENRE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_KEYWORD"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LEMME"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_IMAGE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetcreditscompleted(lngmovieid):
    global paris_tz
    global connectioncp
    
    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetkeywordscompleted(lngmovieid):
    global paris_tz
    global connectioncp
    
    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimkeywordscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_KEYWORDS_COMPLETED = '{strtimkeywordscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetwikidatacompleted(lngmovieid):
    global paris_tz
    global connectioncp
    
    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetwikipediacompleted(lngmovieid):
    global paris_tz
    global connectioncp
    
    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIPEDIA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmovieimagestosql(lngmovieid):
    f_tmdbcontentimagesstosql(lngmovieid, "movie", "T_WC_TMDB_MOVIE", "T_WC_TMDB_MOVIE_IMAGE", "ID_MOVIE")
    
def f_tmdbmovietosqleverything(lngmovieid):
    f_tmdbmovietosql(lngmovieid)
    f_tmdbmovielangtosql(lngmovieid,'fr')
    f_tmdbmoviesetcreditscompleted(lngmovieid)
    f_tmdbmoviekeywordstosql(lngmovieid)
    f_tmdbmoviesetkeywordscompleted(lngmovieid)
    f_tmdbmovieimagestosql(lngmovieid)

# https://developer.themoviedb.org/reference/tv-series-details

def f_tmdbserietosql(lngserieid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    global strsqlns
    
    if lngserieid > 0:
        # New TMDb API call with append_to_response
        strtmdbapiserieurl = "3/tv/" + str(lngserieid) + "?append_to_response=credits,alternative_titles,external_ids&language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiserieurl
        #print(strtmdbapifullurl)
        
        # Add retry logic with error handling
        intencore = True
        intattemptsremaining = 5
        intsuccess = False
        while intencore:
            try:
                response = requests.get(strtmdbapifullurl, headers=headers)
                intencore = False
                intsuccess = True
            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
            except requests.exceptions.ConnectionError as conn_err:
                print(f'Connection error occurred: {conn_err}')  # Handle connection errors
            except requests.exceptions.Timeout as timeout_err:
                print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
            except requests.exceptions.RequestException as req_err:
                print(f'Request error occurred: {req_err}')  # Handle other request-related errors
            except Exception as err:
                print(f'An error occurred: {err}')  # Handle any other exceptions
            if intencore:
                intattemptsremaining = intattemptsremaining - 1
                if intattemptsremaining >= 0:
                    time.sleep(1)  # Wait for 1 second before next request
                else:
                    intencore = False
        
        if not intsuccess:
            print(f"f_tmdbserietosql({lngserieid}) failed!")
            return
        
        # Parse the JSON data into a dictionary
        data = response.json()
        
        lngseriestatuscode = 0
        if 'status_code' in data:
            lngseriestatuscode = data['status_code']
        if lngseriestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strserieidimdb = ""
            if 'external_ids' in data:
                if 'imdb_id' in data['external_ids']:
                    strserieidimdb = data['external_ids']['imdb_id']
            
            strserieoverview = ""
            if 'overview' in data:
                strserieoverview = data['overview']
            
            # Process first air date with proper validation
            strseriefirstairdate = ""
            lngseriefirstairyear = 0
            lngseriefirstairmonth = 0
            lngseriefirstairday = 0
            if 'first_air_date' in data:
                strseriefirstairdate = data['first_air_date']
                if strseriefirstairdate and re.match(strdatepattern, strseriefirstairdate):
                    lngseriefirstairyear, lngseriefirstairmonth, lngseriefirstairday = map(int, strseriefirstairdate.split('-'))
                else:
                    strseriefirstairdate = None
            
            # Process last air date with proper validation
            strserielastairdate = ""
            lngserielastairyear = 0
            lngserielastairmonth = 0
            lngserielastairday = 0
            if 'last_air_date' in data:
                strserielastairdate = data['last_air_date']
                if strserielastairdate and re.match(strdatepattern, strserielastairdate):
                    lngserielastairyear, lngserielastairmonth, lngserielastairday = map(int, strserielastairdate.split('-'))
                else:
                    strserielastairdate = None
            
            strserieposterpath = ""
            if 'poster_path' in data:
                strserieposterpath = data['poster_path']
            
            strseriehomepage = ""
            if 'homepage' in data:
                strseriehomepage = data['homepage']
            
            strserietitle = ""
            if 'name' in data:
                strserietitle = data['name']
            
            strserieoriginallanguage = ""
            if 'original_language' in data:
                strserieoriginallanguage = data['original_language']
            
            dblseriepopularity = 0
            if 'popularity' in data:
                dblseriepopularity = data['popularity']
            
            strseriebackdroppath = ""
            if 'backdrop_path' in data:
                strseriebackdroppath = data['backdrop_path']
            
            intserieadult = 0
            if 'adult' in data:
                booserieadult = data['adult']
                if booserieadult:
                    intserieadult = 1
            
            strserieoriginaltitle = ""
            if 'original_name' in data:
                strserieoriginaltitle = data['original_name']
            
            strseriestatus = ""
            if 'status' in data:
                strseriestatus = data['status']
            
            strseriegenres = ""
            if 'genres' in data:
                arrseriegenres = data['genres']
                strseriegenres = "|"
                arrgenrenames = [genre['name'] for genre in arrseriegenres]
                for strvalue in arrgenrenames:
                    strseriegenres += strvalue + "|"
            
            strserietagline = ""
            if 'tagline' in data:
                strserietagline = data['tagline']
            
            dblserievoteaverage = 0
            if 'vote_average' in data:
                dblserievoteaverage = data['vote_average']
            
            lngserievotecount = 0
            if 'vote_count' in data:
                lngserievotecount = data['vote_count']
            
            strserieidwikidata = ""
            if 'external_ids' in data:
                if 'wikidata_id' in data['external_ids']:
                    strserieidwikidata = data['external_ids']['wikidata_id']
            
            # Add TV-specific fields
            lngnumberofepisodes = 0
            if 'number_of_episodes' in data:
                lngnumberofepisodes = data['number_of_episodes']
            
            lngnumberofseasons = 0
            if 'number_of_seasons' in data:
                lngnumberofseasons = data['number_of_seasons']
            
            strserietype = ""
            if 'type' in data:
                strserietype = data['type']
            
            # Process production countries
            strseriecountries = ""
            strcountryidlist = ""
            lngcountrydisplayorder = 0
            if 'production_countries' in data:
                arrseriecountries = data['production_countries']
                strseriecountries = "|"
                arrcountrynames = [country['iso_3166_1'] for country in arrseriecountries]
                for strvalue in arrcountrynames:
                    strseriecountries += strvalue + "|"
                    lngcountrydisplayorder = lngcountrydisplayorder + 1
                    if strcountryidlist != "":
                        strcountryidlist += ","
                    strcountryidlist += "'" + strvalue + "'"
                    arrseriecountrycouples = {}
                    arrseriecountrycouples["ID_SERIE"] = lngserieid
                    arrseriecountrycouples["COUNTRY_CODE"] = strvalue
                    arrseriecountrycouples["DISPLAY_ORDER"] = lngcountrydisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_PRODUCTION_COUNTRY"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND COUNTRY_CODE = '{strvalue}'"
                    f_sqlupdatearray(strsqltablename, arrseriecountrycouples, strsqlupdatecondition, 1)
            
            if strcountryidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_PRODUCTION_COUNTRY WHERE ID_SERIE = " + str(lngserieid) + " AND COUNTRY_CODE NOT IN (" + strcountryidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process spoken languages
            strseriespokenlanguages = ""
            strspokenlanguageidlist = ""
            lngspokenlanguagedisplayorder = 0
            if 'spoken_languages' in data:
                arrseriespokenlanguages = data['spoken_languages']
                strseriespokenlanguages = "|"
                arrspokenlanguagenames = [spokenlanguage['iso_639_1'] for spokenlanguage in arrseriespokenlanguages]
                for strvalue in arrspokenlanguagenames:
                    strseriespokenlanguages += strvalue + "|"
                    lngspokenlanguagedisplayorder = lngspokenlanguagedisplayorder + 1
                    if strspokenlanguageidlist != "":
                        strspokenlanguageidlist += ","
                    strspokenlanguageidlist += "'" + strvalue + "'"
                    arrseriespokenlanguagecouples = {}
                    arrseriespokenlanguagecouples["ID_SERIE"] = lngserieid
                    arrseriespokenlanguagecouples["SPOKEN_LANGUAGE"] = strvalue
                    arrseriespokenlanguagecouples["DISPLAY_ORDER"] = lngspokenlanguagedisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_SPOKEN_LANGUAGE"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND SPOKEN_LANGUAGE = '{strvalue}'"
                    f_sqlupdatearray(strsqltablename, arrseriespokenlanguagecouples, strsqlupdatecondition, 1)
            
            if strspokenlanguageidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_SPOKEN_LANGUAGE WHERE ID_SERIE = " + str(lngserieid) + " AND SPOKEN_LANGUAGE NOT IN (" + strspokenlanguageidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process networks (TV-specific)
            strnetworkidlist = ""
            lngnetworkdisplayorder = 0
            if 'networks' in data and data['networks']:
                for onecontent in data['networks']:
                    lngnetworkid = onecontent['id']
                    lngnetworkdisplayorder = lngnetworkdisplayorder + 1
                    if strnetworkidlist != "":
                        strnetworkidlist += ","
                    strnetworkidlist += str(lngnetworkid)
                    arrserieneworkcouples = {}
                    arrserieneworkcouples["ID_SERIE"] = lngserieid
                    arrserieneworkcouples["ID_NETWORK"] = lngnetworkid
                    arrserieneworkcouples["DISPLAY_ORDER"] = lngnetworkdisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_NETWORK"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_NETWORK = {lngnetworkid}"
                    f_sqlupdatearray(strsqltablename, arrserieneworkcouples, strsqlupdatecondition, 1)
            
            if strnetworkidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_NETWORK WHERE ID_SERIE = " + str(lngserieid) + " AND ID_NETWORK NOT IN (" + strnetworkidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process production companies
            strcompanyidlist = ""
            lngcompanydisplayorder = 0
            if 'production_companies' in data and data['production_companies']:
                for onecontent in data['production_companies']:
                    lngcompanyid = onecontent['id']
                    lngcompanydisplayorder = lngcompanydisplayorder + 1
                    if strcompanyidlist != "":
                        strcompanyidlist += ","
                    strcompanyidlist += str(lngcompanyid)
                    arrseriecompanycouples = {}
                    arrseriecompanycouples["ID_SERIE"] = lngserieid
                    arrseriecompanycouples["ID_COMPANY"] = lngcompanyid
                    arrseriecompanycouples["DISPLAY_ORDER"] = lngcompanydisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_COMPANY"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_COMPANY = {lngcompanyid}"
                    f_sqlupdatearray(strsqltablename, arrseriecompanycouples, strsqlupdatecondition, 1)
            
            if strcompanyidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_COMPANY WHERE ID_SERIE = " + str(lngserieid) + " AND ID_COMPANY NOT IN (" + strcompanyidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process genres
            strgenreidlist = ""
            lnggenredisplayorder = 0
            if 'genres' in data and data['genres']:
                for onecontent in data['genres']:
                    lnggenreid = onecontent['id']
                    lnggenredisplayorder = lnggenredisplayorder + 1
                    if strgenreidlist != "":
                        strgenreidlist += ","
                    strgenreidlist += str(lnggenreid)
                    arrseriegenrecouples = {}
                    arrseriegenrecouples["ID_SERIE"] = lngserieid
                    arrseriegenrecouples["ID_GENRE"] = lnggenreid
                    arrseriegenrecouples["DISPLAY_ORDER"] = lnggenredisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_GENRE"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_GENRE = {lnggenreid}"
                    f_sqlupdatearray(strsqltablename, arrseriegenrecouples, strsqlupdatecondition, 1)
            
            if strgenreidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_GENRE WHERE ID_SERIE = " + str(lngserieid) + " AND ID_GENRE NOT IN (" + strgenreidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Field length validation
            if strserietitle:
                if len(strserietitle) > 250:
                    strserietitle = strserietitle[:250]
            
            if strserieoriginaltitle:
                if len(strserieoriginaltitle) > 250:
                    strserieoriginaltitle = strserieoriginaltitle[:250]
            
            if strseriehomepage:
                if len(strseriehomepage) > 500:
                    strseriehomepage = strseriehomepage[:500]
            
            # Prepare main record data
            arrseriecouples = {}
            #arrseriecouples["API_URL"] = strtmdbapiserieurl
            arrseriecouples["ID_SERIE"] = lngserieid
            
            if strserieidimdb:
                arrseriecouples["ID_IMDB"] = strserieidimdb
            else:
                arrseriecouples["ID_IMDB"] = ""
            
            if strserieidwikidata:
                arrseriecouples["ID_WIKIDATA"] = strserieidwikidata
            else:
                arrseriecouples["ID_WIKIDATA"] = ""
            
            arrseriecouples["OVERVIEW"] = strserieoverview
            
            # Date fields
            if strseriefirstairdate:
                arrseriecouples["DAT_FIRST_AIR"] = strseriefirstairdate
                arrseriecouples["FIRST_AIR_YEAR"] = lngseriefirstairyear
                arrseriecouples["FIRST_AIR_MONTH"] = lngseriefirstairmonth
                arrseriecouples["FIRST_AIR_DAY"] = lngseriefirstairday
            
            if strserielastairdate:
                arrseriecouples["DAT_LAST_AIR"] = strserielastairdate
                arrseriecouples["LAST_AIR_YEAR"] = lngserielastairyear
                arrseriecouples["LAST_AIR_MONTH"] = lngserielastairmonth
                arrseriecouples["LAST_AIR_DAY"] = lngserielastairday
            
            arrseriecouples["POSTER_PATH"] = strserieposterpath
            arrseriecouples["HOMEPAGE_URL"] = strseriehomepage
            
            if strserietitle != "":
                arrseriecouples["TITLE"] = strserietitle
            else:
                arrseriecouples["TITLE"] = ""
            
            arrseriecouples["ORIGINAL_LANGUAGE"] = strserieoriginallanguage
            arrseriecouples["POPULARITY"] = dblseriepopularity
            arrseriecouples["BACKDROP_PATH"] = strseriebackdroppath
            arrseriecouples["ORIGINAL_TITLE"] = strserieoriginaltitle
            arrseriecouples["STATUS"] = strseriestatus
            arrseriecouples["GENRES"] = strseriegenres
            arrseriecouples["ADULT"] = intserieadult
            arrseriecouples["TAGLINE"] = strserietagline
            arrseriecouples["VOTE_AVERAGE"] = dblserievoteaverage
            arrseriecouples["VOTE_COUNT"] = lngserievotecount
            arrseriecouples["COUNTRIES"] = strseriecountries
            arrseriecouples["SPOKEN_LANGUAGES"] = strseriespokenlanguages
            
            # TV-specific fields
            arrseriecouples["NUMBER_OF_EPISODES"] = lngnumberofepisodes
            arrseriecouples["NUMBER_OF_SEASONS"] = lngnumberofseasons
            arrseriecouples["SERIE_TYPE"] = strserietype
            
            # Store created_by array for later use with credits
            arrcreatedby = []
            if 'created_by' in data and data['created_by']:
                arrcreatedby = data['created_by']  # Store the created_by array for later use with credits
            
            # Process credits
            strpersoncredits = ""
            lngcreditdisplayorder = 0
            arrcredittype = {1: 'cast', 2: 'crew'}
            for intcredittype, strseriecreditcredittype in arrcredittype.items():
                if intcredittype == 1:
                    strtitle = "Cast"
                else:
                    strtitle = "Crew"
                
                if 'credits' in data:
                    if strseriecreditcredittype in data['credits']:
                        arrseriecredits = data['credits'][strseriecreditcredittype]
                        lngdisplayorder = 0
                        arrcredits = {}
                        if arrseriecredits:
                            for onecontent in arrseriecredits:
                                lngdisplayorder += 1
                                strpersoncreditname = onecontent['name']
                                strpersoncreditcreditid = onecontent['credit_id']
                                lngpersonid = onecontent['id']
                                if lngpersonid in arrcredits:
                                    lngcreditdisplayorder = arrcredits[lngpersonid]
                                else:
                                    lngcreditdisplayorder = lngdisplayorder
                                    arrcredits[lngpersonid] = lngdisplayorder
                                
                                if strpersoncredits != "":
                                    strpersoncredits += ","
                                strpersoncredits += "'" + strpersoncreditcreditid + "'"
                                
                                arrpersonseriecouples = {}
                                if intcredittype == 1:
                                    strpersoncreditcharacter = onecontent['character']
                                    strpersoncreditdepartment = ""
                                    strpersoncreditjob = ""
                                else:
                                    strpersoncreditcharacter = ""
                                    strpersoncreditdepartment = onecontent['department']
                                    strpersoncreditjob = onecontent['job']
                                    
                                    # Check if this person is in the created_by array
                                    is_creator = False
                                    for creator in arrcreatedby:
                                        if creator['id'] == lngpersonid:
                                            is_creator = True
                                            strcreatorcreditid = creator['credit_id']
                                            break
                                    
                                    # If person is a creator, add a special record for them
                                    if is_creator:
                                        # Generate a unique credit ID for the creator entry
                                        if strpersoncredits != "":
                                            strpersoncredits += ","
                                        strpersoncredits += "'" + strcreatorcreditid + "'"
                                        
                                        arrcreatorcouples = {}
                                        arrcreatorcouples["ID_PERSON"] = lngpersonid
                                        arrcreatorcouples["ID_SERIE"] = lngserieid
                                        arrcreatorcouples["ID_CREDIT"] = strcreatorcreditid
                                        arrcreatorcouples["CAST_CHARACTER"] = ""
                                        arrcreatorcouples["CREW_DEPARTMENT"] = "Creator"
                                        arrcreatorcouples["CREW_JOB"] = "Creator"
                                        arrcreatorcouples["CREDIT_TYPE"] = "crew"
                                        arrcreatorcouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                                        
                                        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                                        strsqlupdatecondition = f"ID_CREDIT = '{strcreatorcreditid}'"
                                        f_sqlupdatearray(strsqltablename, arrcreatorcouples, strsqlupdatecondition, 1)
                                        lngcreditdisplayorder += 1
                                
                                arrpersonseriecouples["ID_PERSON"] = lngpersonid
                                arrpersonseriecouples["ID_SERIE"] = lngserieid
                                arrpersonseriecouples["ID_CREDIT"] = strpersoncreditcreditid
                                arrpersonseriecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                arrpersonseriecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                arrpersonseriecouples["CREW_JOB"] = strpersoncreditjob
                                arrpersonseriecouples["CREDIT_TYPE"] = strseriecreditcredittype
                                arrpersonseriecouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                                
                                strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                                strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                f_sqlupdatearray(strsqltablename, arrpersonseriecouples, strsqlupdatecondition, 1)
            
            # Encode and store API result
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            
            #strapiseriefordb = json.dumps(data, ensure_ascii=False)
            #arrseriecouples["API_RESULT"] = strapiseriefordb
            #arrseriecouples["CRAWLER_VERSION"] = 3
            
            # Update main serie record
            strsqltablename = "T_WC_TMDB_SERIE"
            strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
            f_sqlupdatearray(strsqltablename, arrseriecouples, strsqlupdatecondition, 1)
            
            # Process any creators that weren't found in the crew credits
            if arrcreatedby:
                # Keep track of which creators have been processed
                processed_creator_ids = set()
                
                # Check which creators were already processed during credit processing
                for intcredittype, strseriecreditcredittype in arrcredittype.items():
                    if strseriecreditcredittype in data['credits']:
                        for onecontent in data['credits'][strseriecreditcredittype]:
                            lngpersonid = onecontent['id']
                            for creator in arrcreatedby:
                                if creator['id'] == lngpersonid:
                                    processed_creator_ids.add(lngpersonid)
                
                # Add any creators that weren't processed
                for creator in arrcreatedby:
                    lngpersonid = creator['id']
                    if lngpersonid not in processed_creator_ids:
                        lngcreditdisplayorder += 1
                        strcreatorcreditid = creator['credit_id']
                        
                        if strpersoncredits != "":
                            strpersoncredits += ","
                        strpersoncredits += "'" + strcreatorcreditid + "'"
                        
                        arrcreatorcouples = {}
                        arrcreatorcouples["ID_PERSON"] = lngpersonid
                        arrcreatorcouples["ID_SERIE"] = lngserieid
                        arrcreatorcouples["ID_CREDIT"] = strcreatorcreditid
                        arrcreatorcouples["CAST_CHARACTER"] = ""
                        arrcreatorcouples["CREW_DEPARTMENT"] = "Creator"
                        arrcreatorcouples["CREW_JOB"] = "Creator"
                        arrcreatorcouples["CREDIT_TYPE"] = "crew"
                        arrcreatorcouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                        
                        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                        strsqlupdatecondition = f"ID_CREDIT = '{strcreatorcreditid}'"
                        f_sqlupdatearray(strsqltablename, arrcreatorcouples, strsqlupdatecondition, 1)
                        
            
            # Clean up obsolete credits
            if strpersoncredits == "":
                strpersoncredits = "'0'"
            strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_SERIE WHERE ID_SERIE = " + str(lngserieid) + " AND ID_CREDIT NOT IN (" + strpersoncredits + ")"
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()

def f_tmdbserielangtosql(lngserieid, strlang):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    
    if lngserieid > 0:
        # New TMDb API call with append_to_response since 2024-05-24 10:00
        strtmdbapiserieurl = "3/tv/" + str(lngserieid) + "?language=" + strlang
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiserieurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        lngseriestatuscode = 0
        if 'status_code' in data:
            lngseriestatuscode = data['status_code']
        if lngseriestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strserieoverview = ""
            if 'overview' in data:
                strserieoverview = data['overview']
            strserieposterpath = ""
            if 'poster_path' in data:
                strserieposterpath = data['poster_path']
            strserietitle = ""
            if 'name' in data:
                strserietitle = data['name']
            strseriebackdroppath = ""
            if 'backdrop_path' in data:
                strseriebackdroppath = data['backdrop_path']
            strserietagline = ""
            if 'tagline' in data:
                strserietagline = data['tagline']
            
            if strserietitle:
                if len(strserietitle) > 250:
                    # If title is too long, we chop it
                    strserietitle = strserietitle[:250]
            
            if strserietitle:
                if len(strserietitle) > 250:
                    # If title is too long, we chop it
                    strserietitle = strserietitle[:250]
            
            arrseriecouples = {}
            #arrseriecouples["API_URL"] = strtmdbapiserieurl
            arrseriecouples["OVERVIEW"] = strserieoverview
            arrseriecouples["ID_SERIE"] = lngserieid
            arrseriecouples["LANG"] = strlang
            arrseriecouples["POSTER_PATH"] = strserieposterpath
            if strserietitle != "":
                arrseriecouples["TITLE"] = strserietitle
            arrseriecouples["BACKDROP_PATH"] = strseriebackdroppath
            arrseriecouples["TAGLINE"] = strserietagline
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            #strapiseriefordb = json.dumps(data, ensure_ascii=False)
            #arrseriecouples["API_RESULT"] = strapiseriefordb
            #arrseriecouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_SERIE_LANG"
            strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND LANG = '{strlang}'"
            f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)

def f_tmdbseriekeywordstosql(lngserieid):
    global strtmdbapidomainurl
    global headers
    
    if lngserieid > 0:
        strtmdbapiseriekeywordsurl = "3/tv/" + str(lngserieid) + "/keywords"
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiseriekeywordsurl
        # print(strtmdbapifullurl)
        intencore = True
        intattemptsremaining = 5
        intsuccess = False
        while intencore:
            try:
                response = requests.get(strtmdbapifullurl, headers=headers)
                intencore = False
                intsuccess = True
            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
            except requests.exceptions.ConnectionError as conn_err:
                print(f'Connection error occurred: {conn_err}')  # Handle connection errors
            except requests.exceptions.Timeout as timeout_err:
                print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
            except requests.exceptions.RequestException as req_err:
                print(f'Request error occurred: {req_err}')  # Handle other request-related errors
            except Exception as err:
                print(f'An error occurred: {err}')  # Handle any other exceptions
            if intencore:
                intattemptsremaining = intattemptsremaining - 1
                if intattemptsremaining >= 0:
                    time.sleep(1)  # Wait for 1 second before next request
                else:
                    intencore = False
        if not intsuccess:
            print(f"f_tmdbseriekeywordstosql({lngkeywordid}) failed!")
        else:
            response = requests.get(strtmdbapifullurl, headers=headers)
            strapiseriekeywords = response.text
            jsonseriekeywords = response.json()
            lngseriekeywordsstatuscode = 0
            if 'status_code' in jsonseriekeywords:
                lngseriekeywordsstatuscode = jsonseriekeywords['status_code']
            if lngseriekeywordsstatuscode <= 1:
                # API request result is not an error
                lngkeyworddisplayorder = 0
                if 'results' in jsonseriekeywords:
                    if jsonseriekeywords['results']:
                        # Array is not empty
                        for onecontent in jsonseriekeywords['results']:
                            strkeywordname = onecontent['name']
                            lngkeywordid = onecontent['id']
                            lngkeyworddisplayorder = lngkeyworddisplayorder + 1
                            arrseriekeywordcouples = {}
                            arrseriekeywordcouples["ID_SERIE"] = lngserieid
                            arrseriekeywordcouples["ID_KEYWORD"] = lngkeywordid
                            arrseriekeywordcouples["DISPLAY_ORDER"] = lngkeyworddisplayorder
                            # print(arrseriekeywordcouples)
                            
                            strsqltablename = "T_WC_TMDB_SERIE_KEYWORD"
                            strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_KEYWORD = {lngkeywordid}"
                            f_sqlupdatearray(strsqltablename,arrseriekeywordcouples,strsqlupdatecondition,1)

def f_tmdbserieexist(lngserieid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    # By default, we assume that this serie exists
    intresult = True
    if lngserieid > 0:
        strtmdbapiserieurl = "3/tv/" + str(lngserieid) + "?language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiserieurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        lngseriestatuscode = 0
        if 'status_code' in data:
            lngseriestatuscode = data['status_code']
        if lngseriestatuscode == 34:
            # API request result is an error:
            # The resource you requested could not be found
            intresult = False
    return intresult

def f_tmdbseriedelete(lngserieid):
    global connectioncp
    
    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_LANG"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        """
        strsqltablename = "T_WC_TMDB_SERIE_LANG_META"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        """
        strsqltablename = "T_WC_TMDB_SERIE_LIST"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_GENRE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_KEYWORD"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        """
        strsqltablename = "T_WC_TMDB_SERIE_LEMME"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        """
        strsqltablename = "T_WC_TMDB_SERIE_IMAGE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetcreditscompleted(lngserieid):
    global paris_tz
    global connectioncp
    
    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetkeywordscompleted(lngserieid):
    global paris_tz
    global connectioncp
    
    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimkeywordscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_KEYWORDS_COMPLETED = '{strtimkeywordscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetwikidatacompleted(lngserieid):
    global paris_tz
    global connectioncp
    
    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetwikipediacompleted(lngserieid):
    global paris_tz
    global connectioncp
    
    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIPEDIA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbserieimagestosql(lngserieid):
    f_tmdbcontentimagesstosql(lngserieid, "tv", "T_WC_TMDB_SERIE", "T_WC_TMDB_SERIE_IMAGE", "ID_SERIE")
    
def f_tmdbserietosqleverything(lngserieid):
    f_tmdbserietosql(lngserieid)
    f_tmdbserielangtosql(lngserieid,'fr')
    f_tmdbseriesetcreditscompleted(lngserieid)
    f_tmdbseriekeywordstosql(lngserieid)
    f_tmdbseriesetkeywordscompleted(lngserieid)
    f_tmdbserieimagestosql(lngserieid)

# https://developer.themoviedb.org/reference/collection-details

def f_tmdbcollectiontosql(lngcollectionid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lngcollectionid > 0:
        strtmdbapicollectionurl = "3/collection/" + str(lngcollectionid) + "?language=" + strlanguagecountry
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapicollectionurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        
        #strapicollectionfordb = json.dumps(data, ensure_ascii=False)
        
        # print(response.json)
        lngcollectionstatuscode = 0
        if 'status_code' in data:
            lngcollectionstatuscode = data['status_code']
        if lngcollectionstatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strcollectionoverview = ""
            if 'overview' in data:
                strcollectionoverview = data['overview']
            strcollectionposterpath = ""
            if 'poster_path' in data:
                strcollectionposterpath = data['poster_path']
            strcollectionname = ""
            if 'name' in data:
                strcollectionname = data['name']
            strcollectionbackdroppath = ""
            if 'backdrop_path' in data:
                strcollectionbackdroppath = data['backdrop_path']
            
            # print(f"{strcollectionname}")
            # print(f"{strcollectionbackdroppath}")
            # print(f"Overview: {strcollectionoverview}")
            
            arrcollectioncouples = {}
            arrcollectioncouples["ID_COLLECTION"] = lngcollectionid
            #arrcollectioncouples["API_URL"] = strtmdbapicollectionurl
            #arrcollectioncouples["CRAWLER_VERSION"] = 1
            #arrcollectioncouples["API_RESULT"] = strapicollectionfordb
            arrcollectioncouples["OVERVIEW"] = strcollectionoverview
            arrcollectioncouples["POSTER_PATH"] = strcollectionposterpath
            if strcollectionname != "":
                arrcollectioncouples["NAME"] = strcollectionname
            arrcollectioncouples["BACKDROP_PATH"] = strcollectionbackdroppath
            
            strsqltablename = "T_WC_TMDB_COLLECTION"
            strsqlupdatecondition = f"ID_COLLECTION = {lngcollectionid}"
            f_sqlupdatearray(strsqltablename,arrcollectioncouples,strsqlupdatecondition,1)

def f_tmdbcollectionlangtosql(lngcollectionid, strlang):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    
    if lngcollectionid > 0:
        # New TMDb API call with append_to_response since 2024-05-24 10:00
        strtmdbapicollectionurl = "3/collection/" + str(lngcollectionid) + "?language=" + strlang
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapicollectionurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        lngcollectionstatuscode = 0
        if 'status_code' in data:
            lngcollectionstatuscode = data['status_code']
        if lngcollectionstatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strcollectionoverview = ""
            if 'overview' in data:
                strcollectionoverview = data['overview']
            strcollectionposterpath = ""
            if 'poster_path' in data:
                strcollectionposterpath = data['poster_path']
            strcollectionname = ""
            if 'name' in data:
                strcollectionname = data['name']
            strcollectionbackdroppath = ""
            if 'backdrop_path' in data:
                strcollectionbackdroppath = data['backdrop_path']
            
            if strcollectionname:
                if len(strcollectionname) > 250:
                    # If title is too long, we chop it
                    strcollectionname = strcollectionname[:250]
            
            arrcollectioncouples = {}
            #arrcollectioncouples["API_URL"] = strtmdbapicollectionurl
            arrcollectioncouples["OVERVIEW"] = strcollectionoverview
            arrcollectioncouples["ID_COLLECTION"] = lngcollectionid
            arrcollectioncouples["LANG"] = strlang
            arrcollectioncouples["POSTER_PATH"] = strcollectionposterpath
            if strcollectionname != "":
                arrcollectioncouples["NAME"] = strcollectionname
            arrcollectioncouples["BACKDROP_PATH"] = strcollectionbackdroppath
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            #strapicollectionfordb = json.dumps(data, ensure_ascii=False)
            #arrcollectioncouples["API_RESULT"] = strapicollectionfordb
            #arrcollectioncouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_COLLECTION_LANG"
            strsqlupdatecondition = f"ID_COLLECTION = {lngcollectionid} AND LANG = '{strlang}'"
            f_sqlupdatearray(strsqltablename,arrcollectioncouples,strsqlupdatecondition,1)

def f_tmdbcollectionsetcreditscompleted(lngcollectionid):
    global paris_tz
    global connectioncp
    
    if lngcollectionid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_COLLECTION"
        strsqlupdatecondition = f"ID_COLLECTION = {lngcollectionid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbcollectionimagestosql(lngcollectionid):
    f_tmdbcontentimagesstosql(lngcollectionid, "collection", "T_WC_TMDB_COLLECTION", "T_WC_TMDB_COLLECTION_IMAGE", "ID_COLLECTION")
    
def f_tmdbcollectiontosqleverything(lngcollectionid):
    f_tmdbcollectiontosql(lngcollectionid)
    f_tmdbcollectionlangtosql(lngcollectionid,'fr')
    f_tmdbcollectionsetcreditscompleted(lngcollectionid)
    f_tmdbcollectionimagestosql(lngcollectionid)

def f_tmdbcompanytosql(lngcompanyid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lngcompanyid > 0:
        strtmdbapicompanyurl = "3/company/" + str(lngcompanyid)
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapicompanyurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        
        #strapicompanyfordb = json.dumps(data, ensure_ascii=False)
        lngcompanystatuscode = 0
        if 'status_code' in data:
            lngcompanystatuscode = data['status_code']
        if lngcompanystatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strcompanydescription = ""
            if 'description' in data:
                strcompanydescription = data['description']
            strcompanylogopath = ""
            if 'logo_path' in data:
                strcompanylogopath = data['logo_path']
            strcompanyname = ""
            if 'name' in data:
                strcompanyname = data['name']
            strcompanyheadquarters = ""
            if 'headquarters' in data:
                strcompanyheadquarters = data['headquarters']
            strcompanyhomepage = ""
            if 'homepage' in data:
                strcompanyhomepage = data['homepage']
            strcompanyorigincountry = ""
            if 'origin_country' in data:
                strcompanyorigincountry = data['origin_country']
            lngcompanyparentid = 0
            if 'parent_company' in data:
                if data['parent_company']:
                    if 'id' in data['parent_company']:
                        lngcompanyparentid = data['parent_company']['id']
            
            if strcompanyhomepage: 
                if len(strcompanyhomepage) > 500:
                    # If homepage URL is too long, we chop it
                    strcompanyhomepage = strcompanyhomepage[:500]
            if strcompanyheadquarters: 
                if len(strcompanyheadquarters) > 200:
                    # If headquarters data is too long, we chop it
                    strcompanyheadquarters = strcompanyheadquarters[:200]
            
            # print(f"{strcompanyname}")
            
            arrcompanycouples = {}
            arrcompanycouples["ID_COMPANY"] = lngcompanyid
            #arrcompanycouples["API_URL"] = strtmdbapicompanyurl
            #arrcompanycouples["CRAWLER_VERSION"] = 1
            #arrcompanycouples["API_RESULT"] = strapicompanyfordb
            if strcompanydescription != "":
                arrcompanycouples["DESCRIPTION"] = strcompanydescription
            if strcompanylogopath != "":
                arrcompanycouples["LOGO_PATH"] = strcompanylogopath
            if strcompanyname != "":
                arrcompanycouples["NAME"] = strcompanyname
            if strcompanyheadquarters != "":
                arrcompanycouples["HEADQUARTERS"] = strcompanyheadquarters
            if strcompanyhomepage != "":
                arrcompanycouples["HOMEPAGE_URL"] = strcompanyhomepage
            if strcompanyorigincountry != "":
                arrcompanycouples["ORIGIN_COUNTRY"] = strcompanyorigincountry
            arrcompanycouples["ID_PARENT"] = lngcompanyparentid
            
            strsqltablename = "T_WC_TMDB_COMPANY"
            strsqlupdatecondition = f"ID_COMPANY = {lngcompanyid}"
            f_sqlupdatearray(strsqltablename,arrcompanycouples,strsqlupdatecondition,1)

def f_tmdbcompanysetcreditscompleted(lngcompanyid):
    global paris_tz
    global connectioncp
    
    if lngcompanyid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_COMPANY"
        strsqlupdatecondition = f"ID_COMPANY = {lngcompanyid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbcompanyimagestosql(lngcompanyid):
    f_tmdbcontentimagesstosql(lngcompanyid, "company", "T_WC_TMDB_COMPANY", "T_WC_TMDB_COMPANY_IMAGE", "ID_COMPANY")
    
def f_tmdbcompanytosqleverything(lngcompanyid):
    f_tmdbcompanytosql(lngcompanyid)
    f_tmdbcompanysetcreditscompleted(lngcompanyid)
    f_tmdbcompanyimagestosql(lngcompanyid)

# https://developer.themoviedb.org/reference/network-details

def f_tmdbnetworktosql(lngnetworkid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lngnetworkid > 0:
        strtmdbapinetworkurl = "3/network/" + str(lngnetworkid)
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapinetworkurl
        # print(strtmdbapifullurl)
        response = requests.get(strtmdbapifullurl, headers=headers)
        data = response.json()
        
        #strapinetworkfordb = json.dumps(data, ensure_ascii=False)
        lngnetworkstatuscode = 0
        if 'status_code' in data:
            lngnetworkstatuscode = data['status_code']
        if lngnetworkstatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strnetworklogopath = ""
            if 'logo_path' in data:
                strnetworklogopath = data['logo_path']
            strnetworkname = ""
            if 'name' in data:
                strnetworkname = data['name']
            strnetworkheadquarters = ""
            if 'headquarters' in data:
                strnetworkheadquarters = data['headquarters']
            strnetworkhomepage = ""
            if 'homepage' in data:
                strnetworkhomepage = data['homepage']
            strnetworkorigincountry = ""
            if 'origin_country' in data:
                strnetworkorigincountry = data['origin_country']
            lngnetworkparentid = 0
            
            if strnetworkhomepage: 
                if len(strnetworkhomepage) > 500:
                    # If homepage URL is too long, we chop it
                    strnetworkhomepage = strnetworkhomepage[:500]
            
            # print(f"{strnetworkname}")
            
            arrnetworkcouples = {}
            arrnetworkcouples["ID_NETWORK"] = lngnetworkid
            #arrnetworkcouples["API_URL"] = strtmdbapinetworkurl
            #arrnetworkcouples["CRAWLER_VERSION"] = 1
            #arrnetworkcouples["API_RESULT"] = strapinetworkfordb
            if strnetworklogopath != "":
                arrnetworkcouples["LOGO_PATH"] = strnetworklogopath
            if strnetworkname != "":
                arrnetworkcouples["NAME"] = strnetworkname
            if strnetworkhomepage != "":
                arrnetworkcouples["HOMEPAGE_URL"] = strnetworkhomepage
            if strnetworkorigincountry != "":
                arrnetworkcouples["ORIGIN_COUNTRY"] = strnetworkorigincountry
            if strnetworkheadquarters != "":
                arrnetworkcouples["HEADQUARTERS"] = strnetworkheadquarters
            
            strsqltablename = "T_WC_TMDB_NETWORK"
            strsqlupdatecondition = f"ID_NETWORK = {lngnetworkid}"
            f_sqlupdatearray(strsqltablename,arrnetworkcouples,strsqlupdatecondition,1)

def f_tmdbnetworksetcreditscompleted(lngnetworkid):
    global paris_tz
    global connectioncp
    
    if lngnetworkid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_NETWORK"
        strsqlupdatecondition = f"ID_NETWORK = {lngnetworkid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbnetworkimagestosql(lngnetworkid):
    f_tmdbcontentimagesstosql(lngnetworkid, "network", "T_WC_TMDB_NETWORK", "T_WC_TMDB_NETWORK_IMAGE", "ID_NETWORK")
    
def f_tmdbnetworktosqleverything(lngnetworkid):
    f_tmdbnetworktosql(lngnetworkid)
    f_tmdbnetworksetcreditscompleted(lngnetworkid)
    f_tmdbnetworkimagestosql(lngnetworkid)

def f_tmdbkeywordtosql(lngkeywordid, strkeywordname):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lngkeywordid > 0:
        arrkeywordcouples = {}
        arrkeywordcouples["ID_KEYWORD"] = lngkeywordid
        if strkeywordname != "":
            arrkeywordcouples["NAME"] = strkeywordname
        
        strsqltablename = "T_WC_TMDB_KEYWORD"
        strsqlupdatecondition = f"ID_KEYWORD = {lngkeywordid}"
        f_sqlupdatearray(strsqltablename,arrkeywordcouples,strsqlupdatecondition,1)

def f_tmdbkeywordsetcreditscompleted(lngkeywordid):
    global paris_tz
    global connectioncp
    
    if lngkeywordid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_KEYWORD"
        strsqlupdatecondition = f"ID_KEYWORD = {lngkeywordid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbkeywordtosqleverything(lngkeywordid, strkeywordname):
    f_tmdbkeywordtosql(lngkeywordid, strkeywordname)
    f_tmdbkeywordsetcreditscompleted(lngkeywordid)

# https://developer.themoviedb.org/reference/list-details

def f_tmdblisttosql(lnglistid):
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lnglistid > 0:
        lngpage = 1
        lngdisplayorder = 0
        lngtotalpages = 0
        strmovieidlist = ""
        strserieidlist = ""
        intencore = True
        while intencore:
            strtmdbapilisturl = "3/list/" + str(lnglistid) + "?language=" + strlanguagecountry + "&page=" + str(lngpage)
            strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapilisturl
            # print(strtmdbapifullurl)
            response = requests.get(strtmdbapifullurl, headers=headers)
            data = response.json()
            #strapilistfordb = json.dumps(data, ensure_ascii=False)
            lngliststatuscode = 0
            if 'status_code' in data:
                lngliststatuscode = data['status_code']
            if lngliststatuscode <= 1:
                # API request result is not an error
                if lngpage == 1:
                    # Extract data using the keys
                    strlistdesc = ""
                    if 'description' in data:
                        strlistdesc = data['description']
                    strlistposterpath = ""
                    if 'poster_path' in data:
                        strlistposterpath = data['poster_path']
                    strlistname = ""
                    if 'name' in data:
                        strlistname = data['name']
                    strcreatedby = ""
                    if 'created_by' in data:
                        strcreatedby = data['created_by']
                    
                    # print(f"{strlistname}")
                    # print(f"{strlistposterpath}")
                    # print(f"Description: {strlistdesc}")
                    
                    arrlistcouples = {}
                    arrlistcouples["ID_LIST"] = lnglistid
                    #arrlistcouples["API_URL"] = strtmdbapilisturl
                    #arrlistcouples["CRAWLER_VERSION"] = 1
                    #arrlistcouples["API_RESULT"] = strapilistfordb
                    arrlistcouples["DESCRIPTION"] = strlistdesc
                    arrlistcouples["POSTER_PATH"] = strlistposterpath
                    if strlistname != "":
                        arrlistcouples["NAME"] = strlistname
                    if strcreatedby != "":
                        arrlistcouples["CREATED_BY"] = strcreatedby
                    
                    strsqltablename = "T_WC_TMDB_LIST"
                    strsqlupdatecondition = f"ID_LIST = {lnglistid}"
                    f_sqlupdatearray(strsqltablename,arrlistcouples,strsqlupdatecondition,1)
                results = data['items']
                lngtotalpages = data['total_pages']
                for row in results:
                    lngmovieid = row['id']
                    intadult = row['adult']
                    strmediatype = row['media_type'];
                    if strmediatype == "movie":
                        # It is a movie
                        lngdisplayorder += 1
                        if strmovieidlist != "":
                            strmovieidlist += ","
                        strmovieidlist += str(lngmovieid)
                        arrlistcouples = {}
                        arrlistcouples["ID_LIST"] = lnglistid
                        arrlistcouples["ID_MOVIE"] = lngmovieid
                        arrlistcouples["DISPLAY_ORDER"] = lngdisplayorder
                        strsqltablename = "T_WC_TMDB_MOVIE_LIST"
                        strsqlupdatecondition = f"ID_LIST = {lnglistid} AND ID_MOVIE = {lngmovieid}"
                        f_sqlupdatearray(strsqltablename,arrlistcouples,strsqlupdatecondition,1)
                    else:
                        # It is a TV serie
                        lngdisplayorder += 1
                        if strserieidlist != "":
                            strserieidlist += ","
                        strserieidlist += str(lngmovieid)
                        arrlistcouples = {}
                        arrlistcouples["ID_LIST"] = lnglistid
                        arrlistcouples["ID_SERIE"] = lngmovieid
                        arrlistcouples["DISPLAY_ORDER"] = lngdisplayorder
                        strsqltablename = "T_WC_TMDB_SERIE_LIST"
                        strsqlupdatecondition = f"ID_LIST = {lnglistid} AND ID_SERIE = {lngmovieid}"
                        f_sqlupdatearray(strsqltablename,arrlistcouples,strsqlupdatecondition,1)
            lngpage += 1
            if lngpage > lngtotalpages:
                intencore = False
        if strmovieidlist != "":
            strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_LIST WHERE ID_LIST = " + str(lnglistid) + " AND ID_MOVIE NOT IN (" + strmovieidlist + ") "
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()
        if strserieidlist != "":
            strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_LIST WHERE ID_LIST = " + str(lnglistid) + " AND ID_SERIE NOT IN (" + strserieidlist + ") "
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()

def f_tmdblistsetcreditscompleted(lnglistid):
    global paris_tz
    global connectioncp
    
    if lnglistid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_LIST"
        strsqlupdatecondition = f"ID_LIST = {lnglistid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdblisttosqleverything(lnglistid):
    f_tmdblisttosql(lnglistid)
    f_tmdblistsetcreditscompleted(lnglistid)

def f_stringtosql(strtext):
    return "'" + strtext.replace("'","\\'") + "'"

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

def f_genrestranslatefr(strmoviegenres):
    strmoviegenres = strmoviegenres.replace("|Action|","|Action|")
    strmoviegenres = strmoviegenres.replace("|Adventure|","|Aventure|")
    strmoviegenres = strmoviegenres.replace("|Animation|","|Animation|")
    strmoviegenres = strmoviegenres.replace("|Comedy|","|Comédie|")
    strmoviegenres = strmoviegenres.replace("|Crime|","|Crime|")
    strmoviegenres = strmoviegenres.replace("|Documentary|","|Documentaire|")
    strmoviegenres = strmoviegenres.replace("|Drama|","|Drame|")
    strmoviegenres = strmoviegenres.replace("|Family|","|Familial|")
    strmoviegenres = strmoviegenres.replace("|Fantasy|","|Fantastique|")
    strmoviegenres = strmoviegenres.replace("|History|","|Histoire|")
    strmoviegenres = strmoviegenres.replace("|Horror|","|Horreur|")
    strmoviegenres = strmoviegenres.replace("|Music|","|Musique|")
    strmoviegenres = strmoviegenres.replace("|Mystery|","|Mystère|")
    strmoviegenres = strmoviegenres.replace("|Romance|","|Romance|")
    strmoviegenres = strmoviegenres.replace("|Science Fiction|","|Science-Fiction|")
    strmoviegenres = strmoviegenres.replace("|Thriller|","|Thriller|")
    strmoviegenres = strmoviegenres.replace("|TV Movie|","|Téléfilm|")
    strmoviegenres = strmoviegenres.replace("|War|","|Guerre|")
    strmoviegenres = strmoviegenres.replace("|Western|","|Western|")
    return strmoviegenres

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

