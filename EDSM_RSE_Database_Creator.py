# -*- coding:utf-8 -*-
# ! python3

"""
  Copyright 2017 Sebastian Bauer
  
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. 
"""

import os
import sys
import json
import sqlite3
import time
import re
import multiprocessing as mp
import requests
from tqdm import tqdm
from edts.edtslib import system as edtsSystem

LENGTH_OF_DAY = 86400
NUMBER_OF_PROCESSES = 6

def coordinatesFromName(name):
    s = edtsSystem.from_name(name, allow_known=False)
    if s:
        return (s.name, s.position.x, s.position.y, s.position.z)
    else:
        return None

def main():
    jsonFile =  os.path.join(os.getcwd(), "systemsWithoutCoordinates.json")
    versionFile = os.path.join(os.getcwd(), "version.txt")
    dbFileName = "systemsWithoutCoordinates"
    dbFile = os.path.join(os.getcwd(), dbFileName + ".sqlite")
    dbJournalFile = os.path.join(os.getcwd(), dbFileName + ".sqlite-journal")
    permitSectorsFile = os.path.join(os.getcwd(), "permit_sectors.txt")

    # delete the json file if it's older than 1 day
    if os.path.exists(jsonFile) and (time.time() - os.path.getctime(jsonFile)) > LENGTH_OF_DAY:
        print("json is older than 1 day. It will be removed.")
        os.remove(jsonFile)

    # download json file if it doesn't exist
    if not os.path.exists(jsonFile):
        print("Downloading systemsWithoutCoordinates.json from EDSM...")
        r = requests.get("https://www.edsm.net/dump/systemsWithoutCoordinates.json", stream=True)
        total_size = int(r.headers.get('content-length', 0)); 
        with tqdm(r.iter_content(32*1024), total=total_size, unit='B', unit_scale=True) as pbar:
            with open(jsonFile, 'b+w') as f:
                for data in r.iter_content(chunk_size=32*1024):
                    if data:
                        f.write(data)
                        pbar.update(32*1024)
            pbar.close()

    # load sectors that require a permit
    permitSectorsList = list()
    if os.path.exists(permitSectorsFile):
        with open(permitSectorsFile) as file:
            for line in file:
                permitSectorsList.append(line.strip())
    permitLocked = re.compile("^({0})".format("|".join(permitSectorsList)), re.IGNORECASE)

    print("Reading json file...")
    systemNames = list()
    useRegex = True if len(permitSectorsList) > 0 else False
    with open(jsonFile) as file:
        j = json.load(file)
        print("Applying filters...")
        with tqdm(total=len(j), unit="systems") as pbar:
            for entry in j:
                pbar.update(1)
                if useRegex and permitLocked.match(entry["name"]):
                    continue # filter out system
                systemNames.append(entry["name"])
            pbar.close()

    # remove old sqlite files
    if os.path.exists(dbFile):
        os.remove(dbFile)
    if os.path.exists(dbJournalFile):
        os.remove(dbJournalFile)

    conn = sqlite3.connect(dbFile)
    conn.text_factory = str
    c = conn.cursor()

    c.execute("""CREATE TABLE 'systems' (
	        'id'	                        INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
	        'name'                  	    TEXT NOT NULL,
            'x'                     	    REAL NOT NULL,
	        'y'	                            REAL NOT NULL,
	        'z' 	                        REAL NOT NULL,
            'last_checked'                  INTEGER
            );""")
    c.execute("""CREATE TABLE 'version' (
	        'date'	                        INTEGER NOT NULL
            );""")

    t = int(time.time())
    c.execute("INSERT INTO version (date) VALUES (?)", (t,)) # we need a tuple here, create one with only 1 entry

    print("Calculating the coordinates...")
    pool = mp.Pool(processes = NUMBER_OF_PROCESSES)

    for result in tqdm(pool.imap_unordered(coordinatesFromName, systemNames, chunksize=100), total=len(systemNames), unit="systems"):
        if result:
            c.execute("INSERT INTO systems (name, x, y, z) VALUES (?,?,?,?)", result)

    print("Writing the database...")
    conn.commit()

    # write version text file
    print("Writing the version file...")
    if os.path.exists(versionFile):
        os.remove(versionFile)
    with open(versionFile, "w") as file:
        file.write(str(t))
    
    print("All done :)")

if __name__ == "__main__":
    main()