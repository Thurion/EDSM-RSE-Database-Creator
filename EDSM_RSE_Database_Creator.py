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
import urllib.request
import json
import sqlite3
import time
import re
from edts.edtslib import system as edtsSystem

LENGTH_OF_DAY = 86400

def main():
    jsonFile =  os.path.join(os.getcwd(), "systemsWithoutCoordinates.json")
    dbFile = os.path.join(os.getcwd(), "systemsWithoutCoordinates.sqlite")
    permitSectorsFile = os.path.join(os.getcwd(), "permit_sectors.txt")

    # delete the json file if it's older than 1 day
    if os.path.exists(jsonFile) and (time.time() - os.path.getctime(jsonFile)) > LENGTH_OF_DAY:
        print("json is older than 1 day. It will be removed.")
        os.remove(jsonFile)
    if not os.path.exists(jsonFile):
        print("Downloading systemsWithoutCoordinates.json from EDSM.")
        r = urllib.request.urlopen("https://www.edsm.net/dump/systemsWithoutCoordinates.json")
        with open(jsonFile, 'b+w') as f:
            f.write(r.read())

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
        for entry in j:
            if useRegex and permitLocked.match(entry["name"]):
                continue # ignore
            systemNames.append(entry["name"])

    if os.path.exists(dbFile):
        os.remove(dbFile)

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

    numberOfSystems = len(systemNames)
    currentProgress = 0
    print("Calculating the coordinates and filling the database...")
    print("00 % complete")
    for i, name in enumerate(systemNames):
        newProgess = int((i / numberOfSystems) * 100)
        if newProgess > currentProgress:
            currentProgress = newProgess
            print("{0:02d} % complete".format(currentProgress))
        #s = edtsSystem.from_name(name, allow_known=False) # EDTS needs an update for this to work
        s = None
        if s:
            c.execute("INSERT INTO systems (name, x, y, z) VALUES (?,?,?,?)", (s.name, s.position.x, s.position.y, s.position.z))

    conn.commit()


if __name__ == "__main__":
    main()