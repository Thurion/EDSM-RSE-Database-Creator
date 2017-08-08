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
                permitSectorsList.append(line.rstrip())
    permitLocked = re.compile("^({0})".format("|".join(permitSectorsList)), re.IGNORECASE)

    systemNames = list()
    useRegex = True if len(permitSectorsList) > 0 else False
    with open(jsonFile) as file:
        j = json.load(file)
        for entry in j:
            if useRegex and permitLocked.match(entry["name"]):
                continue # ignore
            systemNames.append(entry["name"])



if __name__ == "__main__":
    main()