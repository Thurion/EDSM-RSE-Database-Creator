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
import logging
import requests
from tqdm import tqdm
from edts.edtslib import pgnames, id64data, system as edtsSystem

LENGTH_OF_DAY = 86400
NUMBER_OF_PROCESSES = 6

logging.basicConfig(filename='edsm-rse.log',level=logging.WARN)
from edts.edtslib import system as edtsSystem

def coordinatesFromName(name):
    # this function needs to stay outside of the class or the class needs to implement pickling
    s = edtsSystem.from_name(name, allow_known=False)
    if s:
        return (s.name, s.position.x, s.position.y, s.position.z)
    else:
        return None

class EDSM_RSE_DB():
    def __init__(self):
        self.jsonFile =  os.path.join(os.getcwd(), "systemsWithoutCoordinates.json")
        self.versionFile = os.path.join(os.getcwd(), "version.txt")
        dbFileName = "systemsWithoutCoordinates"
        self.dbFile = os.path.join(os.getcwd(), dbFileName + ".sqlite")
        self.dbJournalFile = os.path.join(os.getcwd(), dbFileName + ".sqlite-journal")
        self.permitSectorsFile = os.path.join(os.getcwd(), "permit_sectors.txt")

        self.conn = None
        self.c = None


    def openDatabase(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.dbFile)
            self.conn.text_factory = str
            self.c = self.conn.cursor()


    def createDatabase(self, currentTime):
        if os.path.exists(self.dbFile):
            os.remove(self.dbFile)
        if os.path.exists(self.dbJournalFile):
            os.remove(self.dbJournalFile)

        self.openDatabase()

        self.c.execute("""CREATE TABLE 'systems' (
                'id'           INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                'name'         TEXT NOT NULL,
                'x'            REAL NOT NULL,
                'y'            REAL NOT NULL,
                'z'            REAL NOT NULL,
                'last_checked' INTEGER
                );""")
        self.c.execute("""CREATE TABLE 'duplicates' (
                'id'           INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                'real_name'    TEXT NOT NULL,
                'pq_name'      TEXT NOT NULL
                );""")
        self.c.execute("""CREATE TABLE 'version' (
                'date'         INTEGER NOT NULL
                );""")

        self.c.execute("INSERT INTO version (date) VALUES (?)", (currentTime,)) # we need a tuple here, create one with only 1 entry

        print("Calculating the coordinates...")
        pool = mp.Pool(processes = NUMBER_OF_PROCESSES)

        for result in tqdm(pool.imap_unordered(coordinatesFromName, self.systemNames, chunksize=100), total=len(self.systemNames), unit="systems"):
            if result:
                self.c.execute("INSERT INTO systems (name, x, y, z) VALUES (?,?,?,?)", result)

        print("Creating index...")
        self.c.execute("CREATE INDEX 'systems_coordinates' ON 'systems' ('x' ASC,'y' ASC,'z' ASC)")
        print("Writing the database...")
        for realName, pgSystems in self.duplicates.items():
            for pgSystem in pgSystems:
                self.c.execute("INSERT INTO systems (name, x, y, z) VALUES (?,?,?,?)", (pgSystem.name, pgSystem.position.x, pgSystem.position.y, pgSystem.position.z))
                self.c.execute("INSERT INTO duplicates (real_name, pq_name) VALUES (?,?)", (realName, pgSystem.name))
        self.conn.commit()

        self.createVersionFile(currentTime)


    def createVersionFile(self, currentTime):
        print("Writing the version file...")
        if os.path.exists(self.versionFile):
            os.remove(self.versionFile)
        with open(self.versionFile, "w") as file:
            file.write(str(currentTime))


    def isDatabasePresentAndValid(self):
        if os.path.exists(self.dbFile):
            try:
                self.openDatabase()
                # try all tables in case one is corrupted
                self.c.execute("SELECT * from version")
                results1 = self.c.fetchall()
                self.c.execute("SELECT * from systems LIMIT 100")
                results2 = self.c.fetchall()
                self.c.execute("SELECT * from duplicates LIMIT 10")
                results3 = self.c.fetchall()
                self.conn.close()
                self.c = None
                self.conn = None
                return len(results1) > 0 and len(results2) > 0 # no guarantee that duplicates are present
            except:
                self.c = None
                self.conn = None
        return False


    def checkAndDownloadJSON(self):
        # doesn't handle faulty json (e.g. aborted download)
        # delete the json file if it's older than 1 day
        # use modification time of file because the creation time might not change on windows even if the file was deleted
        if os.path.exists(self.jsonFile) and (time.time() - os.path.getmtime(self.jsonFile)) > LENGTH_OF_DAY: 
            print("json is older than 1 day. It will be removed.")
            os.remove(self.jsonFile)

        # download json file if it doesn't exist
        if not os.path.exists(self.jsonFile):
            print("Downloading systemsWithoutCoordinates.json from EDSM...")
            r = requests.get("https://www.edsm.net/dump/systemsWithoutCoordinates.json", stream=True)
            total_size = int(r.headers.get('content-length', 0)); 
            with tqdm(r.iter_content(32*1024), total=total_size, unit='B', unit_scale=True) as pbar:
                with open(self.jsonFile, 'b+w') as f:
                    for data in r.iter_content(chunk_size=32*1024):
                        if data:
                            f.write(data)
                            pbar.update(32*1024)
                pbar.close()


    def applyFilters(self):
        # load sectors that require a permit
        permitSectorsList = list()
        if os.path.exists(self.permitSectorsFile):
            with open(self.permitSectorsFile) as file:
                for line in file:
                    if len(line) > 2:
                        permitSectorsList.append(line.strip())
        permitLocked = re.compile("^({0})".format("|".join(permitSectorsList)), re.IGNORECASE)

        print("Reading json file...")
        self.systemNames = list()
        self.duplicates = dict()
        useRegex = True if len(permitSectorsList) > 0 else False
        
        def processDuplicate(name, id):
            dupeSystem = edtsSystem.from_id64(id, allow_known=False)
            if useRegex and permitLocked.match(dupeSystem.name):
                return  None
            return (name, dupeSystem)

        with open(self.jsonFile) as file:
            j = json.load(file)
            print("Applying filters and processing duplicates...")
            with tqdm(total=len(j), unit="systems") as pbar:
                for entry in j:
                    pbar.update(1)
                    name = entry["name"]
                    if useRegex and permitLocked.match(name):
                        continue # filter out system
                    if not pgnames.is_pg_system_name(name):
                        id64 = id64data.known_systems.get(name.lower(), None)
                        if isinstance(id64, list):
                            for id in id64:
                                dupe = processDuplicate(name, id)
                                if dupe: 
                                    realName, pgSystem = dupe
                                    self.duplicates.setdefault(realName, list())
                                    self.duplicates[realName].append(pgSystem)
                        else:
                            self.systemNames.append(name)
                    else:
                        self.systemNames.append(name)
                pbar.close()


    def applyDelta(self, currentTime):
        print("Applying changes only...")
        print("Reading database...")
        self.openDatabase()
        dbSystems = dict()
        self.c.execute("SELECT systems.name, systems.id FROM systems")
        rows = self.c.fetchall()

        print("Searching for changes...")
        total = len(rows) + len(self.systemNames) + len(self.duplicates.keys())
        pbar = tqdm(total=total, unit="steps")

        for row in rows:
            dbSystems.setdefault(row[0], row[1])
            pbar.update(1)

        removed = 0
        needsAdding = list()

        for systemName in self.systemNames:
            if systemName in dbSystems:
                # system is already present, do nothing
                # only dupe PG name is stored in systems table
                dbSystems.pop(systemName, 0)
            else:
                result = coordinatesFromName(systemName)
                if result:
                    needsAdding.append(result)
            pbar.update(1)

        dupes = dict()
        for realName, pgSystems in self.duplicates.items():
            # remove pg system names or they will be deleted otherwise
            for pgSystem in pgSystems:
                if pgSystem.name in dbSystems:
                    dbSystems.pop(pgSystem.name, 0)
                else:
                    needsAdding.append((pgSystem.name, pgSystem.position.x, pgSystem.position.y, pgSystem.position.z))
                    dupes.setdefault(realName, list())
                    dupes[realName].append(pgSystem)
            pbar.update(1)
        pbar.close()

        print("Deleting {0} entries and adding {1} new ones...".format(len(dbSystems.keys()), len(needsAdding)))
        for id in dbSystems.values():
            self.c.execute("DELETE FROM systems WHERE systems.id = ?", (id,))
        for edSystem in needsAdding:
            self.c.execute("INSERT INTO systems (name, x, y, z) VALUES (?,?,?,?)", edSystem)

        print("Handling {0} duplicate systems...".format(len(dupes.keys())))
        # TODO also delete entries for duplicates
        # TODO prevent adding duplicates of duplicate pairs
        for realName, pgSystems in dupes.items():
            for pgSystem in pgSystems:
                self.c.execute("INSERT INTO duplicates (real_name, pq_name) VALUES (?,?)", (realName, pgSystem.name))

        self.c.execute("UPDATE version SET date = ? WHERE version.date IN (SELECT * FROM version)", (currentTime,))
        self.conn.commit()
        if len(dbSystems.keys()) > 0:
            print("Running VACUUM...")
            self.conn.execute("VACUUM")
        self.createVersionFile(currentTime)


def main():
    edsmRse = EDSM_RSE_DB()
    edsmRse.checkAndDownloadJSON()
    edsmRse.applyFilters()

    currentTime = int(time.time())
    if not edsmRse.isDatabasePresentAndValid():
        edsmRse.createDatabase(currentTime)
    else:
        edsmRse.applyDelta(currentTime)
    print("All done :)")

if __name__ == "__main__":
    main()