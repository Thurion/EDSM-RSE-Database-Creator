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
import time
import re
import math
import queue
import threading
import multiprocessing as mp
import logging
import requests
import psycopg2
import csv
import urllib.request
from tqdm import tqdm
from edts.edtslib import pgnames, id64data, system as edtsSystem

LENGTH_OF_DAY = 60 * 60 * 22 # set to 22 hours to make sure the json is downloaded every day

ACTIONX = (2**32)-1
DISTANCE_MULTIPLIER = 1.732 # sqrt(3) or length of vector (1, 1, 1)

logging.basicConfig(filename="edsm-rse.log",level=logging.WARN)
from edts.edtslib import system as edtsSystem

def generateCoordinates(system):
    return system.getCoordinates()

def insertWorker(cur, conn, sql, q, pbar):
    while True:
        systems = q.get()
        if systems is None:
            break
        cur.executemany(sql, systems)
        conn.commit()
        pbar.update(len(systems))
        q.task_done()

class EliteSystem():
    def __init__(self, name, id64 = None, x = None, y = None, z = None, uncertainty = 0):
        self.name = name
        self.id64 = id64
        self.x = x
        self.y = y
        self.z = z
        self.uncertainty = uncertainty

    def __str__(self):
        return "id: {id}, name: {name}, x: {x}, y: {y}, z: {z}, uncertainty: {uncertainty}".format(id=self.id64, name=self.name, x=self.x, y=self.y, z=self.z, uncertainty=self.uncertainty)

    def fromJSON(self, j):
        self.id64 = j["id64"]
        if self.id64 == None:
            self.getCoordinates() # initialize
        if "estimatedCoordinates" in j:
            coordinates = j["estimatedCoordinates"]
            self.x = coordinates["x"]
            self.y = coordinates["y"]
            self.z = coordinates["z"]
            self.uncertainty = coordinates["precision"]

    def getCoordinates(self):
        if self.x == None or self.y == None or self.z == None:
            s = edtsSystem.from_name(self.name, allow_known=False)
            if s:
                self.id64 = s.id64
                self.x = s.position.x
                self.y = s.position.y
                self.z = s.position.z
                self.uncertainty = int(s.uncertainty * DISTANCE_MULTIPLIER + .5) # round it so it matches the values from EDSM
        if self.id64:
            return { "id": self.id64, "name": self.name, "uncertainty": self.uncertainty, "x": self.x, "y": self.y, "z": self.z }
        else:
            return None

class EDSM_RSE_DB():
    def __init__(self, number_of_processes, number_of_inserts, size_of_queue, db_host, db_port, db_name, db_user, db_password):
        self.jsonFile = os.path.join(os.getcwd(), "systemsWithoutCoordinates.json")
        self.permitSectorsFile = os.path.join(os.getcwd(), "permit_sectors.txt")
        self.systemFilterFile = os.path.join(os.getcwd(), "system_filter.txt")
        self.number_of_processes = number_of_processes
        self.number_of_inserts = number_of_inserts
        self.size_of_queue = size_of_queue
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.conn = None
        self.c = None

    def openDatabase(self):
        if not self.conn:
            self.conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_name, user=self.db_user, password=self.db_password)
            self.c = self.conn.cursor()

    def createDatabase(self):
        self.openDatabase()

        self.c.execute(" ".join([
            "CREATE TABLE IF NOT EXISTS systems (",
            "id           BIGINT PRIMARY KEY,",
            "name         TEXT NOT NULL,",
            "x            REAL NOT NULL,",
            "y            REAL NOT NULL,",
            "z            REAL NOT NULL,",
            "uncertainty  INTEGER DEFAULT 0,",
            "action_todo  INTEGER DEFAULT 0,",
            "created_at   TIMESTAMPTZ NULL DEFAULT NULL,",
            "updated_at   TIMESTAMPTZ NULL DEFAULT NULL,",
            "deleted_at   TIMESTAMPTZ NULL DEFAULT NULL);"
            ]))
        self.c.execute(" ".join([
            "CREATE TABLE IF NOT EXISTS projects(",
            "id integer NOT NULL,",
            "action_text TEXT DEFAULT NULL,",
            "explanation TEXT DEFAULT NULL,",
            "CONSTRAINT projects_pkey PRIMARY KEY (id)",
            ");"
            ]))
        self.c.execute(" ".join([
            "DROP TRIGGER IF EXISTS systems_update_at ON systems;",
            "CREATE OR REPLACE FUNCTION systems_update_trigger() RETURNS TRIGGER AS $$",
            "DECLARE",
                "nstring varchar := '';",
                "act projects%ROWTYPE;",
            "BEGIN",
                "IF TG_OP = 'INSERT' THEN",
                    "NEW.created_at := current_timestamp;",
                "END IF;",
                "NEW.updated_at := current_timestamp;",
                "RETURN NEW;",
            "END;",
            "$$ LANGUAGE plpgsql;",
            "CREATE TRIGGER systems_update_at BEFORE INSERT OR UPDATE",
            "ON systems FOR EACH ROW ",
            "EXECUTE PROCEDURE systems_update_trigger();"
            ]))
        self.c.execute(" ".join([
            "CREATE TABLE IF NOT EXISTS duplicates (",
            "id           BIGSERIAL PRIMARY KEY,",
            "real_name    TEXT NOT NULL,",
            "pg_name      TEXT NOT NULL",
            ");"
            ]))
        self.c.execute("CREATE UNIQUE INDEX IF NOT EXISTS systems_pkey ON systems (id)")
        self.conn.commit()

        sql_insert = " ".join([
            "INSERT INTO systems (id, name, x, y, z, uncertainty, action_todo)",
            "VALUES (%(id)s, %(name)s, %(x)s, %(y)s, %(z)s, %(uncertainty)s, %(action_todo)s);",
        ])
            
        print("Calculating the coordinates and adding them to the database...")
        pool = mp.Pool(processes = self.number_of_processes)
        q = queue.Queue()
        q.maxsize = self.size_of_queue
        
        pbar = tqdm(total=len(self.systems), unit=" inserts", desc="Database", position=0)
        thread = threading.Thread(target=insertWorker, args=(self.c, self.conn, sql_insert, q, pbar))
        thread.start()
        systems = list()
        for count, result in enumerate(tqdm(pool.imap_unordered(generateCoordinates, self.systems, chunksize=100), total=len(self.systems), unit=" systems", desc="Calculation", position=1)):
            if result:
                result["action_todo"] = 1
                systems.append(result)
            if count % self.number_of_inserts == 0:
                q.put(systems, block=True)
                systems = list()
        q.put(systems)
        q.put(None)
        thread.join()
        pbar.close()

        print("Creating index...")
        self.c.execute("CREATE INDEX IF NOT EXISTS systems_coordinates ON systems (x, y, z)")
        self.conn.commit()

    def isDatabasePresentAndValid(self):
        try:
            self.openDatabase()
            # test if connection is really open
            self.c.execute("SELECT * from systems LIMIT 10")
            results = self.c.fetchall()
            self.conn.close()
            self.c = None
            self.conn = None
            return len(results) > 0
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
            with tqdm(unit="B", unit_scale=True) as pbar, open(self.jsonFile, "wb") as f:
                for data in r.iter_content(chunk_size=32*1024):
                    if data:
                        f.write(data)
                        pbar.update(32*1024)

    def applyFilters(self):
        # load sectors that require a permit
        permitSectorsList = list()
        if os.path.exists(self.permitSectorsFile):
            with open(self.permitSectorsFile) as file:
                for line in file:
                    if len(line) > 2: # ignore empty lines
                        permitSectorsList.append(line.strip())
        permitLocked = re.compile("^({0})".format("|".join(permitSectorsList)), re.IGNORECASE)

        systemSet = set()
        if os.path.exists(self.systemFilterFile):
            with open(self.systemFilterFile) as file:
                for line in file:
                    if len(line) > 2: # ignore empty lines
                        systemSet.add(line.strip().lower())

        self.systems = list()
        useRegex = True if len(permitSectorsList) > 0 else False

        with open(self.jsonFile) as file:
            print("Reading json file...")
            print("Applying filters...")
            for linejson in tqdm(file, unit=" systems"):
                linejson = linejson.strip(" [],\n\r")
                if linejson:
                    entry = json.loads(linejson)
                else:
                    continue
                name = entry["name"]
                if useRegex and permitLocked.match(name) or name.lower() in systemSet:
                    continue # filter out system
                if not pgnames.is_pg_system_name(name):
                    id64 = id64data.known_systems.get(name.lower(), None)
                    if isinstance(id64, list):
                        logging.warning("Possible dupe systems", id64)
                    else:
                        system = EliteSystem(name)
                        system.fromJSON(entry)
                        self.systems.append(system)
                else:
                    system = EliteSystem(name)
                    system.fromJSON(entry)
                    self.systems.append(system)

    def applyDelta(self):
        print("Applying changes only...")
        print("Reading database...")
        self.openDatabase()
        dbSystems = set()
        self.c.execute("SELECT id FROM systems WHERE (action_todo & 1)=1 AND deleted_at IS NULL")
        rows = self.c.fetchall()

        print("Searching for changes...")
        total = len(rows) + len(self.systems)
        pbar = tqdm(total=total, unit=" steps")

        for row in rows:
            dbSystems.add(row[0])
            pbar.update(1)

        removed = 0
        needsAdding = list()

        for system in self.systems:
            if system.id64 in dbSystems:
                # system is already present, do nothing
                # only dupe PG name is stored in systems table
                dbSystems.remove(system.id64)
            else:
                systemDictionary = system.getCoordinates()
                if systemDictionary:
                    needsAdding.append(systemDictionary)
                else:
                    logging.warning("No valid coordinates: {}".format(system))
            pbar.update(1)
        pbar.close()

        print("Deleting {0} entries and adding {1} new ones...".format(len(dbSystems), len(needsAdding)))
        if len(dbSystems) > 0:
            sql1 = " ".join([
                "UPDATE systems SET action_todo = (action_todo & %(acx)s)",  # delete 2^0
                "WHERE id = %(id64)s;",
                "UPDATE systems SET deleted_at = current_timestamp",
                "WHERE action_todo = 0 AND id = %(id64)s;",
            ])
            for id64 in tqdm(dbSystems, desc="Deleting..."):
                self.c.execute(sql1, { "acx": ACTIONX-1, "id64": id64 })
            self.conn.commit()
    
        if len(needsAdding) > 0:
            sql1 = " ".join([
                "INSERT INTO systems (name, x, y, z, uncertainty, deleted_at, id)",
                "VALUES (%(name)s, %(x)s, %(y)s, %(z)s, %(uncertainty)s, NULL, %(id)s)",
                "ON CONFLICT (id) DO UPDATE",
                "SET name = %(name)s, uncertainty = %(uncertainty)s,",
                "x = %(x)s, y = %(y)s, z = %(z)s,",
                "deleted_at = NULL;",
                "UPDATE systems SET action_todo = (action_todo | %(action_todo)s)",
                "WHERE id = %(id)s;"
            ])
            for edSystem in tqdm(needsAdding, desc="Adding..."):
                edSystem["action_todo"] = 1
                self.c.execute(sql1, edSystem)
        self.conn.commit()

    def scan_Navbeacons(self):
        sql1 = " ".join([
            "INSERT INTO systems (name, x, y, z,  id)",
            "VALUES (%(name)s, %(x)s, %(y)s, %(z)s, %(id)s)",
            "ON CONFLICT (id) DO UPDATE",
            "SET name = %(name)s, deleted_at = NULL,",
            "x = %(x)s, y = %(y)s, z = %(z)s;",
            "UPDATE systems SET action_todo = (action_todo | %(action_todo)s)",
            "WHERE id = %(id)s;"
        ])

        sheetUrl = "https://docs.google.com/spreadsheets/d/1uX_VXA66FNoDRsF0FngpgDhmGj2p4vBO1Ik0aALLO8o/export?format=csv"
        sheetResponse = urllib.request.urlopen(sheetUrl)
        navBeaconCSV = csv.DictReader(sheetResponse.read().decode("utf-8").split("\r\n"), delimiter=",")
        id64list = list()
        for datacsv in tqdm(navBeaconCSV, desc="Navbeacons..."):
            data = datacsv
            id64list.append(data['id'])
            data["action_todo"] = 2
            self.c.execute(sql1, data)
        self.conn.commit()

        if len(id64list)>0:
            sql1 = " ".join([
                "UPDATE systems SET action_todo = (action_todo & %(acx)s)",
                "WHERE id NOT IN (",
                ",".join([str(x) for x in id64list]),
                ") AND (action_todo & %(action_todo)s) = %(action_todo)s;",
            ])
            self.c.execute(sql1, { "acx": ACTIONX-2, "action_todo": 2 })
        self.c.execute("UPDATE systems SET deleted_at = current_timestamp WHERE action_todo = 0;")
        self.conn.commit()

def main():
    if not os.path.isfile("config.json"):
        sys.exit("No config file present. Please copy config.json.example to config.json and edit it")

    with open("config.json") as jf:
        j = json.load(jf)
        rse = j["edsm_rse"]
        db = j["database"]
        edsmRse = EDSM_RSE_DB(rse["number_of_processes"], rse["number_of_simultatnous_inserts"], rse["size_of_queue"],
                    db["host"], db["port"], db["dbname"], db["user"], db["password"])

    edsmRse.checkAndDownloadJSON()
    edsmRse.applyFilters()

    if not edsmRse.isDatabasePresentAndValid():
        edsmRse.createDatabase()
    else:
        edsmRse.applyDelta()
    edsmRse.scan_Navbeacons()
    print("All done :)")

if __name__ == "__main__":
    main()