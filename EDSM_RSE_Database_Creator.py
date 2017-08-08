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

LENGTH_OF_DAY = 86400

def main():
    file =  os.path.join(os.getcwd(), "systemsWithoutCoordinates.json")
    dbFile = os.path.join(os.getcwd(), "systemsWithoutCoordinates.sqlite")

    # delete the json file if it's older than 1 day
    if os.path.exists(file) and (time.time() - os.path.getctime(file)) > LENGTH_OF_DAY:
        os.remove(file)
    if not os.path.exists(file):
        r = urllib.request.urlopen("https://www.edsm.net/dump/systemsWithoutCoordinates.json")
        with open(file, 'b+w') as f:
            f.write(r.read())



if __name__ == "__main__":
    main()