#-  Python 3 source code

#-  find-longest-gap.py ~~
#
#   This program finds the longest gap in time in which CSC108 is not running
#   any jobs in backfill.
#
#   I resorted to Python to do the search, and that's why this is so ugly. It
#   would be much prettier if it were all in SQL, but I haven't figured that
#   out yet.
#
#                                                       ~~ (c) SRW, 03 Aug 2018
#                                                   ~~ last updated 04 Dec 2018

from datetime import datetime

import json
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  SampleID,
                SampleTime
            FROM
                active
            WHERE
                Account = "CSC108"
                AND User = "doleynik"
                AND JobName LIKE "SAGA-Python-PBSJobScript.%"
            ORDER BY
                SampleTime
        """

    data = []
    for row in cursor.execute(query):
        data.append({
            "id": row["SampleID"],
            "time": row["SampleTime"]
        })

    deltas = []
    previous = data[0]
    for current in data:
        deltas.append(current["time"] - previous["time"])
        previous = current

    idx = deltas.index(max(deltas))

    print(json.dumps(data[idx - 1], indent = 4))
    print(json.dumps(data[idx], indent = 4))

    left_time = datetime.utcfromtimestamp(data[idx - 1]["time"])
    right_time = datetime.utcfromtimestamp(data[idx]["time"])

    print("Longest gap starts at %s" % left_time)
    print("Longest gap ends at %s" % right_time)

    print("Gap length is %s (%s hours)" % (max(deltas), max(deltas) / 3600.0))

###

def main():

  # Store current working directory.

    cwd = os.getcwd()

  # Find the data directory, where this script is running remotely at OLCF and
  # locally on a personal laptop, for example.

    if os.path.isdir("/lustre/atlas/proj-shared/csc108/data/moab/"):
        data_dir = "/lustre/atlas/proj-shared/csc108/data/moab/"
    elif os.path.isdir(os.path.join(cwd, "moab")):
        data_dir = os.path.join(cwd, "moab")
    else:
        raise Exception("Data directory not found.")

  # Create string to represent path to database file.

    dbfilename = os.path.join(data_dir, "moab-data.sqlite")

  # Open connection to the database (file).

    connection = sqlite3.connect(dbfilename)

  # Enable users to access columns by name instead of by index.

    connection.row_factory = sqlite3.Row

  # Ensure read-only access to the database

    connection.execute("PRAGMA query_only = true;")

  # Run custom analyis code.

    analyze(connection)

  # Commit any changes and close the connection to the database.

    connection.commit()
    connection.close()

###

if __name__ == "__main__":
    main()

#-  vim:set syntax=python:
