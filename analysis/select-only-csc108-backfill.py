#-  Python 3 source code

#-  select-only-csc108-backfill.py ~~
#
#   This program exists to allow the continued refinement of queries to filter
#   out everything except CSC108's ATLAS production jobs that run in backfill.
#   The idea is that, once I know for sure that I have a query that selects
#   precisely what we need, I will copy it directly into all the existing
#   queries I have already written.
#
#                                                       ~~ (c) SRW, 29 Aug 2018
#                                                   ~~ last updated 04 Dec 2018

import json
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  count(DISTINCT JobID) AS jobs

            FROM

                active

            WHERE

                Account = "CSC108"
                AND User = "doleynik"
                AND JobName LIKE "SAGA-Python-PBSJobScript.%"

             -- The following predicates filter on requested nodes and
             -- requested wall time, but according to Danila, the values have
             -- fluctuated a little bit historically and thus they cannot be
             -- relied upon.
             /*
                AND ((ReqNodes IS NULL
                        AND 10 <= (ReqProcs / 16)
                        AND (ReqProcs / 16) <= 350)
                    OR (ReqNodes IS NOT NULL
                        AND 10 <= ReqNodes
                        AND ReqNodes <= 350))

                AND (100 * 60) <= ReqAWDuration
              */
        ;
        """

    for row in cursor.execute(query):
        print("# of jobs: %s" % row["jobs"])

    return

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
