#-  Python 2.6 source code

#-  compute-csc108-max-nodes.py ~~
#
#   This program is for verifying claims made in the PMBS 2018 submission.
#   There are prettier ways to do it, but I'm in a hurry.
#
#                                                       ~~ (c) SRW, 28 Aug 2018
#                                                   ~~ last updated 29 Aug 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  SampleID,
                strftime("%d-%m-%Y %H:%M", SampleTime, "unixepoch",
                    "localtime") AS time,
                max(nodes) AS n
            FROM (
                SELECT  *,
                        CASE
                            WHEN ReqNodes IS NULL THEN
                                ReqProcs / 16
                            ELSE
                                ReqNodes
                        END nodes
                    FROM active
                    WHERE
                        Account = "CSC108"
                        AND User = "doleynik"
                        AND JobName LIKE "SAGA-Python-PBSJobScript.%"
            )
        ;
        """

    id = None
    nodes = None
    time = None
    for row in cursor.execute(query):
        id = row["SampleID"]
        nodes = row["n"]
        time = row["time"]

    print "SampleID: %s (%s)" % (id, time)
    print "# of nodes: %s" % (nodes)

    query = """
        SELECT count(DISTINCT JobID) AS n
            FROM active
            WHERE
                SampleID = "{id}"
                AND Account = "CSC108"
                AND User = "doleynik"
                AND JobName LIKE "SAGA-Python-PBSJobScript.%"
        ;
        """.format(id = id)

    for row in cursor.execute(query):
        print "# of jobs: %s" % (row["n"])

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
        raise "Data directory not found."

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
