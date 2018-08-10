#-  Python 2.6 source code

#-  integrate-pure-waste.py ~~
#
#   This program computes a "reclaimed waste" statistic that is defined as the
#   sum over all CSC108 backfill jobs of the products of the nodes used by the
#   CSC108 job and the length of the time interval between that CSC108 job's
#   start and the start of the next job owned by another project.
#
#                                                       ~~ (c) SRW, 09 Aug 2018
#                                                   ~~ last updated 10 Aug 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  StartTime,
                CASE
                    WHEN ReqNodes IS NULL THEN
                        ReqProcs / 16
                    ELSE
                        ReqNodes
                END nodes
            FROM
                completed
            WHERE
                Account = "CSC108"
                AND User = "doleynik"
            ORDER BY
                StartTime
        ;
        """

    csc108 = []
    for row in cursor.execute(query):
        csc108.append({
            "StartTime": row["StartTime"],
            "nodes": row["nodes"]
        })

    query = """
        SELECT  StartTime
            FROM
                completed
            WHERE
                Account != "CSC108"
                OR User != "doleynik"
            ORDER BY
                StartTime
        ;
        """

    other = []
    for row in cursor.execute(query):
        other.append({
            "StartTime": row["StartTime"]
        })

  # So, I hate this. The order over iteration over a data structure with a
  # "for each" loop varies by language, and I hate them in general because they
  # destroy parallelism. Nevertheless, in Python, apparently iteration over a
  # list is guaranteed to proceed over the elements in the list's order, so I
  # will use the assumption. And anyone who reads this will know I'm nuts.

    index = 0
    waste = []
    for csc108_job in csc108:
        while other[index]["StartTime"] < csc108_job["StartTime"]:
            index += 1
        dt = (other[index]["StartTime"] - csc108_job["StartTime"]) / 3600.0
        waste.append(csc108_job["nodes"] * dt)

    print "Waste (in node-hours):   %s" % (sum(waste))
    print "Waste reclaimed per job: %s" % (sum(waste) / len(waste))

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
