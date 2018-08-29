#-  Python 2.6 source code

#-  count-samples-with-csc108-backfill.py ~~
#
#   This program counts the number of samples with (and without) jobs that are
#   being run by CSC108 in backfill mode on Titan.
#
#                                                       ~~ (c) SRW, 16 Jun 2018
#                                                   ~~ last updated 29 Aug 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  count(DISTINCT SampleID) AS n
            FROM
                active
            WHERE
                SampleID NOT IN (
                    SELECT  SampleID
                        FROM
                            active
                        WHERE
                            Account = "CSC108"
                            AND User = "doleynik"
                            AND JobName LIKE "SAGA-Python-PBSJobScript.%"
            )
        ;
        """

    for row in cursor.execute(query):
        no_csc108 = row["n"]

    query = """
        SELECT  count(DISTINCT SampleID) AS n
            FROM
                active
        ;
        """

    for row in cursor.execute(query):
        total = row["n"]

    print "Number of samples without CSC108 backfill present: %s" % no_csc108
    print "Number of total samples: %s" % total

    percent_not_present = 100.0 * no_csc108 / total

    print "%% where CSC108 is not using backfill: %s" % percent_not_present
    print "%% where CSC108 is using backfill: %s" % (100 - percent_not_present)

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
