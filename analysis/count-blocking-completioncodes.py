#-  Python 2.6 source code

#-  count-blocking-completioncodes.py ~~
#
#   This self-contained program answers the question, "what are the different
#   completion codes of jobs run by blocking CSC108 backfill jobs, and how many
#   times did each code appear?"
#
#                                                       ~~ (c) SRW, 25 Jul 2018
#                                                   ~~ last updated 25 Jul 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        WITH
            blocked AS (
                SELECT *
                    FROM eligible
                    INNER JOIN (
                        SELECT SampleID, sum(ReqProcs) AS procs
                            FROM csc108
                            GROUP BY SampleID
                    ) bp ON eligible.SampleID = bp.SampleID
                    INNER JOIN backfill ON
                        backfill.SampleID = eligible.SampleID
                    WHERE
                        eligible.ReqAWDuration < backfill.duration
                        AND
                        eligible.ReqProcs < (backfill.proccount + bp.procs)
                        AND
                        backfill.starttime = backfill.SampleTime
                        AND
                        eligible.EEDuration > 0
                        AND
                        eligible.Class = "batch"
            ),
            blocking AS (
                SELECT *
                    FROM csc108
                    WHERE SampleID IN (SELECT SampleID FROM blocked)
            ),
            csc108 AS (
                SELECT *
                    FROM active
                    WHERE Account = "CSC108" AND User = "doleynik"
            )
        SELECT CompletionCode AS code, count(CompletionCode) AS n
            FROM completed
            WHERE JobID IN (SELECT JobID FROM blocking)
            GROUP BY CompletionCode
        ;
        """

    for row in cursor.execute(query):
        print 'Code "%s" occurred %s time(s).' % (row["code"], row["n"])

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
