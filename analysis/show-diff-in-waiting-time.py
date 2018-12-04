#-  Python 3 source code

#-  show-diff-in-waiting-time.py ~~
#
#   This program provides a rough answer to the question, what is the
#   difference in jobs' wait times on Titan when CSC108 is running backfill
#   jobs and when CSC108 is not running backfill jobs?
#
#   It is hard-coded to investigate two particular time intervals for which
#   CSC108 was not running any jobs for approximately 100 hours each.
#
#                                                       ~~ (c) SRW, 08 Aug 2018
#                                                   ~~ last updated 04 Dec 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    def avg_wait(caption, left, right):

        query_template = """
            WITH other_jobs_during_drought AS (
                SELECT  *,
                        (StartTime - SubmissionTime) AS WaitTime
                    FROM
                        completed
                    WHERE
                        SubmissionTime <= StartTime
                        AND SubmissionTime >= {left}
                        AND StartTime <= {right}
            )
            SELECT  count(WaitTime) AS num_jobs,
                    avg(WaitTime) AS avg_wait
                FROM other_jobs_during_drought
            ;
            """

        query = query_template.format(left = left, right = right)
        results = {}

        for row in cursor.execute(query):
            print(caption)
            print("Avg wait: %s (%s jobs)" % (
                row["avg_wait"], row["num_jobs"]))

    ###

    avg_wait("With CSC108, interval length 349799:", 1531923003, 1532272802)

    avg_wait("Without CSC108, interval length 349799 immediately after:",
        1532272802, 1532622601)

    avg_wait("With CSC108, interval length 386700:", 1532596501, 1532983201)

    avg_wait( "Without CSC108, interval length 386700 immediately after:",
        1532983201, 1533369901)

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
