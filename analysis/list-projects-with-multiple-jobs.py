#-  Python 2.6 source code

#-  list-projects-with-multiple-jobs.py ~~
#
#   This is in response to a request for a list of projects which have multiple
#   jobs running or submitted on Titan.
#
#                                                       ~~ (c) SRW, 04 Aug 2018
#                                                   ~~ last updated 04 Aug 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT DISTINCT
                Account,
                strftime("%m/%d/%Y %H:%M", SampleTime, "unixepoch",
                    "localtime") AS time,
                active_jobs,
                eligible_jobs,
                max(active_jobs + eligible_jobs) AS total_jobs
            FROM (
                SELECT  active.*,
                        count(DISTINCT active.JobID) AS active_jobs,
                        count(DISTINCT eligible.JobID) AS eligible_jobs
                    FROM
                        active
                    INNER JOIN
                        eligible ON
                            active.Account = eligible.Account
                            AND active.SampleID = eligible.SampleID
                    GROUP BY
                        active.Account, active.SampleID
                    HAVING
                        (active_jobs + eligible_jobs) > 1
                    ORDER BY
                        active.Account
            )
            GROUP BY
                Account
        ;
        """

    for row in cursor.execute(query):
      # Match the format that SQLite uses, because we can.
        print "%s|%s|%s|%s|%s" % (row["Account"], row["time"],
            row["active_jobs"], row["eligible_jobs"], row["total_jobs"])

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
