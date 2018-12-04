#-  Python 3 source code

#-  count-small-jobs-by-user.py ~~
#
#   This program answers a question that arose from answering the following:
#
#   Find the SampleID at which a user was running his/her highest number of
#   concurrent jobs within the last 30 days, along with the number of nodes
#   being used by those jobs.
#
#   While answering this question, results showed that some users were running
#   large numbers of so-called "bin 5" jobs, which use less than 125 nodes. The
#   official OLCF policy only allows a user to run 2 such jobs simultaneously. 
#
#                                                       ~~ (c) SRW, 19 Jul 2018
#                                                   ~~ last updated 04 Dec 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        WITH bin5 AS (
            SELECT  *,
                    CASE
                        WHEN ReqNodes IS NULL THEN
                            ReqProcs / 16
                        ELSE
                            ReqNodes
                    END nodes
                FROM active
                WHERE
                    nodes <= 125
        )
        SELECT  strftime("%m-%d-%Y %H:%M", SampleTime, "unixepoch",
                                                            "localtime") AS t,
                SampleID,
                Account,
                User,
                max(total_jobs) AS jobs,
                total_nodes AS nodes
            FROM (
                SELECT  *,
                        count(*) AS total_jobs,
                        sum(nodes) AS total_nodes
                    FROM bin5
                    GROUP BY
                        SampleID, User
                    HAVING
                        total_jobs > 2
            )
            WHERE
                SampleTime > (strftime("%s", "now") - 30*24*60*60)
            GROUP BY User
            ORDER BY Account
        ;
        """

    for row in cursor.execute(query):
        print("%s|%s|%s|%s|%s|%s" % (row["t"], row["SampleID"], row["Account"],
                row["User"], row["jobs"], row["nodes"]))

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
