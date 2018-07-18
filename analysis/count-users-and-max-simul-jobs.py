#-  Python 2.6 source code

#-  count-users-and-max-simul-jobs.py ~~
#
#   This program counts the number of users who ran 10 or more simultaneous
#   jobs in the last 30 days, and it also returns the highest number of
#   simultaneous jobs for that time period. It also returns a lot of extra
#   fields because this program is the saved result of an interactive
#   exploration.
#
#                                                       ~~ (c) SRW, 18 Jul 2018
#                                                   ~~ last updated 18 Jul 2018

import json
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT count(DISTINCT User) AS users, max(jobs) AS jobs
            FROM (
                SELECT  User,
                        Account,
                        SampleID,
                        strftime("%m-%d-%Y %H:%M", SampleTime, "unixepoch",
                            "localtime") AS t,
                        count(*) AS jobs,
                        Sum(ReqProcs) AS procs
                    FROM showq_active
                    WHERE
                        SampleTime > (strftime("%s", "now") - 30*24*60*60)
                    GROUP BY
                        User, SampleID
                    ORDER BY
                        SampleTime
            ) usage
            WHERE
                usage.jobs >= 10
        ;
        """

    for row in cursor.execute(query):
        jobs = row["jobs"]
        users = row["users"]
        print "Users with 10 or more simul. jobs in last 30 days: %s" % users
        print "Max number of simultaneous jobs in last 30 days: %s" % jobs

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

  # Run custom analyis code.

    analyze(connection)

  # Commit any changes and close the connection to the database.

    connection.commit()
    connection.close()

###

if __name__ == "__main__":
    main()

#-  vim:set syntax=python:
