#-  Python 2.6 source code

#-  compute-avg-jobs-procs-csc108.py ~~
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 15 Jun 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT count(*) As Jobs, sum(ReqProcs) AS Procs FROM showq_active
            WHERE Account="CSC108" AND User_="doleynik"
            GROUP BY time;
        """

    jobs = []
    procs = []
    for row in cursor.execute(query):
        jobs.append(row["Jobs"])
        procs.append(row["Procs"])

    avg_jobs = int(round(float(sum(jobs)) / len(jobs)))
    avg_procs = int(round(float(sum(procs)) / len(procs)))
    print "CSC108 is typically running ~%s backfill jobs on ~%s processors." % \
        (avg_jobs, avg_procs)

###

def main():

  # Create string to represent path to database file.

    dbfilename = os.path.join(os.getenv("PWD"), "moab-data.sqlite")

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
