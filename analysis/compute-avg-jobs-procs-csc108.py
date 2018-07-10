#-  Python 2.6 source code

#-  compute-avg-jobs-procs-csc108.py ~~
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 10 Jul 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT count(*) As Jobs, sum(ReqProcs) AS Procs FROM showq_active
            WHERE Account="CSC108" AND User="doleynik"
            GROUP BY SampleID;
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
