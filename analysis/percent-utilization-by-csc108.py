#-  Python 2.6 source code

#-  percent-utilization-by-csc108.py ~~
#
#   This program is another step toward the ultimate goal of computing the
#   "blocking probability" of CSC108 backfill on Titan. This program computes
#   the percent utilization of available processors on Titan by CSC108 backfill
#   as a way to check that the numbers are coming out as expected.
#
#                                                       ~~ (c) SRW, 20 Jun 2018
#                                                   ~~ last updated 28 Jun 2018

import json
import os
import sqlite3

###

def mean(x):
    return sum(x)/len(x)

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  count(showq_active.JobID) AS jobs,
                sum(showq_active.ReqProcs) AS procs,
                showq_meta.LocalUpProcs AS total_procs
        FROM showq_active
        INNER JOIN showq_meta ON showq_active.SampleTime=showq_meta.SampleTime
        WHERE showq_active.Account="CSC108" AND showq_active.User="doleynik"
        GROUP BY showq_active.SampleTime;
        """

    jobs = []
    percents = []
    procs = []
    total_procs = []
    for row in cursor.execute(query):
        percent = 100 * float(row["procs"]) / row["total_procs"]
        #print "%s jobs using %s/%s processors (%s)" %\
        #    (row["jobs"], row["procs"], row["total_procs"], percent)

        jobs.append(row["jobs"])
        percents.append(percent)
        procs.append(row["procs"])
        total_procs.append(row["total_procs"])

    def prettyprint(title, x):
        print "%s: %s" % \
            (title, json.dumps({"max": max(x), "mean": mean(x), "min": min(x)},
                indent=4))

    prettyprint("Jobs on Titan by CSC108", jobs)
    prettyprint("Processors in use by CSC108", procs)
    prettyprint("Processors available on Titan", total_procs)
    prettyprint("Percent utilization by CSC108", percents)

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
