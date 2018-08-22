#-  Python 2.7 source code

#-  hist-wait-times-all-other-wo-csc108.py ~~
#
#   This creates a histogram to visualize the distribution of the wait times
#   for all jobs not belonging to CSC108 which were submitted while CSC108 was
#   not running any active jobs in backfill.
#
#   As always, remember to run the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 22 Aug 2018
#                                                   ~~ last updated 22 Aug 2018

from datetime import datetime

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    start_of_month = datetime.today().replace(day=1, hour=0, minute=0)

  # The following query computes WaitTime for projets other than CSC108, when
  # CSC108 is running jobs in backfill. It checks to make sure that the various
  # time-related fields are properly sequenced and returns rows representing
  # other projects' jobs which were submitted while CSC108 had at least one
  # active job running in backfill. I would argue that there is no need to
  # consider the CSC108 jobs' submission times because they always have lower
  # priority than other jobs.

    query = """
        WITH conflicting AS (
            SELECT DISTINCT
                        B.JobID
                FROM
                    completed A, completed B
                WHERE
                    A.JobID != B.JobID
                    AND (A.Account = "CSC108" AND A.User = "doleynik")
                    AND (B.Account != "CSC108" OR B.User != "doleynik")
                    AND A.SubmissionTime <= A.StartTime
                    AND A.StartTime <= A.CompletionTime
                    AND B.SubmissionTime <= B.StartTime
                    AND B.StartTime <= B.CompletionTime
                    AND A.StartTime <= B.SubmissionTime
                    AND B.SubmissionTime <= A.CompletionTime
        )
        SELECT DISTINCT JobID,
                (StartTime - SubmissionTime) AS WaitTime
            FROM completed
            WHERE
                (Account != "CSC108" OR User != "doleynik")
                AND JobID NOT IN (SELECT * FROM conflicting)
                AND SubmissionTime < StartTime
        ;
        """

    waits = []
    for row in cursor.execute(query):
        waits.append(row["WaitTime"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(waits, 50, facecolor = "b", alpha = 0.75)

    pyplot.xlabel("Wait Time (seconds)")
    pyplot.ylabel("Number of Samples")
    pyplot.title("Wait Times for All Other Jobs While CSC108 Is Dormant")
    pyplot.grid(True)

    current_script = os.path.basename(__file__)
    fig.savefig(os.path.splitext(current_script)[0] + ".png", dpi = 300)

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
