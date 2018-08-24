#-  Python 2.7 source code

#-  qq-wait-times-small.py ~~
#
#   This program creates a Quantile-Quantile plot (or more accurately here, a
#   Percentile-Percentile plot) of the distributions for the wait times of
#   "small" jobs submitted by non-CSC108 projects, with and without CSC108
#   running active jobs in backfill.
#
#   Here, I have defined "small" as any job that is not "big", which means all
#   bin 3, bin 4, and bin 5 jobs, which is any job using 3749 nodes or less.
#
#   This program will not run correctly on OLCF machines unless the appropriate
#   module has already been loaded:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 23 Aug 2018
#                                                   ~~ last updated 24 Aug 2018

from datetime import datetime
import matplotlib
import matplotlib.pyplot as pyplot
import numpy
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # First, gather data that represents two distributions. This query computes
  # WaitTime for "small" jobs submitted by projects other than CSC108, when
  # CSC108 is running jobs in backfill. It checks to make sure that the various
  # time-related fields are properly sequenced and returns rows representing
  # other projects' jobs which were submitted while CSC108 had at least one
  # active job running in backfill. I would argue that there is no need to
  # consider the CSC108 jobs' submission times because they always have lower
  # priority than other jobs.

  # Note that there is significantly more data available if I use the "active"
  # table instead of "completed", but the query runs much slower in that case.
  # This will instead serve as a placeholder for new plots that will be created
  # once dates are chosen for specific time intervals.

    query = """
        SELECT DISTINCT
                    B.JobID,
                    (B.StartTime - B.SubmissionTime) AS WaitTime
            FROM
                completed A, completed B
            WHERE
                A.JobID != B.JobID
                AND (A.Account = "CSC108" AND A.User = "doleynik")
                AND (B.Account != "CSC108" OR B.User != "doleynik")

                AND ((B.ReqNodes IS NULL
                        AND (B.ReqProcs / 16) <= 3749)
                    OR (B.ReqNodes IS NOT NULL
                        AND B.ReqNodes <= 3749))

                AND A.SubmissionTime <= A.StartTime
                AND A.StartTime <= A.CompletionTime
                AND B.SubmissionTime <= B.StartTime
                AND B.StartTime <= B.CompletionTime

                AND A.StartTime <= B.SubmissionTime
                AND B.SubmissionTime <= A.CompletionTime

                --AND (B.StartTime - B.SubmissionTime < 200000)
        ;
        """

    with_csc108 = []
    for row in cursor.execute(query):
        with_csc108.append(row["WaitTime"])

  # Now we will change the query to find WaitTimes for jobs that ran while
  # CSC108 was inactive.

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
                AND ((ReqNodes IS NULL
                        AND (ReqProcs / 16) <= 3749)
                    OR (ReqNodes IS NOT NULL
                        AND ReqNodes <= 3749))
                AND JobID NOT IN (SELECT * FROM conflicting)
                AND SubmissionTime < StartTime
        ;
        """

    wo_csc108 = []
    for row in cursor.execute(query):
        wo_csc108.append(row["WaitTime"])

  # Next, compute the percentiles or quantiles. It really doesn't matter which,
  # because we are only going to use those to relate the two distributions. I
  # will just call them "marks".

    marks_to_use = range(10, 90)
    marks_with = numpy.percentile(with_csc108, marks_to_use)
    marks_wo = numpy.percentile(wo_csc108, marks_to_use)

  # Create the QQ plot.

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.plot(marks_with, marks_wo, 'bo')

    ax.set(xlabel = "With CSC108 Running", ylabel = "Without CSC108 Running",
        title = "QQ Plot: Wait Times for Small Jobs")
    ax.grid()

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
