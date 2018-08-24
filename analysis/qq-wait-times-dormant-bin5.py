#-  Python 2.7 source code

#-  qq-wait-times-dormant-bin5.py ~~
#
#   This program creates a Quantile-Quantile plot (or more accurately here, a
#   Percentile-Percentile plot) of the distributions for the wait times of
#   bin 5 jobs submitted by non-CSC108 projects during the two weeks prior to
#   the "dormant" period from July 21 through August 4, when CSC108 was not
#   running ATLAS jobs, versus the wait times experienced during the dormant
#   period itself.
#
#   This program will not run correctly on OLCF machines unless the appropriate
#   module has already been loaded:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 24 Aug 2018
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

    query = """
        SELECT DISTINCT
                    JobID,
                    (StartTime - SubmissionTime) AS WaitTime
            FROM
                active
            WHERE
                (Account != "CSC108" OR User != "doleynik")

                AND SubmissionTime <= StartTime

             -- An estimate for July 7 through July 21
                AND (1530939600 < SampleTime AND SampleTime < 1532149200)

                AND ((ReqNodes IS NULL
                        AND (ReqProcs / 16) <= 125)
                    OR (ReqNodes IS NOT NULL
                        AND ReqNodes <= 125))
        ;
        """

    with_csc108 = []
    for row in cursor.execute(query):
        with_csc108.append(row["WaitTime"])

  # Now we will change the query to find WaitTimes for jobs that ran while
  # CSC108 was "dormant".

    query = """
        SELECT DISTINCT JobID,
                (StartTime - SubmissionTime) AS WaitTime
            FROM
                active
            WHERE
                (Account != "CSC108" OR User != "doleynik")
                AND SubmissionTime < StartTime
             -- An estimate for July 21 through August 4
                AND (1532149200 < SampleTime AND SampleTime < 1533358800)
                AND ((ReqNodes IS NULL
                        AND (ReqProcs / 16) <= 125)
                    OR (ReqNodes IS NOT NULL
                        AND ReqNodes <= 125))
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

    ax.set(xlabel = "Pre-Dormant (July 7 - 21)",
        ylabel = "Dormant (July 21 - August 4)",
        title = "QQ Plot: Wait Times for Bin 5 Jobs for Fixed Time Periods")
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
