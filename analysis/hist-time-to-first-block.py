#-  Python 2.7 source code

#-  hist-time-to-first-block.py ~~
#
#   This program provides a solution to the following problem:
#
#   Find the distribution of "time to first block" for CSC108 backfill jobs
#   that cause blocks, where "time to first block" is defined as the smallest
#   interval between the start time of the job and the sample time of a sample
#   in which blocks were observed.
#
#   As always, remember to run the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 26 Jul 2018
#                                                   ~~ last updated 17 Aug 2018

import matplotlib.pyplot as pyplot
from matplotlib.ticker import MaxNLocator
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        WITH
            blocked AS (
                SELECT *
                    FROM eligible
                    INNER JOIN (
                        SELECT SampleID, sum(ReqProcs) AS procs
                            FROM csc108
                            GROUP BY SampleID
                    ) bp ON eligible.SampleID = bp.SampleID
                    INNER JOIN backfill ON
                        backfill.SampleID = eligible.SampleID
                    WHERE
                        eligible.ReqAWDuration < backfill.duration
                        AND
                        eligible.ReqProcs < (backfill.proccount + bp.procs)
                        AND
                        backfill.starttime = backfill.SampleTime
                        AND
                        eligible.EEDuration > 0
                        AND
                        eligible.Class = "batch"
            ),

            blocking AS (
                SELECT *
                    FROM csc108
                    WHERE SampleID IN (SELECT SampleID FROM blocked)
            ),

            csc108 AS (
                SELECT *
                    FROM active
                    WHERE Account = "CSC108" AND User = "doleynik"
            )

        SELECT min(SampleTime - StartTime) AS ttfb
            FROM blocking
            GROUP BY JobID
        ;
        """

    times = []
    for row in cursor.execute(query):
        times.append(row["ttfb"])

    print len(times)

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(times, 50, facecolor="b", alpha=0.75)

  # Force the y axis to use integers at the tick marks.
    ax.yaxis.set_major_locator(MaxNLocator(integer = True))

    pyplot.xlabel("Length of time interval (seconds)")
    pyplot.ylabel("Number of Jobs")
    pyplot.title("Time to First Block for CSC108 Backfill Jobs")
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
