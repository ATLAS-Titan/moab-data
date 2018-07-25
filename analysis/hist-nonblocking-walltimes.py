#-  Python 2.7 source code

#-  hist-nonblocking-walltimes.py ~~
#
#   This program plots a histogram representing the distribution of CSC108
#   backfill jobs' walltimes which ran, presumably to successful completion,
#   without ever causing a "block".
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 11 Jul 2018
#                                                   ~~ last updated 25 Jul 2018

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        WITH
            csc108 AS (
                SELECT *
                    FROM active
                    WHERE
                        Account = "CSC108"
                        AND
                        User = "doleynik"
            ),
            blocked AS (
                SELECT *
                    FROM eligible
                    INNER JOIN (
                        SELECT SampleID, sum(ReqProcs) AS procs
                            FROM csc108
                            GROUP BY SampleID
                    ) bp ON eligible.SampleID = bp.SampleID
                    INNER JOIN backfill
                        ON backfill.SampleID = eligible.SampleID
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
            )
        SELECT DISTINCT JobID, ReqAWDuration AS walltime
            FROM csc108
            WHERE
                JobID NOT IN (
                    SELECT DISTINCT JobID
                        FROM csc108
                        WHERE
                            SampleID IN (
                                SELECT DISTINCT SampleID FROM blocked
                            )
                )
        ;
        """

    walltimes = []
    for row in cursor.execute(query):
        walltimes.append(row["walltime"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(walltimes, 50, facecolor="b", alpha=0.75)

    pyplot.xlabel("Walltime (seconds)")
    pyplot.ylabel("Number of Jobs")
    pyplot.title("Histogram of Non-blocking CSC108 Backfill Jobs' Walltimes")
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

  # Run custom analyis code.

    analyze(connection)

  # Commit any changes and close the connection to the database.

    connection.commit()
    connection.close()

###

if __name__ == "__main__":
    main()

#-  vim:set syntax=python:
