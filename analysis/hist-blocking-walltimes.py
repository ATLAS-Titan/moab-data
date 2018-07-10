#-  Python 2.7 source code

#-  hist-blocking-walltimes.py ~~
#
#   This program plots a histogram representing the distribution of CSC108
#   backfill jobs' walltimes which were running at the time of a "block".
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 10 Jul 2018
#                                                   ~~ last updated 10 Jul 2018

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT DISTINCT csc108.JobID, csc108.ReqAWDuration AS walltime
            FROM (
                SELECT SampleTime, JobID, ReqAWDuration
                    FROM showq_active
                    WHERE
                        Account = "CSC108"
                        AND
                        User = "doleynik"
            ) csc108
            WHERE
                csc108.SampleTime IN (
                    SELECT DISTINCT showq_eligible.SampleTime
                        FROM showq_eligible
                        INNER JOIN (
                            SELECT SampleTime, sum(ReqProcs) AS procs
                                FROM showq_active
                                WHERE
                                    Account="CSC108"
                                    AND
                                    User="doleynik"
                                GROUP BY SampleTime
                        ) bp ON showq_eligible.SampleTime = bp.SampleTime
                        INNER JOIN showbf ON
                            showbf.SampleTime = showq_eligible.SampleTime
                        WHERE
                            showq_eligible.ReqAWDuration < showbf.duration
                            AND
                            showq_eligible.ReqProcs < (showbf.proccount +
                                                                    bp.procs)
                            AND
                            showbf.starttime = showbf.SampleTime
                            AND
                            showq_eligible.EEDuration > 0
                )
        ;
        """

    walltimes = []
    for row in cursor.execute(query):
        walltimes.append(row["walltime"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(walltimes, 50, facecolor="b", alpha=0.75)

    pyplot.xlabel("Walltime (not sure about units)")
    pyplot.ylabel("Number of Jobs")
    pyplot.title("Histogram of Blocking CSC108 Backfill Jobs' Walltimes")
    pyplot.grid(True)

    current_script = os.path.basename(__file__)
    fig.savefig(os.path.splitext(current_script)[0] + ".png")

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
