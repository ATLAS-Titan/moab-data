#-  Python 2.7 source code

#-  highlight-blocked-procs-by-time.py ~~
#
#   This program plots the total processors in use at a given sample time by
#   CSC108 backfill, and it highlights the ones which are blocking other jobs
#   by plotting them in red, while all other points are blue.
#
#   As always, remember to run the following if using an OLCF machine:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 25 Jun 2018
#                                                   ~~ last updated 02 Jul 2018

from datetime import datetime
import matplotlib
#import matplotlib.pyplot as pyplot
import matplotlib.pylab as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    blocked_query = """
        SELECT DISTINCT showq_eligible.SampleTime AS time,
                        csc108.procs AS procs
        FROM showq_eligible
        INNER JOIN (
            SELECT SampleTime, sum(ReqProcs) AS procs
                FROM showq_active
                WHERE Account="CSC108" AND User="doleynik"
                GROUP BY SampleTime
        ) csc108 ON showq_eligible.SampleTime=csc108.SampleTime
        INNER JOIN showbf ON
            showbf.SampleTime = showq_eligible.SampleTime
        INNER JOIN showq_meta ON
            showq_meta.SampleTime = showq_eligible.SampleTime
        WHERE
            showq_eligible.ReqAWDuration < showbf.duration
            AND
            showq_eligible.ReqProcs < (showbf.proccount + csc108.procs)
            AND
            showbf.starttime = showbf.SampleTime
            AND
            showq_eligible.EEDuration > 0
        ;
        """

    blocked_times = []
    procs = []
    times = []
    for row in cursor.execute(blocked_query):
        blocked_times.append(row["time"])
        procs.append(row["procs"])
        times.append(datetime.utcfromtimestamp(row["time"]))

  # Now print these results in red real quick.

    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    ax.plot_date(times, procs, linestyle="none", marker="o", color="red")

  # Now compute the rest of the results and add them to the figure in blue.

    all_query = """
        SELECT SampleTime, sum(ReqProcs) AS procs
            FROM showq_active
            WHERE Account="CSC108" AND User="doleynik"
            GROUP BY SampleTime;
        """

    procs = []
    times = []
    for row in cursor.execute(all_query):
      # Need to convert Unix time (in seconds) into Python `datetime` object
        if row["SampleTime"] not in blocked_times:
            procs.append(row["procs"])
            times.append(datetime.utcfromtimestamp(row["SampleTime"]))

    ax.plot_date(times, procs, linestyle="none", marker="o", color="blue")

  # Angle the x-axis labels so that the dates don't overlap so badly
    pyplot.gcf().autofmt_xdate()

    ax.set(ylabel="Total Processors", title="CSC108 Backfill Usage")
    ax.grid()

  # Add a legend

    red_patch = matplotlib.patches.Patch(color='red', label='Blocking')
    blue_patch = matplotlib.patches.Patch(color='blue', label='Non-blocking')

    pyplot.legend(handles=[red_patch, blue_patch], bbox_to_anchor=(1, 1),
           bbox_transform=pyplot.gcf().transFigure)


  # Now, adjust the output format and save plot as an image file.

    fig.savefig("highlight-blocked-procs-by-time.png", dpi = 300)

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
