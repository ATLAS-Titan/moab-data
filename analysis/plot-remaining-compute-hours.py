#-  Python 2.7 source code

#-  plot-remaining-compute-hours.py ~~
#
#   This self-contained program visualizes the live "compute hours remaining"
#   for CSC108 backfill jobs on Titan, which is defined as the sum of the
#   products of the active jobs' requested processors with their remaining
#   requested walltime. This is intended to provide an estimate for how many
#   total resources have been set claimed by CSC108 at a given sample time.
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 12 Jul 2018
#                                                   ~~ last updated 12 Jul 2018

from datetime import datetime

import matplotlib
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  SampleID,
                SampleTime,
                sum(ReqProcs *
                    (StartTime + ReqAWDuration - SampleTime) / 3600.0) AS hrs
            FROM showq_active
            WHERE
                Account = "CSC108"
                AND
                User = "doleynik"
            GROUP BY SampleID
        """

    times = []
    compute_hours = []
    for row in cursor.execute(query):
        compute_hours.append(row["hrs"])
      # Need to convert Unix time (in seconds) into Python `datetime` object
        times.append(datetime.utcfromtimestamp(row["SampleTime"]))

    #print "min compute hours: %s" % min(compute_hours)
    #print "max compute hours: %s" % max(compute_hours)
    #print "mean compute hours: %s" % (sum(compute_hours) / len(compute_hours))

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    ax.plot_date(times, compute_hours, linestyle="none", marker="o")

  # Angle the x-axis labels so that the dates don't overlap so badly
    pyplot.gcf().autofmt_xdate()

    ax.grid()

    pyplot.title("CSC108 Backfill Remaining Allocated Compute Hours")
    pyplot.ylabel("Total Compute Hours")

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
