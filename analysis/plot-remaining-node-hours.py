#-  Python 2.7 source code

#-  plot-remaining-node-hours.py ~~
#
#   This program visualizes the live "node hours remaining" for CSC108 backfill
#   jobs on Titan, which is defined as the sum of the products of the active
#   jobs' requested processors with their remaining requested walltime. This is
#   intended to provide an estimate for how many total resources have been set
#   claimed by CSC108 at a given sample time.
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 12 Jul 2018
#                                                   ~~ last updated 24 Jul 2018

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
                sum((ReqProcs / 16) *
                    (StartTime + ReqAWDuration - SampleTime) / 3600.0) AS hrs
            FROM showq_active
            WHERE
                Account = "CSC108"
                AND
                User = "doleynik"
            GROUP BY SampleID
        """

    times = []
    node_hours = []
    for row in cursor.execute(query):
        node_hours.append(row["hrs"])
      # Need to convert Unix time (in seconds) into Python `datetime` object
        times.append(datetime.utcfromtimestamp(row["SampleTime"]))

    #print "min node hours: %s" % min(node_hours)
    #print "max node hours: %s" % max(node_hours)
    #print "avg node hours: %s" % (sum(node_hours) / len(node_hours))

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    ax.plot_date(times, node_hours, linestyle="none", marker="o")

  # Angle the x-axis labels so that the dates don't overlap so badly
    pyplot.gcf().autofmt_xdate()

    ax.grid()

    pyplot.title("CSC108 Backfill Remaining Allocated Node Hours")
    pyplot.ylabel("Total Node Hours")

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
