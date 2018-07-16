#-  Python 2.7 source code

#-  plot-total-nodes-by-time.py ~~
#
#   This program plots the total nodes in use by CSC108 backfill by time. As
#   always, this program may or may not run on OLCF machines until you have
#   invoked
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 25 Jun 2018
#                                                   ~~ last updated 16 Jul 2018

from datetime import datetime
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT SampleTime, sum(ReqProcs / 16) AS nodes
            FROM showq_active
            WHERE Account="CSC108" AND User="doleynik"
            GROUP BY SampleID;
        """

    times = []
    nodes = []
    for row in cursor.execute(query):
        nodes.append(row["nodes"])
      # Need to convert Unix time (in seconds) into Python `datetime` object
        times.append(datetime.utcfromtimestamp(row["SampleTime"]))

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    ax.plot_date(times, nodes, linestyle="none", marker="o")

  # Angle the x-axis labels so that the dates don't overlap so badly
    pyplot.gcf().autofmt_xdate()

    ax.set(ylabel="Total Nodes", title="CSC108 Backfill Usage")
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

  # Run custom analyis code.

    analyze(connection)

  # Commit any changes and close the connection to the database.

    connection.commit()
    connection.close()

###

if __name__ == "__main__":
    main()

#-  vim:set syntax=python:
