#-  Python 3 source code

#-  plot-moving-avg-local-active-nodes.py ~~
#
#   This program computes a moving average of "local active nodes" on Titan,
#   partly as an experiment for me to learn how to phrase this with SQL and
#   partly because some of these plots may benefit from smoothing to make it
#   easier for humans to spot the patterns.
#
#   In the end, though, the data for the LocalActiveNodes appears to have such
#   deep, narrow downward spikes that smoothing it too much fails to represent
#   the data correctly.
#
#   As always with the programs that produce visuals, remember to load the
#   appropriate module when running on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 20 Jul 2018
#                                                   ~~ last updated 04 Dec 2018

from datetime import datetime

import matplotlib
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  SampleTime AS time,
                (SELECT avg(LocalActiveNodes)
                    FROM cluster
                    WHERE
                     -- One hour window preceding the sample
                        SampleTime > (active.SampleTime - 1*60*60)
                        AND
                        SampleTime <= active.SampleTime
                ) AS nodes
            FROM
                active
            WHERE
             -- Last seven days' data
                SampleTime > (strftime("%s", "now") - 7*24*60*60)
            ORDER BY
                SampleTime
        ;
        """

    nodes = []
    times = []
    for row in cursor.execute(query):
        nodes.append(row["nodes"])
      # Need to convert Unix time (in seconds) into Python `datetime` object
        times.append(datetime.utcfromtimestamp(row["time"]))

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    ax.plot_date(times, nodes, linestyle="-", marker=".")

  # Angle the x-axis labels so that the dates don't overlap so badly
    pyplot.gcf().autofmt_xdate()

    ax.set(ylabel="Local Active Nodes", title="Titan Node Data")
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
        raise Exception("Data directory not found.")

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
