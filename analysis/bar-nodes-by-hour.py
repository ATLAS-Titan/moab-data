#-  Python 2.7 source code

#-  bar-nodes-by-hour.py ~~
#
#   This self-contained program creates a bar plot that shows the average use
#   of nodes by CSC108 backfill jobs by hour of the day, in the OLCF timezone.
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 12 Jul 2018
#                                                   ~~ last updated 16 Jul 2018

from datetime import datetime

import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  strftime("%H", SampleTime, "unixepoch", "localtime") AS hour,
                avg(ReqProcs / 16) AS nodes
            FROM showq_active
            WHERE
                Account = "CSC108"
                AND
                User = "doleynik"
            GROUP BY hour
        ;
        """

    hours = []
    nodes = []
    for row in cursor.execute(query):
        hours.append(int(row["hour"]))
        nodes.append(row["nodes"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.bar(hours, nodes, align = "center")
    pyplot.title("Node Usage by Time of Day for CSC108 Backfill")

    pyplot.xticks(range(0, 24, 3),
        ("12 AM", "3 AM", "6 AM", "9 AM", "12 PM", "3 PM", "6 PM", "9 PM"))

    pyplot.ylabel("Average Nodes")

    ax.xaxis.grid(True)
    pyplot.grid()

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
