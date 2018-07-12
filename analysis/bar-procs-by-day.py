#-  Python 2.7 source code

#-  bar-procs-by-day.py ~~
#
#   This self-contained program creates a bar plot that shows the average use
#   of processors by CSC108 backfill jobs by day of the week, in the OLCF
#   timezone.
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 12 Jul 2018
#                                                   ~~ last updated 12 Jul 2018

from datetime import datetime

import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT  strftime("%w", SampleTime, "unixepoch", "localtime") AS day,
                avg(ReqProcs) AS procs
            FROM showq_active
            WHERE
                Account = "CSC108"
                AND
                User = "doleynik"
            GROUP BY day
        ;
        """

    days_of_week = []
    procs = []
    for row in cursor.execute(query):
        days_of_week.append(int(row["day"]))
        procs.append(row["procs"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.bar(days_of_week, procs, align = "center")
    pyplot.title("Processor Usage by Day of Week for CSC108 Backfill")

    pyplot.xticks(range(7), ("S", "M", "T", "W", "T", "F", "S"))

    pyplot.ylabel("Average Processors")

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
