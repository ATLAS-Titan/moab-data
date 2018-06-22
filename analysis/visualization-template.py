#-  Python 2.7 source code

#-  visualization-template.py ~~
#
#   This program is just a template with some useful examples shown, so that
#   it's easy to start modifying the program in order to explore the data. It
#   will not run correctly on OLCF machines unless the appropriate module has
#   been loaded beforehand:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 22 Jun 2018
#                                                   ~~ last updated 22 Jun 2018

import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT time, sum(ReqProcs) AS procs
            FROM showq_active
            WHERE Account="CSC108" AND User_="doleynik"
            GROUP BY time;
        """

    times = []
    procs = []
    for row in cursor.execute(query):
        times.append(row["time"])
        procs.append(row["procs"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    ax.plot(times, procs, linestyle="none", marker="o")

    ax.set(xlabel="Unix Time", ylabel="Processors",
        title="CSC108 Backfill Usage")
    ax.grid()

    fig.savefig("csc108-backfill-usage.png")

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
