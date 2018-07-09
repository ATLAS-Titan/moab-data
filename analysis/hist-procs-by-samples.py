#-  Python 2.7 source code

#-  hist-procs-by-samples.py ~~
#
#   This program plots a distribution of samples of log-scaled total processors
#   in use by CSC108 backfill. Note that this is a distribution of *samples*,
#   not a distribution of *jobs*.
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 09 Jul 2018
#                                                   ~~ last updated 09 Jul 2018

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT sum(ReqProcs) AS procs
            FROM showq_active
            WHERE Account="CSC108" AND User="doleynik"
            GROUP BY SampleTime;
        """

    procs = []
    for row in cursor.execute(query):
        procs.append(math.log10(row["procs"]))

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(procs, 50, facecolor="b", alpha=0.75)

    locs, labels = pyplot.xticks()
    new_labels = []
    for a, b in zip(locs, labels):
        new_labels.append(str(int(round(math.pow(10, a)))))

    pyplot.xticks(locs, new_labels)

    pyplot.xlabel("Processors (log-scaled)")
    pyplot.ylabel("Samples")
    pyplot.title("Histogram of CSC108 Backfill Processors")
    pyplot.grid(True)

    fig.savefig("hist-procs-by-samples.png")

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
