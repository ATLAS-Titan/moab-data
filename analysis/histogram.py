#-  Python 2.7 source code

#-  histogram.py ~~
#
#   This program is a template for making histograms, and it contains a lot of
#   extra stuff for date manipulation also, for when I need that later.
#
#   As always, remember to run the following on OLCF machines:
#
#       $ module load python_anaconda
#
#                                                       ~~ (c) SRW, 09 Jul 2018
#                                                   ~~ last updated 11 Jul 2018

from datetime import datetime

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    start_of_month = datetime.today().replace(day=1, hour=0, minute=0)

    query = """
        SELECT sum(ReqProcs) AS procs
            FROM showq_active
            WHERE
                Account="CSC108"
                AND
                User="doleynik"
                AND
                SampleTime > {unix_start_of_month}
            GROUP BY SampleID;
        """.format(unix_start_of_month = int(start_of_month.strftime("%s")))

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
    pyplot.title("Histogram of CSC108 Backfill Processors This Month")
    pyplot.grid(True)

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
