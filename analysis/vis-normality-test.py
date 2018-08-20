#-  Python 2.7 source code

#-  vis-normality-test.py ~~
#
#   This program provides a visual test of normality for a distribution by
#   using SciPy and Matplotlib. This idea is so related to QQ plots that I
#   initially committed this as "qq-plot-template.py".
#
#   Note that this program will not run correctly on OLCF machines unless the
#   appropriate module has already been loaded:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 20 Aug 2018
#                                                   ~~ last updated 20 Aug 2018

from datetime import datetime
import matplotlib
import matplotlib.pyplot as pyplot
import os
import scipy.stats as stats
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT sum(ReqProcs) AS procs
            FROM active
            WHERE Account="CSC108" AND User="doleynik"
            GROUP BY SampleID;
        """

    procs = []
    for row in cursor.execute(query):
        procs.append(row["procs"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    stats.probplot(procs, plot = ax)

    ax.set(ylabel = "Total Processors", title = "Normality Test")
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
