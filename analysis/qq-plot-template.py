#-  Python 2.7 source code

#-  qq-plot-template.py ~~
#
#   This program provides a template with which to construct Quantile-Quantile
#   (QQ) plots via NumPy and Matplotlib. This program will not run correctly on
#   OLCF machines unless the appropriate module has already been loaded:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 20 Aug 2018
#                                                   ~~ last updated 20 Aug 2018

from datetime import datetime
import matplotlib
import matplotlib.pyplot as pyplot
import numpy
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # First, gather data that represents two distributions.

    query = """
        SELECT ReqProcs AS procs
            FROM active
            WHERE Account = "CSC108" AND User = "doleynik"
        """

    csc108_procs = []
    for row in cursor.execute(query):
        csc108_procs.append(row["procs"])

    query = """
        SELECT ReqProcs AS procs
            FROM active
            WHERE (Account != "CSC108" OR User != "doleynik")
        """

    other_procs = []
    for row in cursor.execute(query):
        other_procs.append(row["procs"])

  # Next, compute the percentiles or quantiles. It really doesn't matter which,
  # because we are only going to use those to relate the two distributions. I
  # will just call them "marks".

    marks_to_use = range(10, 90)
    csc108_marks = numpy.percentile(csc108_procs, marks_to_use)
    other_marks = numpy.percentile(other_procs, marks_to_use)

    print csc108_marks

  # Create the QQ plot.

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.plot(csc108_marks, other_marks, 'bo')

    ax.set(xlabel = "CSC108", ylabel = "Other", title = "QQ Plot: Processors")
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
