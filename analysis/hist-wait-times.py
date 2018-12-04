#-  Python 3 source code

#-  hist-wait-times.py ~~
#
#   As always, remember to run the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 21 Aug 2018
#                                                   ~~ last updated 04 Dec 2018

from datetime import datetime

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # Note that simple queries like this one still run reasonably quickly when
  # using the "active" table instead of "completed", but everything falls apart
  # in other programs when the queries get complicated.

    query = """
        SELECT  DISTINCT JobID,
                (StartTime - SubmissionTime) AS WaitTime
            FROM
                active
                --completed
            WHERE
                SubmissionTime <= StartTime
                --AND (ReqProcs / 16) > 3749
                --AND CompletionCode = "0"
        ;
        """

    waits = []
    for row in cursor.execute(query):
        waits.append(row["WaitTime"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(waits, 50, facecolor = "b", alpha = 0.75)

    pyplot.xlabel("Wait Time (seconds)")
    pyplot.ylabel("Number of Samples")
    pyplot.title("Distribution of Wait Times for All Jobs on Titan")
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
