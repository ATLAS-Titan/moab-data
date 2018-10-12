#-  Python 2.7 source code

#-  plot-us-vs-them-throughput-bin4.py ~~
#
#   So, this program's name isn't the best...
#
#   This program visualizes the relationship between throughput of CSC108, in
#   units of jobs per day, and non-CSC108.
#
#   Remember to run the following beforehand:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 12 Oct 2018
#                                                   ~~ last updated 12 Oct 2018

from datetime import datetime

import matplotlib
import matplotlib.pyplot as pyplot
import os
import sqlite3

import numpy

###

def analyze(connection):

    cursor = connection.cursor()

  # First, we will count the number of jobs per day for "us" (CSC108).

    query = """
        SELECT  strftime("%m-%d-%Y", completed.CompletionTime, "unixepoch",
                    "localtime")
                        AS day,
                count(DISTINCT csc108.JobID)
                        AS us,
                count(DISTINCT completed.JobID) - count(DISTINCT csc108.JobID)
                        AS them
            FROM
                completed
            LEFT OUTER JOIN (
                SELECT  CompletionTime, 
                        JobID
                    FROM
                        completed
                    WHERE
                        Account = "CSC108"
                        AND User = "doleynik"
                        AND JobName LIKE "SAGA-Python-PBSJobScript.%"
            ) csc108 ON csc108.JobID = completed.JobID
            WHERE
                ((completed.ReqNodes IS NULL
                    AND 126 <= (completed.ReqProcs / 16)
                    AND (completed.ReqProcs / 16) <= 312)
                OR (completed.ReqNodes IS NOT NULL
                    AND 126 <= completed.ReqNodes
                    AND completed.ReqNodes <= 312))
            GROUP BY
                day

         -- Something is definitely wrong with part of the data, so for now,
         -- we have to filter it a little bit.
            HAVING
                (0 < us)
                AND (us < 1000)
        ;
        """

    jobs_us = []
    jobs_them = []
    for row in cursor.execute(query):
        jobs_us.append(row["us"])
        jobs_them.append(row["them"])
        print "%s: (%s, %s)" % (row["day"], row["us"], row["them"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    x = jobs_us
    y = jobs_them

    fit = numpy.polyfit(x, y, 1)
    fit_fn = numpy.poly1d(fit)

    ax.plot(x, y, "bo", x, fit_fn(x), "--r")

    ax.set(xlabel = "CSC108 Throughput (jobs per day)",
        ylabel = "Non-CSC108 Throughput (jobs per day)",
        title = "Dependency of Other Projects' \"Bin 4\" Throughput on CSC108"
    )
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
