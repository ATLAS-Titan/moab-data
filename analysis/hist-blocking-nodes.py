#-  Python 2.7 source code

#-  hist-blocking-nodes.py ~~
#
#   This program plots a histogram representing the distribution of CSC108
#   backfill jobs' total nodes in use which were running at the time of a
#   "block".
#
#   As always, remember to use the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 11 Jul 2018
#                                                   ~~ last updated 17 Aug 2018

import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # NOTE: I don't think that using `NOT IN` in place of `IN` will be sufficient
  # to find the set complement, which is jobs which never blocked at any point.

    query = """
        SELECT DISTINCT csc108.JobID, (csc108.ReqProcs / 16) AS nodes
            FROM (
                SELECT SampleID, JobID, ReqProcs
                    FROM active
                    WHERE Account = "CSC108" AND User = "doleynik"
            ) csc108
            WHERE
                csc108.SampleID IN (
                    SELECT DISTINCT eligible.SampleID
                        FROM eligible
                        INNER JOIN (
                            SELECT SampleID, sum(ReqProcs) AS procs
                                FROM active
                                WHERE Account="CSC108" AND User="doleynik"
                                GROUP BY SampleID
                        ) bp ON eligible.SampleID = bp.SampleID
                        INNER JOIN backfill ON
                            backfill.SampleID = eligible.SampleID
                        WHERE
                            eligible.ReqAWDuration < backfill.duration
                            AND
                            eligible.ReqProcs < (backfill.proccount + bp.procs)
                            AND
                            backfill.starttime = backfill.SampleTime
                            AND
                            eligible.EEDuration > 0
                            AND
                            eligible.Class = "batch"
                )
        ;
        """

    nodes = []
    for row in cursor.execute(query):
        nodes.append(math.log10(row["nodes"]))

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(nodes, 50, facecolor="b", alpha=0.75)

    locs, labels = pyplot.xticks()
    new_labels = []
    for a, b in zip(locs, labels):
        new_labels.append(str(int(round(math.pow(10, a)))))

    pyplot.xticks(locs, new_labels)

    pyplot.xlabel("Nodes (log-scaled)")
    pyplot.ylabel("Jobs")
    pyplot.title("Histogram of Blocking CSC108 Backfill Jobs' Nodes")
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
