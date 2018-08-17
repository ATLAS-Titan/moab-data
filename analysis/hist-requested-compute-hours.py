#-  Python 2.7 source code

#-  hist-requested-compute-hours.py ~~
#
#   This program computes the distribution of CSC108 backfill jobs' "size".
#   Here, that is defined as the requested node hours for a job, which is
#   the product of each job's requested nodes and its requested walltime.
#
#   As always, remember to run the following on OLCF machines:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 11 Jul 2018
#                                                   ~~ last updated 17 Aug 2018

#import math
import matplotlib.pyplot as pyplot
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT DISTINCT JobID,
                        ((ReqProcs / 16.0) * ReqAWDuration / 3600.0) AS hours
            FROM active
            WHERE Account="CSC108" AND User="doleynik"
            GROUP BY SampleID;
        """

    sizes = []
    for row in cursor.execute(query):
        sizes.append(row["hours"])

    fig = pyplot.figure()
    ax = fig.add_subplot(111)

    pyplot.hist(sizes, 50, facecolor="b", alpha=0.75)

    #locs, labels = pyplot.xticks()
    #new_labels = []
    #for a, b in zip(locs, labels):
    #    new_labels.append(str(int(round(math.pow(10, a)))))
    #
    #pyplot.xticks(locs, new_labels)

    pyplot.xlabel("Node Hours")
    pyplot.ylabel("Number of Samples")
    pyplot.title("Histogram of CSC108 Backfill Total Requested Resources")
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
