#-  Python 2.6 source code

#-  eligible-for-bf-but-idle.py ~~
#
#   This program demonstrates some results which are confusing to me, because
#   they indicate that, within the dataset, there exist jobs which sat in the
#   eligible queue on Titan despite fitting into backfill, based on requested
#   walltime duration and requested number of processors.
#
#                                                       ~~ (c) SRW, 21 Jun 2018
#                                                   ~~ last updated 28 Jun 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # Some definitions on the fields used below:
  #
  # - EEDuration: the duration of time the job has been eligible for scheduling
  # - ReqAWDuration: requested active walltime duration
  # - ReqProcs: requested processors

    query = """
        SELECT count(DISTINCT showq_eligible.JobID) AS n
            FROM showq_eligible
            INNER JOIN showbf ON showbf.SampleTime = showq_eligible.SampleTime
            WHERE
                showq_eligible.ReqAWDuration < showbf.duration
                AND
                showq_eligible.ReqProcs < showbf.proccount
                AND
                showbf.starttime = showbf.SampleTime
                AND
                showq_eligible.EEDuration > 0;
        """

    num_eligible = 0
    for row in cursor.execute(query):
        num_eligible = row["n"]
        print "Number eligible but idle: %s" % num_eligible

    query = """
        SELECT count(DISTINCT showq_eligible.JobID) AS n FROM showq_eligible;
        """

    num_total = 0
    for row in cursor.execute(query):
        num_total = row["n"]
        print "Number of distinct job IDs: %s" % num_total

    print "=> This has occurred for ~ %s%% of all jobs." % \
        round(100.0 * num_eligible / num_total, 2)

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
