#-  Python 2.6 source code

#-  blocking-probability.py ~~
#
#   This is a first stab at estimating the "blocking probability", which is
#   equal to the fraction of sample times in which jobs in the eligible queue
#   would have run in backfill except CSC108 had already claimed backfill's
#   resources.
#
#                                                       ~~ (c) SRW, 21 Jun 2018
#                                                   ~~ last updated 21 Jun 2018

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
        SELECT count(DISTINCT showq_eligible.time) AS n

        FROM showq_eligible

        INNER JOIN (
            SELECT time, sum(ReqProcs) AS procs
                FROM showq_active
                WHERE Account="CSC108" AND User_="doleynik"
                GROUP BY time
        ) csc108 ON showq_eligible.time=csc108.time

        INNER JOIN showbf ON showbf.time = showq_eligible.time

        INNER JOIN showq_meta ON showq_meta.time = showq_eligible.time

        WHERE
            showq_eligible.ReqAWDuration < showbf.duration
            AND
         -- This is still not right, though, because the CSC108 processors do
         -- not take duration into account here ...
            showq_eligible.ReqProcs < (showbf.proccount + csc108.procs)
            AND
            showbf.starttime = showbf.time
            AND
            showq_eligible.EEDuration > 0
        ;
        """

    num_blocked = 0
    for row in cursor.execute(query):
        num_blocked = row["n"]
        print "Number of sample times in which > 1 job was blocked: %s" % \
            num_blocked

    query = """
        SELECT count(DISTINCT showq_eligible.time) AS n
        FROM showq_eligible
        ;
        """

    num_total = 0
    for row in cursor.execute(query):
        num_total = row["n"]
        print "Total number of sample times: %s" % num_total

    print "=> The blocking probability is ~ %s." % \
        round(float(num_blocked) / num_total, 2)

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
