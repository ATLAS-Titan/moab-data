#-  Python 2.6 source code

#-  blocking-probability.py ~~
#
#   This is a first stab at estimating the "blocking probability", which is
#   equal to the fraction of sample times in which jobs in the eligible queue
#   would have run in backfill except CSC108 had already claimed backfill's
#   resources.
#
#   Initial value using data through June 20 is 842/1758 (0.48), which is much
#   higher than expected. This value may indicate that I got the conditions
#   totally wrong.
#
#   An edit to use SampleID to join tables instead of SampleTime resulted in an
#   expected increase from 2268/6750 (0.34) to 3506/6751 (0.52). This was
#   expected due to the undercounting that resulted when `showbf` and `showq`
#   from the same sample returned different SampleTime values. The increase (by
#   1) in SampleID versus SampleTime, however, is not yet explained.
#
#                                                       ~~ (c) SRW, 21 Jun 2018
#                                                   ~~ last updated 29 Aug 2018

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
        SELECT count(DISTINCT eligible.SampleID) AS n

        FROM eligible

        INNER JOIN (
            SELECT  SampleID,
                    sum(ReqProcs) AS procs
                FROM
                    active
                WHERE
                    Account = "CSC108"
                    AND User = "doleynik"
                    AND JobName LIKE "SAGA-Python-PBSJobScript.%"
                GROUP BY
                    SampleID
        ) csc108 ON eligible.SampleID = csc108.SampleID

        INNER JOIN backfill ON
            backfill.SampleID = eligible.SampleID

        --INNER JOIN cluster ON
        --    cluster.SampleID = eligible.SampleID

        WHERE
            eligible.ReqAWDuration < backfill.duration
         -- This is still not right, though, because the CSC108 processors do
         -- not take duration into account here ...
            AND eligible.ReqProcs < (backfill.proccount + csc108.procs)
            AND backfill.starttime = backfill.SampleTime
            AND eligible.EEDuration > 0
            AND eligible.Class = "batch"
        ;
        """

    num_blocked = 0
    for row in cursor.execute(query):
        num_blocked = row["n"]
        print "Number of sample times in which >= 1 job was blocked: %s" % \
            num_blocked

    query = """
        SELECT  count(DISTINCT eligible.SampleID) AS n
            FROM
                eligible
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
