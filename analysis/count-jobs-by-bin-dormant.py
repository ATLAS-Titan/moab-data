#-  Python 3 source code

#-  count-jobs-by-bin-dormant.py ~~
#
#   This program counts jobs by their assigned bin (1-5) during the "dormant
#   period" that occurred between July 21 and August 4, when CSC108 was not
#   running any ATLAS jobs on Titan.
#
#                                                       ~~ (c) SRW, 23 Aug 2018
#                                                   ~~ last updated 04 Dec 2018

import json
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    ###

    def count_between(num_a, num_b, time_a, time_b):

        query = """
            SELECT count(DISTINCT JobID) AS n
                FROM
                    active
                WHERE
                    SubmissionTime <= StartTime
                    AND ((ReqNodes IS NULL
                            AND {na} <= (ReqProcs / 16)
                            AND (ReqProcs / 16) <= {nb})
                        OR (ReqNodes IS NOT NULL
                            AND {na} <= ReqNodes
                            AND ReqNodes <= {nb}))
                 -- An estimate for July 21 through August 4
                    AND ({ta} < SampleTime AND SampleTime < {tb})
            ;
            """.format(na = num_a, nb = num_b, ta = time_a, tb = time_b)

        n = None
        for row in cursor.execute(query):
            n = row["n"]

        return n

    ###

    print("# of all jobs:\n    Pre-dormant: %s\n        Dormant: %s" % (
        count_between(1, 20000, 1530939600, 1532149200),
        count_between(1, 20000, 1532149200, 1533358800)))

    print("# of bin 1 jobs:\n    Pre-dormant: %s\n        Dormant: %s" % (
        count_between(11250, 20000, 1530939600, 1532149200),
        count_between(11250, 20000, 1532149200, 1533358800)))

    print("# of bin 2 jobs:\n    Pre-dormant: %s\n        Dormant: %s" % (
        count_between(3750, 11249, 1530939600, 1532149200),
        count_between(3750, 11249, 1532149200, 1533358800)))

    print("# of bin 3 jobs:\n    Pre-dormant: %s\n        Dormant: %s" % (
        count_between(313, 3749, 1530939600, 1532149200),
        count_between(313, 3749, 1532149200, 1533358800)))

    print("# of bin 4 jobs:\n    Pre-dormant: %s\n        Dormant: %s" % (
        count_between(126, 312, 1530939600, 1532149200),
        count_between(126, 312, 1532149200, 1533358800)))

    print("# of bin 5 jobs:\n    Pre-dormant: %s\n        Dormant: %s" % (
        count_between(1, 125, 1530939600, 1532149200),
        count_between(1, 125, 1532149200, 1533358800)))

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
