#-  Python 2.6 source code

#-  compute-summary-stats-dormant.py ~~
#
#   This program computes summary statistics for various subsets of the data
#   from two periods of time: July 7-21, and July 22-August 4. These periods
#   correspond to the two week period that CSC108 wasn't running ATLAS jobs
#   and to the two week period that immediately preceded that.
#
#   On ORNL machines, remember to load the appropriate module before running
#   this program:
#
#       $ module load python_anaconda2
#
#                                                       ~~ (c) SRW, 24 Aug 2018
#                                                   ~~ last updated 24 Aug 2018

import json
import numpy
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    ###

    def with_csc108(params):

        query = """
            SELECT DISTINCT
                        JobID,
                        (StartTime - SubmissionTime) AS WaitTime
                FROM
                    active
                WHERE
                    (Account != "CSC108" OR User != "doleynik")
                    AND SubmissionTime <= StartTime
                    AND ((ReqNodes IS NULL
                            AND {lo_nodes} <= (ReqProcs / 16)
                            AND (ReqProcs / 16) <= {hi_nodes})
                        OR (ReqNodes IS NOT NULL
                            AND {lo_nodes} <= ReqNodes
                            AND ReqNodes <= {hi_nodes}))
                    AND ({lo_time} < SampleTime AND SampleTime < {hi_time})
            ;
            """.format(lo_nodes = params["lo_nodes"],
                hi_nodes = params["hi_nodes"],
                lo_time = params["lo_time"],
                hi_time = params["hi_time"]
            )

        results = []
        for row in cursor.execute(query):
            results.append(row["WaitTime"])

        print "- Mean =   %s" % numpy.mean(results)
        print "- Median = %s" % numpy.median(results)
        print "- Sigma =  %s" % numpy.std(results)
        print "- n =      %s" % len(results)

        return results

    ###

    start = 1530939600
    middle = 1532149200
    end = 1533358800

    print "All bins before dormant period:"
    with_csc108({
        "lo_nodes": 1,
        "hi_nodes": 100000,
        "lo_time": start,
        "hi_time": middle
    })

    print "All bins during dormant period:"
    with_csc108({
        "lo_nodes": 1,
        "hi_nodes": 100000,
        "lo_time": middle,
        "hi_time": end
    })

    print "Bin 1 before dormant period:"
    with_csc108({
        "lo_nodes": 11250,
        "hi_nodes": 100000,
        "lo_time": start,
        "hi_time": middle
    })

    print "Bin 1 during dormant period:"
    with_csc108({
        "lo_nodes": 11250,
        "hi_nodes": 100000,
        "lo_time": middle,
        "hi_time": end
    })

    print "Bin 2 before dormant period:"
    with_csc108({
        "lo_nodes": 3750,
        "hi_nodes": 11249,
        "lo_time": start,
        "hi_time": middle
    })

    print "Bin 2 during dormant period:"
    with_csc108({
        "lo_nodes": 3750,
        "hi_nodes": 11249,
        "lo_time": middle,
        "hi_time": end
    })

    print "Bin 3 before dormant period:"
    with_csc108({
        "lo_nodes": 313,
        "hi_nodes": 3749,
        "lo_time": start,
        "hi_time": middle
    })

    print "Bin 3 during dormant period:"
    with_csc108({
        "lo_nodes": 313,
        "hi_nodes": 3749,
        "lo_time": middle,
        "hi_time": end
    })

    print "Bin 4 before dormant period:"
    with_csc108({
        "lo_nodes": 126,
        "hi_nodes": 312,
        "lo_time": start,
        "hi_time": middle
    })

    print "Bin 4 during dormant period:"
    with_csc108({
        "lo_nodes": 126,
        "hi_nodes": 312,
        "lo_time": middle,
        "hi_time": end
    })

    print "Bin 5 before dormant period:"
    with_csc108({
        "lo_nodes": 1,
        "hi_nodes": 125,
        "lo_time": start,
        "hi_time": middle
    })

    print "Bin 5 during dormant period:"
    with_csc108({
        "lo_nodes": 1,
        "hi_nodes": 125,
        "lo_time": middle,
        "hi_time": end
    })

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
