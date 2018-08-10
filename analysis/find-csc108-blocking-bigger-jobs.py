#-  Python 2.6 source code

#-  find-csc108-blocking-bigger-jobs.py ~~
#
#   This program will investigate samples in which CSC108 backfill jobs block
#   or otherwise interfere with "bigger" jobs. There are lots of possibilities
#   here, so I will write more description here later to summarize what I
#   actually ended up doing.
#
#                                                       ~~ (c) SRW, 09 Aug 2018
#                                                   ~~ last updated 09 Aug 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # First, I want to answer the question not of how much we delay other jobs,
  # but of how much we delay other jobs that are "big". To define big in this
  # case, I'll use the OLCF definition for the different bins:
  #     bin 1: 11250+ nodes, 24 hour max walltime, 15 day aging boost
  #     bin 2: 3750 - 11249 nodes, 24 hour max walltime, 5 day aging boost
  #     bin 3: 313 - 3749 nodes, 12 hour max walltime
  #     bin 4: 126 - 312 nodes, 6 hour max walltime
  #     bin 5: 125 or less nodes, 2 hour max walltime
  # Since bins 1 and 2 both have the same (largest) max walltime and receive
  # aging boosts, I will consider those to be the "big" jobs for this question.
  # Note that the code is adapted from "show-diff-in-waiting-time.py".

    def avg_wait(caption, left, right):

        query_template = """
            WITH other_big_jobs AS (
                SELECT  *,
                        (StartTime - SubmissionTime) AS WaitTime
                    FROM
                        completed
                    WHERE
                        SubmissionTime <= StartTime
                        AND SubmissionTime >= {left}
                        AND StartTime <= {right}
                        AND ReqNodes >= 3750
            )
            SELECT  count(WaitTime) AS num_jobs,
                    avg(WaitTime) AS avg_wait
                FROM other_big_jobs
            ;
            """

        query = query_template.format(left = left, right = right)
        results = {}

        for row in cursor.execute(query):
            print "---"
            print caption
            print "Avg wait: %s (%s jobs)" % (row["avg_wait"], row["num_jobs"])

    avg_wait("With CSC108, interval length 349799:", 1531923003, 1532272802)

    avg_wait("Without CSC108, interval length 349799 immediately after:",
        1532272802, 1532622601)

    avg_wait("With CSC108, interval length 386700:", 1532596501, 1532983201)

    avg_wait( "Without CSC108, interval length 386700 immediately after:",
        1532983201, 1533369901)

    print "---"

  # Those results are kind of weird, so I'm not sure what to make of them.

  # This time, let's try calculating the "time to first block" statistic again,
  # but restricting the definition of a block to be a "big block", which will
  # be an event representing interference with a job classified as bin 1 or 2.
  # To do this, we will adapt code from "hist-time-to-first-block.py".

    query = """
        WITH
            blocked AS (
                SELECT *
                    FROM eligible
                    INNER JOIN (
                        SELECT SampleID, sum(ReqProcs) AS procs
                            FROM csc108
                            GROUP BY SampleID
                    ) bp ON eligible.SampleID = bp.SampleID
                    INNER JOIN backfill ON
                        backfill.SampleID = eligible.SampleID
                    WHERE
                        eligible.ReqAWDuration < backfill.duration
                        AND eligible.ReqProcs < (backfill.proccount + bp.procs)
                        AND backfill.starttime = backfill.SampleTime
                        AND eligible.EEDuration > 0
                        AND eligible.Class = "batch"
                        AND (eligible.ReqProcs / 16) > 3750
            ),

            blocking AS (
                SELECT *
                    FROM csc108
                    WHERE SampleID IN (SELECT SampleID FROM blocked)
            ),

            csc108 AS (
                SELECT *
                    FROM active
                    WHERE Account = "CSC108" AND User = "doleynik"
            )

        SELECT min(SampleTime - StartTime) AS ttfb
            FROM blocking
            GROUP BY JobID
        ;
        """

    times = []
    for row in cursor.execute(query):
        times.append(row["ttfb"])

    avg_ttfb = 1.0 * sum(times) / len(times)

    print "Average TTFB (bins 1 and 2 jobs):  %s seconds" % (avg_ttfb)
    print 'Number of "big blocks" (bins 1,2): %s' % (len(times))

    query = """
        SELECT count(DISTINCT JobID) AS total_jobs
            FROM active
            WHERE Account = "CSC108" AND User = "doleynik";
        """

    for row in cursor.execute(query):
        total_jobs = row["total_jobs"]
        percent = 100.0 * len(times) / total_jobs
        print "# of CSC108 backfill jobs: %s" % (total_jobs)
        print "%% of CSC108 backfill jobs that block big jobs: %s" % (percent)

    print "---"

  # Okay, that's been interesting, but now let's try the original problem,
  # which was posed in terms of relative job size: when are we blocking jobs
  # bigger than our own?

    query = """
        SELECT  count(DISTINCT active.JobID)
            FROM
                active
            INNER JOIN
                eligible ON active.SampleID = eligible.SampleID
            WHERE
                active.Account = "CSC108"
                AND active.User = "doleynik"
                AND active.ReqProcs < eligible.ReqProcs
                --AND (eligible.ReqProcs / 16.0) >= 3750
        ;
        """

    for row in cursor.execute(query):
        print "# of CSC108 jobs which block bigger jobs: %s" % (row[0])
        print "%% of CSC108 jobs for which this occurs: %s" % (100.0 * row[0] /
            total_jobs)

    print "---"

  # Count the number of times a job larger than ours started running while ours
  # was already running.

    query = """
        SELECT count(DISTINCT A.JobID)
            FROM
                active A, active B
            WHERE
                A.SampleID = B.SampleID
                AND A.Account = "CSC108"
                AND A.User = "doleynik"
                AND (B.Account != "CSC108" OR B.User != "doleynik")
                AND A.ReqProcs < B.ReqProcs
                AND A.StartTime < B.StartTime
                AND A.Class = "batch"
                AND B.Class = "batch"
                AND (B.ReqProcs / 16.0) >= 3750
        ;
        """
    for row in cursor.execute(query):
        print "# of CSC108 jobs running when bigger job starts: %s" % (row[0])
        print "%% of CSC108 jobs for which this occurs: %s" % (100.0 * row[0] /             total_jobs)

    print "---"

    print "To be continued ..."

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
