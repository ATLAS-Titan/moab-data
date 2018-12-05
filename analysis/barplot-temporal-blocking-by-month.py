#-  Python 3 source code

#-  barplot-temporal-blocking-by-month.py ~~
#
#   This program creates a barplot by month that shows the temporal-only
#   blocking probability, separated into two categories: "Blocked by CSC108"
#   and "Unexplained". The reason that this is useful is because it illustrates
#   the difference when we change one tiny constraint in the SQL query.
#
#   The important change in query that separates the two categories is that in
#   one case, I have computed based on requested wall time being less than the
#   backfill opportunity for wall time, and in the other, I have computed
#   based on the backfill opportunity wall time and the remaining wall time for
#   CSC108 jobs.
#
#   NOTE: These percentages must be understood not to be from all samples, but
#   rather from all samples for which CSC108 was actually utilizing backfill.
#
#                                                       ~~ (c) SRW, 05 Dec 2018
#                                                   ~~ last updated 05 Dec 2018

import datetime
import json

import matplotlib.pyplot as plt
import numpy as np
import os
import scipy.stats as stats
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()


    data = {
    #   month: {
    #       "blocks":   0,
    #       "total":    0,
    #       "weird":    0
    #   }
    }

  # First, compute the number of samples blocked *spatially* by CSC108 every
  # month.

    query = """
        SELECT  strftime("%m-%Y", eligible.SampleTime, "unixepoch") AS month,
                count(DISTINCT eligible.SampleID) AS n

        FROM eligible

        INNER JOIN (
            SELECT  SampleID,
                    max((StartTime + ReqAWDuration) - SampleTime)
                        AS max_remaining
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

        WHERE
            csc108.max_remaining > 0

            AND (eligible.ReqAWDuration < backfill.duration
                OR eligible.ReqAWDuration < csc108.max_remaining)

            AND eligible.Class = "batch"

        GROUP BY
            month

        ORDER BY
            eligible.SampleTime

        ;
        """

    for row in cursor.execute(query):
        month = row["month"]
        if month not in data:
            data[month] = {
                "blocks":   0,
                "total":    0,
                "weird":    0
            }
        data[month]["blocks"] = row["n"]

  # Second, compute the blocks that had nothing to do with CSC108.

    query = """
        SELECT  strftime("%m-%Y", eligible.SampleTime, "unixepoch") AS month,
                count(DISTINCT eligible.SampleID) AS n

        FROM eligible

        INNER JOIN (
            SELECT  SampleID,
                    max((StartTime + ReqAWDuration) - SampleTime)
                        AS max_remaining
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

        WHERE
            csc108.max_remaining > 0

            AND eligible.ReqAWDuration < backfill.duration

            AND eligible.Class = "batch"

        GROUP BY
            month

        ORDER BY
            eligible.SampleTime

        ;
        """

    for row in cursor.execute(query):
        month = row["month"]
        data[month]["weird"] = row["n"]

  # Finally, compute the total number of samples every month.

    query = """
        SELECT  strftime("%m-%Y", eligible.SampleTime, "unixepoch") AS month,
                count(DISTINCT eligible.SampleID) AS n
            FROM
                eligible
            INNER JOIN (
                SELECT  SampleID,
                        max((StartTime + ReqAWDuration) - SampleTime)
                            AS max_remaining
                    FROM
                        active
                    WHERE
                        Account = "CSC108"
                        AND User = "doleynik"
                        AND JobName LIKE "SAGA-Python-PBSJobScript.%"
                    GROUP BY
                        SampleID
            ) csc108 ON eligible.SampleID = csc108.SampleID

            WHERE
                csc108.max_remaining > 0

            GROUP BY
                month
            ORDER BY
                eligible.SampleTime
        ;
        """

    for row in cursor.execute(query):
        month = row["month"]
        data[month]["total"] = row["n"]

    print(json.dumps(data, indent = 4))

  # Start putting the data together for the plot.

    months = []
    blocks = []
    weirds = []

    for key in data:
        months.append(key)
        weirds.append(100.0 * data[key]["weird"] / data[key]["total"])
        blocks.append((100.0 * data[key]["blocks"] / data[key]["total"])
            - weirds[-1])

  # Set up the plot

    fig = plt.figure()
    ax = fig.add_subplot(111)

    ind = np.arange(len(blocks))

    ax.bar(ind, blocks,
        bottom = weirds,
        color = "r",
        label = "Due to CSC108",
        zorder = 3
    )
    ax.bar(ind, weirds,
        color = "b",
        label = "Other",
        zorder = 3
    )

  # Make the month values pretty.

    pretty_months = []
    for each in months:
        pretty_months.append({
            "01":   "Jan",
            "02":   "Feb",
            "03":   "Mar",
            "04":   "Apr",
            "05":   "May",
            "06":   "Jun",
            "07":   "Jul",
            "08":   "Aug",
            "09":   "Sep",
            "10":   "Oct",
            "11":   "Nov",
            "12":   "Dec"
        }[each[0:2]] + " " + each[-4:])

    plt.xticks(ind, pretty_months)

    #plt.ylim(bottom = 86)
    plt.ylim(top = 100)

  # Angle the x-axis labels so that the dates don't overlap so badly
    plt.gcf().autofmt_xdate()

    ax.legend(loc = "lower left", framealpha = 1)

    ax.set(
        xlabel = "",
        ylabel = "Blocking probability (%)",
        title = "Temporal Blocking Probability on Titan"
    )
    ax.grid(zorder = 0)

    current_script = os.path.basename(__file__)
    fig.savefig(
        os.path.splitext(current_script)[0] + ".png",
        bbox_inches = "tight",
        dpi = 300
    )

    return

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
