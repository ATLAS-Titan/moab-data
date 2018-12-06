#-  Python 3 source code

#-  barplot-spatial-blocking-by-month.py ~~
#
#   This program creates a barplot by month that shows the spatial-only
#   blocking probability, separated into two categories: "Blocked by CSC108"
#   and "Unexplained". The reason that this is useful is because it illustrates
#   the difference when we change one tiny constraint in the SQL query.
#
#   The important change in query that separates the two categories is that in
#   one case, I have computed based on requested processors being less than the
#   backfill opportunity for processors, and in the other, I have computed
#   based on the sum of the requested processors and the processors in use by
#   CSC108 being less than the backfill opportunity for processors.
#
#   The results show that the percentage of spatial blocks alone due to CSC108
#   lower than we thought, and it suggests that spatiotemporal blocks will be
#   even less. Therefore, CSC108 is having less blocking impact than I thought!
#
#   NOTE: These percentages must be understood not to be from all samples, but
#   rather from all samples for which CSC108 was actually utilizing backfill.
#
#                                                       ~~ (c) SRW, 05 Dec 2018
#                                                   ~~ last updated 06 Dec 2018

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
    #       "all_blocks":       0,
    #       "csc108_blocks":    0,
    #       "total_samples":    0,
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

        WHERE

            eligible.Class = "batch"

         -- Make sure CSC108 is running in backfill. (This should be redundant,
         -- however, based on the construction of the query.)

            AND csc108.procs > 0

         -- Find the rows where a job needs too many processors for backfill,
         -- but which would no longer be blocked if backfill were bigger
         -- because CSC108 wasn't running anything.

            AND eligible.ReqProcs > backfill.proccount
            AND eligible.ReqProcs < (backfill.proccount + csc108.procs)

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
                "all_blocks":       0,
                "csc108_blocks":    0,
                "total_samples":    0
            }
        data[month]["csc108_blocks"] = row["n"]

  # Second, compute all spatial blocks.

    query = """
        SELECT  strftime("%m-%Y", eligible.SampleTime, "unixepoch") AS month,
                count(DISTINCT eligible.SampleID) AS n

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

        WHERE

            eligible.Class = "batch"

         -- Make sure CSC108 is running in backfill. (This should be redundant,
         -- however, based on the construction of the query.)

            AND csc108.procs > 0

         -- Find the rows where the job needs too many processors for backfill

            AND eligible.ReqProcs > backfill.proccount

        GROUP BY
            month

        ORDER BY
            eligible.SampleTime

        ;
        """

    for row in cursor.execute(query):
        month = row["month"]
        data[month]["all_blocks"] = row["n"]

  # Finally, compute the total number of samples every month.

    query = """
        SELECT  strftime("%m-%Y", eligible.SampleTime, "unixepoch") AS month,
                count(DISTINCT eligible.SampleID) AS n
            FROM
                eligible
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

            WHERE
                csc108.procs > 0

            GROUP BY
                month
            ORDER BY
                eligible.SampleTime
        ;
        """

    for row in cursor.execute(query):
        month = row["month"]
        data[month]["total_samples"] = row["n"]

    print(json.dumps(data, indent = 4))

  # Start putting the data together for the plot.

    csc108 = []
    months = []
    others = []

    for key in data:
        csc108.append(100.0 * data[key]["csc108_blocks"] /
            data[key]["total_samples"])
        months.append(key)
        others.append((100.0 * data[key]["all_blocks"] /
            data[key]["total_samples"]) - csc108[-1])

  # Set up the plot

    fig = plt.figure()
    ax = fig.add_subplot(111)

    ind = np.arange(len(months))

    ax.bar(ind, csc108,
        bottom = others,
        color = "r",
        label = "Due to CSC108",
        zorder = 3
    )
    ax.bar(ind, others,
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

    ax.legend(loc = "center left", framealpha = 1)

    ax.set(
        xlabel = "",
        ylabel = "Blocking probability (%)",
        title = "Spatial Blocking Probability on Titan"
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
