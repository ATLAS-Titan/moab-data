#-  Python 2.6 source code

#-  explore-nodes-and-procs.py ~~
#
#   This program explores the "cluster" table on a very preliminary level.
#
#                                                       ~~ (c) SRW, 19 Jun 2018
#                                                   ~~ last updated 25 Jul 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    def compute_average(fieldname):

        query = "SELECT avg({fieldname}) AS val FROM cluster".\
                format(fieldname=fieldname)

        vals = []
        for row in cursor.execute(query):
            vals.append(row["val"])

        if sum(vals) == 0:
            print "All values of %s are 0." % fieldname
        else:
            avg_val = int(round(float(sum(vals) / len(vals))))
            print "Average %s: ~%s" % (fieldname, avg_val)

    ###

  # Now we need to compute the fieldnames. I could definitely hardcode them,
  # but I'd hate myself later if I changed the column names...

    cursor = connection.execute("SELECT * FROM cluster LIMIT 1")
    row = cursor.fetchone()
    fieldnames = row.keys()

    for field in fieldnames:
        if field not in ["SampleID", "SampleTime"]:
            compute_average(field)

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
