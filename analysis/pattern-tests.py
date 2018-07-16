#-  Python 2.6 source code

#-  pattern-tests.py ~~
#
#   This program helps me confirm that the patterns and relationships I think
#   I have noticed in the data actually hold for the entire dataset.
#
#                                                       ~~ (c) SRW, 20 Jun 2018
#                                                   ~~ last updated 16 Jul 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT * FROM showq_meta;
        """

  # Check that the ratio between LocalUpNodes and LocalUpProcs is always 16:1.

    for row in cursor.execute(query):

        if row["LocalUpNodes"] != 0:
            ratio = float(row["LocalUpProcs"]) / row["LocalUpNodes"]
            if ratio != 16.0:
                print "Up ratio: %s" % ratio

  # Check that certain values are always zero in the "showbf" table.

    query = """
        SELECT * FROM showbf;
        """

    for row in cursor.execute(query):
        fields_that_should_be_zero = ["index_", "reqid"]
        for field in fields_that_should_be_zero:
            if row[field] != 0:
                print "%s is non-zero in showbf: %s" % (field, row[field])

    query = """
        SELECT * FROM showq_meta;
        """

    for row in cursor.execute(query):
        fields_that_should_be_zero = [
            "RemoteAllocProcs", "RemoteConfigNodes", "RemoteIdleNodes",
            "RemoteIdleProcs", "RemoteUpNodes", "RemoteUpProcs"
        ]
        for field in fields_that_should_be_zero:
            if row[field] != 0:
                print "%s is non-zero in showq_meta: %s" % (field, row[field])

  # Check that the relationship between nodes and processors is always 1:16
  # by checking to make sure that `ReqProcs` is always divisible by 16.

    query = """
        SELECT DISTINCT (ReqProcs % 16) AS remainder FROM showq_active;
        """

    for row in cursor.execute(query):
        if row["remainder"] != 0:
            print "Some value of `ReqProcs` is not divisible by 16."

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
