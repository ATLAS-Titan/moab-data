#-  Python 2.6 source code

#-  pattern-tests.py ~~
#
#   This program helps me confirm that the patterns and relationships I think
#   I have noticed in the data actually hold for the entire dataset.
#
#                                                       ~~ (c) SRW, 20 Jun 2018
#                                                   ~~ last updated 21 Jun 2018

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
        if row["index_"] != 0:
            print "index_ is non-zero in showbf table: %s" % row["index_"]
        if row["reqid"] != 0:
            print "reqid is non-zero in showbf table: %s" % row["reqid"]

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
