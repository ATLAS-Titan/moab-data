#-  Python 2.6 source code

#-  query-sqlite-template.py ~~
#
#   This program is just a template with some useful examples shown, so that
#   it's easy to start modifying the program in order to explore the data.
#
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 25 Jul 2018

import json
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT COUNT(SampleID) FROM cluster
            WHERE SampleTime > (strftime('%s','now') - 3*24*60*60);
        """

    for row in cursor.execute(query):
        print row[0]

    query = """
        SELECT DISTINCT Account FROM active;
        """

    for row in cursor.execute(query):
        print "Account: %s" % row["Account"]

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
