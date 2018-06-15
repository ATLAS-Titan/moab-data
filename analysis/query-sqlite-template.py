#-  Python 2.6 source code

#-  query-sqlite-template.py ~~
#
#   This program is just a template with some useful examples shown, so that
#   it's easy to start modifying the program in order to explore the data.
#
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 15 Jun 2018

import json
import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT COUNT(time) FROM showq_active
            WHERE time > (strftime('%s','now') - 3*24*60*60);
        """

    for row in cursor.execute(query):
        print row[0]

    query = """
        SELECT DISTINCT Account FROM showq_active;
        """

    for row in cursor.execute(query):
        print "Account: %s" % row["Account"]

###

def main():

  # Create string to represent path to database file.

    dbfilename = os.path.join(os.getenv("PWD"), "moab-data.sqlite")

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
