#-  Python 2.6 source code

#-  compute-avg-local-idle-procs.py ~~
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 15 Jun 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # In this particular case, the "GROUP BY time" doesn't do anything.

    query = """
        SELECT LocalIdleProcs AS procs FROM showq_meta
            GROUP BY time;
        """

    procs = []
    for row in cursor.execute(query):
        procs.append(row["procs"])

    avg_procs = int(round(float(sum(procs)) / len(procs)))
    print "There are ~%s 'local idle processors' at any given instant." % \
        (avg_procs)

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
