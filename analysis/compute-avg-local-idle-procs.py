#-  Python 3 source code

#-  compute-avg-local-idle-procs.py ~~
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 04 Dec 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

  # In this particular case, the "GROUP BY time" doesn't do anything.

    query = """
        SELECT LocalIdleProcs AS procs FROM cluster
            GROUP BY SampleID;
        """

    procs = []
    for row in cursor.execute(query):
        procs.append(row["procs"])

    avg_procs = int(round(float(sum(procs)) / len(procs)))
    print("There are ~%s 'local idle processors' at any given instant." % (
        avg_procs))

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
