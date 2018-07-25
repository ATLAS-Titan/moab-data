#-  Python 2.6 source code

#-  compute-max-reqnodes.py ~~
#
#   This program is an example to help me understand how to use a searched
#   CASE statement in SQL in order to solve the following problem:
#
#       We need to do almost everything in terms of compute nodes, not
#       processors. The columns in the data always give processors and only
#       give nodes in 3% of rows. Processors is always a multiple of nodes
#       (factor of 16), but in the cases when nodes is present, the factor is
#       not 16. This means we need to use the nodes value when it is present
#       and use the processors value divided by 16 when nodes is null.
#
#   I'm not sure if this happens due to the "ppn" (processors per node) option
#   in PBS or what, but this program exists to show how to work around the
#   problem without simply populating the non-null values in ReqNodes.
#
#                                                       ~~ (c) SRW, 18 Jul 2018
#                                                   ~~ last updated 25 Jul 2018

import os
import sqlite3

###

def analyze(connection):

    cursor = connection.cursor()

    query = """
        SELECT max(nodes) AS n
            FROM (
                SELECT
                    CASE
                        WHEN ReqNodes IS NULL THEN
                            ReqProcs / 16
                        ELSE
                            ReqNodes
                    END nodes
                    FROM active
            )
        ;
        """

    for row in cursor.execute(query):
        print "Max requested nodes: %s" % row["n"]

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
