#-  Python 2.6 source code

#-  from-xml-to-sqlite.py ~~
#
#   This program converts the raw XML files into a queryable SQL database
#   using SQLite. Right now, this script is being tested for use on machines
#   that are local to the data as well as machines that are local to the user.
#
#   I used Python here with extensive inline SQL using heredocs because that's
#   what the group has used elsewhere in the BigPanDA project as well.
#
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 22 Jun 2018

import os
import sqlite3
from xml.etree import ElementTree

###

def initDB(conn):

    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS showbf (
            duration INTEGER NOT NULL,
            index_ INTEGER NOT NULL,
            proccount INTEGER NOT NULL,
            nodecount INTEGER NOT NULL,
            reqid INTEGER NOT NULL,
            starttime INTEGER NOT NULL,
            time INTEGER NOT NULL,

            CONSTRAINT unique_rows UNIQUE (
                duration, index_, proccount, nodecount, reqid, starttime,
                time
            )
        );
        """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS showq_active (
            Account STRING NOT NULL,
            AWDuration INTEGER,
            Class STRING NOT NULL,
            DRMJID INTEGER NOT NULL,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID INTEGER NOT NULL,
            JobName STRING NOT NULL,
            MasterHost INTEGER,
            PAL STRING,
            QOS STRING NOT NULL,
            ReqAWDuration INTEGER NOT NULL,
            ReqNodes INTEGER,
            ReqProcs INTEGER NOT NULL,
            RsvStartTime INTEGER,
            RunPriority INTEGER,
            StartPriority INTEGER NOT NULL,
            StartTime INTEGER NOT NULL,
            State STRING NOT NULL,
            StatPSDed REAL NOT NULL,
            StatPSUtl REAL NOT NULL,
            SubmissionTime TEXT NOT NULL,
            SuspendDuration INTEGER NOT NULL,
            time INTEGER NOT NULL,
            User_ STRING NOT NULL,

            CONSTRAINT unique_rows UNIQUE (
                Account, Class, DRMJID, GJID, Group_, JobID, JobName, QOS,
                ReqAWDuration, ReqProcs, RunPriority, StartPriority, StartTime,
                State, StatPSDed, StatPSUtl, SubmissionTime, SuspendDuration,
                time, User_
            )
        )
        """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS showq_blocked (
            Account STRING NOT NULL,
            Class STRING NOT NULL,
            DRMJID INTEGER NOT NULL,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID INTEGER NOT NULL,
            JobName STRING NOT NULL,
            QOS STRING NOT NULL,
            ReqAWDuration INTEGER NOT NULL,
            ReqProcs INTEGER NOT NULL,
            StartPriority INTEGER NOT NULL,
            StartTime INTEGER NOT NULL,
            State STRING NOT NULL,
            SubmissionTime INTEGER NOT NULL,
            SuspendDuration INTEGER NOT NULL,
            time INTEGER NOT NULL,
            User_ STRING NOT NULL,

            CONSTRAINT unique_rows UNIQUE (
                Account, Class, DRMJID, GJID, Group_, JobID, JobName, QOS,
                ReqAWDuration, ReqProcs, StartPriority, StartTime, State,
                SubmissionTime, SuspendDuration, time, User_
            )
        )
        """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS showq_eligible (
            Account STRING NOT NULL,
            Class STRING NOT NULL,
            DRMJID INTEGER NOT NULL,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID INTEGER NOT NULL,
            JobName STRING NOT NULL,
            QOS STRING NOT NULL,
            ReqAWDuration INTEGER NOT NULL,
            ReqProcs INTEGER NOT NULL,
            RsvStartTime INTEGER,
            StartPriority INTEGER NOT NULL,
            StartTime INTEGER NOT NULL,
            State STRING NOT NULL,
            SubmissionTime INTEGER NOT NULL,
            SuspendDuration INTEGER NOT NULL,
            time INTEGER NOT NULL,
            User_ STRING NOT NULL,

            CONSTRAINT unique_rows UNIQUE (
                Account, Class, DRMJID, GJID, Group_, JobID, JobName, QOS,
                ReqAWDuration, ReqProcs, StartPriority, StartTime, State,
                SubmissionTime, SuspendDuration, time, User_
            )
        )
        """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS showq_meta (
            LocalActiveNodes INTEGER NOT NULL,
            LocalAllocProcs INTEGER NOT NULL,
            LocalConfigNodes INTEGER NOT NULL,
            LocalIdleNodes INTEGER NOT NULL,
            LocalIdleProcs INTEGER NOT NULL,
            LocalUpNodes INTEGER NOT NULL,
            LocalUpProcs INTEGER NOT NULL,
            RemoteActiveNodes INTEGER NOT NULL,
            RemoteAllocProcs INTEGER NOT NULL,
            RemoteConfigNodes INTEGER NOT NULL,
            RemoteIdleNodes INTEGER NOT NULL,
            RemoteIdleProcs INTEGER NOT NULL,
            RemoteUpNodes INTEGER NOT NULL,
            RemoteUpProcs INTEGER NOT NULL,
            time INTEGER NOT NULL,

            CONSTRAINT unique_rows UNIQUE (
                LocalActiveNodes, LocalAllocProcs, LocalConfigNodes,
                LocalIdleNodes, LocalIdleProcs, LocalUpNodes, LocalUpProcs,
                RemoteActiveNodes, RemoteAllocProcs, RemoteConfigNodes,
                RemoteIdleNodes, RemoteIdleProcs, RemoteUpNodes,
                RemoteUpProcs, time
            )
        )
        """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS files_showbf (
            errfilecontents STRING,
            errfilename STRING UNIQUE NOT NULL,
            outfilename STRING UNIQUE NOT NULL
        );
        """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS files_showq (
            errfilecontents STRING,
            errfilename STRING UNIQUE NOT NULL,
            outfilename STRING UNIQUE NOT NULL
        );
        """)

  # Commit changes

    conn.commit()

###

def readXML(filename):

    xmltext = ""
    with open(filename, "r") as xmlfile:
        xmltext = "".join(xmlfile.readlines()).strip()

    errfilename = filename[:-8] + "-err.xml"
    errtext = None
    tree = None

    if len(xmltext) == 0:
        with open(errfilename, "r") as errfile:
            errtext = "".join(errfile.readlines()).strip()
    else:
        tree = ElementTree.fromstring(xmltext)

    return {"errtext": errtext, "tree": tree}

###

def showbfImport(conn, data_dir):

    c = conn.cursor()

    query = """
        SELECT outfilename FROM files_showbf;
        """

    outfilenames = []
    for row in c.execute(query):
        outfilenames.append(row["outfilename"])

    showbf_dir = os.path.join(data_dir, "showbf")

    for filename in os.listdir(showbf_dir):
        if filename.endswith("-out.xml") and (filename not in outfilenames):
            abspath = os.path.join(showbf_dir, filename)
            obj = readXML(abspath)
            if obj["tree"] is None:
                c.execute("""
                    INSERT INTO files_showbf (
                        errfilecontents,
                        errfilename,
                        outfilename
                    ) VALUES (?, ?, ?);
                    """, (obj["errtext"], filename[:-8] + "-err.xml", filename))
            else:
                c.execute("""
                    INSERT INTO files_showbf (
                        errfilecontents,
                        errfilename,
                        outfilename
                    ) VALUES (?, ?, ?);
                    """, (obj["errtext"], filename[:-8] + "-err.xml", filename))
                showbfXMLtoSQL(obj["tree"], conn)

    conn.commit()

###

def showbfXMLtoSQL(root, conn):

    c = conn.cursor()

    data = {
        "meta": {},
        "partitions": {}
    }

    for elem in root:

    # The very first 'elem' will have a tag of "Object" which will say
    # "cluster", and it doesn't do anything, so we can ignore it.

        if elem.tag == "job":
            data["meta"] = elem.attrib

        elif elem.tag == "par" and elem.attrib["Name"] != "template":
            data["partitions"][elem.attrib["Name"]] = []
            for each in elem.getchildren():
                data["partitions"][elem.attrib["Name"]].append(each.attrib)

    # Now the fun part -- the SQL.

    if "time" not in data["meta"]:
        return

    time = data["meta"]["time"]
    for key in data["partitions"]:
        for each in data["partitions"][key]:
            c.execute("""
                INSERT OR IGNORE INTO showbf (
                    duration, index_, proccount, nodecount, reqid,
                    starttime, time
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?
                )
                """, (each["duration"], each["index"], each["proccount"],
                each["nodecount"], each["reqid"], each["starttime"], time))

  # Committing changes

    conn.commit()

###

def showqImport(conn, data_dir):

    c = conn.cursor()

    query = """
        SELECT outfilename FROM files_showq;
        """

    outfilenames = []
    for row in c.execute(query):
        outfilenames.append(row["outfilename"])

    showq_dir = os.path.join(data_dir, "showq")

    for filename in os.listdir(showq_dir):
        if filename.endswith("-out.xml") and (filename not in outfilenames):
            abspath = os.path.join(showq_dir, filename)
            obj = readXML(abspath)
            if obj["tree"] is None:
                c.execute("""
                    INSERT INTO files_showq (
                        errfilecontents,
                        errfilename,
                        outfilename
                    ) VALUES (?, ?, ?);
                    """, (obj["errtext"], filename[:-8] + "-err.xml", filename))
            else:
                c.execute("""
                    INSERT INTO files_showq (
                        errfilecontents,
                        errfilename,
                        outfilename
                    ) VALUES (?, ?, ?);
                    """, (obj["errtext"], filename[:-8] + "-err.xml", filename))
                showqXMLtoSQL(obj["tree"], conn)

    conn.commit()

###

def showqXMLtoSQL(root, conn):

    c = conn.cursor()

    data = {
        "jobs": {
            "active": [],
            "eligible": [],
            "blocked": []
        },
        "meta": {}
    }

    for elem in root:

      # The very first 'elem' will have a tag of "Object" which will say
      # "queue", and it doesn't do anything, so we can ignore it.

        if elem.tag == "cluster":

            data["meta"] = elem.attrib

        elif elem.tag == "queue":

            if elem.get("option") == "active":
                for job in elem.getchildren():
                    data["jobs"]["active"].append(job.attrib)

            elif elem.get("option") == "eligible":
                for job in elem.getchildren():
                    data["jobs"]["eligible"].append(job.attrib)

            elif elem.get("option") == "blocked":
                for job in elem.getchildren():
                    data["jobs"]["blocked"].append(job.attrib)

  # Now the fun part -- the SQL.

    if "time" not in data["meta"]:
        return

    time = data["meta"]["time"]

  # First, showq_active ...

    active_fields = [
        "Account", "AWDuration", "Class", "DRMJID", "EEDuration",
        "GJID", "Group", "JobID", "JobName", "MasterHost", "PAL",
        "QOS", "ReqAWDuration", "ReqNodes", "ReqProcs", "RsvStartTime",
        "RunPriority", "StartPriority", "StartTime", "State",
        "StatPSDed", "StatPSUtl", "SubmissionTime", "SuspendDuration",
        "User"
    ]

    for job in data["jobs"]["active"]:

        vals = {}
        for field in active_fields:
            if field in job:
                vals[field] = job[field]
            else:
                vals[field] = None

        c.execute("""
            INSERT OR IGNORE INTO showq_active (
                Account, AWDuration, Class, DRMJID, EEDuration,
                GJID, Group_, JobID, JobName, MasterHost, PAL,
                QOS, ReqAWDuration, ReqNodes, ReqProcs, RsvStartTime,
                RunPriority, StartPriority, StartTime, State,
                StatPSDed, StatPSUtl, SubmissionTime, SuspendDuration,
                time, User_
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?
            )
            """, (vals["Account"], vals["AWDuration"], vals["Class"],
                vals["DRMJID"], vals["EEDuration"], vals["GJID"],
                vals["Group"], vals["JobID"], vals["JobName"],
                vals["MasterHost"], vals["PAL"], vals["QOS"],
                vals["ReqAWDuration"], vals["ReqNodes"], vals["ReqProcs"],
                vals["RsvStartTime"], vals["RunPriority"],
                vals["StartPriority"], vals["StartTime"], vals["State"],
                vals["StatPSDed"], vals["StatPSUtl"], vals["SubmissionTime"],
                vals["SuspendDuration"], time, vals["User"]))

    conn.commit()

  # Next, showq_blocked ...

    blocked_fields = [
        "Account", "Class", "DRMJID", "EEDuration", "GJID", "Group", "JobID",
        "JobName", "ReqAWDuration", "ReqProcs", "QOS", "StartPriority",
        "StartTime", "State", "SubmissionTime", "SuspendDuration", "User"
    ]

    for job in data["jobs"]["blocked"]:

        vals = {}
        for field in blocked_fields:
            if field in job:
                vals[field] = job[field]
            else:
                vals[field] = None

        c.execute("""
            INSERT OR IGNORE INTO showq_blocked (
                Account, Class, DRMJID, EEDuration, GJID, Group_, JobID,
                JobName, QOS, ReqAWDuration, ReqProcs, StartPriority,
                StartTime, State, SubmissionTime, SuspendDuration, time, User_
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (vals["Account"], vals["Class"], vals["DRMJID"],
                vals["EEDuration"], vals["GJID"], vals["Group"], vals["JobID"],
                vals["JobName"], vals["QOS"], vals["ReqAWDuration"],
                vals["ReqProcs"], vals["StartPriority"], vals["StartTime"],
                vals["State"], vals["SubmissionTime"], vals["SuspendDuration"],
                time, vals["User"]))

    conn.commit()

  # Next, showq_eligible ...

    eligible_fields = [
        "Account", "Class", "DRMJID", "EEDuration", "GJID", "Group", "JobID",
        "JobName", "QOS", "ReqAWDuration", "ReqProcs", "RsvStartTime",
        "StartPriority", "StartTime", "State", "SubmissionTime",
        "SuspendDuration", "time", "User"
    ]

    for job in data["jobs"]["eligible"]:

        vals = {}
        for field in eligible_fields:
            if field in job:
                vals[field] = job[field]
            else:
                vals[field] = None

        c.execute("""
            INSERT OR IGNORE INTO showq_eligible (
                Account, Class, DRMJID, EEDuration, GJID, Group_, JobID,
                JobName, QOS, ReqAWDuration, ReqProcs, RsvStartTime,
                StartPriority, StartTime, State, SubmissionTime,
                SuspendDuration, time, User_
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (vals["Account"], vals["Class"], vals["DRMJID"],
                vals["EEDuration"], vals["GJID"], vals["Group"], vals["JobID"],
                vals["JobName"], vals["QOS"], vals["ReqAWDuration"],
                vals["ReqProcs"], vals["RsvStartTime"], vals["StartPriority"],
                vals["StartTime"], vals["State"], vals["SubmissionTime"],
                vals["SuspendDuration"], time, vals["User"]))

    conn.commit()

  # Finally, showq_meta.

    vals = data["meta"]

    c.execute("""
        INSERT OR IGNORE INTO showq_meta (
            LocalActiveNodes, LocalAllocProcs, LocalConfigNodes,
            LocalIdleNodes, LocalIdleProcs, LocalUpNodes, LocalUpProcs,
            RemoteActiveNodes, RemoteAllocProcs, RemoteConfigNodes,
            RemoteIdleNodes, RemoteIdleProcs, RemoteUpNodes, RemoteUpProcs,
            time
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """, (vals["LocalActiveNodes"], vals["LocalAllocProcs"],
        vals["LocalConfigNodes"], vals["LocalIdleNodes"],
        vals["LocalIdleProcs"], vals["LocalUpNodes"], vals["LocalUpProcs"],
        vals["RemoteActiveNodes"], vals["RemoteAllocProcs"],
        vals["RemoteConfigNodes"], vals["RemoteIdleNodes"],
        vals["RemoteIdleProcs"], vals["RemoteUpNodes"],
        vals["RemoteUpProcs"], time))

  # Committing changes

    conn.commit()

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

  # Create the database file in the data directory and connect Python to it.

    dbfilename = os.path.join(data_dir, "moab-data.sqlite")

    connection = sqlite3.connect(dbfilename)

  # Enable users to access columns by name instead of by index.

    connection.row_factory = sqlite3.Row

  # Create the database itself, if it doesn't exist.

    initDB(connection)

  # Start populating the database from the raw XML files.

    showbfImport(connection, data_dir)
    showqImport(connection, data_dir)

  # When we are finished, close the connection to the database.

    connection.close()

###

if __name__ == "__main__":
    main()

#-  vim:set syntax=python:
