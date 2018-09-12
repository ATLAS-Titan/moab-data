#-  Python 2.6 source code

#-  from-xml-to-sqlite.py ~~
#
#   This program converts the raw XML files into a queryable SQL database
#   using SQLite. Right now, this script is being tested for use on machines
#   that are local to the data as well as machines that are local to the user.
#
#   This version now takes advantage of the UUIDs I used to name the files in
#   order to sync data taken by `showbf` and `showq`, which may or may not have
#   identical timestamps.
#
#   I used Python here with extensive inline SQL using heredocs because that's
#   what the group has used elsewhere in the BigPanDA project as well.
#
#                                                       ~~ (c) SRW, 15 Jun 2018
#                                                   ~~ last updated 12 Sep 2018

#import json
import os
import sqlite3
from xml.etree import ElementTree

###

def findNewUUIDs(connection, data_dir):

  # Given a `Connection` object and a "data_dir" string indicating the path to
  # the data directory, this function finds newly collected XML files that have
  # not been imported into SQLite yet. This function returns a list of UUIDs
  # that can be used to generate the filenames that should be imported.

  # First, get a list of SampleIDs out of the SQLite database, so we know what
  # has already been imported.

    cursor = connection.cursor()

    query = """
        SELECT DISTINCT SampleID FROM sample_info;
        """

    sampleids = []
    for row in cursor.execute(query):
        sampleids.append(row["SampleID"])

    connection.commit()

  # Now, filter lists of the data directories to find UUIDs that have not been
  # imported yet.

    uuids = []

    showbf_dir = os.path.join(data_dir, "showbf")
    showq_dir = os.path.join(data_dir, "showq")
    showqc_dir = os.path.join(data_dir, "showqc")

    for each in ["showbf", "showq", "showqc"]:
        dirname = os.path.join(data_dir, each)
        for filename in os.listdir(dirname):
            uuid = filename[:-8]
            if (uuid not in sampleids) and (uuid not in uuids):
                uuids.append(uuid)

  # Return the list of UUIDs.

    return uuids

###

def importBackfill(connection, data_dir, uuids):

  # Given a `Connection` object and a "data_dir" string indicating the path to
  # the data directory, this function imports the `showbf` data into SQLite.

    cursor = connection.cursor()

    showbf_dir = os.path.join(data_dir, "showbf")

    for sampleid in uuids:

        abspath = os.path.join(showbf_dir, sampleid + "-out.xml")
        obj = readXML(abspath)

      # A version of UPSERT that works with SQLite versions older than 3.24:
        cursor.execute("""
            UPDATE sample_info SET showbfError = ? WHERE SampleID = ?;
        """, (obj["errtext"], sampleid))
        cursor.execute("""
            INSERT OR IGNORE INTO sample_info (SampleID, showbfError)
                VALUES (?, ?);
            """, (sampleid, obj["errtext"]))

        connection.commit()

        if obj["tree"] is not None:
            showbfXMLtoSQL(connection, obj)

    return

###

def importCompleted(connection, data_dir, uuids):

  # Given a `Connection` object and a "data_dir" string indicating the path to
  # the data directory, this function imports the `showq -c` (abbreviated
  # throughout as "showqc") data into SQLite.

  # Notice also that I really don't worry about error handling with the
  # completed queue data, thanks to ridiculous amounts of multiple coverage.
  # Sample identifiers don't matter at all in the completed queue data except
  # that heeding them allows us avoid attempting to insert old files again.

    cursor = connection.cursor()

    showqc_dir = os.path.join(data_dir, "showqc")

    for sampleid in uuids:
        abspath = os.path.join(showqc_dir, sampleid + "-out.xml")
        obj = readXML(abspath)

        if obj["tree"] is not None:
            showqcXMLtoSQL(connection, obj)

    return

###

def importQueues(connection, data_dir, uuids):

  # Given a `Connection` object and a "data_dir" string indicating the path to
  # the data directory, this function imports the `showq` data into SQLite.

    cursor = connection.cursor()

    showq_dir = os.path.join(data_dir, "showq")

    for sampleid in uuids:
        abspath = os.path.join(showq_dir, sampleid + "-out.xml")
        obj = readXML(abspath)

      # A version of UPSERT that works with SQLite versions older than 3.24:
        cursor.execute("""
            UPDATE sample_info SET showqError = ? WHERE SampleID = ?;
        """, (obj["errtext"], sampleid))
        cursor.execute("""
            INSERT OR IGNORE INTO sample_info (SampleID, showqError)
                VALUES (?, ?);
            """, (sampleid, obj["errtext"]))

        connection.commit()

        if obj["tree"] is not None:
            showqXMLtoSQL(connection, obj)

    return

###

def initializeDatabase(connection):

  # Given a `Connection` object, this function constructs the tables and
  # indexes for the SQLite3 database.

    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS active (

         -- Metadata for our study

            SampleID STRING NOT NULL,
            SampleTime INTEGER NOT NULL,

         -- Data

            Account STRING NOT NULL,
            AWDuration INTEGER,
            Class STRING NOT NULL,
            DRMJID INTEGER,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID STRING NOT NULL,
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
            User STRING NOT NULL,

         -- Other table-specific information

            CONSTRAINT unique_rows UNIQUE (

             -- This can probably simplify to SampleID, SampleTime, JobID ...

                SampleID, SampleTime, Account, Class, GJID, Group_, JobID,
                JobName, QOS, ReqAWDuration, ReqProcs, RunPriority,
                StartPriority, StartTime, State, StatPSDed, StatPSUtl,
                SubmissionTime, SuspendDuration, User
            ),

            FOREIGN KEY(SampleID) REFERENCES sample_info(SampleID)
        )
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS backfill (

         -- Metadata for our study

            SampleID STRING NOT NULL,
            SampleTime INTEGER NOT NULL,

         -- Data

            duration INTEGER NOT NULL,
            index_ INTEGER NOT NULL,
            proccount INTEGER NOT NULL,
            nodecount INTEGER NOT NULL,
            reqid INTEGER NOT NULL,
            starttime INTEGER NOT NULL,

         -- Other table-specific information

            CONSTRAINT unique_rows UNIQUE (
                SampleID, SampleTime, duration, index_, proccount, nodecount,
                reqid, starttime
            ),

            FOREIGN KEY(SampleID) REFERENCES sample_info(SampleID)
        );
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blocked (

         -- Metadata for our study

            SampleID STRING NOT NULL,
            SampleTime INTEGER NOT NULL,

         -- Data

            Account STRING NOT NULL,
            Class STRING NOT NULL,
            DRMJID INTEGER,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID STRING NOT NULL,
            JobName STRING NOT NULL,
            QOS STRING NOT NULL,
            ReqAWDuration INTEGER NOT NULL,
            ReqProcs INTEGER NOT NULL,
            StartPriority INTEGER NOT NULL,
            StartTime INTEGER NOT NULL,
            State STRING NOT NULL,
            SubmissionTime INTEGER NOT NULL,
            SuspendDuration INTEGER NOT NULL,
            User STRING NOT NULL,

         -- Other table-specific information

            CONSTRAINT unique_rows UNIQUE (

             -- This can probably simplify to SampleID, SampleTime, JobID ...

                SampleID, SampleTime, Account, Class, GJID, Group_, JobID,
                JobName, QOS, ReqAWDuration, ReqProcs, StartPriority,
                StartTime, State, SubmissionTime, SuspendDuration, User

            ),

            FOREIGN KEY(SampleID) REFERENCES sample_info(SampleID)
        )
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cluster (

         -- Metadata for our study

            SampleID STRING NOT NULL,
            SampleTime INTEGER NOT NULL,

         -- Data

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

         -- Other table-specific information

            CONSTRAINT unique_rows UNIQUE (

             -- This can probably simplify to SampleID, SampleTime

                SampleID, SampleTime, LocalActiveNodes, LocalAllocProcs,
                LocalConfigNodes, LocalIdleNodes, LocalIdleProcs, LocalUpNodes,
                LocalUpProcs, RemoteActiveNodes, RemoteAllocProcs,
                RemoteConfigNodes, RemoteIdleNodes, RemoteIdleProcs,
                RemoteUpNodes, RemoteUpProcs

            ),

            FOREIGN KEY(SampleID) REFERENCES sample_info(SampleID)
        )
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS completed (
            AWDuration INTEGER,
            Account STRING NOT NULL,
            Class STRING NOT NULL,
            CompletionCode STRING NOT NULL, -- because "CNCLD" can occur
            CompletionTime INTEGER NOT NULL,
            DRMJID INTEGER,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID STRING NOT NULL PRIMARY KEY,
            JobName STRING NOT NULL,
            MasterHost INTEGER,
            PAL STRING,
            QOS STRING NOT NULL,
            ReqAWDuration INTEGER NOT NULL,
            ReqNodes INTEGER,
            ReqProcs INTEGER NOT NULL,
            StartTime INTEGER NOT NULL,
            StatPSDed REAL NOT NULL,
            StatPSUtl REAL NOT NULL,
            State STRING NOT NULL,
            SubmissionTime INTEGER NOT NULL,
            SuspendDuration INTEGER NOT NULL,
            User STRING NOT NULL
        );
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eligible (

         -- Metadata for our study

            SampleID STRING NOT NULL,
            SampleTime INTEGER NOT NULL,

         -- Data

            Account STRING NOT NULL,
            Class STRING NOT NULL,
            DRMJID INTEGER,
            EEDuration INTEGER,
            GJID INTEGER NOT NULL,
            Group_ STRING NOT NULL,
            JobID STRING NOT NULL,
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
            User STRING NOT NULL,

         -- Other table-specific information

            CONSTRAINT unique_rows UNIQUE (

             -- This can probably simplify to SampleID, SampleTime, JobID ...

                SampleID, SampleTime, Account, Class, GJID, Group_, JobID,
                JobName, QOS, ReqAWDuration, ReqProcs, StartPriority,
                StartTime, State, SubmissionTime, SuspendDuration, User

            ),

            FOREIGN KEY(SampleID) REFERENCES sample_info(SampleID)
        )
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sample_info (
            SampleID STRING PRIMARY KEY,
            showbfError STRING,
            showqError STRING
        );
        """)

  # Commit changes

    connection.commit()

    return

###

def main():

  # This is the first function that will execute.

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

    initializeDatabase(connection)

  # Filter directories to find the UUIDs (SampleIDs) of newly collected data
  # that needs to be imported.

    uuids = findNewUUIDs(connection, data_dir)

  # Start populating the database from the raw XML files.

    importBackfill(connection, data_dir, uuids)
    importCompleted(connection, data_dir, uuids)
    importQueues(connection, data_dir, uuids)

  # When we are finished, close the connection to the database.

    connection.close()

    return

###

def readXML(outfilename):

  # Given a string "outfilename", this function returns a dictionary containing
  # the file's SampleID, associated error information about when it was
  # collected, and an `ElementTree` object if the data were collected without
  # errors.

  # Extract the UUID, which is the base name without "-out.xml".
    uuid = os.path.basename(outfilename)[:-8]

  # The XML files are small (typically < 250K) so it's safe to slurp them.
    if os.path.isfile(outfilename) is False:
        return {
            "errtext": "No output file found",
            "SampleID": uuid,
            "tree": None
        }

    xmltext = ""
    with open(outfilename, "r") as xmlfile:
        xmltext = "".join(xmlfile.readlines()).strip()

  # Now, prepare default return values and try to parse the XML content. Note
  # that we do not use the UUID to construct the "errfilename". This is because
  # "errfilename" may be an absolute path, depending on if "outfilename" was an
  # absolute path.

    errfilename = outfilename[:-8] + "-err.xml"
    errtext = None
    tree = None

    if len(xmltext) == 0:
        with open(errfilename, "r") as errfile:
            errtext = "".join(errfile.readlines()).strip()
    else:
        tree = ElementTree.fromstring(xmltext)

    return {
        "errtext": errtext,
        "SampleID": uuid,
        "tree": tree
    }

###

def showbfXMLtoSQL(connection, obj):

    cursor = connection.cursor()

    data = {
        "meta": {},
        "partitions": {}
    }

    root = obj["tree"]

    sampleid = obj["SampleID"]

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
            cursor.execute("""
                INSERT OR IGNORE INTO backfill (
                    SampleID, SampleTime, duration, index_, proccount,
                    nodecount, reqid, starttime
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
                """, (sampleid, time, each["duration"], each["index"],
                each["proccount"], each["nodecount"], each["reqid"],
                each["starttime"]))

  # Commit changes

    connection.commit()

    return

###

def showqXMLtoSQL(connection, obj):

    cursor = connection.cursor()

    data = {
        "cluster": {},
        "jobs": {
            "active": [],
            "blocked": [],
            "eligible": []
        }
    }

    root = obj["tree"]

    sampleid = obj["SampleID"]

    for elem in root:

      # The very first 'elem' will have a tag of "Object" which will say
      # "queue", and it doesn't do anything, so we can ignore it.

        if elem.tag == "cluster":

            data["cluster"] = elem.attrib

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

    if "time" not in data["cluster"]:
        return

    time = data["cluster"]["time"]

  # First, the "active" table ...

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

        cursor.execute("""
            INSERT OR IGNORE INTO active (
                SampleID, SampleTime, Account, AWDuration, Class, DRMJID,
                EEDuration, GJID, Group_, JobID, JobName, MasterHost, PAL,
                QOS, ReqAWDuration, ReqNodes, ReqProcs, RsvStartTime,
                RunPriority, StartPriority, StartTime, State,
                StatPSDed, StatPSUtl, SubmissionTime, SuspendDuration,
                User
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )
            """, (sampleid, time, vals["Account"], vals["AWDuration"],
                vals["Class"], vals["DRMJID"], vals["EEDuration"],
                vals["GJID"], vals["Group"], vals["JobID"], vals["JobName"],
                vals["MasterHost"], vals["PAL"], vals["QOS"],
                vals["ReqAWDuration"], vals["ReqNodes"], vals["ReqProcs"],
                vals["RsvStartTime"], vals["RunPriority"],
                vals["StartPriority"], vals["StartTime"], vals["State"],
                vals["StatPSDed"], vals["StatPSUtl"], vals["SubmissionTime"],
                vals["SuspendDuration"], vals["User"]))

    connection.commit()

  # Next, the "blocked" table ...

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

        cursor.execute("""
            INSERT OR IGNORE INTO blocked (
                SampleID, SampleTime, Account, Class, DRMJID, EEDuration, GJID,
                Group_, JobID, JobName, QOS, ReqAWDuration, ReqProcs,
                StartPriority, StartTime, State, SubmissionTime,
                SuspendDuration, User
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (sampleid, time, vals["Account"], vals["Class"],
                vals["DRMJID"], vals["EEDuration"], vals["GJID"],
                vals["Group"], vals["JobID"], vals["JobName"], vals["QOS"],
                vals["ReqAWDuration"], vals["ReqProcs"], vals["StartPriority"],
                vals["StartTime"], vals["State"], vals["SubmissionTime"],
                vals["SuspendDuration"], vals["User"]))

    connection.commit()

  # Next, the live data about the "cluster", which was previously named "meta".

    vals = data["cluster"]

    cursor.execute("""
        INSERT OR IGNORE INTO cluster (
            SampleID, SampleTime, LocalActiveNodes, LocalAllocProcs,
            LocalConfigNodes, LocalIdleNodes, LocalIdleProcs, LocalUpNodes,
            LocalUpProcs, RemoteActiveNodes, RemoteAllocProcs,
            RemoteConfigNodes, RemoteIdleNodes, RemoteIdleProcs, RemoteUpNodes,
            RemoteUpProcs
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """, (sampleid, time, vals["LocalActiveNodes"],
            vals["LocalAllocProcs"], vals["LocalConfigNodes"],
            vals["LocalIdleNodes"], vals["LocalIdleProcs"],
            vals["LocalUpNodes"], vals["LocalUpProcs"],
            vals["RemoteActiveNodes"], vals["RemoteAllocProcs"],
            vals["RemoteConfigNodes"], vals["RemoteIdleNodes"],
            vals["RemoteIdleProcs"], vals["RemoteUpNodes"],
            vals["RemoteUpProcs"]))

  # Finally, the "eligible" table ...

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

        cursor.execute("""
            INSERT OR IGNORE INTO eligible (
                SampleID, SampleTime, Account, Class, DRMJID, EEDuration, GJID,
                Group_, JobID, JobName, QOS, ReqAWDuration, ReqProcs,
                RsvStartTime, StartPriority, StartTime, State, SubmissionTime,
                SuspendDuration, User
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (sampleid, time, vals["Account"], vals["Class"],
                vals["DRMJID"], vals["EEDuration"], vals["GJID"],
                vals["Group"], vals["JobID"], vals["JobName"], vals["QOS"],
                vals["ReqAWDuration"], vals["ReqProcs"], vals["RsvStartTime"],
                vals["StartPriority"], vals["StartTime"], vals["State"],
                vals["SubmissionTime"], vals["SuspendDuration"], vals["User"]))

    connection.commit()

  # Commit changes

    connection.commit()

    return

###

def showqcXMLtoSQL(connection, obj):

    cursor = connection.cursor()

    data = {
        "jobs": []
    }

    root = obj["tree"]

    for elem in root:
        if elem.tag == "queue":
            for job in elem.getchildren():
                data["jobs"].append(job.attrib)

  # And already we are on to the SQL part.

    completed_fields = [
        "AWDuration", "Account", "Class", "CompletionCode", "CompletionTime",
        "DRMJID", "EEDuration", "GJID", "Group", "JobID", "JobName",
        "MasterHost", "PAL", "QOS", "ReqAWDuration", "ReqNodes", "ReqProcs",
        "StartTime", "StatPSDed", "StatPSUtl", "State", "SubmissionTime",
        "SuspendDuration", "User"
    ]

    for job in data["jobs"]:
        vals = {}
        for field in completed_fields:
            if field in job:
                vals[field] = job[field]
            else:
                vals[field] = None

        cursor.execute("""
            INSERT OR IGNORE INTO completed (
                AWDuration, Account, Class, CompletionCode, CompletionTime,
                DRMJID, EEDuration, GJID, Group_, JobID, JobName, MasterHost,
                PAL, QOS, ReqAWDuration, ReqNodes, ReqProcs, StartTime,
                StatPSDed, StatPSUtl, State, SubmissionTime, SuspendDuration,
                User
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
            """, (vals["AWDuration"], vals["Account"], vals["Class"],
            vals["CompletionCode"], vals["CompletionTime"], vals["DRMJID"],
            vals["EEDuration"], vals["GJID"], vals["Group"], vals["JobID"],
            vals["JobName"], vals["MasterHost"], vals["PAL"], vals["QOS"],
            vals["ReqAWDuration"], vals["ReqNodes"], vals["ReqProcs"],
            vals["StartTime"], vals["StatPSDed"], vals["StatPSUtl"],
            vals["State"], vals["SubmissionTime"], vals["SuspendDuration"],
            vals["User"]))

  # Commit changes

    connection.commit()

    return

###

if __name__ == "__main__":
    main()

#-  vim:set syntax=python:
