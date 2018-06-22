#-  Python 2.6 source code

#-  count-xml-fields.py ~~
#
#   This is a self-contained program that I used to count the number of
#   occurrences of various fields in the XML data, in order to determine which
#   columns were optional and therefore might contain NULL values once the data
#   were imported into SQLite.
#
#                                                       ~~ (c) SRW, 13 Jun 2018
#                                                   ~~ last updated 22 Jun 2018

import json
import os
from xml.etree import ElementTree

###

def readXML(filename):
    xmltext = ""
    with open(filename, "r") as xmlfile:
        xmltext = "".join(xmlfile.readlines()).strip()

    if len(xmltext) == 0:
      # Now we need to check the error file to see what happened.
        errfilename = filename[:-8] + "-err.xml"
        with open(errfilename, "r") as errfile:
            errtext = "".join(errfile.readlines()).strip()
            #print errtext
        xmltext = "<placeholder>so the rest of the program runs</placeholder>"
    return xmltext

###

def showbfCount():

    showbf_dir = "/lustre/atlas/proj-shared/csc108/data/moab/showbf"

    fields = {
        "meta": {},
        "partitions": {}
    }
    for filename in os.listdir(showbf_dir):
        if filename.endswith("-out.xml"):

            abspath = os.path.join(showbf_dir, filename)
            xmltext = readXML(abspath)
            data = showbfXMLtoDict(xmltext)

            for key in data["meta"]:
                if key not in fields["meta"]:
                    fields["meta"][key] = 0
                fields["meta"][key] += 1

            for key in data["partitions"]:
                if key not in fields["partitions"]:
                    fields["partitions"][key] = {}
                for each in data["partitions"][key]:
                    for k in each:
                        if k not in fields["partitions"][key]:
                            fields["partitions"][key][k] = 0
                        fields["partitions"][key][k] += 1

    return fields

###

def showbfXMLtoDict(text):

    data = {
        "meta": {},
        "partitions": {}
    }

    root = ElementTree.fromstring(text)

    counter = 0
    for elem in root:

    # The very first 'elem' will have a tag of "Object" which will say
    # "cluster", and it doesn't do anything, so we can ignore it.

        if elem.tag == "job":
            data["meta"] = elem.attrib

        elif elem.tag == "par" and elem.attrib["Name"] != "template":
            data["partitions"][elem.attrib["Name"]] = []
            for each in elem.getchildren():
                data["partitions"][elem.attrib["Name"]].append(each.attrib)

    return data

###

def showqCount():

    showq_dir = "/lustre/atlas/proj-shared/csc108/data/moab/showq"

    fields = {
        "jobs": {
            "active": {},
            "eligible": {},
            "blocked": {}
        },
        "meta": {}
    }

    counter = 0
    limit = 1000
    for filename in os.listdir(showq_dir):
        if filename.endswith("-out.xml"):
            #print "Output file: %s" % filename

            abspath = os.path.join(showq_dir, filename)
            xmltext = readXML(abspath)
            data = showqXMLtoDict(xmltext)

            for key in data["meta"]:
                if key not in fields["meta"]:
                    fields["meta"][key] = 0
                fields["meta"][key] += 1

            for each in data["jobs"]["active"]:
                for key in each:
                    if key not in fields["jobs"]["active"]:
                        fields["jobs"]["active"][key] = 0
                    fields["jobs"]["active"][key] += 1

            for each in data["jobs"]["eligible"]:
                for key in each:
                    if key not in fields["jobs"]["eligible"]:
                        fields["jobs"]["eligible"][key] = 0
                    fields["jobs"]["eligible"][key] += 1

            for each in data["jobs"]["blocked"]:
                for key in each:
                    if key not in fields["jobs"]["blocked"]:
                        fields["jobs"]["blocked"][key] = 0
                    fields["jobs"]["blocked"][key] += 1

            counter += 1
            if counter == limit:
                break

    return fields

###

def showqXMLtoDict(text):

    data = {
        "jobs": {
            "active": [],
            "eligible": [],
            "blocked": []
        },
        "meta": {}
    }

    root = ElementTree.fromstring(text)

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

    return data

###

def main():

    print "showbf: %s" % json.dumps(showbfCount(), indent=4)

    print "showq: %s" % json.dumps(showqCount(), indent=4)

###

if __name__ == '__main__':
    main()

#-  vim:set syntax=python:
