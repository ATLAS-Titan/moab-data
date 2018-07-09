#-  Python 2.6 source code (also tested with 2.7)

#-  collect-xml-data.py ~~
#
#   This program is designed to run as a cron job that will capture XML output
#   from MOAB's "showbf" and "showq" to files to be processed later. This
#   program is self-contained, and it will create a folder on Lustre at
#
#       /lustre/atlas/proj-shared/csc108/data/moab/
#
#   with subdirectories to store the XML files.
#
#                                                       ~~ (c) SRW, 06 Jun 2018
#                                                   ~~ last updated 09 Jul 2018

import os
import subprocess
import uuid

###

def sample():

  # Set proper environment variables for getting MOAB data for Titan while the
  # script is running on a DTN (data transfer node), which has different
  # defaults. The reason for copying the environment variables here, instead of
  # setting them directly with
  #
  #     os.environ["MOABSERVER"] = "titan-batch.ccs.ornl.gov"
  #     os.environ["PBS_DEFAULT"] = "titan-batch.ccs.ornl.gov"
  #
  # is to avoid modifying the environment of the script's own process. Also, in
  # order to run this script as a cron job, it is necessary to add the MOAB
  # tools directory to the PATH.

    env = os.environ.copy()
    env["MOABSERVER"] = "titan-batch.ccs.ornl.gov"
    env["PBS_DEFAULT"] = "titan-batch.ccs.ornl.gov"
    env["PATH"] += ":/opt/moab/bin"

  # Generate a hexadecimal representation of a random UUID with which to build
  # unique filenames.

    unique_hex = uuid.uuid4().hex

  # Create directories for storing the data, just in case they're not present
  # when the script runs for the first time.

    showbf_dir = "/lustre/atlas/proj-shared/csc108/data/moab/showbf"
    if not os.path.exists(showbf_dir):
        os.makedirs(showbf_dir)

    showq_dir = "/lustre/atlas/proj-shared/csc108/data/moab/showq"
    if not os.path.exists(showq_dir):
        os.makedirs(showq_dir)

  # Capture stdout and stderr from "showbf" as XML files. The "-p titan"
  # returns the values for the "titan" partition only. Note that there is also
  # a "--blocking" flag which fetches fresh (non-cached) data, but that I have
  # elected not to use it, due to the fact that 10% of my queries on the first
  # day of using it failed due to "resources unavailable" errors. The previous
  # day, when I used cached data, I had zero errors. Since we are only sampling
  # every 5 minutes anyway, there is no reason to worry about stale query
  # results. The "--blocking" flag is extremely important for the operation of
  # BigPanDA but not for gathering data about MOAB.

    showbf_out = os.path.join(showbf_dir, unique_hex + "-out.xml")
    showbf_err = os.path.join(showbf_dir, unique_hex + "-err.xml")
    with open(showbf_out, "wb") as out:
        with open(showbf_err, "wb") as err:
            subprocess.Popen(["showbf", "--format=xml", "-p", "titan"],
                    stdout=out, stderr=err, env=env)

  # Capture stdout and stderr from "showq" as XML files. The flags used here
  # are the same as previously used for "showbf".

    showq_out = os.path.join(showq_dir, unique_hex + "-out.xml")
    showq_err = os.path.join(showq_dir, unique_hex + "-err.xml")
    with open(showq_out, "wb") as out:
        with open(showq_err, "wb") as err:
            subprocess.Popen(["showq", "--format=xml", "-p", "titan"],
                    stdout=out, stderr=err, env=env)

  # This part is experimental. There's nothing wrong with collecting data
  # before it is known for certain that it will be needed, but for now, I
  # will leave all of this as comments. Read more here:
  #
  #     http://docs.adaptivecomputing.com/maui/commands/mjobctl.php

  # mjobctl_dir = "/lustre/atlas/proj-shared/csc108/data/moab/mjobctl"
  # if not os.path.exists(mjobctl_dir):
  #     os.makedirs(mjobctl_dir)
  #
  # mjobctl_out = os.path.join(mjobctl_dir, unique_hex + "-out.xml")
  # mjobctl_err = os.path.join(mjobctl_dir, unique_hex + "-err.xml")
  # with open(mjobctl_out, "wb") as out:
  #     with open(mjobctl_err, "wb") as err:
  #         subprocess.Popen(["mjobctl", "-q", "diag", "ALL", "--format=xml"],
  #                 stdout=out, stderr=err, env=env)

###

def main():

  # Initially, `sample` was separated from `main` only so that I could fake a
  # cron-type functionality with the following loop:
  #
  #     import time
  #     while True:
  #         sample()
  #         time.sleep(60)
  #
  # I had to do that in order to implement periodic sampling without cron
  # because I didn't have privileges to run cron on a Titan login node and I
  # didn't realize that setting environment variables would allow the DTNs to
  # access the correct MOAB server. Now, this script is run as a cron job.

    sample()

###

if __name__ == "__main__":
  main()

#-  vim:set syntax=python:
