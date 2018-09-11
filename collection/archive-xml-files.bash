#-  Bash shell script

#-  archive-xml-files.bash ~~
#
#   This program solves the obvious problems that occur when periodic data
#   sampling proceeds long enough by saving lots of files as one compressed
#   file. The main reason for doing this at OLCF is not because of the size of
#   files but because of the number of files.
#
#   The idea here is that, once the XML files have been imported into SQLite,
#   they can be safely archived as tarballs. I have created one large tarball
#   for all data that was collected and imported prior to 11 Sep 2018, and this
#   script will be used to create "delta" tarballs periodically so that the
#   entire contents of the SQLite database may be reconstructed by
#   decompressing and importing the data stored in each of the individual
#   tarballs.
#
#                                                       ~~ (c) SRW, 10 Sep 2018
#                                                   ~~ last updated 11 Sep 2018

#-  Define variables that represent directories where the data are being
#   collected and where the data will be relocated.

data_dir='/lustre/atlas/proj-shared/csc108/data/moab'
dest_dir="${HOME}/delta-$(date "+%d-%b-%Y" | tr '[:upper:]' '[:lower:]')"

#-  Ensure that child directories exist in the destination directory.

mkdir -p ${dest_dir}/showbf
mkdir -p ${dest_dir}/showq
mkdir -p ${dest_dir}/showqc

function mv_if_exists() {
  # This is a shell function which encapsulates logic for checking for the
  # existence of files before trying to move them, because errors sometimes
  # occur during collection which cause files not to be created.
    data_dir=$1
    dest_dir=$2
    sample_id=$3
    if [ -f ${data_dir}/showbf/${sample_id}-err.xml ]; then
        mv -fv ${data_dir}/showbf/${sample_id}-err.xml ${dest_dir}/showbf/
    fi
    if [ -f ${data_dir}/showbf/${sample_id}-out.xml ]; then
        mv -fv ${data_dir}/showbf/${sample_id}-out.xml ${dest_dir}/showbf/
    fi
    if [ -f ${data_dir}/showq/${sample_id}-err.xml ]; then
        mv -fv ${data_dir}/showq/${sample_id}-err.xml ${dest_dir}/showq/
    fi
    if [ -f ${data_dir}/showq/${sample_id}-out.xml ]; then
        mv -fv ${data_dir}/showq/${sample_id}-out.xml ${dest_dir}/showq/
    fi
    if [ -f ${data_dir}/showqc/${sample_id}-err.xml ]; then
        mv -fv ${data_dir}/showqc/${sample_id}-err.xml ${dest_dir}/showqc/
    fi
    if [ -f ${data_dir}/showqc/${sample_id}-out.xml ]; then
        mv -fv ${data_dir}/showqc/${sample_id}-out.xml ${dest_dir}/showqc/
    fi
}

#-  Export the shell function so that it can be used by forked processes in the
#   `xargs` call below.

export -f mv_if_exists

#-  This next line is a fun one! We call the command-line interface to SQLite
#   version 3 interactively in order to get a list of sample IDs which have
#   already been imported into the database, and then we feed this list into
#   `xargs` because the list is potentially longer than Bash can loop over. The
#   `xargs` call sends the elements of the list to a number of Bash contexts
#   equal to the number of processors on the machine this script is running on,
#   and each of these contexts evaluates the shell function defined above.

echo '.quit' | \
    sqlite3 ${data_dir}/moab-data.sqlite \
        -cmd 'SELECT SampleID FROM sample_info;' | \
            xargs -n 1 -P $(nproc) -I{} bash -c \
                "mv_if_exists ${data_dir} ${dest_dir} {}"

#-  Finally, create a tarball of the new destination directory.

tar cvfz ${dest_dir}.tar.gz ${dest_dir}/

#-  NOTE: This program does not delete the new directory which contains all the
#   raw XML data. This is a deliberate choice that is made in order to prevent
#   data loss. Ultimately, stuff happens, and humans need to check the work.

#-  vim:set syntax=sh:
