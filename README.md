moab-data
=========

Summary
-------

This project contains programs for the collection and analysis of live
[MOAB](http://www.adaptivecomputing.com/support/documentation-index/moab-hpc-suite-documentation/moab-hpc-suite-9-1-documentation/)
data on [Titan](https://en.wikipedia.org/wiki/Titan_(supercomputer)), which
will be used for the "blocking probability" study undertaken as part of the
following paper:

https://docs.google.com/document/d/1EbOKNCbhoSIAkrhRxuNB52Dy5BnpRz9bWPxcnAADins/edit


Running the programs
--------------------

Most of the programs are self-contained, but visualization programs will use
[matplotlib](https://matplotlib.org/), which is not part of a standard Python
distribution. To this end, the easiest way I've found so far to use matplotlib
on Titan is to
[load Anaconda](https://www.olcf.ornl.gov/software_package/anaconda/) prior to
running any of the analysis scripts:

    $ module load python_anaconda
    $ python analysis/visualization-template.py

