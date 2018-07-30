#-  Bash shell script

#-  launch-all.bash ~~
#
#   So this script doesn't do much, but it's useful for rendering new versions
#   of all the plots. I could do this much more efficiently with a Makefile,
#   though.
#
#                                                       ~~ (c) SRW, 30 Jul 2018
#                                                   ~~ last updated 30 Jul 2018

module load python_anaconda2;

DIR_OF_THIS_SCRIPT=$(cd `dirname $0` && pwd)
NUM_OF_PROCESSORS=$(nproc)

ls $DIR_OF_THIS_SCRIPT/*.py | xargs -n 1 -P $NUM_OF_PROCESSORS -I{} python {};

#-  vim:set syntax=sh:
