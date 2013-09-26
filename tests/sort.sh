#!/usr/bin/env sh

# Tested with MPICH 3.0.4 with hydra process manager

# ENVIRONMENT VARIABLES
#     NODES
#         Number of nodes on which to launch mpififo 
#
#     NPROC
#         Number of sort processes

set -o errexit -o nounset

if [ ! "${PMI_RANK:-}" ] # not running under MPI
then
  i=0
  while [ ${i} -lt ${NPROC} ]
  do
    FIFOS="iw${i}.fifo ir${i}.fifo ow${i}.fifo or${i}.fifo ${FIFOS:-}"
    i=$((i+1))
  done
  # clean up fifos upon exit
  trap "rm -f ${FIFOS}" EXIT HUP INT QUIT ABRT ALRM TERM
    
  mkfifo ${FIFOS}
  sleep 1
  mpiexec -ppn 1 -n ${NODES} mpififo ${FIFOS} &
  mpiexec -ppn 1 -n ${NPROC} ${0}

else # running under MPI
  sort ir${PMI_RANK}.fifo > ow${PMI_RANK}.fifo &
  if [ ${PMI_RANK} -eq 0 ]
  then
    # generate pseudorandom list of real numbers, evenly distributed among
    # ranks
    awk -v NPROC=${NPROC} -v NLINES=100000 '
    BEGIN {
        for (i = 0; i < NLINES; i++)
            print rand() > "iw" i % NPROC ".fifo"
      }' &

    # merge sort & check to ensure sorted
    if sort -m or*.fifo | sort -c
    then
      echo 'success'
    else
      echo 'failure'
    fi
  fi

  wait # wait for sort processes to complete

fi

