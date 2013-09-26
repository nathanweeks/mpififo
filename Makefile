.POSIX:

CC = mpicc

# macros used by "make test"
NODES = 1 # number of nodes
NPROC = 4 # number of (non-mpififo) processes

# mpicc uses gcc
CFLAGS = -std=c99 -pedantic -Wall -Wextra -fopenmp -O2

# mpicc uses icc
#CFLAGS = -std=c99 -openmp -O2 -Wall -strict-ansi

mpififo: mpififo.c

check:
	NODES=$(NODES) NPROC=$(NPROC) tests/sort.sh

clean:
	rm -f mpififo
