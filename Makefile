.POSIX:

CC = mpicc

# mpicc uses gcc
CFLAGS = -std=c99 -pedantic -Wall -Wextra -fopenmp -O2

# mpicc uses icc
#CFLAGS = -std=c99 -openmp -O2 -Wall -strict-ansi

mpififo: mpififo.c

clean:
	rm -f mpififo
