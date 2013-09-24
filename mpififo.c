// NAME
//     mpififo - inter-node named pipes over MPI
//
// SYNOPSIS
//     [mpiexec] mpififo [-crv] WRITEFIFO READFIFO [WRITEFIFO READFIFO...]
//
// NOTES
//     If N fifo pairs are specified, each process will create threads
//     {0,1,...,2N-1}, except the rank 0 process, which will create threads
//     {0,1,...,3N-1}, with an extra N coordinator threads. 
//
//     Each thread t in {0,1,...,2N-1} communicates with coordinator thread:
//     c(t) = N + floor(t/2)
//
// AUTHOR
//     Nathan T. Weeks <weeks@iastate.edu>
//
// LICENSE
//     Copyright 2011,2013 Nathan T. Weeks
//
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
#define _XOPEN_SOURCE 700
#include <fcntl.h>
#include <mpi.h>
#include <omp.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stddef.h>  // for NULL (mpich-1.3 requires this)
#include <stdio.h>
#include <sys/stat.h> // for mkfifo() & file mode bits
#include <unistd.h>
#define BUFSIZE 65536 // Read/send this many bytes at a time 
                      // a command-line option or environment variable)

// MPI tag values
typedef enum {
    READY_READER,
    READY_WRITER,
    READER_RANK_THREAD_NUM,
    WRITER_RANK_THREAD_NUM,
    DATA,
    NUM_COMMANDS
} command_t;

// lookup table a readfifo/writefifo thread uses to determine the MPI tag
inline int tag_lookup(int thread_num, command_t command) {
    return thread_num / 2 * NUM_COMMANDS + command; 
}

// lookup table a coordinator thread uses to determine the MPI tag
inline int coordinator_tag_lookup(
    int thread_num, // coordinator thread_num
    command_t command, 
    int num_fifo_threads
) {
    return (thread_num - num_fifo_threads)*NUM_COMMANDS + command;
}

// check arguments (both number & type)
int main(int argc, char *argv[]) {
    int c;
    bool create_fifos = false, remove_fifos=false,
         verbose = false;
    while ((c = getopt(argc, argv, "cdrv")) != -1) {
        switch(c) {
            case 'c':
                create_fifos = true;
                break;
            case 'r':
                remove_fifos = true;
                break;
            case 'v':
                verbose = true;
                break;
            case '?':
                fprintf(stderr,
                    "Usage: mpififo [-cdrv] writefifo1 readfifo1 [writefifo2 readfifo2...]\n"
                    "Options:\n"
                    "   -c : create fifos upon startup\n"
                    "   -r : remove fifos after mpififo exits\n"
                    "   -v : verbose output to stderr (for debugging)\n");
                fprintf(stderr, "Unrecognized option: '-%c'\n", optopt);
                exit(1);
        }
    }

    if (verbose)
        fprintf(stderr, "beginning execution\n");

    int provided; 
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) { // need MPI_THREAD_MULTIPLE support
        fprintf(stderr, "%s:%d: ERROR: MPI_THREAD_MULTIPLE not supported\n",
                                __FILE__, __LINE__);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (verbose)
        fprintf(stderr, "rank %i reporting for duty\n", rank);

    // if "-c" option is specified, rank 0 process creates FIFO with owner
    // read, write permissions
    if (create_fifos && rank == 0)
        for (int i = optind; i < argc; i++)
            if (mkfifo(argv[i], S_IRUSR | S_IWUSR) == -1) {
                perror("mkfifo"); 
                MPI_Abort(MPI_COMM_WORLD, 2);
            }

    omp_set_dynamic(false); // need exactly the number of threads requested

// Each process will have argc threads, one for each command-line fifo
// argument, except the rank 0 process, which has one extra "coordinator" 
// thread per fifo pair.

    const int NUM_FIFOS = argc - optind;

#ifndef OPEN_MPI
#pragma omp parallel num_threads(rank == 0 ? NUM_FIFOS+NUM_FIFOS/2 : NUM_FIFOS)\
 shared(argv, optind, stderr, rank, verbose) default(none)
#else
#pragma omp parallel num_threads(rank == 0 ? NUM_FIFOS+NUM_FIFOS/2 : NUM_FIFOS) \
 shared(argv, optind, stderr, rank)
#endif
{
    char buf[BUFSIZE];
    MPI_Status status;
    enum {RANK, THREAD_NUM}; // index into reader_rank_thread_num[]
    int count, reader_rank_thread_num[2], writer_rank_thread_num[2];
    int thread_num = omp_get_thread_num();
    if (thread_num >= NUM_FIFOS) { // coordinator thread (only on rank 0)
        // receive ready writer
        MPI_Recv(&writer_rank_thread_num[THREAD_NUM], 1, MPI_INT,
                 MPI_ANY_SOURCE, coordinator_tag_lookup(thread_num,
                 READY_WRITER, NUM_FIFOS), MPI_COMM_WORLD, &status);
        writer_rank_thread_num[RANK] = status.MPI_SOURCE;

        if (verbose)
            fprintf(stderr, "coordinator (%i): "
                            "received tag (%i) from writer (%i,%i,'%s')\n",
                    thread_num, coordinator_tag_lookup(thread_num, 
                    READY_WRITER, NUM_FIFOS), writer_rank_thread_num[RANK], 
                    writer_rank_thread_num[THREAD_NUM],
                    argv[optind+writer_rank_thread_num[THREAD_NUM]]);

        // receive ready reader
        MPI_Recv(&reader_rank_thread_num[THREAD_NUM], 1, MPI_INT, 
                 MPI_ANY_SOURCE, 
                 coordinator_tag_lookup(thread_num, READY_READER, NUM_FIFOS),
                 MPI_COMM_WORLD, &status);
        reader_rank_thread_num[RANK] = status.MPI_SOURCE;

        if (verbose)
            fprintf(stderr, "coordinator (%i): "
                            "received tag (%i) from reader (%i,%i,'%s')\n",
                    thread_num, 
                    coordinator_tag_lookup(thread_num, READY_READER, NUM_FIFOS),
                    reader_rank_thread_num[RANK], 
                    reader_rank_thread_num[THREAD_NUM],
                    argv[optind+reader_rank_thread_num[THREAD_NUM]]);

        // send the writers's rank & thread_num to the reader
        MPI_Send(writer_rank_thread_num, 2, MPI_INT, 
             reader_rank_thread_num[RANK],
             coordinator_tag_lookup(thread_num, WRITER_RANK_THREAD_NUM, 
             NUM_FIFOS), MPI_COMM_WORLD);
        
        if (verbose)
            fprintf(stderr,
                    "coordinator(%i): sent writer (%i,%i,'%s') tag (%i) to reader (%i,%i,'%s')\n",
                    thread_num,
                    writer_rank_thread_num[RANK],
                    writer_rank_thread_num[THREAD_NUM],
                    argv[optind+writer_rank_thread_num[THREAD_NUM]],
                    coordinator_tag_lookup(thread_num, WRITER_RANK_THREAD_NUM, NUM_FIFOS),
                    reader_rank_thread_num[RANK],
                    reader_rank_thread_num[THREAD_NUM],
                    argv[optind+reader_rank_thread_num[THREAD_NUM]]);

        // send the reader's rank & thread_num to the writer
        MPI_Send(reader_rank_thread_num, 2, MPI_INT, 
             writer_rank_thread_num[RANK],
             coordinator_tag_lookup(thread_num, READER_RANK_THREAD_NUM, 
             NUM_FIFOS), MPI_COMM_WORLD);

        if (verbose)
            fprintf(stderr,
                    "coordinator(%i): sent reader (%i,%i,'%s') tag (%i) to writer (%i,%i,'%s')\n",
                    thread_num, 
                    reader_rank_thread_num[RANK],
                    reader_rank_thread_num[THREAD_NUM],
                    argv[optind+reader_rank_thread_num[THREAD_NUM]],
                    coordinator_tag_lookup(thread_num, READER_RANK_THREAD_NUM, NUM_FIFOS),
                    writer_rank_thread_num[RANK],
                    writer_rank_thread_num[THREAD_NUM],
                    argv[optind+writer_rank_thread_num[THREAD_NUM]]);

    } else if (thread_num % 2 == 0) {
//****************************************
// writer thread
//****************************************
        FILE *writefifo;
        if((writefifo = fopen(argv[optind + thread_num], "r")) == NULL) {
            perror("fopen writefifo");
            MPI_Abort(MPI_COMM_WORLD, 3);
        }

        if (verbose)
            fprintf(stderr, "writer (%i,%i,'%s'): fifo opened\n", 
                    rank, thread_num, argv[optind + thread_num]);

        // at this point, a writer has opened the fifo; notify coordinator
        MPI_Send(&thread_num, 1, MPI_INT, 0, 
          tag_lookup(thread_num, READY_WRITER), MPI_COMM_WORLD);

        if (verbose)
            fprintf(stderr,
                    "writer (%i,%i,'%s'): notified rank 0 tag (%i)\n",
                    rank, thread_num, argv[optind+thread_num],
                    tag_lookup(thread_num, READY_WRITER));

        // get the rank & thread_num of a process that is ready to receive
        // (i.e., a process on that host opened the output fifo for
        // reading)
        MPI_Recv(reader_rank_thread_num, 2, MPI_INT, 0, 
            tag_lookup(thread_num, READER_RANK_THREAD_NUM), MPI_COMM_WORLD,
            &status);

        if (verbose)
            fprintf(stderr,
                    "writer (%i,%i,'%s'): got reader: (%i,%i,'%s') tag (%i)\n",
                    rank,
                    thread_num,
                    argv[optind+thread_num],
                    reader_rank_thread_num[RANK],
                    reader_rank_thread_num[THREAD_NUM],
                    argv[optind+reader_rank_thread_num[THREAD_NUM]],
                    tag_lookup(thread_num, READER_RANK_THREAD_NUM));

        while ((count = (int)fread(buf, 1, BUFSIZE, writefifo))) {
            if (verbose)
                fprintf(stderr,
                        "writer (%i,%i,'%s'): read data from fifo\n", 
                        rank,
                        thread_num,
                        argv[optind+thread_num]);

            MPI_Send(buf, count, MPI_BYTE, reader_rank_thread_num[RANK], 
                     tag_lookup(thread_num, DATA), MPI_COMM_WORLD); 

            if (verbose)
                fprintf(stderr,
                        "writer (%i,%i,'%s'): sent %i bytes to reader (%i,%i,'%s') tag(%i)\n",
                        rank,
                        thread_num,
                        argv[optind+thread_num],
                        count,
                        reader_rank_thread_num[RANK], 
                        reader_rank_thread_num[THREAD_NUM],
                        argv[optind+reader_rank_thread_num[THREAD_NUM]],
                        tag_lookup(thread_num, DATA));
        }
        if (ferror(writefifo))
            MPI_Abort(MPI_COMM_WORLD, 4);
        else // send empty message to receiver to indicate EOF
            MPI_Send(NULL, 0, MPI_BYTE, reader_rank_thread_num[RANK], 
                 tag_lookup(thread_num, DATA), MPI_COMM_WORLD); 
     }
//****************************************
// reader thread
//****************************************
    else {
        FILE *readfifo;
        // TODO: ensure fifo exists; fopen() will create a file if it doesn't!
        if ((readfifo = fopen(argv[optind + thread_num], "w")) == NULL) {
            perror("fopen readfifo");
            MPI_Abort(MPI_COMM_WORLD, 5);
        }

        if (verbose)
            fprintf(stderr, "reader(%i,%i,'%s'): fifo opened\n", rank,
                    thread_num, argv[optind+thread_num]);

        // notify the coordinator that a process on this node has opened 
        // the readfifo
        MPI_Send(&thread_num, 1, MPI_INT, 0, 
          tag_lookup(thread_num, READY_READER), MPI_COMM_WORLD);

        if (verbose)
            fprintf(stderr,
                    "reader (%i,%i,'%s'): notified rank 0 tag(%i)\n",
                    rank, thread_num, argv[optind+thread_num], 
                    tag_lookup(thread_num, READY_READER));

        // get the rank & thread_num of a process that is ready to send
        // (i.e., a process on that host opened the output fifo for
        // writing).
        MPI_Recv(writer_rank_thread_num, 2, MPI_INT, 0, 
            tag_lookup(thread_num, WRITER_RANK_THREAD_NUM), MPI_COMM_WORLD,
            &status);

        if (verbose)
            fprintf(stderr,
                    "reader (%i,%i,'%s'): got writer (%i,%i,'%s') tag (%i)\n",
                    rank, thread_num, argv[optind+thread_num],
                    writer_rank_thread_num[RANK],
                    writer_rank_thread_num[THREAD_NUM],
                    argv[optind+writer_rank_thread_num[THREAD_NUM]],
                    tag_lookup(thread_num, WRITER_RANK_THREAD_NUM));

        do {
            MPI_Recv(buf, BUFSIZE, MPI_BYTE, writer_rank_thread_num[RANK],
                     tag_lookup(thread_num, DATA), MPI_COMM_WORLD, 
                     &status);

            MPI_Get_count(&status, MPI_BYTE, &count);

            if (verbose)
                fprintf(stderr,
                        "reader(%i,%i,'%s'): received from writer (%i,%i,'%s') tag (%i) %i bytes\n",
                        rank, thread_num, argv[optind+thread_num],
                        writer_rank_thread_num[RANK], 
                        writer_rank_thread_num[THREAD_NUM], 
                        argv[optind+writer_rank_thread_num[THREAD_NUM]], 
                        tag_lookup(thread_num, DATA), count);

            if (count > 0) { // if there is data to output
                if (fwrite(buf, 1, count, readfifo) < (size_t)count) {
                    perror("fwrite");
                    MPI_Abort(MPI_COMM_WORLD, 6);
                }
            } else {
                if (fclose(readfifo) == EOF) {
                    perror("fclose");
                    MPI_Abort(MPI_COMM_WORLD, 7);
                }
            }
        } while (count > 0);

    }
    if (verbose)
        fprintf(stderr, "rank %d thread %d at end of parallel region\n", rank, thread_num);
}
    MPI_Finalize();

    // if "-r" option is specified, rank 0 process removes fifos
    if (remove_fifos && rank == 0)
        for (int i = optind; i < argc; i++)
            if (unlink(argv[i]) == -1) {
                perror("unlink"); 
                exit(1);
            }
    return 0;
}
