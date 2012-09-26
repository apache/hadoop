#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "webhdfs.h"

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

void current_utc_time(struct timespec *ts) {
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    ts->tv_sec = mts.tv_sec;
    ts->tv_nsec = mts.tv_nsec;
#else
    clock_gettime(CLOCK_REALTIME, ts);
#endif
    
}

long get_time() {
    struct timespec tp;
    current_utc_time(&tp);
    return (long)((tp.tv_sec * 1000000000) + tp.tv_nsec);
}

#define SIZE 512*1024*1024
#define READ_SIZE 512*1024*1024
#define DISCARD_COUNT 5

int main(int argc, char** argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: test_read_bm <namenode> <user_name> <iteration_number>\n");
        exit(0);
    }
    
    hdfsFS fs = hdfsConnectAsUser(argv[1], 50070, argv[2]);
    
    /* printf("File is null: %d\n", file == NULL ? 1 : 0); */
    
    char *buf = (char *) malloc(sizeof(unsigned char) * SIZE);
    
    printf("Read size: %d\n", READ_SIZE);
    
    int iterations = atoi(argv[3]);
    
    if (iterations <= DISCARD_COUNT) {
        printf("Iterations should be at least %d\n", DISCARD_COUNT + 1);
        exit(0);
    }
    
    printf("Running %d iterations\n", iterations);
    float time_total;
    float max = 0.f;
    float min = 999999999999999.f;
    
    printf("Start...\n");
    int i;
    for (i=0; i<iterations; ++i) {
        long start = get_time();
        hdfsFile file = hdfsOpenFile(fs, "/tmp/512_mb.txt", O_RDONLY, 0, 0, 0);
        int n = 0;
        
        while (n < SIZE) {
            int nread = hdfsRead(fs, file, buf + n, READ_SIZE);
            if (nread <= 0) {
                printf("EOF before finished, read %d bytes\n", n);
                hdfsDisconnect(fs);
                return 0;
            }
            n += nread;
            printf("Read %d kilobytes\n", nread / 1024);
        }
        
        long end = get_time();
        printf("Read %d bytes, hoping for %d.\n", n, SIZE);
        long elapsed = (end - start);
        printf("Start: %lu, end: %lu\n", start, end);
        float time = elapsed / (1000000000.0f);
        printf ("Took %2.6fs\n", time);
        printf("Throughput: %2.2fMB/s\n", SIZE * 1.0f / (1024 * 1024 * time));
        if (i >= DISCARD_COUNT) {
            time_total += time;
            if (time < min) {
                min = time;
            }
            if (time > max) {
                max = time;
            }
        }
    }
    hdfsDisconnect(fs);
    printf("------\n");
    printf("Average time: %2.2fs\n", time_total / (iterations - DISCARD_COUNT));
    printf("Max. time: %2.2f, min. time: %2.2f\n", max, min);
    float maxt = SIZE * 1.f / (1024 * 1024 * max);
    float mint = SIZE * 1.f / (1024 * 1024 * min);
    printf("Average throughput: %2.2fMB/s\n", 1.f * SIZE * (iterations - DISCARD_COUNT) / (1024 * 1024 * time_total));
    printf("Max. throughput: %2.2f, min. throughput: %2.2f\n", maxt, mint);
    
    //  printf("File contents: %d\n", buf[0]);
    return 0;
}

