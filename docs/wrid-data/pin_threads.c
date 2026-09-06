#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static _Atomic unsigned worker_index;
static _Atomic unsigned poller_index;
static void pin(pthread_t thread, unsigned cpu, const char *name) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    int result = pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
    if (result != 0) {
        fprintf(stderr, "pin_threads: failed cpu=%u name=%s error=%d\n", cpu, name, result);
        abort();
    }
    fprintf(stderr, "pin_threads: %s -> cpu=%u\n", name, cpu);
}
__attribute__((constructor)) static void pin_main(void) {
    pin(pthread_self(), 8, "main");
}
int pthread_setname_np(pthread_t thread, const char *name) {
    int (*real_setname)(pthread_t, const char *) = dlsym(RTLD_NEXT, "pthread_setname_np");
    int result = real_setname(thread, name);
    if ((strncmp(name, "tokio-runtime-w", 15) == 0 || strcmp(name, "tokio-rt-worker") == 0)) {
        unsigned index = atomic_fetch_add(&worker_index, 1);
        pin(thread, index % 8, name);
    } else if (strncmp(name, "ruapc-rdma-poll", 14) == 0) {
        unsigned index = atomic_fetch_add(&poller_index, 1);
        pin(thread, 9 + index % 2, name);
    }
    return result;
}
