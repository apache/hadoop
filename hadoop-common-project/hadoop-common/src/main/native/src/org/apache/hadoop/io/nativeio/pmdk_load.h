/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"

#ifdef UNIX
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dlfcn.h>
#endif

#ifndef _PMDK_LOAD_H_
#define _PMDK_LOAD_H_


#ifdef UNIX
// For libpmem.h
typedef void * (*__d_pmem_map_file)(const char *path, size_t len, int flags, mode_t mode,
    size_t *mapped_lenp, int *is_pmemp);
typedef int (* __d_pmem_unmap)(void *addr, size_t len);
typedef int (*__d_pmem_is_pmem)(const void *addr, size_t len);
typedef void (*__d_pmem_drain)(void);
typedef void * (*__d_pmem_memcpy_nodrain)(void *pmemdest, const void *src, size_t len);
typedef int (* __d_pmem_msync)(const void *addr, size_t len);

#endif

typedef struct __PmdkLibLoader {
  // The loaded library handle
  void* libec;
  char* libname;
  __d_pmem_map_file pmem_map_file;
  __d_pmem_unmap pmem_unmap;
  __d_pmem_is_pmem pmem_is_pmem;
  __d_pmem_drain pmem_drain;
  __d_pmem_memcpy_nodrain pmem_memcpy_nodrain;
  __d_pmem_msync pmem_msync;
} PmdkLibLoader;

extern PmdkLibLoader * pmdkLoader;

/**
 * A helper function to dlsym a 'symbol' from a given library-handle.
 */

#ifdef UNIX

static __attribute__ ((unused))
void *myDlsym(void *handle, const char *symbol) {
  void *func_ptr = dlsym(handle, symbol);
  return func_ptr;
}

/* A helper macro to dlsym the requisite dynamic symbol in NON-JNI env. */
#define PMDK_LOAD_DYNAMIC_SYMBOL(func_ptr, symbol) \
  if ((func_ptr = myDlsym(pmdkLoader->libec, symbol)) == NULL) { \
    return "Failed to load symbol" symbol; \
  }

#endif

/**
 * Initialize and load PMDK library, returning error message if any.
 *
 * @param err     The err message buffer.
 * @param err_len The length of the message buffer.
 */
void load_pmdk_lib(char* err, size_t err_len);

#endif //_PMDK_LOAD_H_