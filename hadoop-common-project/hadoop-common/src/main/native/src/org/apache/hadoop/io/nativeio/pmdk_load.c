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
#include "pmdk_load.h"
#include "org_apache_hadoop_io_nativeio_NativeIO.h"
#include "org_apache_hadoop_io_nativeio_NativeIO_POSIX.h"

#ifdef UNIX
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dlfcn.h>

#include "config.h"
#endif

PmdkLibLoader * pmdkLoader;

/**
 *  pmdk_load.c
 *  Utility of loading the libpmem library and the required functions.
 *  Building of this codes won't rely on any libpmem source codes, but running
 *  into this will rely on successfully loading of the dynamic library.
 *
 */

static const char* load_functions() {
#ifdef UNIX
  PMDK_LOAD_DYNAMIC_SYMBOL((pmdkLoader->pmem_map_file), "pmem_map_file");
  PMDK_LOAD_DYNAMIC_SYMBOL((pmdkLoader->pmem_unmap), "pmem_unmap");
  PMDK_LOAD_DYNAMIC_SYMBOL((pmdkLoader->pmem_is_pmem), "pmem_is_pmem");
  PMDK_LOAD_DYNAMIC_SYMBOL((pmdkLoader->pmem_drain), "pmem_drain");
  PMDK_LOAD_DYNAMIC_SYMBOL((pmdkLoader->pmem_memcpy_nodrain), "pmem_memcpy_nodrain");
  PMDK_LOAD_DYNAMIC_SYMBOL((pmdkLoader->pmem_msync), "pmem_msync");
#endif
  return NULL;
}

void load_pmdk_lib(char* err, size_t err_len) {
  const char* errMsg;
  const char* library = NULL;
  #ifdef UNIX
    Dl_info dl_info;
  #else
    LPTSTR filename = NULL;
  #endif

  err[0] = '\0';

  if (pmdkLoader != NULL) {
    return;
  }
  pmdkLoader = calloc(1, sizeof(PmdkLibLoader));

  // Load PMDK library
  #ifdef UNIX
  pmdkLoader->libec = dlopen(HADOOP_PMDK_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (pmdkLoader->libec == NULL) {
    snprintf(err, err_len, "Failed to load %s (%s)",
        HADOOP_PMDK_LIBRARY, dlerror());
    return;
  }
  // Clear any existing error
  dlerror();
  #endif
  errMsg = load_functions(pmdkLoader->libec);
  if (errMsg != NULL) {
    snprintf(err, err_len, "Loading functions from PMDK failed: %s", errMsg);
  }

  #ifdef UNIX
    if (dladdr(pmdkLoader->pmem_map_file, &dl_info)) {
      library = dl_info.dli_fname;
    }
  #else
    if (GetModuleFileName(pmdkLoader->libec, filename, 256) > 0) {
      library = filename;
    }
  #endif

  if (library == NULL) {
    library = HADOOP_PMDK_LIBRARY;
  }

  pmdkLoader->libname = strdup(library);
}
