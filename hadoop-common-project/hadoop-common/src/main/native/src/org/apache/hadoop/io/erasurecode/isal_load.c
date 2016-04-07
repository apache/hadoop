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
#include "isal_load.h"

#ifdef UNIX
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dlfcn.h>

#include "config.h"
#endif

#ifdef WINDOWS
#include <Windows.h>
#endif

IsaLibLoader* isaLoader;

/**
 *  isal_load.c
 *  Utility of loading the ISA-L library and the required functions.
 *  Building of this codes won't rely on any ISA-L source codes, but running
 *  into this will rely on successfully loading of the dynamic library.
 *
 */

static const char* load_functions() {
#ifdef UNIX
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_mul), "gf_mul");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_inv), "gf_inv");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_gen_rs_matrix), "gf_gen_rs_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_gen_cauchy_matrix), "gf_gen_cauchy1_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_invert_matrix), "gf_invert_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_vect_mul), "gf_vect_mul");

  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_init_tables), "ec_init_tables");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_encode_data), "ec_encode_data");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_encode_data_update), "ec_encode_data_update");
#endif

#ifdef WINDOWS
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_mul, (isaLoader->gf_mul), "gf_mul");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_inv, (isaLoader->gf_inv), "gf_inv");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_gen_rs_matrix, (isaLoader->gf_gen_rs_matrix), "gf_gen_rs_matrix");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_gen_cauchy_matrix, (isaLoader->gf_gen_cauchy_matrix), "gf_gen_cauchy1_matrix");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_invert_matrix, (isaLoader->gf_invert_matrix), "gf_invert_matrix");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_vect_mul, (isaLoader->gf_vect_mul), "gf_vect_mul");

  EC_LOAD_DYNAMIC_SYMBOL(__d_ec_init_tables, (isaLoader->ec_init_tables), "ec_init_tables");
  EC_LOAD_DYNAMIC_SYMBOL(__d_ec_encode_data, (isaLoader->ec_encode_data), "ec_encode_data");
  EC_LOAD_DYNAMIC_SYMBOL(__d_ec_encode_data_update, (isaLoader->ec_encode_data_update), "ec_encode_data_update");
#endif

  return NULL;
}

void load_erasurecode_lib(char* err, size_t err_len) {
  const char* errMsg;
  const char* library = NULL;
#ifdef UNIX
  Dl_info dl_info;
#else
  LPTSTR filename = NULL;
#endif

  err[0] = '\0';

  if (isaLoader != NULL) {
    return;
  }
  isaLoader = calloc(1, sizeof(IsaLibLoader));
  memset(isaLoader, 0, sizeof(IsaLibLoader));

  // Load Intel ISA-L
  #ifdef UNIX
  isaLoader->libec = dlopen(HADOOP_ISAL_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (isaLoader->libec == NULL) {
    snprintf(err, err_len, "Failed to load %s (%s)",
                             HADOOP_ISAL_LIBRARY, dlerror());
    return;
  }
  // Clear any existing error
  dlerror();
  #endif

  #ifdef WINDOWS
  isaLoader->libec = LoadLibrary(HADOOP_ISAL_LIBRARY);
  if (isaLoader->libec == NULL) {
    snprintf(err, err_len, "Failed to load %s", HADOOP_ISAL_LIBRARY);
    return;
  }
  #endif

  errMsg = load_functions(isaLoader->libec);
  if (errMsg != NULL) {
    snprintf(err, err_len, "Loading functions from ISA-L failed: %s", errMsg);
  }

#ifdef UNIX
  if(dladdr(isaLoader->ec_encode_data, &dl_info)) {
    library = dl_info.dli_fname;
  }
#else
  if (GetModuleFileName(isaLoader->libec, filename, 256) > 0) {
    library = filename;
  }
#endif

  if (library == NULL) {
    library = HADOOP_ISAL_LIBRARY;
  }

  isaLoader->libname = strdup(library);
}

int build_support_erasurecode() {
#ifdef HADOOP_ISAL_LIBRARY
  return 1;
#else
  return 0;
#endif
}