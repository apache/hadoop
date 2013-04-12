/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fuse_dfs.h"
#include "fuse_init.h"
#include "fuse_options.h"
#include "fuse_context_handle.h"
#include "fuse_connect.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_env_vars(void)
{
  const char *cp = getenv("CLASSPATH");
  const char *ld = getenv("LD_LIBRARY_PATH");

  ERROR("LD_LIBRARY_PATH=%s",ld == NULL ? "NULL" : ld);
  ERROR("CLASSPATH=%s",cp == NULL ? "NULL" : cp);
}

// Hacked up function to basically do:
//  protectedpaths = split(options.protected,':');

static void init_protectedpaths(dfs_context *dfs)
{
  char *tmp = options.protected;

  // handle degenerate case up front.
  if (tmp == NULL || 0 == *tmp) {
    dfs->protectedpaths = (char**)malloc(sizeof(char*));
    dfs->protectedpaths[0] = NULL;
    return;
  }

  if (options.debug) {
    print_options();
  }

  int i = 0;
  while (tmp && (NULL != (tmp = index(tmp,':')))) {
    tmp++; // pass the ,
    i++;
  }
  i++; // for the last entry
  i++; // for the final NULL
  dfs->protectedpaths = (char**)malloc(sizeof(char*)*i);
  assert(dfs->protectedpaths);
  tmp = options.protected;
  int j  = 0;
  while (NULL != tmp && j < i) {
    int length;
    char *eos = index(tmp,':');
    if (NULL != eos) {
      length = eos - tmp; // length of this value
    } else {
      length = strlen(tmp);
    }
    dfs->protectedpaths[j] = (char*)malloc(sizeof(char)*length+1);
    assert(dfs->protectedpaths[j]);
    strncpy(dfs->protectedpaths[j], tmp, length);
    dfs->protectedpaths[j][length] = '\0';
    if (eos) {
      tmp = eos + 1;
    } else {
      tmp = NULL;
    }
    j++;
  }
  dfs->protectedpaths[j] = NULL;
}

static void dfsPrintOptions(FILE *fp, const struct options *o)
{
  INFO("Mounting with options: [ protected=%s, nn_uri=%s, nn_port=%d, "
          "debug=%d, read_only=%d, initchecks=%d, "
          "no_permissions=%d, usetrash=%d, entry_timeout=%d, "
          "attribute_timeout=%d, rdbuffer_size=%zd, direct_io=%d ]",
          (o->protected ? o->protected : "(NULL)"), o->nn_uri, o->nn_port, 
          o->debug, o->read_only, o->initchecks,
          o->no_permissions, o->usetrash, o->entry_timeout,
          o->attribute_timeout, o->rdbuffer_size, o->direct_io);
}

void *dfs_init(struct fuse_conn_info *conn)
{
  int ret;

  //
  // Create a private struct of data we will pass to fuse here and which
  // will then be accessible on every call.
  //
  dfs_context *dfs = calloc(1, sizeof(*dfs));
  if (!dfs) {
    ERROR("FATAL: could not malloc dfs_context");
    exit(1);
  }

  // initialize the context
  dfs->debug                 = options.debug;
  dfs->usetrash              = options.usetrash;
  dfs->protectedpaths        = NULL;
  dfs->rdbuffer_size         = options.rdbuffer_size;
  dfs->direct_io             = options.direct_io;

  dfsPrintOptions(stderr, &options);

  init_protectedpaths(dfs);
  assert(dfs->protectedpaths != NULL);

  if (dfs->rdbuffer_size <= 0) {
    DEBUG("dfs->rdbuffersize <= 0 = %zd", dfs->rdbuffer_size);
    dfs->rdbuffer_size = 32768;
  }

  ret = fuseConnectInit(options.nn_uri, options.nn_port);
  if (ret) {
    ERROR("FATAL: dfs_init: fuseConnectInit failed with error %d!", ret);
    print_env_vars();
    exit(EXIT_FAILURE);
  }
  if (options.initchecks == 1) {
    ret = fuseConnectTest();
    if (ret) {
      ERROR("FATAL: dfs_init: fuseConnectTest failed with error %d!", ret);
      print_env_vars();
      exit(EXIT_FAILURE);
    }
  }

#ifdef FUSE_CAP_ATOMIC_O_TRUNC
  // If FUSE_CAP_ATOMIC_O_TRUNC is set, open("foo", O_CREAT | O_TRUNC) will
  // result in dfs_open being called with O_TRUNC.
  //
  // If this capability is not present, fuse will try to use multiple
  // operation to "simulate" open(O_TRUNC).  This doesn't work very well with
  // HDFS.
  // Unfortunately, this capability is only implemented on Linux 2.6.29 or so.
  // See HDFS-4140 for details.
  if (conn->capable & FUSE_CAP_ATOMIC_O_TRUNC) {
    conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;
  }
#endif

#ifdef FUSE_CAP_ASYNC_READ
  // We're OK with doing reads at the same time as writes.
  if (conn->capable & FUSE_CAP_ASYNC_READ) {
    conn->want |= FUSE_CAP_ASYNC_READ;
  }
#endif
  
#ifdef FUSE_CAP_BIG_WRITES
  // Yes, we can read more than 4kb at a time.  In fact, please do!
  if (conn->capable & FUSE_CAP_BIG_WRITES) {
    conn->want |= FUSE_CAP_BIG_WRITES;
  }
#endif

#ifdef FUSE_CAP_DONT_MASK
  if ((options.no_permissions) && (conn->capable & FUSE_CAP_DONT_MASK)) {
    // If we're handing permissions ourselves, we don't want the kernel
    // applying its own umask.  HDFS already implements its own per-user
    // umasks!  Sadly, this only actually does something on kernels 2.6.31 and
    // later.
    conn->want |= FUSE_CAP_DONT_MASK;
  }
#endif

  return (void*)dfs;
}


void dfs_destroy(void *ptr)
{
  TRACE("destroy")
}
