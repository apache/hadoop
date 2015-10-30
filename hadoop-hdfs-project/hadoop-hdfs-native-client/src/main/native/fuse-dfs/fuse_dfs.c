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
#include "fuse_options.h"
#include "fuse_impls.h"
#include "fuse_init.h"
#include "fuse_connect.h"

#include <string.h>
#include <stdlib.h>
#include <unistd.h>

int is_protected(const char *path) {

  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;
  assert(dfs != NULL);
  assert(dfs->protectedpaths);

  int i ;
  for (i = 0; dfs->protectedpaths[i]; i++) {
    if (strcmp(path, dfs->protectedpaths[i]) == 0) {
      return 1;
    }
  }
  return 0;
}

static struct fuse_operations dfs_oper = {
  .getattr  = dfs_getattr,
  .access   = dfs_access,
  .readdir  = dfs_readdir,
  .destroy  = dfs_destroy,
  .init     = dfs_init,
  .open     = dfs_open,
  .read     = dfs_read,
  .symlink  = dfs_symlink,
  .statfs   = dfs_statfs,
  .mkdir    = dfs_mkdir,
  .rmdir    = dfs_rmdir,
  .rename   = dfs_rename,
  .unlink   = dfs_unlink,
  .release  = dfs_release,
  .create   = dfs_create,
  .write    = dfs_write,
  .flush    = dfs_flush,
  .mknod    = dfs_mknod,
  .utimens  = dfs_utimens,
  .chmod    = dfs_chmod,
  .chown    = dfs_chown,
  .truncate = dfs_truncate,
};

int main(int argc, char *argv[])
{
  int ret;

  umask(0);

  extern const char *program;  
  program = argv[0];
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

  memset(&options, 0, sizeof(struct options));

  options.rdbuffer_size = 10*1024*1024; 
  options.attribute_timeout = 60; 
  options.entry_timeout = 60;

  if (-1 == fuse_opt_parse(&args, &options, dfs_opts, dfs_options)) {
    return -1;
  }

  if (!options.private) {
    fuse_opt_add_arg(&args, "-oallow_other");
  }

  if (!options.no_permissions) {
    fuse_opt_add_arg(&args, "-odefault_permissions");
  }
  /*
   * FUSE already has a built-in parameter for mounting the filesystem as
   * read-only, -r.  We defined our own parameter for doing this called -oro.
   * We support it by translating it into -r internally.
   * The kernel intercepts and returns an error message for any "write"
   * operations that the user attempts to perform on a read-only filesystem.
   * That means that we don't have to write any code to handle read-only mode.
   * See HDFS-4139 for more details.
   */
  if (options.read_only) {
    fuse_opt_add_arg(&args, "-r");
  }

  {
    char buf[80];

    snprintf(buf, sizeof buf, "-oattr_timeout=%d",options.attribute_timeout);
    fuse_opt_add_arg(&args, buf);

    snprintf(buf, sizeof buf, "-oentry_timeout=%d",options.entry_timeout);
    fuse_opt_add_arg(&args, buf);
  }

  if (options.nn_uri == NULL) {
    print_usage(argv[0]);
    exit(EXIT_SUCCESS);
  }

  /* Note: do not call any libhdfs functions until fuse_main has been invoked.
   *
   * fuse_main will daemonize this process, by calling fork().  This will cause
   * any extant threads to be destroyed, which could cause problems if 
   * libhdfs has started some Java threads.
   *
   * Most initialization code should go in dfs_init, which is invoked after the
   * fork.  See HDFS-3808 for details.
   */
  ret = fuse_main(args.argc, args.argv, &dfs_oper, NULL);
  fuse_opt_free_args(&args);
  return ret;
}
