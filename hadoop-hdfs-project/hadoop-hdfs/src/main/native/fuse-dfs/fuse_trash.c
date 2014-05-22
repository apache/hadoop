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

#include <hdfs.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "fuse_context_handle.h"
#include "fuse_dfs.h"
#include "fuse_trash.h"
#include "fuse_users.h"

#define TRASH_RENAME_TRIES  100
#define ALREADY_IN_TRASH_ERR 9000

/**
 * Split a path into a parent directory and a base path component.
 *
 * @param abs_path    The absolute path.
 * @param pcomp       (out param) Will be set to the last path component.
 *                        Malloced.
 * @param parent_dir  (out param) Will be set to the parent directory.
 *                        Malloced.
 *
 * @return            0 on success.
 *                    On success, both *pcomp and *parent_dir will contain
 *                    malloc'ed strings.
 *                    EINVAL if the path wasn't absolute.
 *                    EINVAL if there is no parent directory (i.e. abs_path=/)
 *                    ENOMEM if we ran out of memory.
 */
static int get_parent_dir(const char *abs_path, char **pcomp,
                          char **parent_dir)
{
  int ret;
  char *pdir = NULL, *pc = NULL, *last_slash;

  pdir = strdup(abs_path);
  if (!pdir) {
    ret = ENOMEM;
    goto done;
  }
  last_slash = rindex(pdir, '/');
  if (!last_slash) {
    ERROR("get_parent_dir(%s): expected absolute path.\n", abs_path);
    ret = EINVAL;
    goto done;
  }
  if (last_slash[1] == '\0') {
    *last_slash = '\0';
    last_slash = rindex(pdir, '/');
    if (!last_slash) {
      ERROR("get_parent_dir(%s): there is no parent dir.\n", abs_path);
      ret = EINVAL;
      goto done;
    }
  }
  pc = strdup(last_slash + 1);
  if (!pc) {
    ret = ENOMEM;
    goto done;
  }
  *last_slash = '\0';
  ret = 0;
done:
  if (ret) {
    free(pdir);
    free(pc);
    return ret;
  }
  *pcomp = pc;
  *parent_dir = pdir;
  return 0;
}

/**
 * Get the base path to the trash.  This will depend on the user ID.
 * For example, a user whose ID maps to 'foo' will get back the path
 * "/user/foo/.Trash/Current".
 *
 * @param trash_base       (out param) the base path to the trash.
 *                             Malloced.
 *
 * @return                 0 on success; error code otherwise.
 */
static int get_trash_base(char **trash_base)
{
  const char * const PREFIX = "/user/";
  const char * const SUFFIX = "/.Trash/Current";
  char *user_name = NULL, *base = NULL;
  uid_t uid = fuse_get_context()->uid;
  int ret;

  user_name = getUsername(uid);
  if (!user_name) {
    ERROR("get_trash_base(): failed to get username for uid %"PRId64"\n",
          (uint64_t)uid);
    ret = EIO;
    goto done;
  }
  if (asprintf(&base, "%s%s%s", PREFIX, user_name, SUFFIX) < 0) {
    base = NULL;
    ret = ENOMEM;
    goto done;
  }
  ret = 0;
done:
  free(user_name);
  if (ret) {
    free(base);
    return ret;
  }
  *trash_base = base;
  return 0;
}

//
// NOTE: this function is a c implementation of org.apache.hadoop.fs.Trash.moveToTrash(Path path).
//
int move_to_trash(const char *abs_path, hdfsFS userFS)
{
  int ret;
  char *pcomp = NULL, *parent_dir = NULL, *trash_base = NULL;
  char *target_dir = NULL, *target = NULL;

  ret = get_parent_dir(abs_path, &pcomp, &parent_dir);
  if (ret) {
    goto done;
  }
  ret = get_trash_base(&trash_base);
  if (ret) {
    goto done;
  }
  if (!strncmp(trash_base, abs_path, strlen(trash_base))) {
    INFO("move_to_trash(%s): file is already in the trash; deleting.",
         abs_path);
    ret = ALREADY_IN_TRASH_ERR;
    goto done;
  }
  fprintf(stderr, "trash_base='%s'\n", trash_base);
  if (asprintf(&target_dir, "%s%s", trash_base, parent_dir) < 0) {
    ret = ENOMEM;
    target_dir = NULL;
    goto done;
  }
  if (asprintf(&target, "%s/%s", target_dir, pcomp) < 0) {
    ret = ENOMEM;
    target = NULL;
    goto done;
  }
  // create the target trash directory in trash (if needed)
  if (hdfsExists(userFS, target_dir) != 0) {
    // make the directory to put it in in the Trash - NOTE
    // hdfsCreateDirectory also creates parents, so Current will be created if it does not exist.
    if (hdfsCreateDirectory(userFS, target_dir)) {
      ret = errno;
      ERROR("move_to_trash(%s) error: hdfsCreateDirectory(%s) failed with error %d",
            abs_path, target_dir, ret);
      goto done;
    }
  } else if (hdfsExists(userFS, target) == 0) {
    // If there is already a file in the trash with this path, append a number.
    int idx;
    for (idx = 1; idx < TRASH_RENAME_TRIES; idx++) {
      free(target);
      if (asprintf(&target, "%s%s.%d", target_dir, pcomp, idx) < 0) {
        target = NULL;
        ret = ENOMEM;
        goto done;
      }
      if (hdfsExists(userFS, target) != 0) {
        break;
      }
    }
    if (idx == TRASH_RENAME_TRIES) {
      ERROR("move_to_trash(%s) error: there are already %d files in the trash "
            "with this name.\n", abs_path, TRASH_RENAME_TRIES);
      ret = EINVAL;
      goto done;
    }
  }
  if (hdfsRename(userFS, abs_path, target)) {
    ret = errno;
    ERROR("move_to_trash(%s): failed to rename the file to %s: error %d",
          abs_path, target, ret);
    goto done;
  }

  ret = 0;
done:
  if ((ret != 0) && (ret != ALREADY_IN_TRASH_ERR)) {
    ERROR("move_to_trash(%s) failed with error %d", abs_path, ret);
  }
  free(pcomp);
  free(parent_dir);
  free(trash_base);
  free(target_dir);
  free(target);
  return ret;
}

int hdfsDeleteWithTrash(hdfsFS userFS, const char *path, int useTrash)
{
  int tried_to_move_to_trash = 0;
  if (useTrash) {
    tried_to_move_to_trash = 1;
    if (move_to_trash(path, userFS) == 0) {
      return 0;
    }
  }
  if (hdfsDelete(userFS, path, 1)) {
    int err = errno;
    if (err < 0) {
      err = -err;
    }
    ERROR("hdfsDeleteWithTrash(%s): hdfsDelete failed: error %d.",
          path, err);
    return -err;
  }
  if (tried_to_move_to_trash) {
    ERROR("hdfsDeleteWithTrash(%s): deleted the file instead.\n", path);
  }
  return 0;
}
