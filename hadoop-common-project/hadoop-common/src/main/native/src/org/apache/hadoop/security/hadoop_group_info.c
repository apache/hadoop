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

#include "hadoop_group_info.h"

#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_GROUP_LOOKUP_TRIES 5

struct hadoop_group_info *hadoop_group_info_alloc(void)
{
  struct hadoop_group_info *ginfo;
  size_t buf_sz;
  char *buf;

  ginfo = calloc(1, sizeof(struct hadoop_group_info));
  buf_sz = sysconf(_SC_GETGR_R_SIZE_MAX);
  if (buf_sz < 1024) {
    buf_sz = 1024;
  }
  buf = malloc(buf_sz);
  if (!buf) {
    free(ginfo);
    return NULL;
  }
  ginfo->buf_sz = buf_sz;
  ginfo->buf = buf;
  return ginfo;
}

void hadoop_group_info_clear(struct hadoop_group_info *ginfo)
{
  struct group *group = &ginfo->group;

  group->gr_name = NULL;
  group->gr_passwd = NULL;
  group->gr_gid = 0;
  group->gr_mem = NULL;
}

void hadoop_group_info_free(struct hadoop_group_info *ginfo)
{
  free(ginfo->buf);
  free(ginfo);
}

/**
 * Different platforms use different error codes to represent "group not found."
 * So whitelist the errors which do _not_ mean "group not found."
 *
 * @param err           The errno
 *
 * @return              The error code to use
 */
static int getgrgid_error_translate(int err)
{
  if ((err == EIO) || (err == EMFILE) || (err == ENFILE) ||
      (err == ENOMEM) || (err == ERANGE)) {
    return err;
  }
  return ENOENT;
}

int hadoop_group_info_fetch(struct hadoop_group_info *ginfo, gid_t gid)
{
  struct group *group;
  int ret, i; 
  size_t buf_sz;
  char *nbuf;

  hadoop_group_info_clear(ginfo);
  for (i = 0, ret = 0; i < MAX_GROUP_LOOKUP_TRIES; i++) {
    // If the previous call returned ERANGE, increase the buffer size
    if (ret == ERANGE) {
      buf_sz = ginfo->buf_sz * 2;
      nbuf = realloc(ginfo->buf, buf_sz);
      if (!nbuf) {
        return ENOMEM;
      }
      ginfo->buf = nbuf;
      ginfo->buf_sz = buf_sz;
    }

    // The following call returns errno. Reading the global errno wihtout
    // locking is not thread-safe.
    group = NULL;
    ret = getgrgid_r(gid, &ginfo->group, ginfo->buf,
                       ginfo->buf_sz, &group);
    switch(ret) {
      case 0:
        if (!group) {
          // The underlying library likely has a bug.
          return EIO;
        }
        return 0;
      case EINTR:
      case ERANGE:
        // Retry on these errors.
        // EINTR: a signal was handled and this thread was allowed to continue.
        // ERANGE: the buffer was not big enough.
        break;
      default:
        // Lookup failed.
        return getgrgid_error_translate(ret);
    }
  }
  // Did not succeed after the retries. Return the last error.
  return getgrgid_error_translate(ret);
}

#ifdef GROUP_TESTING
/**
 * A main() is provided so that quick testing of this
 * library can be done. 
 */
int main(int argc, char **argv) {
  char **groupname;
  struct hadoop_group_info *ginfo;
  int ret;
  
  ginfo = hadoop_group_info_alloc();
  if (!ginfo) {
    fprintf(stderr, "hadoop_group_info_alloc returned NULL.\n");
    return EXIT_FAILURE;
  }
  for (groupname = argv + 1; *groupname; groupname++) {
    gid_t gid = atoi(*groupname);
    if (gid == 0) {
      fprintf(stderr, "won't accept non-parseable group-name or gid 0: %s\n",
              *groupname);
      return EXIT_FAILURE;
    }
    ret = hadoop_group_info_fetch(ginfo, gid);
    if (!ret) {
      fprintf(stderr, "gid[%lld] : gr_name = %s\n",
              (long long)gid, ginfo->group.gr_name);
    } else {
      fprintf(stderr, "group[%lld] : error %d (%s)\n",
              (long long)gid, ret, strerror(ret));
    }
  }
  hadoop_group_info_free(ginfo);
  return EXIT_SUCCESS;
}
#endif
