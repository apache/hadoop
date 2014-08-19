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

// Assuming the average of user name length of 15 bytes,
// 8KB buffer will be large enough for a group with about 500 members.
// 2MB buffer will be large enough for a group with about 130K members.
#define INITIAL_GROUP_BUFFER_SIZE (8*1024)
#define MAX_GROUP_BUFFER_SIZE (2*1024*1024)

struct hadoop_group_info *hadoop_group_info_alloc(void)
{
  struct hadoop_group_info *ginfo;
  char *buf;

  ginfo = calloc(1, sizeof(struct hadoop_group_info));
  buf = malloc(INITIAL_GROUP_BUFFER_SIZE);
  if (!buf) {
    free(ginfo);
    return NULL;
  }
  ginfo->buf_sz = INITIAL_GROUP_BUFFER_SIZE;
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
  int ret; 
  size_t buf_sz;
  char *nbuf;

  hadoop_group_info_clear(ginfo);
  for (;;) {
    // On success, the following call returns 0 and group is set to non-NULL.
    group = NULL;
    ret = getgrgid_r(gid, &ginfo->group, ginfo->buf,
                       ginfo->buf_sz, &group);
    switch(ret) {
      case 0:
        if (!group) {
          // Not found.
          return ENOENT;
        }
        // Found.
        return 0;
      case EINTR:
        // EINTR: a signal was handled and this thread was allowed to continue.
        break;
      case ERANGE:
        // ERANGE: the buffer was not big enough.
        if (ginfo->buf_sz == MAX_GROUP_BUFFER_SIZE) {
          // Already tried with the max size.
          return ENOMEM;
        }
        buf_sz = ginfo->buf_sz * 2;
        if (buf_sz > MAX_GROUP_BUFFER_SIZE) {
          buf_sz = MAX_GROUP_BUFFER_SIZE;
        }
        nbuf = realloc(ginfo->buf, buf_sz);
        if (!nbuf) {
          return ENOMEM;
        }
        ginfo->buf = nbuf;
        ginfo->buf_sz = buf_sz;
        break;
      default:
        // Lookup failed.
        return getgrgid_error_translate(ret);
    }
  }
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
