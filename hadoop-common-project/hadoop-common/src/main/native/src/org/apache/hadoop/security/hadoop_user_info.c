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

#include "hadoop_user_info.h"

#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define INITIAL_GIDS_SIZE 32
// 1KB buffer should be large enough to store a passwd record in most
// cases, but it can get bigger if each field is maximally used. The
// max is defined to avoid buggy libraries making us run out of memory.
#define MAX_USER_BUFFER_SIZE (32*1024)

struct hadoop_user_info *hadoop_user_info_alloc(void)
{
  struct hadoop_user_info *uinfo;
  long buf_sz;
  char *buf;

  uinfo = calloc(1, sizeof(struct hadoop_user_info));
  buf_sz = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (buf_sz < 1024) {
    buf_sz = 1024;
  }
  buf = malloc(buf_sz);
  if (!buf) {
    free(uinfo);
    return NULL;
  }
  uinfo->buf_sz = buf_sz;
  uinfo->buf = buf;
  return uinfo;
}

static void hadoop_user_info_clear(struct hadoop_user_info *uinfo)
{
  struct passwd *pwd = &uinfo->pwd;

  pwd->pw_name = NULL;
  pwd->pw_uid = 0;
  pwd->pw_gid = 0;
  pwd->pw_passwd = NULL;
  pwd->pw_gecos = NULL;
  pwd->pw_dir = NULL;
  pwd->pw_shell = NULL;
  free(uinfo->gids);
  uinfo->gids = 0;
  uinfo->num_gids = 0;
  uinfo->gids_size = 0;
}

void hadoop_user_info_free(struct hadoop_user_info *uinfo)
{
  free(uinfo->buf);
  hadoop_user_info_clear(uinfo);
  free(uinfo);
}

/**
 * Different platforms use different error codes to represent "user not found."
 * So whitelist the errors which do _not_ mean "user not found."
 *
 * @param err           The errno
 *
 * @return              The error code to use
 */
static int getpwnam_error_translate(int err)
{
  if ((err == EIO) || (err == EMFILE) || (err == ENFILE) ||
      (err == ENOMEM) || (err == ERANGE)) {
    return err;
  }
  return ENOENT;
}

int hadoop_user_info_fetch(struct hadoop_user_info *uinfo,
                           const char *username)
{
  struct passwd *pwd;
  int ret;
  size_t buf_sz;
  char *nbuf;

  hadoop_user_info_clear(uinfo);
  for (;;) {
    // On success, the following call returns 0 and pwd is set to non-NULL.
    pwd = NULL;
    ret = getpwnam_r(username, &uinfo->pwd, uinfo->buf,
                         uinfo->buf_sz, &pwd);
    switch(ret) {
      case 0:
        if (!pwd) {
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
        if (uinfo->buf_sz == MAX_USER_BUFFER_SIZE) {
          // Already tried with the max size.
          return ENOMEM;
        }
        buf_sz = uinfo->buf_sz * 2;
        if (buf_sz > MAX_USER_BUFFER_SIZE) {
          buf_sz = MAX_USER_BUFFER_SIZE;
        }
        nbuf = realloc(uinfo->buf, buf_sz);
        if (!nbuf) {
          return ENOMEM;
        }
        uinfo->buf = nbuf;
        uinfo->buf_sz = buf_sz;
        break;
      default:
        // Lookup failed.
        return getpwnam_error_translate(ret);
    }
  }
}

static int put_primary_gid_first(struct hadoop_user_info *uinfo)
{
  int i, num_gids = uinfo->num_gids;
  gid_t first_gid;
  gid_t gid;
  gid_t primary = uinfo->pwd.pw_gid;

  if (num_gids < 1) {
    // There are no gids, but we expected at least one.
    return EINVAL;
  }
  first_gid = uinfo->gids[0];
  if (first_gid == primary) {
    // First gid is already the primary.
    return 0;
  }
  for (i = 1; i < num_gids; i++) {
    gid = uinfo->gids[i];
    if (gid == primary) {
      // swap first gid and this gid.
      uinfo->gids[0] = gid;
      uinfo->gids[i] = first_gid;
      return 0;
    }
  }
  // Did not find the primary gid in the list.
  return EINVAL;
}

int hadoop_user_info_getgroups(struct hadoop_user_info *uinfo)
{
  int ret, ngroups;
  gid_t *ngids;

  if (!uinfo->pwd.pw_name) {
    // invalid user info
    return EINVAL;
  }
  uinfo->num_gids = 0;
  if (!uinfo->gids) {
    uinfo->gids = malloc(sizeof(uinfo->gids[0]) * INITIAL_GIDS_SIZE);
    if (!uinfo->gids) {
      return ENOMEM;
    }
    uinfo->gids_size = INITIAL_GIDS_SIZE;
  }
  ngroups = uinfo->gids_size;
  ret = getgrouplist(uinfo->pwd.pw_name, uinfo->pwd.pw_gid, 
                         uinfo->gids, &ngroups);
  // Return value is different on Linux vs. FreeBSD.  Linux: the number of groups
  // or -1 on error.  FreeBSD: 0 on success or -1 on error.  Unfortunately, we
  // can't accept a 0 return on Linux, because buggy implementations have been
  // observed to return 0 but leave the other out parameters in an indeterminate
  // state.  This deviates from the man page, but it has been observed in
  // practice.  See issue HADOOP-10989 for details.
#ifdef __linux__
  if (ret > 0) {
#else
  if (ret >= 0) {
#endif
    uinfo->num_gids = ngroups;
    ret = put_primary_gid_first(uinfo);
    if (ret) {
      return ret;
    }
    return 0;
  } else if (ret != -1) {
    // Any return code that is not -1 is considered as error.
    // Since the user lookup was successful, there should be at least one
    // group for this user.
    return EIO;
  }
  ngids = realloc(uinfo->gids, sizeof(uinfo->gids[0]) * ngroups);
  if (!ngids) {
    return ENOMEM;
  }
  uinfo->gids = ngids;
  uinfo->gids_size = ngroups;
  ret = getgrouplist(uinfo->pwd.pw_name, uinfo->pwd.pw_gid, 
                         uinfo->gids, &ngroups);
  if (ret < 0) {
    return EIO;
  }
  uinfo->num_gids = ngroups;
  ret = put_primary_gid_first(uinfo);
  return ret;
}

#ifdef USER_TESTING
/**
 * A main() is provided so that quick testing of this
 * library can be done. 
 */
int main(int argc, char **argv) {
  char **username, *prefix;
  struct hadoop_user_info *uinfo;
  int i, ret;
  
  uinfo = hadoop_user_info_alloc();
  if (!uinfo) {
    fprintf(stderr, "hadoop_user_info_alloc returned NULL.\n");
    return EXIT_FAILURE;
  }
  for (username = argv + 1; *username; username++) {
    ret = hadoop_user_info_fetch(uinfo, *username);
    if (!ret) {
      fprintf(stderr, "user[%s] : pw_uid = %lld\n",
              *username, (long long)uinfo->pwd.pw_uid);
    } else {
      fprintf(stderr, "user[%s] : error %d (%s)\n",
              *username, ret, strerror(ret));
    }
    ret = hadoop_user_info_getgroups(uinfo);
    if (!ret) {
      fprintf(stderr, "          getgroups: ");
      prefix = "";
      for (i = 0; i < uinfo->num_gids; i++) {
        fprintf(stderr, "%s%lld", prefix, (long long)uinfo->gids[i]);
        prefix = ", ";
      }
      fprintf(stderr, "\n");
    } else {
      fprintf(stderr, "          getgroups: error %d\n", ret);
    }
  }
  hadoop_user_info_free(uinfo);
  return EXIT_SUCCESS;
}
#endif
