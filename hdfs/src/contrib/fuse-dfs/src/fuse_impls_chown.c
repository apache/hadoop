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
#include "fuse_users.h"
#include "fuse_impls.h"
#include "fuse_connect.h"

 int dfs_chown(const char *path, uid_t uid, gid_t gid)
{
  TRACE1("chown", path)

  int ret = 0;

#if PERMS
  char *user = NULL;
  char *group = NULL;

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  user = getUsername(uid);
  if (NULL == user) {
    syslog(LOG_ERR,"Could not lookup the user id string %d\n",(int)uid); 
    fprintf(stderr, "could not lookup userid %d\n", (int)uid); 
    ret = -EIO;
  }

  if (0 == ret) {
    group = getGroup(gid);
    if (group == NULL) {
      syslog(LOG_ERR,"Could not lookup the group id string %d\n",(int)gid); 
      fprintf(stderr, "could not lookup group %d\n", (int)gid); 
      ret = -EIO;
    } 
  }

  hdfsFS userFS = NULL;
  if (0 == ret) {
    // if not connected, try to connect and fail out if we can't.
    if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
      syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
      ret = -EIO;
    }
  }

  if (0 == ret) {
    //  fprintf(stderr, "DEBUG: chown %s %d->%s %d->%s\n", path, (int)uid, user, (int)gid, group);
    if (hdfsChown(userFS, path, user, group)) {
      syslog(LOG_ERR,"ERROR: hdfs trying to chown %s to %d/%d",path, (int)uid, gid);
      ret = -EIO;
    }
  }
  if (user) 
    free(user);
  if (group)
    free(group);
#endif
  return ret;

}
