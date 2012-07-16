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

#include "hdfs.h"
#include "fuse_dfs.h"
#include "fuse_connect.h"
#include "fuse_users.h" 

#include <limits.h>
#include <search.h>
#include <stdio.h>
#include <stdlib.h>

#define HADOOP_SECURITY_AUTHENTICATION "hadoop.security.authentication"

enum authConf {
    AUTH_CONF_UNKNOWN,
    AUTH_CONF_KERBEROS,
    AUTH_CONF_OTHER,
};

#define MAX_ELEMENTS (16 * 1024)
static struct hsearch_data *fsTable = NULL;
static enum authConf hdfsAuthConf = AUTH_CONF_UNKNOWN;
static pthread_mutex_t tableMutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Allocate a hash table for fs handles. Returns 0 on success,
 * -1 on failure.
 */
int allocFsTable(void) {
  assert(NULL == fsTable);
  fsTable = calloc(1, sizeof(struct hsearch_data));
  if (0 == hcreate_r(MAX_ELEMENTS, fsTable)) {
    ERROR("Unable to initialize connection table");
    return -1;
  }
  return 0;
}

/*
 * Find a fs handle for the given key. Returns a fs handle, 
 * or NULL if there is no fs for the given key.
 */
static hdfsFS findFs(char *key) {
  ENTRY entry;
  ENTRY *entryP = NULL;
  entry.key = key;
  if (0 == hsearch_r(entry, FIND, &entryP, fsTable)) {
    return NULL;
  }
  assert(NULL != entryP->data);
  return (hdfsFS)entryP->data;
}

/*
 * Insert the given fs handle into the table.
 * Returns 0 on success, -1 on failure.
 */
static int insertFs(char *key, hdfsFS fs) {
  ENTRY entry;
  ENTRY *entryP = NULL;
  assert(NULL != fs);
  entry.key = strdup(key);
  if (entry.key == NULL) {
    return -1;
  }
  entry.data = (void*)fs;
  if (0 == hsearch_r(entry, ENTER, &entryP, fsTable)) {
    return -1;
  }
  return 0;
}

/** 
 * Find out what type of authentication the system administrator
 * has configured.
 *
 * @return     the type of authentication, or AUTH_CONF_UNKNOWN on error.
 */
static enum authConf discoverAuthConf(void)
{
    int ret;
    char *val = NULL;
    enum authConf authConf;

    ret = hdfsConfGet(HADOOP_SECURITY_AUTHENTICATION, &val);
    if (ret)
        authConf = AUTH_CONF_UNKNOWN;
    else if (!strcmp(val, "kerberos"))
        authConf = AUTH_CONF_KERBEROS;
    else
        authConf = AUTH_CONF_OTHER;
    free(val);
    return authConf;
}

/**
 * Find the Kerberos ticket cache path.
 *
 * This function finds the Kerberos ticket cache path from the thread ID and
 * user ID of the process making the request.
 *
 * Normally, the ticket cache path is in a well-known location in /tmp.
 * However, it's possible that the calling process could set the KRB5CCNAME
 * environment variable, indicating that its Kerberos ticket cache is at a
 * non-default location.  We try to handle this possibility by reading the
 * process' environment here.  This will be allowed if we have root
 * capabilities, or if our UID is the same as the remote process' UID.
 *
 * Note that we don't check to see if the cache file actually exists or not.
 * We're just trying to find out where it would be if it did exist. 
 *
 * @param path          (out param) the path to the ticket cache file
 * @param pathLen       length of the path buffer
 */
static void findKerbTicketCachePath(char *path, size_t pathLen)
{
  struct fuse_context *ctx = fuse_get_context();
  FILE *fp = NULL;
  static const char * const KRB5CCNAME = "\0KRB5CCNAME=";
  int c = '\0', pathIdx = 0, keyIdx = 0;
  size_t KRB5CCNAME_LEN = strlen(KRB5CCNAME + 1) + 1;

  // /proc/<tid>/environ contains the remote process' environment.  It is
  // exposed to us as a series of KEY=VALUE pairs, separated by NULL bytes.
  snprintf(path, pathLen, "/proc/%d/environ", ctx->pid);
  fp = fopen(path, "r");
  if (!fp)
    goto done;
  while (1) {
    if (c == EOF)
      goto done;
    if (keyIdx == KRB5CCNAME_LEN) {
      if (pathIdx >= pathLen - 1)
        goto done;
      if (c == '\0')
        goto done;
      path[pathIdx++] = c;
    } else if (KRB5CCNAME[keyIdx++] != c) {
      keyIdx = 0;
    }
    c = fgetc(fp);
  }

done:
  if (fp)
    fclose(fp);
  if (pathIdx == 0) {
    snprintf(path, pathLen, "/tmp/krb5cc_%d", ctx->uid);
  } else {
    path[pathIdx] = '\0';
  }
}

/*
 * Connect to the NN as the current user/group.
 * Returns a fs handle on success, or NULL on failure.
 */
hdfsFS doConnectAsUser(const char *nn_uri, int nn_port) {
  struct hdfsBuilder *bld;
  uid_t uid = fuse_get_context()->uid;
  char *user = getUsername(uid);
  char kpath[PATH_MAX];
  int ret;
  hdfsFS fs = NULL;
  if (NULL == user) {
    goto done;
  }

  ret = pthread_mutex_lock(&tableMutex);
  assert(0 == ret);

  fs = findFs(user);
  if (NULL == fs) {
    if (hdfsAuthConf == AUTH_CONF_UNKNOWN) {
      hdfsAuthConf = discoverAuthConf();
      if (hdfsAuthConf == AUTH_CONF_UNKNOWN) {
        ERROR("Unable to determine the configured value for %s.",
              HADOOP_SECURITY_AUTHENTICATION);
        goto done;
      }
    }
    bld = hdfsNewBuilder();
    if (!bld) {
      ERROR("Unable to create hdfs builder");
      goto done;
    }
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, nn_uri);
    if (nn_port) {
        hdfsBuilderSetNameNodePort(bld, nn_port);
    }
    hdfsBuilderSetUserName(bld, user);
    if (hdfsAuthConf == AUTH_CONF_KERBEROS) {
      findKerbTicketCachePath(kpath, sizeof(kpath));
      hdfsBuilderSetKerbTicketCachePath(bld, kpath);
    }
    fs = hdfsBuilderConnect(bld);
    if (NULL == fs) {
      int err = errno;
      ERROR("Unable to create fs for user %s: error code %d", user, err);
      goto done;
    }
    if (-1 == insertFs(user, fs)) {
      ERROR("Unable to cache fs for user %s", user);
    }
  }

done:
  ret = pthread_mutex_unlock(&tableMutex);
  assert(0 == ret);
  free(user);
  return fs;
}

/*
 * We currently cache a fs handle per-user in this module rather
 * than use the FileSystem cache in the java client. Therefore
 * we do not disconnect the fs handle here.
 */
int doDisconnect(hdfsFS fs) {
  return 0;
}
