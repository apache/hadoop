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

#include "fuse_connect.h"
#include "fuse_dfs.h"
#include "fuse_users.h" 
#include "libhdfs/hdfs.h"
#include "util/tree.h"

#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <search.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <utime.h>

#define FUSE_CONN_DEFAULT_TIMER_PERIOD      5
#define FUSE_CONN_DEFAULT_EXPIRY_PERIOD     (5 * 60)
#define HADOOP_SECURITY_AUTHENTICATION      "hadoop.security.authentication"
#define HADOOP_FUSE_CONNECTION_TIMEOUT      "hadoop.fuse.connection.timeout"
#define HADOOP_FUSE_TIMER_PERIOD            "hadoop.fuse.timer.period"

/** Length of the buffer needed by asctime_r */
#define TIME_STR_LEN 26

struct hdfsConn;

static int hdfsConnCompare(const struct hdfsConn *a, const struct hdfsConn *b);
static void hdfsConnExpiry(void);
static void* hdfsConnExpiryThread(void *v);

RB_HEAD(hdfsConnTree, hdfsConn);

enum authConf {
    AUTH_CONF_UNKNOWN,
    AUTH_CONF_KERBEROS,
    AUTH_CONF_OTHER,
};

struct hdfsConn {
  RB_ENTRY(hdfsConn) entry;
  /** How many threads are currently using this hdfsConnection object */
  int64_t refcnt;
  /** The username used to make this connection.  Dynamically allocated. */
  char *usrname;
  /** Kerberos ticket cache path, or NULL if this is not a kerberized
   * connection.  Dynamically allocated. */
  char *kpath;
  /** mtime of the kpath, if the kpath is non-NULL */
  time_t kPathMtime;
  /** nanosecond component of the mtime of the kpath, if the kpath is non-NULL */
  long kPathMtimeNs;
  /** The cached libhdfs fs instance */
  hdfsFS fs;
  /** Nonzero if this hdfs connection needs to be closed as soon as possible.
   * If this is true, the connection has been removed from the tree. */
  int condemned;
  /** Number of times we should run the expiration timer on this connection
   * before removing it. */
  int expirationCount;
};

RB_GENERATE(hdfsConnTree, hdfsConn, entry, hdfsConnCompare);

/** Current cached libhdfs connections */
static struct hdfsConnTree gConnTree;

/** The URI used to make our connections.  Dynamically allocated. */
static char *gUri;

/** The port used to make our connections, or 0. */
static int gPort;

/** Lock which protects gConnTree and gConnectTimer->active */
static pthread_mutex_t gConnMutex;

/** Type of authentication configured */
static enum authConf gHdfsAuthConf;

/** FUSE connection timer expiration period */
static int32_t gTimerPeriod;

/** FUSE connection expiry period */
static int32_t gExpiryPeriod;

/** FUSE timer expiration thread */
static pthread_t gTimerThread;

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

  ret = hdfsConfGetStr(HADOOP_SECURITY_AUTHENTICATION, &val);
  if (ret)
    authConf = AUTH_CONF_UNKNOWN;
  else if (!val)
    authConf = AUTH_CONF_OTHER;
  else if (!strcmp(val, "kerberos"))
    authConf = AUTH_CONF_KERBEROS;
  else
    authConf = AUTH_CONF_OTHER;
  free(val);
  return authConf;
}

int fuseConnectInit(const char *nnUri, int port)
{
  int ret;

  gTimerPeriod = FUSE_CONN_DEFAULT_TIMER_PERIOD;
  ret = hdfsConfGetInt(HADOOP_FUSE_TIMER_PERIOD, &gTimerPeriod);
  if (ret) {
    fprintf(stderr, "Unable to determine the configured value for %s.",
          HADOOP_FUSE_TIMER_PERIOD);
    return -EINVAL;
  }
  if (gTimerPeriod < 1) {
    fprintf(stderr, "Invalid value %d given for %s.\n",
          gTimerPeriod, HADOOP_FUSE_TIMER_PERIOD);
    return -EINVAL;
  }
  gExpiryPeriod = FUSE_CONN_DEFAULT_EXPIRY_PERIOD;
  ret = hdfsConfGetInt(HADOOP_FUSE_CONNECTION_TIMEOUT, &gExpiryPeriod);
  if (ret) {
    fprintf(stderr, "Unable to determine the configured value for %s.",
          HADOOP_FUSE_CONNECTION_TIMEOUT);
    return -EINVAL;
  }
  if (gExpiryPeriod < 1) {
    fprintf(stderr, "Invalid value %d given for %s.\n",
          gExpiryPeriod, HADOOP_FUSE_CONNECTION_TIMEOUT);
    return -EINVAL;
  }
  gHdfsAuthConf = discoverAuthConf();
  if (gHdfsAuthConf == AUTH_CONF_UNKNOWN) {
    fprintf(stderr, "Unable to determine the configured value for %s.",
          HADOOP_SECURITY_AUTHENTICATION);
    return -EINVAL;
  }
  gPort = port;
  gUri = strdup(nnUri);
  if (!gUri) {
    fprintf(stderr, "fuseConnectInit: OOM allocting nnUri\n");
    return -ENOMEM;
  }
  ret = pthread_mutex_init(&gConnMutex, NULL);
  if (ret) {
    free(gUri);
    fprintf(stderr, "fuseConnectInit: pthread_mutex_init failed with error %d\n",
            ret);
    return -ret;
  }
  RB_INIT(&gConnTree);
  ret = pthread_create(&gTimerThread, NULL, hdfsConnExpiryThread, NULL);
  if (ret) {
    free(gUri);
    pthread_mutex_destroy(&gConnMutex);
    fprintf(stderr, "fuseConnectInit: pthread_create failed with error %d\n",
            ret);
    return -ret;
  }
  fprintf(stderr, "fuseConnectInit: initialized with timer period %d, "
          "expiry period %d\n", gTimerPeriod, gExpiryPeriod);
  return 0;
}

/**
 * Compare two libhdfs connections by username
 *
 * @param a                The first libhdfs connection
 * @param b                The second libhdfs connection
 *
 * @return                 -1, 0, or 1 depending on a < b, a ==b, a > b
 */
static int hdfsConnCompare(const struct hdfsConn *a, const struct hdfsConn *b)
{
  return strcmp(a->usrname, b->usrname);
}

/**
 * Find a libhdfs connection by username
 *
 * @param usrname         The username to look up
 *
 * @return                The connection, or NULL if none could be found
 */
static struct hdfsConn* hdfsConnFind(const char *usrname)
{
  struct hdfsConn exemplar;

  memset(&exemplar, 0, sizeof(exemplar));
  exemplar.usrname = (char*)usrname;
  return RB_FIND(hdfsConnTree, &gConnTree, &exemplar);
}

/**
 * Free the resource associated with a libhdfs connection.
 *
 * You must remove the connection from the tree before calling this function.
 *
 * @param conn            The libhdfs connection
 */
static void hdfsConnFree(struct hdfsConn *conn)
{
  int ret;

  ret = hdfsDisconnect(conn->fs);
  if (ret) {
    fprintf(stderr, "hdfsConnFree(username=%s): "
      "hdfsDisconnect failed with error %d\n",
      (conn->usrname ? conn->usrname : "(null)"), ret);
  }
  free(conn->usrname);
  free(conn->kpath);
  free(conn);
}

/**
 * Convert a time_t to a string.
 *
 * @param sec           time in seconds since the epoch
 * @param buf           (out param) output buffer
 * @param bufLen        length of output buffer
 *
 * @return              0 on success; ENAMETOOLONG if the provided buffer was
 *                      too short
 */
static int timeToStr(time_t sec, char *buf, size_t bufLen)
{
  struct tm tm, *out;
  size_t l;

  if (bufLen < TIME_STR_LEN) {
    return -ENAMETOOLONG;
  }
  out = localtime_r(&sec, &tm);
  asctime_r(out, buf);
  // strip trailing newline
  l = strlen(buf);
  if (l != 0)
    buf[l - 1] = '\0';
  return 0;
}

/** 
 * Check an HDFS connection's Kerberos path.
 *
 * If the mtime of the Kerberos ticket cache file has changed since we first
 * opened the connection, mark the connection as condemned and remove it from
 * the hdfs connection tree.
 *
 * @param conn      The HDFS connection
 */
static int hdfsConnCheckKpath(const struct hdfsConn *conn)
{
  int ret;
  struct stat st;
  char prevTimeBuf[TIME_STR_LEN], newTimeBuf[TIME_STR_LEN];

  if (stat(conn->kpath, &st) < 0) {
    ret = errno;
    if (ret == ENOENT) {
      fprintf(stderr, "hdfsConnCheckKpath(conn.usrname=%s): the kerberos "
              "ticket cache file '%s' has disappeared.  Condemning the "
              "connection.\n", conn->usrname, conn->kpath);
    } else {
      fprintf(stderr, "hdfsConnCheckKpath(conn.usrname=%s): stat(%s) "
              "failed with error code %d.  Pessimistically condemning the "
              "connection.\n", conn->usrname, conn->kpath, ret);
    }
    return -ret;
  }
  if ((st.st_mtim.tv_sec != conn->kPathMtime) ||
      (st.st_mtim.tv_nsec != conn->kPathMtimeNs)) {
    timeToStr(conn->kPathMtime, prevTimeBuf, sizeof(prevTimeBuf));
    timeToStr(st.st_mtim.tv_sec, newTimeBuf, sizeof(newTimeBuf));
    fprintf(stderr, "hdfsConnCheckKpath(conn.usrname=%s): mtime on '%s' "
            "has changed from '%s' to '%s'.  Condemning the connection "
            "because our cached Kerberos credentials have probably "
            "changed.\n", conn->usrname, conn->kpath, prevTimeBuf, newTimeBuf);
    return -EINTERNAL;
  }
  return 0;
}

/**
 * Cache expiration logic.
 *
 * This function is called periodically by the cache expiration thread.  For
 * each FUSE connection not currently in use (refcnt == 0) it will decrement the
 * expirationCount for that connection.  Once the expirationCount reaches 0 for
 * a connection, it can be garbage collected.
 *
 * We also check to see if the Kerberos credentials have changed.  If so, the
 * connecton is immediately condemned, even if it is currently in use.
 */
static void hdfsConnExpiry(void)
{
  struct hdfsConn *conn, *tmpConn;

  pthread_mutex_lock(&gConnMutex);
  RB_FOREACH_SAFE(conn, hdfsConnTree, &gConnTree, tmpConn) {
    if (conn->kpath) {
      if (hdfsConnCheckKpath(conn)) {
        conn->condemned = 1;
        RB_REMOVE(hdfsConnTree, &gConnTree, conn);
        if (conn->refcnt == 0) {
          /* If the connection is not in use by any threads, delete it
           * immediately.  If it is still in use by some threads, the last
           * thread using it will clean it up later inside hdfsConnRelease. */
          hdfsConnFree(conn);
          continue;
        }
      }
    }
    if (conn->refcnt == 0) {
      /* If the connection is not currently in use by a thread, check to see if
       * it ought to be removed because it's too old. */
      conn->expirationCount--;
      if (conn->expirationCount <= 0) {
        if (conn->condemned) {
          fprintf(stderr, "hdfsConnExpiry: LOGIC ERROR: condemned connection "
                  "as %s is still in the tree!\n", conn->usrname);
        }
        fprintf(stderr, "hdfsConnExpiry: freeing and removing connection as "
                "%s because it's now too old.\n", conn->usrname);
        RB_REMOVE(hdfsConnTree, &gConnTree, conn);
        hdfsConnFree(conn);
      }
    }
  }
  pthread_mutex_unlock(&gConnMutex);
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
static void findKerbTicketCachePath(struct fuse_context *ctx,
                                    char *path, size_t pathLen)
{
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

/**
 * Create a new libhdfs connection.
 *
 * @param usrname       Username to use for the new connection
 * @param ctx           FUSE context to use for the new connection
 * @param out           (out param) the new libhdfs connection
 *
 * @return              0 on success; error code otherwise
 */
static int fuseNewConnect(const char *usrname, struct fuse_context *ctx,
        struct hdfsConn **out)
{
  struct hdfsBuilder *bld = NULL;
  char kpath[PATH_MAX] = { 0 };
  struct hdfsConn *conn = NULL;
  int ret;
  struct stat st;

  conn = calloc(1, sizeof(struct hdfsConn));
  if (!conn) {
    fprintf(stderr, "fuseNewConnect: OOM allocating struct hdfsConn\n");
    ret = -ENOMEM;
    goto error;
  }
  bld = hdfsNewBuilder();
  if (!bld) {
    fprintf(stderr, "Unable to create hdfs builder\n");
    ret = -ENOMEM;
    goto error;
  }
  /* We always want to get a new FileSystem instance here-- that's why we call
   * hdfsBuilderSetForceNewInstance.  Otherwise the 'cache condemnation' logic
   * in hdfsConnExpiry will not work correctly, since FileSystem might re-use the
   * existing cached connection which we wanted to get rid of.
   */
  hdfsBuilderSetForceNewInstance(bld);
  hdfsBuilderSetNameNode(bld, gUri);
  if (gPort) {
    hdfsBuilderSetNameNodePort(bld, gPort);
  }
  hdfsBuilderSetUserName(bld, usrname);
  if (gHdfsAuthConf == AUTH_CONF_KERBEROS) {
    findKerbTicketCachePath(ctx, kpath, sizeof(kpath));
    if (stat(kpath, &st) < 0) {
      fprintf(stderr, "fuseNewConnect: failed to find Kerberos ticket cache "
        "file '%s'.  Did you remember to kinit for UID %d?\n",
        kpath, ctx->uid);
      ret = -EACCES;
      goto error;
    }
    conn->kPathMtime = st.st_mtim.tv_sec;
    conn->kPathMtimeNs = st.st_mtim.tv_nsec;
    hdfsBuilderSetKerbTicketCachePath(bld, kpath);
    conn->kpath = strdup(kpath);
    if (!conn->kpath) {
      fprintf(stderr, "fuseNewConnect: OOM allocating kpath\n");
      ret = -ENOMEM;
      goto error;
    }
  }
  conn->usrname = strdup(usrname);
  if (!conn->usrname) {
    fprintf(stderr, "fuseNewConnect: OOM allocating usrname\n");
    ret = -ENOMEM;
    goto error;
  }
  conn->fs = hdfsBuilderConnect(bld);
  bld = NULL;
  if (!conn->fs) {
    ret = errno;
    fprintf(stderr, "fuseNewConnect(usrname=%s): Unable to create fs: "
            "error code %d\n", usrname, ret);
    goto error;
  }
  RB_INSERT(hdfsConnTree, &gConnTree, conn);
  *out = conn;
  return 0;

error:
  if (bld) {
    hdfsFreeBuilder(bld);
  }
  if (conn) {
    free(conn->kpath);
    free(conn->usrname);
    free(conn);
  }
  return ret;
}

int fuseConnect(const char *usrname, struct fuse_context *ctx,
                struct hdfsConn **out)
{
  int ret;
  struct hdfsConn* conn;

  pthread_mutex_lock(&gConnMutex);
  conn = hdfsConnFind(usrname);
  if (!conn) {
    ret = fuseNewConnect(usrname, ctx, &conn);
    if (ret) {
      pthread_mutex_unlock(&gConnMutex);
      fprintf(stderr, "fuseConnect(usrname=%s): fuseNewConnect failed with "
              "error code %d\n", usrname, ret);
      return ret;
    }
  }
  conn->refcnt++;
  conn->expirationCount = (gExpiryPeriod + gTimerPeriod - 1) / gTimerPeriod;
  if (conn->expirationCount < 2)
    conn->expirationCount = 2;
  pthread_mutex_unlock(&gConnMutex);
  *out = conn;
  return 0;
}

int fuseConnectAsThreadUid(struct hdfsConn **conn)
{
  struct fuse_context *ctx;
  char *usrname;
  int ret;
  
  ctx = fuse_get_context();
  usrname = getUsername(ctx->uid);
  ret = fuseConnect(usrname, ctx, conn);
  free(usrname);
  return ret;
}

int fuseConnectTest(void)
{
  int ret;
  struct hdfsConn *conn;

  if (gHdfsAuthConf == AUTH_CONF_KERBEROS) {
    // TODO: call some method which can tell us whether the FS exists.  In order
    // to implement this, we have to add a method to FileSystem in order to do
    // this without valid Kerberos authentication.  See HDFS-3674 for details.
    return 0;
  }
  ret = fuseNewConnect("root", NULL, &conn);
  if (ret) {
    fprintf(stderr, "fuseConnectTest failed with error code %d\n", ret);
    return ret;
  }
  hdfsConnRelease(conn);
  return 0;
}

struct hdfs_internal* hdfsConnGetFs(struct hdfsConn *conn)
{
  return conn->fs;
}

void hdfsConnRelease(struct hdfsConn *conn)
{
  pthread_mutex_lock(&gConnMutex);
  conn->refcnt--;
  if ((conn->refcnt == 0) && (conn->condemned)) {
    fprintf(stderr, "hdfsConnRelease(usrname=%s): freeing condemend FS!\n",
      conn->usrname);
    /* Notice that we're not removing the connection from gConnTree here.
     * If the connection is condemned, it must have already been removed from
     * the tree, so that no other threads start using it.
     */
    hdfsConnFree(conn);
  }
  pthread_mutex_unlock(&gConnMutex);
}

/**
 * Get the monotonic time.
 *
 * Unlike the wall-clock time, monotonic time only ever goes forward.  If the
 * user adjusts the time, the monotonic time will not be affected.
 *
 * @return        The monotonic time
 */
static time_t getMonotonicTime(void)
{
  int res;
  struct timespec ts;
       
  res = clock_gettime(CLOCK_MONOTONIC, &ts);
  if (res)
    abort();
  return ts.tv_sec;
}

/**
 * FUSE connection expiration thread
 *
 */
static void* hdfsConnExpiryThread(void *v)
{
  time_t nextTime, curTime;
  int waitTime;

  nextTime = getMonotonicTime() + gTimerPeriod;
  while (1) {
    curTime = getMonotonicTime();
    if (curTime >= nextTime) {
      hdfsConnExpiry();
      nextTime = curTime + gTimerPeriod;
    }
    waitTime = (nextTime - curTime) * 1000;
    poll(NULL, 0, waitTime);
  }
  return NULL;
}
