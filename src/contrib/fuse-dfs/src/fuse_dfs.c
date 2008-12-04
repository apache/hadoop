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

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include <fuse.h>
#include <fuse/fuse_opt.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <math.h> // for ceil
#include <getopt.h>
#include <assert.h>
#include <syslog.h>
#include <strings.h>

#include <hdfs.h>
#include <stddef.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <pthread.h>

// Constants
//
static const int default_id       = 99; // nobody  - not configurable since soon uids in dfs, yeah!
static const int blksize = 512;
static const char *const TrashPrefixDir = "/user/root/.Trash";
static const char *const TrashDir = "/user/root/.Trash/Current";
static const char *program;


/** options for fuse_opt.h */
struct options {
  char* protected;
  char* server;
  int port;
  int debug;
  int read_only;
  int initchecks;
  int no_permissions;
  int usetrash;
  int entry_timeout;
  int attribute_timeout;
  int private;
  size_t rdbuffer_size;
  int direct_io;
} options;

void print_options() {
  fprintf(stderr,"options:\n");
  fprintf(stderr, "\tprotected=%s\n",options.protected);
  fprintf(stderr, "\tserver=%s\n",options.server);
  fprintf(stderr, "\tport=%d\n",options.port);
  fprintf(stderr, "\tdebug=%d\n",options.debug);
  fprintf(stderr, "\tread_only=%d\n",options.read_only);
  fprintf(stderr, "\tusetrash=%d\n",options.usetrash);
  fprintf(stderr, "\tentry_timeout=%d\n",options.entry_timeout);
  fprintf(stderr, "\tattribute_timeout=%d\n",options.attribute_timeout);
  fprintf(stderr, "\tprivate=%d\n",options.private);
  fprintf(stderr, "\trdbuffer_size=%d (KBs)\n",(int)options.rdbuffer_size/1024);
}

//#define DOTRACE
#ifdef DOTRACE
#define TRACE(x) \
  syslog(LOG_ERR, "fuse_dfs TRACE - %s\n", x);  \
  fprintf(stderr, "fuse_dfs TRACE - %s\n", x);

#define TRACE1(x,y)                              \
  syslog(LOG_ERR, "fuse_dfs TRACE - %s %s\n", x,y);  \
  fprintf(stderr, "fuse_dfs TRACE - %s %s\n", x,y);
#else
#define TRACE(x) ; 
#define TRACE1(x,y) ; 
#endif

/**
 *
 * dfs_fh_struct is passed around for open files. Fuse provides a hook (the context) 
 * for storing file specific data.
 *
 * 2 Types of information:
 * a) a read buffer for performance reasons since fuse is typically called on 4K chunks only
 * b) the hdfs fs handle 
 *
 */

typedef struct dfs_fh_struct {
  hdfsFile hdfsFH;
  char *buf;
  tSize bufferSize;  //what is the size of the buffer we have
  off_t buffersStartOffset; //where the buffer starts in the file
  hdfsFS fs; // for reads/writes need to access as the real user
  pthread_mutex_t mutex;
} dfs_fh;


/** macro to define options */
#define DFSFS_OPT_KEY(t, p, v) { t, offsetof(struct options, p), v }

static void print_usage(const char *pname)
{
  fprintf(stdout,"USAGE: %s [debug] [--help] [--version] [-oprotected=<colon_seped_list_of_paths] [rw] [-onotrash] [-ousetrash] [-obig_writes] [-oprivate (single user)] [ro] [-oserver=<hadoop_servername>] [-oport=<hadoop_port>] [-oentry_timeout=<secs>] [-oattribute_timeout=<secs>] [-odirect_io] [-onopoermissions] [-o<other fuse option>] <mntpoint> [fuse options]\n",pname);
  fprintf(stdout,"NOTE: debugging option for fuse is -debug\n");
}


/** keys for FUSE_OPT_ options */
enum
  {
    KEY_VERSION,
    KEY_HELP,
    KEY_USETRASH,
    KEY_NOTRASH,
    KEY_RO,
    KEY_RW,
    KEY_PRIVATE,
    KEY_BIGWRITES,
    KEY_DEBUG,
    KEY_INITCHECKS,
    KEY_NOPERMISSIONS,
    KEY_DIRECTIO,
  };

static struct fuse_opt dfs_opts[] =
  {
    DFSFS_OPT_KEY("server=%s", server, 0),
    DFSFS_OPT_KEY("entry_timeout=%d", entry_timeout, 0),
    DFSFS_OPT_KEY("attribute_timeout=%d", attribute_timeout, 0),
    DFSFS_OPT_KEY("protected=%s", protected, 0),
    DFSFS_OPT_KEY("port=%d", port, 0),
    DFSFS_OPT_KEY("rdbuffer=%d", rdbuffer_size,0),

    FUSE_OPT_KEY("private", KEY_PRIVATE),
    FUSE_OPT_KEY("ro", KEY_RO),
    FUSE_OPT_KEY("debug", KEY_DEBUG),
    FUSE_OPT_KEY("initchecks", KEY_INITCHECKS),
    FUSE_OPT_KEY("nopermissions", KEY_NOPERMISSIONS),
    FUSE_OPT_KEY("big_writes", KEY_BIGWRITES),
    FUSE_OPT_KEY("rw", KEY_RW),
    FUSE_OPT_KEY("usetrash", KEY_USETRASH),
    FUSE_OPT_KEY("notrash", KEY_NOTRASH),
    FUSE_OPT_KEY("direct_io", KEY_DIRECTIO),
    FUSE_OPT_KEY("-v",             KEY_VERSION),
    FUSE_OPT_KEY("--version",      KEY_VERSION),
    FUSE_OPT_KEY("-h",             KEY_HELP),
    FUSE_OPT_KEY("--help",         KEY_HELP),
    FUSE_OPT_END
  };

int dfs_options(void *data, const char *arg, int key,  struct fuse_args *outargs)
{
  (void) data;

  switch (key) {
  case FUSE_OPT_KEY_OPT:
    fprintf(stderr,"fuse-dfs ignoring option %s\n",arg);
    return 1;
  case  KEY_VERSION:
    fprintf(stdout,"%s %s\n",program,_FUSE_DFS_VERSION);
    exit(0);
  case KEY_HELP:
    print_usage(program);
    exit(0);
  case KEY_USETRASH:
    options.usetrash = 1;
    break;
  case KEY_NOTRASH:
    options.usetrash = 1;
    break;
  case KEY_RO:
    options.read_only = 1;
    break;
  case KEY_RW:
    options.read_only = 0;
    break;
  case KEY_PRIVATE:
    options.private = 1;
    break;
  case KEY_DEBUG:
    fuse_opt_add_arg(outargs, "-d");
    options.debug = 1;
    break;
  case KEY_INITCHECKS:
    options.initchecks = 1;
    break;
  case KEY_NOPERMISSIONS:
    options.no_permissions = 1;
    break;
  case KEY_DIRECTIO:
    options.direct_io = 1;
    break;
  case KEY_BIGWRITES:
#ifdef FUSE_CAP_BIG_WRITES
    fuse_opt_add_arg(outargs, "-obig_writes");
#endif
    break;
  default: {
    // try and see if the arg is a URI for DFS
    int tmp_port;
    char tmp_server[1024];

    if (!sscanf(arg,"dfs://%1024[a-zA-Z0-9_.-]:%d",tmp_server,&tmp_port)) {
      if (strcmp(arg,"ro") == 0) {
        options.read_only = 1;
      } else if (strcmp(arg,"rw") == 0) {
        options.read_only = 0;
      } else {
        fprintf(stderr,"fuse-dfs didn't recognize %s,%d\n",arg,key);
        fuse_opt_add_arg(outargs,arg);
        return 0;
      }
    } else {
      options.port = tmp_port;
      options.server = strdup(tmp_server);
      fprintf(stderr, "port=%d,server=%s\n", options.port, options.server);
    }
  }
  }
  return 0;
}


//
// Structure to store fuse_dfs specific data
// this will be created and passed to fuse at startup
// and fuse will pass it back to us via the context function
// on every operation.
//
typedef struct dfs_context_struct {
  int debug;
  char *nn_hostname;
  int nn_port;
  hdfsFS fs;
  int read_only;
  int usetrash;
  int direct_io;
  char **protectedpaths;
  size_t rdbuffer_size;
  // todo:
  // total hack city - use this to strip off the dfs url from the filenames
  // that the dfs API is now providing in 0.14.5
  // Will do a better job of fixing this once I am back from vacation
  //
  char dfs_uri[1024];
  int dfs_uri_len;
} dfs_context;

#define TRASH_RENAME_TRIES  100

//
// Some forward declarations
//
static int dfs_mkdir(const char *path, mode_t mode);
static int dfs_rename(const char *from, const char *to);


//
// NOTE: this function is a c implementation of org.apache.hadoop.fs.Trash.moveToTrash(Path path).
//

int move_to_trash(const char *item, hdfsFS userFS) {

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(item);
  assert(dfs);
  assert('/' == *item);
  assert(rindex(item,'/') >= 0);


  char fname[4096]; // or last element of the directory path
  char parent_directory[4096]; // the directory the fname resides in

  if (strlen(item) > sizeof(fname) - strlen(TrashDir)) {
    syslog(LOG_ERR, "ERROR: internal buffer too small to accomodate path of length %d %s:%d\n", (int)strlen(item), __FILE__, __LINE__);
    return -EIO;
  }

  // separate the file name and the parent directory of the item to be deleted
  {
    int length_of_parent_dir = rindex(item, '/') - item ;
    int length_of_fname = strlen(item) - length_of_parent_dir - 1; // the '/'

    // note - the below strncpys should be safe from overflow because of the check on item's string length above.
    strncpy(parent_directory, item, length_of_parent_dir);
    parent_directory[length_of_parent_dir ] = 0;
    strncpy(fname, item + length_of_parent_dir + 1, strlen(item));
    fname[length_of_fname + 1] = 0;
  }

  // create the target trash directory
  char trash_dir[4096];
  if (snprintf(trash_dir, sizeof(trash_dir), "%s%s",TrashDir,parent_directory) >= sizeof trash_dir) {
    syslog(LOG_ERR, "move_to_trash error target is not big enough to hold new name for %s %s:%d\n",item, __FILE__, __LINE__);
    return -EIO;
  }

  // create the target trash directory in trash (if needed)
  if ( hdfsExists(userFS, trash_dir)) {
    int status;
    // make the directory to put it in in the Trash - NOTE
    // dfs_mkdir also creates parents, so Current will be created if it does not exist.
    if ((status = dfs_mkdir(trash_dir,0777)) != 0) {
      return status;
    }
  }

  //
  // if the target path in Trash already exists, then append with
  // a number. Start from 1.
  //
  char target[4096];
  int j ;
  if ( snprintf(target, sizeof target,"%s/%s",trash_dir, fname) >= sizeof target) {
    syslog(LOG_ERR, "move_to_trash error target is not big enough to hold new name for %s %s:%d\n",item, __FILE__, __LINE__);
    return -EIO;
  }

  // NOTE: this loop differs from the java version by capping the #of tries
  for (j = 1; ! hdfsExists(userFS, target) && j < TRASH_RENAME_TRIES ; j++) {
    if (snprintf(target, sizeof target,"%s/%s.%d",trash_dir, fname, j) >= sizeof target) {
      syslog(LOG_ERR, "move_to_trash error target is not big enough to hold new name for %s %s:%d\n",item, __FILE__, __LINE__);
      return -EIO;
    }
  }
  return dfs_rename(item,target);
} 
 

/**
 * getpwuid and getgrgid return static structs so we safeguard the contents
 * while retrieving fields using the 2 structs below.
 * NOTE: if using both, always get the passwd struct firt!
 */
static pthread_mutex_t passwdstruct_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t groupstruct_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Converts from a hdfs hdfsFileInfo to a POSIX stat struct
 *
 */
int fill_stat_structure(hdfsFileInfo *info, struct stat *st) 
{
  assert(st);
  assert(info);

  // initialize the stat structure
  memset(st, 0, sizeof(struct stat));

  // by default: set to 0 to indicate not supported for directory because we cannot (efficiently) get this info for every subdirectory
  st->st_nlink = (info->mKind == kObjectKindDirectory) ? 0 : 1;

  uid_t owner_id = default_id;
#if PERMS
  if (info->mOwner != NULL) {
    //
    // Critical section - protect from concurrent calls in different threads since
    // the struct below is static.
    // (no returns until end)
    //
    pthread_mutex_lock(&passwdstruct_mutex);

    struct passwd *passwd_info = getpwnam(info->mOwner);
    owner_id = passwd_info == NULL ? default_id : passwd_info->pw_uid;

    //
    // End critical section 
    // 
    pthread_mutex_unlock(&passwdstruct_mutex);

  } 
#endif
  gid_t group_id = default_id;
#if PERMS
  if (info->mGroup != NULL) {
    //
    // Critical section - protect from concurrent calls in different threads since
    // the struct below is static.
    // (no returns until end)
    //
    pthread_mutex_lock(&groupstruct_mutex);

    struct group *grp = getgrnam(info->mGroup);
    group_id = grp == NULL ? default_id : grp->gr_gid;

    //
    // End critical section 
    // 
    pthread_mutex_unlock(&groupstruct_mutex);

  }
#endif

  short perm = (info->mKind == kObjectKindDirectory) ? (S_IFDIR | 0777) :  (S_IFREG | 0666);
#if PERMS
  if (info->mPermissions > 0) {
    perm = (info->mKind == kObjectKindDirectory) ? S_IFDIR:  S_IFREG ;
    perm |= info->mPermissions;
  }
#endif

  // set stat metadata
  st->st_size     = (info->mKind == kObjectKindDirectory) ? 4096 : info->mSize;
  st->st_blksize  = blksize;
  st->st_blocks   =  ceil(st->st_size/st->st_blksize);
  st->st_mode     = perm;
  st->st_uid      = owner_id;
  st->st_gid      = group_id;
#if PERMS
  st->st_atime    = info->mLastAccess;
#else
  st->st_atime    = info->mLastMod;
#endif
  st->st_mtime    = info->mLastMod;
  st->st_ctime    = info->mLastMod;

  return 0;
}


#if PERMS

/**
 * Utility for getting the user making the fuse call in char * form
 * NOTE: if non-null return, the return must be freed by the caller.
 */
static char *getUsername(uid_t uid)
{
  //
  // Critical section - protect from concurrent calls in different threads.
  // since the struct below is static.
  // (no returns until end)
  //

  pthread_mutex_lock(&passwdstruct_mutex);

  struct passwd *userinfo = getpwuid(uid);
  char * ret = userinfo && userinfo->pw_name ? strdup(userinfo->pw_name) : NULL;

  pthread_mutex_unlock(&passwdstruct_mutex);

  //
  // End critical section 
  // 
  return ret;
}

/**
 * Cleans up a char ** group pointer
 */

static void freeGroups(char **groups, int numgroups) {
  if (groups == NULL) {
    return;
  }
  int i ;
  for (i = 0; i < numgroups; i++) {
    free(groups[i]);
  }
  free(groups);
}

#define GROUPBUF_SIZE 5

static char *getGroup(gid_t gid) {
  //
  // Critical section - protect from concurrent calls in different threads.
  // since the struct below is static.
  // (no returns until end)
  //

  pthread_mutex_lock(&groupstruct_mutex);

  struct group* grp = getgrgid(gid);
  char * ret = grp && grp->gr_name ? strdup(grp->gr_name) : NULL;

  //
  // End critical section 
  // 
  pthread_mutex_unlock(&groupstruct_mutex);

  return ret;
}


/**
 * Utility for getting the group from the uid
 * NOTE: if non-null return, the return must be freed by the caller.
 */
char *getGroupUid(uid_t uid) {
  //
  // Critical section - protect from concurrent calls in different threads
  // since the structs below are static.
  // (no returns until end)
  //

  pthread_mutex_lock(&passwdstruct_mutex);
  pthread_mutex_lock(&groupstruct_mutex);

  char *ret = NULL;
  struct passwd *userinfo = getpwuid(uid);
  if (NULL != userinfo) {
    struct group* grp = getgrgid( userinfo->pw_gid);
    ret = grp && grp->gr_name ? strdup(grp->gr_name) : NULL;
  }

  //
  // End critical section 
  // 
  pthread_mutex_unlock(&groupstruct_mutex);
  pthread_mutex_unlock(&passwdstruct_mutex);

  return ret;
}


/**
 * lookup the gid based on the uid
 */
gid_t getGidUid(uid_t uid) {
  //
  // Critical section - protect from concurrent calls in different threads
  // since the struct below is static.
  // (no returns until end)
  //

  pthread_mutex_lock(&passwdstruct_mutex);

  struct passwd *userinfo = getpwuid(uid);
  gid_t gid = userinfo == NULL ? 0 : userinfo->pw_gid;

  //
  // End critical section 
  // 
  pthread_mutex_unlock(&passwdstruct_mutex);

  return gid;
}

/**
 * Utility for getting the groups for the user making the fuse call in char * form
 */
static char ** getGroups(uid_t uid, int *num_groups)
{
  char *user = getUsername(uid);

  if (user == NULL)
    return NULL;

  char **groupnames = NULL;

  // see http://www.openldap.org/lists/openldap-devel/199903/msg00023.html

  //#define GETGROUPS_T 1 
#ifdef GETGROUPS_T
  *num_groups = GROUPBUF_SIZE;

  gid_t* grouplist = malloc(GROUPBUF_SIZE * sizeof(gid_t)); 
  assert(grouplist != NULL);
  gid_t* tmp_grouplist; 
  int rtr;

  gid_t gid = getGidUid(uid);

  if ((rtr = getgrouplist(user, gid, grouplist, num_groups)) == -1) {
    // the buffer we passed in is < *num_groups
    if ((tmp_grouplist = realloc(grouplist, *num_groups * sizeof(gid_t))) != NULL) {
      grouplist = tmp_grouplist;
      getgrouplist(user, gid, grouplist, num_groups);
    }
  }

  groupnames = (char**)malloc(sizeof(char*)* (*num_groups) + 1);
  assert(groupnames);
  int i;
  for (i=0; i < *num_groups; i++)  {
    groupnames[i] = getGroup(grouplist[i]);
    if (groupnames[i] == NULL) {
      fprintf(stderr, "error could not lookup group %d\n",(int)grouplist[i]);
    }
  } 
  free(grouplist);
  assert(user != NULL);
  groupnames[i] = user;

  // increment num_groups to include the user being added to the group list
  *num_groups = *num_groups + 1;
#else

  int i = 0;
  assert(user != NULL);
  groupnames[i] = user;
  i++;

  groupnames[i] = getGroupUid(uid);
  if (groupnames[i]) {
    i++;
  }

  *num_groups = i;

#endif
  return groupnames;
}


/**
 * Connects to the NN as the current user/group according to FUSE
 *
 */
static hdfsFS doConnectAsUser(const char *hostname, int port) {
  uid_t uid = fuse_get_context()->uid;

  char *user = getUsername(uid);
  if (NULL == user)
    return NULL;
  int numgroups = 0;
  char **groups = getGroups(uid, &numgroups);
  hdfsFS fs = hdfsConnectAsUser(hostname, port, user, (const char **)groups, numgroups);
  freeGroups(groups, numgroups);
  if (user) 
    free(user);
  return fs;
}
#else
static hdfsFS doConnectAsUser(const char *hostname, int port) {
  return hdfsConnect(hostname, port);
}
#endif

//
// Start of read-only functions
//

static int dfs_getattr(const char *path, struct stat *st)
{
  TRACE1("getattr", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);
  assert(st);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to %s:%d %s:%d\n", dfs->nn_hostname, dfs->nn_port,__FILE__, __LINE__);
    return -EIO;
  }

  // call the dfs API to get the actual information
  hdfsFileInfo *info = hdfsGetPathInfo(dfs->fs,path);

  if (NULL == info) {
    return -ENOENT;
  }

  fill_stat_structure(&info[0], st);

  // setup hard link info - for a file it is 1 else num entries in a dir + 2 (for . and ..)
  if (info[0].mKind == kObjectKindDirectory) {
    int numEntries = 0;
    hdfsFileInfo *info = hdfsListDirectory(dfs->fs,path,&numEntries);

    if (info) {
      hdfsFreeFileInfo(info,numEntries);
    }
    st->st_nlink = numEntries + 2;
  } else {
    // not a directory
    st->st_nlink = 1;
  }

  // free the info pointer
  hdfsFreeFileInfo(info,1);

  return 0;
}

static int dfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
  TRACE1("readdir",path)

  (void) offset;
  (void) fi;

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);
  assert(buf);

  int path_len = strlen(path);

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  // call dfs to read the dir
  int numEntries = 0;
  hdfsFileInfo *info = hdfsListDirectory(userFS,path,&numEntries);
  userFS = NULL;

  // NULL means either the directory doesn't exist or maybe IO error.
  if (NULL == info) {
    return -ENOENT;
  }

  int i ;
  for (i = 0; i < numEntries; i++) {

    // check the info[i] struct
    if (NULL == info[i].mName) {
      syslog(LOG_ERR,"ERROR: for <%s> info[%d].mName==NULL %s:%d", path, i, __FILE__,__LINE__);
      continue;
    }

    struct stat st;
    fill_stat_structure(&info[i], &st);

    // hack city: todo fix the below to something nicer and more maintainable but
    // with good performance
    // strip off the path but be careful if the path is solely '/'
    // NOTE - this API started returning filenames as full dfs uris
    const char *const str = info[i].mName + dfs->dfs_uri_len + path_len + ((path_len == 1 && *path == '/') ? 0 : 1);

    // pack this entry into the fuse buffer
    int res = 0;
    if ((res = filler(buf,str,&st,0)) != 0) {
      syslog(LOG_ERR, "ERROR: readdir filling the buffer %d %s:%d\n",res, __FILE__, __LINE__);
    }
  }

  // insert '.' and '..'
  const char *const dots [] = { ".",".."};
  for (i = 0 ; i < 2 ; i++)
    {
      struct stat st;
      memset(&st, 0, sizeof(struct stat));

      // set to 0 to indicate not supported for directory because we cannot (efficiently) get this info for every subdirectory
      st.st_nlink =  0;

      // setup stat size and acl meta data
      st.st_size    = 512;
      st.st_blksize = 512;
      st.st_blocks  =  1;
      st.st_mode    = (S_IFDIR | 0777);
      st.st_uid     = default_id;
      st.st_gid     = default_id;
      // todo fix below times
      st.st_atime   = 0;
      st.st_mtime   = 0;
      st.st_ctime   = 0;

      const char *const str = dots[i];

      // flatten the info using fuse's function into a buffer
      int res = 0;
      if ((res = filler(buf,str,&st,0)) != 0) {
        syslog(LOG_ERR, "ERROR: readdir filling the buffer %d %s:%d", res, __FILE__, __LINE__);
      }
    }
  // free the info pointers
  hdfsFreeFileInfo(info,numEntries);
  return 0;
}

static size_t min(const size_t x, const size_t y) {
  return x < y ? x : y;
}

/**
 * dfs_read
 *
 * Reads from dfs or the open file's buffer.  Note that fuse requires that
 * either the entire read be satisfied or the EOF is hit or direct_io is enabled
 *
 */
static int dfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
  TRACE1("read",path)
  
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);
  assert(buf);
  assert(offset >= 0);
  assert(size >= 0);
  assert(fi);

  dfs_fh *fh = (dfs_fh*)fi->fh;

  assert(fh != NULL);
  assert(fh->fs != NULL);
  assert(fh->hdfsFH != NULL);

  // special case this as simplifies the rest of the logic to know the caller wanted > 0 bytes
  if (size == 0)
    return 0;

  // If size is bigger than the read buffer, then just read right into the user supplied buffer
  if (size >= dfs->rdbuffer_size) {
    int num_read;
    size_t total_read = 0;
    while (size - total_read > 0 && (num_read = hdfsPread(fh->fs, fh->hdfsFH, offset + total_read, buf + total_read, size - total_read)) > 0) {
      total_read += num_read;
    }
    // if there was an error before satisfying the current read, this logic declares it an error
    // and does not try to return any of the bytes read. Don't think it matters, so the code
    // is just being conservative.
    if (total_read < size && num_read < 0) {
      total_read = -EIO;
    }
    return total_read;
  }

  //
  // Critical section - protect from multiple reads in different threads accessing the read buffer
  // (no returns until end)
  //

  pthread_mutex_lock(&fh->mutex);

  // used only to check the postcondition of this function - namely that we satisfy
  // the entire read or EOF is hit.
  int isEOF = 0;
  int ret = 0;

  // check if the buffer is empty or
  // the read starts before the buffer starts or
  // the read ends after the buffer ends

  if (fh->bufferSize == 0  || 
      offset < fh->buffersStartOffset || 
      offset + size > fh->buffersStartOffset + fh->bufferSize) 
    {
      // Read into the buffer from DFS
      int num_read = 0;
      size_t total_read = 0;

      while (dfs->rdbuffer_size  - total_read > 0 && 
             (num_read = hdfsPread(fh->fs, fh->hdfsFH, offset + total_read, fh->buf + total_read, dfs->rdbuffer_size - total_read)) > 0) {
        total_read += num_read;
      }

      // if there was an error before satisfying the current read, this logic declares it an error
      // and does not try to return any of the bytes read. Don't think it matters, so the code
      // is just being conservative.
      if (total_read < size && num_read < 0) {
        // invalidate the buffer 
        fh->bufferSize = 0; 
        syslog(LOG_ERR, "Read error - pread failed for %s with return code %d %s:%d", path, (int)num_read, __FILE__, __LINE__);
        ret = -EIO;
      } else {
        // Either EOF, all read or read beyond size, but then there was an error
        fh->bufferSize = total_read;
        fh->buffersStartOffset = offset;

        if (dfs->rdbuffer_size - total_read > 0) {
          // assert(num_read == 0); this should be true since if num_read < 0 handled above.
          isEOF = 1;
        }
      }
    }

  //
  // NOTE on EOF, fh->bufferSize == 0 and ret = 0 ,so the logic for copying data into the caller's buffer is bypassed, and
  //  the code returns 0 as required
  //
  if (ret == 0 && fh->bufferSize > 0) {

    assert(offset >= fh->buffersStartOffset);
    assert(fh->buf);

    const size_t bufferReadIndex = offset - fh->buffersStartOffset;
    assert(bufferReadIndex >= 0 && bufferReadIndex < fh->bufferSize);

    const size_t amount = min(fh->buffersStartOffset + fh->bufferSize - offset, size);
    assert(amount >= 0 && amount <= fh->bufferSize);

    const char *offsetPtr = fh->buf + bufferReadIndex;
    assert(offsetPtr >= fh->buf);
    assert(offsetPtr + amount <= fh->buf + fh->bufferSize);
    
    memcpy(buf, offsetPtr, amount);

    ret = amount;
  }

  //
  // Critical section end 
  //
  pthread_mutex_unlock(&fh->mutex);

  // fuse requires the below and the code should guarantee this assertion
  // 3 cases on return:
  //   1. entire read satisfied
  //   2. partial read and isEOF - including 0 size read
  //   3. error 
  assert(ret == size || isEOF || ret < 0);

  return ret;
}



static int dfs_statfs(const char *path, struct statvfs *st)
{
  TRACE1("statfs",path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(st);
  assert(dfs);

  // init the stat structure
  memset(st,0,sizeof(struct statvfs));

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  const long cap   = hdfsGetCapacity(userFS);
  const long used  = hdfsGetUsed(userFS);
  const long bsize = hdfsGetDefaultBlockSize(userFS);

  // fill in the statvfs structure

  /* FOR REFERENCE:
     struct statvfs {
     unsigned long  f_bsize;    // file system block size
     unsigned long  f_frsize;   // fragment size
     fsblkcnt_t     f_blocks;   // size of fs in f_frsize units
     fsblkcnt_t     f_bfree;    // # free blocks
     fsblkcnt_t     f_bavail;   // # free blocks for non-root
     fsfilcnt_t     f_files;    // # inodes
     fsfilcnt_t     f_ffree;    // # free inodes
     fsfilcnt_t     f_favail;   // # free inodes for non-root
     unsigned long  f_fsid;     // file system id
     unsigned long  f_flag;     / mount flags
     unsigned long  f_namemax;  // maximum filename length
     };
  */

  st->f_bsize   =  bsize;
  st->f_frsize  =  bsize;

  st->f_blocks  =  cap/bsize;

  st->f_bfree   =  (cap-used)/bsize;
  st->f_bavail  =  (cap-used)/bsize;

  st->f_files   =  1000;
  st->f_ffree   =  500;
  st->f_favail  =  500;
  st->f_fsid    =  1023;
  st->f_flag    =  ST_RDONLY | ST_NOSUID;
  st->f_namemax =  1023;

  return 0;
}


static int is_protected(const char *path) {

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

//
// Start of write functions
//


static int dfs_mkdir(const char *path, mode_t mode)
{
  TRACE1("mkdir", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  if (is_protected(path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to create the directory: %s", path);
    return -EACCES;
  }

  if (dfs->read_only) {
    syslog(LOG_ERR,"ERROR: hdfs is configured as read-only, cannot create the directory %s\n",path);
    return -EACCES;
  }
  
  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (hdfsCreateDirectory(userFS, path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to create directory %s",path);
    return -EIO;
  }

  return 0;

}

static int dfs_rename(const char *from, const char *to)
{
  TRACE1("rename", from) 

 // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(from);
  assert(to);
  assert(dfs);

  assert('/' == *from);
  assert('/' == *to);

  if (is_protected(from) || is_protected(to)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to rename: %s %s", from, to);
    return -EACCES;
  }

  if (dfs->read_only) {
    syslog(LOG_ERR,"ERROR: hdfs is configured as read-only, cannot rename the directory %s\n",from);
    return -EACCES;
  }

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (hdfsRename(userFS, from, to)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to rename %s to %s",from, to);
    return -EIO;
  }

  return 0;

}


static int dfs_rmdir(const char *path)
{
  TRACE1("rmdir", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  if (is_protected(path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to delete a protected directory: %s ",path);
    return -EACCES;
  }

  if (dfs->read_only) {
    syslog(LOG_ERR,"ERROR: hdfs is configured as read-only, cannot delete the directory %s\n",path);
    return -EACCES;
  }

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  int numEntries = 0;
  hdfsFileInfo *info = hdfsListDirectory(userFS,path,&numEntries);

  // free the info pointers
  hdfsFreeFileInfo(info,numEntries);

  if (numEntries) {
    return -ENOTEMPTY;
  }

  if (dfs->usetrash && strncmp(path, TrashPrefixDir, strlen(TrashPrefixDir)) != 0) {
    fprintf(stderr, "moving to trash %s\n", path);
    int ret= move_to_trash(path, userFS);
    return ret;
  }


  if (hdfsDelete(userFS, path)) {
    syslog(LOG_ERR,"ERROR: hdfs error trying to delete the directory %s\n",path);
    return -EIO;
  }

  return 0;
}


static int dfs_unlink(const char *path)
{
  TRACE1("unlink", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  if (is_protected(path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to delete a protected directory: %s ",path);
    return -EACCES;
  }

  if (dfs->read_only) {
    syslog(LOG_ERR,"ERROR: hdfs is configured as read-only, cannot create the directory %s\n",path);
    return -EACCES;
  }

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  // move the file to the trash if this is enabled and its not actually in the trash.
  if (dfs->usetrash && strncmp(path, TrashPrefixDir, strlen(TrashPrefixDir)) != 0) {
    int ret= move_to_trash(path, userFS);
    return ret;
  }

  if (hdfsDelete(userFS, path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to delete the file %s",path);
    return -EIO;
  }

  return 0;

}

static int dfs_utimens(const char *path, const struct timespec ts[2])
{
  TRACE1("utimens", path)
#if PERMS
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  time_t aTime = ts[0].tv_sec;
  time_t mTime = ts[1].tv_sec;

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (hdfsUtime(userFS, path, mTime, aTime)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to utime %s to %ld/%ld",path, (long)mTime, (long)aTime);
    fprintf(stderr,"ERROR: could not set utime for path %s\n",path);
    return -EIO;
  }
#endif  
  return 0;
}


static int dfs_chmod(const char *path, mode_t mode)
{
  TRACE1("chmod", path)

#if PERMS
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (hdfsChmod(userFS, path, (short)mode)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to chmod %s to %d",path, (int)mode);
    return -EIO;
  }
#endif
  return 0;
}

static int dfs_chown(const char *path, uid_t uid, gid_t gid)
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


static int dfs_open(const char *path, struct fuse_file_info *fi)
{
  TRACE1("open", path)

  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert('/' == *path);
  assert(dfs);

  int ret = 0;

  // 0x8000 is always passed in and hadoop doesn't like it, so killing it here
  // bugbug figure out what this flag is and report problem to Hadoop JIRA
  int flags = (fi->flags & 0x7FFF);

  // retrieve dfs specific data
  dfs_fh *fh = (dfs_fh*)malloc(sizeof (dfs_fh));
  assert(fh != NULL);

  if ((fh->fs = doConnectAsUser(dfs->nn_hostname,dfs->nn_port)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if ((fh->hdfsFH = (hdfsFile)hdfsOpenFile(fh->fs, path, flags,  0, 3, 0)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect open file %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (fi->flags & O_WRONLY || fi->flags & O_CREAT) {
    // write specific initialization

    fh->buf = NULL;
  } else  {
    // read specific initialization

    assert(dfs->rdbuffer_size > 0);

    if (NULL == (fh->buf = (char*)malloc(dfs->rdbuffer_size*sizeof (char)))) {
      syslog(LOG_ERR, "ERROR: could not allocate memory for file buffer for a read for file %s dfs %s:%d\n", path,__FILE__, __LINE__);
      ret = -EIO;
    }

    fh->buffersStartOffset = 0;
    fh->bufferSize = 0;

  }

  // 
  // mutex needed for reads/writes
  //
  pthread_mutex_init(&fh->mutex, NULL);

  fi->fh = (uint64_t)fh;

  return ret;
}

static int dfs_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  TRACE1("write", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;
  int ret = 0;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);
  assert(fi);

  dfs_fh *fh = (dfs_fh*)fi->fh;
  assert(fh);

  hdfsFile file_handle = (hdfsFile)fh->hdfsFH;
  assert(file_handle);

  //
  // Critical section - make the sanity check (tell to see the writes are sequential) and the actual write 
  // (no returns until end)
  //
  pthread_mutex_lock(&fh->mutex);

  tSize length = 0;
  assert(fh->fs);

  tOffset cur_offset = hdfsTell(fh->fs, file_handle);
  if (cur_offset != offset) {
    syslog(LOG_ERR, "ERROR: user trying to random access write to a file %d!=%d for %s %s:%d\n",(int)cur_offset, (int)offset,path, __FILE__, __LINE__);
    ret =  -EIO;

  } else {

    length = hdfsWrite(fh->fs, file_handle, buf, size);

    if (length <= 0) {
      syslog(LOG_ERR, "ERROR: fuse problem - could not write all the bytes for %s %d!=%d%s:%d\n",path,length,(int)size, __FILE__, __LINE__);
      ret = -EIO;
    } 

    if (length != size) {
      syslog(LOG_ERR, "WARN: fuse problem - could not write all the bytes for %s %d!=%d%s:%d\n",path,length,(int)size, __FILE__, __LINE__);
    }
  }

  //
  // Critical section end 
  //

  pthread_mutex_unlock(&fh->mutex);

  return ret == 0 ? length : ret;
}

/**
 * This mutex is to protect releasing a file handle in case the user calls close in different threads
 * and fuse passes these calls to here.
 */
static pthread_mutex_t release_mutex = PTHREAD_MUTEX_INITIALIZER;

int dfs_release (const char *path, struct fuse_file_info *fi) {
  TRACE1("release", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  int ret = 0;

  //
  // Critical section - protect from multiple close calls in different threads.
  // (no returns until end)
  //

  pthread_mutex_lock(&release_mutex);

  if (NULL != (void*)fi->fh) {

    dfs_fh *fh = (dfs_fh*)fi->fh;
    assert(fh);

    hdfsFile file_handle = (hdfsFile)fh->hdfsFH;

    if (NULL != file_handle) {
      if (hdfsCloseFile(fh->fs, file_handle) != 0) {
        syslog(LOG_ERR, "ERROR: dfs problem - could not close file_handle(%ld) for %s %s:%d\n",(long)file_handle,path, __FILE__, __LINE__);
        fprintf(stderr, "ERROR: dfs problem - could not close file_handle(%ld) for %s %s:%d\n",(long)file_handle,path, __FILE__, __LINE__);
        ret = -EIO;
      }
    }

    if (fh->buf != NULL) {
      free(fh->buf);
    }
    // this is always created and initialized, so always destroy it. (see dfs_open)
      pthread_mutex_destroy(&fh->mutex);

    free(fh);

    fi->fh = (uint64_t)0;
  }

  pthread_mutex_unlock(&release_mutex);

  //
  // End critical section 
  // 

  return ret;
}

static int dfs_mknod(const char *path, mode_t mode, dev_t rdev) {
  TRACE1("mknod", path)
  syslog(LOG_DEBUG,"in dfs_mknod");
  return 0;
}

static int dfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  TRACE1("create", path)
  fi->flags |= mode;
  return dfs_open(path, fi);
}

int dfs_flush(const char *path, struct fuse_file_info *fi) {
  TRACE1("flush", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);
  assert(fi);

  if (NULL == (void*)fi->fh) {
    return  0;
  }

  // note that fuse calls flush on RO files too and hdfs does not like that and will return an error
  if (fi->flags & O_WRONLY) {

    dfs_fh *fh = (dfs_fh*)fi->fh;
    assert(fh);
    hdfsFile file_handle = (hdfsFile)fh->hdfsFH;
    assert(file_handle);

    assert(fh->fs);
    if (hdfsFlush(fh->fs, file_handle) != 0) {
      syslog(LOG_ERR, "ERROR: dfs problem - could not flush file_handle(%lx) for %s %s:%d\n",(long)file_handle,path, __FILE__, __LINE__);
      return -EIO;
    }
  }

  return 0;
}

static int dfs_access(const char *path, int mask)
{
  TRACE1("access", path)
  // bugbug - I think we need the FileSystemAPI/libhdfs to expose this!
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);

  hdfsFS userFS;
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }
  //  return hdfsAccess(userFS, path, mask);
  return 0;
}


/**
 * For now implement truncate here and only for size == 0.
 * Weak implementation in that we just delete the file and 
 * then re-create it, but don't set the user, group, and times to the old
 * file's metadata. 
 */
static int dfs_truncate(const char *path, off_t size)
{
  TRACE1("truncate", path)
  if (size != 0) {
    return -ENOTSUP;
  }

  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  assert(path);
  assert('/' == *path);
  assert(dfs);

  int ret = dfs_unlink(path);
  if (ret != 0) {
    return ret;
  }

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  int flags = O_WRONLY | O_CREAT;

  hdfsFile file;
  if ((file = (hdfsFile)hdfsOpenFile(userFS, path, flags,  0, 3, 0)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect open file %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (hdfsCloseFile(userFS, file) != 0) {
    syslog(LOG_ERR, "ERROR: could not connect close file %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }
  return 0;
}


static int dfs_symlink(const char *from, const char *to)
{
  TRACE1("symlink", from)
  (void)from;
  (void)to;
  // bugbug we need the FileSystem to support this posix API
  return -ENOTSUP;
}


void dfs_destroy (void *ptr)
{
  TRACE("destroy")
  dfs_context *dfs = (dfs_context*)ptr;
  dfs->fs = NULL;
}


// Hacked up function to basically do:
//  protectedpaths = split(options.protected,':');

static void init_protectedpaths(dfs_context *dfs) {

  char *tmp = options.protected;


  // handle degenerate case up front.
  if (tmp == NULL || 0 == *tmp) {
    dfs->protectedpaths = (char**)malloc(sizeof(char*));
    dfs->protectedpaths[0] = NULL;
    return;
  }
  assert(tmp);

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

  /*
    j  = 0;
    while (dfs->protectedpaths[j]) {
    printf("dfs->protectedpaths[%d]=%s\n",j,dfs->protectedpaths[j]);
    fflush(stdout);
    j++;
    }
    exit(1);
  */
}


void *dfs_init()
{

  //
  // Create a private struct of data we will pass to fuse here and which
  // will then be accessible on every call.
  //
  dfs_context *dfs = (dfs_context*)malloc(sizeof (dfs_context));

  if (NULL == dfs) {
    syslog(LOG_ERR, "FATAL: could not malloc fuse dfs context struct - out of memory %s:%d", __FILE__, __LINE__);
    exit(1);
  }

  // initialize the context
  dfs->debug                 = options.debug;
  dfs->nn_hostname           = options.server;
  dfs->nn_port               = options.port;
  dfs->fs                    = NULL;
  dfs->read_only             = options.read_only;
  dfs->usetrash              = options.usetrash;
  dfs->protectedpaths        = NULL;
  dfs->rdbuffer_size         = options.rdbuffer_size;
  dfs->direct_io             = options.direct_io;

  bzero(dfs->dfs_uri,0);
  sprintf(dfs->dfs_uri,"dfs://%s:%d/",dfs->nn_hostname,dfs->nn_port);
  dfs->dfs_uri_len = strlen(dfs->dfs_uri);

  // use ERR level to ensure it makes it into the log.
  syslog(LOG_ERR, "mounting %s", dfs->dfs_uri);

  init_protectedpaths(dfs);
  assert(dfs->protectedpaths != NULL);

  if (dfs->rdbuffer_size <= 0) {
    syslog(LOG_DEBUG, "WARN: dfs->rdbuffersize <= 0 = %ld %s:%d", dfs->rdbuffer_size, __FILE__, __LINE__);
    dfs->rdbuffer_size = 32768;
  }
  return (void*)dfs;
}


static struct fuse_operations dfs_oper = {
  .getattr	= dfs_getattr,
  .access	= dfs_access,
  .readdir	= dfs_readdir,
  .destroy       = dfs_destroy,
  .init         = dfs_init,
  .open	        = dfs_open,
  .read	        = dfs_read,
  .symlink	= dfs_symlink,
  .statfs	= dfs_statfs,
  .mkdir	= dfs_mkdir,
  .rmdir	= dfs_rmdir,
  .rename	= dfs_rename,
  .unlink       = dfs_unlink,
  .release      = dfs_release,
  .create       = dfs_create,
  .write	= dfs_write,
  .flush        = dfs_flush,
  .mknod        = dfs_mknod,
	.utimens	= dfs_utimens,
  .chmod	= dfs_chmod,
  .chown	= dfs_chown,
  .truncate	= dfs_truncate,
};


int main(int argc, char *argv[])
{

  umask(0);

  program = argv[0];
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

  /* clear structure that holds our options */
  memset(&options, 0, sizeof(struct options));

  // some defaults
  options.rdbuffer_size = 10*1024*1024; 
  options.attribute_timeout = 60; 
  options.entry_timeout = 60;

  if (fuse_opt_parse(&args, &options, dfs_opts, dfs_options) == -1)
    /** error parsing options */
    return -1;


  // Some fuse options we set
  if (! options.private) {
    fuse_opt_add_arg(&args, "-oallow_other");
  }

  if (!options.no_permissions) {
    fuse_opt_add_arg(&args, "-odefault_permissions");
  }

  {
    char buf[1024];

    snprintf(buf, sizeof buf, "-oattr_timeout=%d",options.attribute_timeout);
    fuse_opt_add_arg(&args, buf);

    snprintf(buf, sizeof buf, "-oentry_timeout=%d",options.entry_timeout);
    fuse_opt_add_arg(&args, buf);
  }

  if (options.server == NULL || options.port == 0) {
    print_usage(argv[0]);
    exit(0);
  }


  // 
  // Check we can connect to hdfs
  // 
  if (options.initchecks == 1) {
    hdfsFS temp;
    if ((temp = hdfsConnect(options.server, options.port)) == NULL) {
      const char *cp = getenv("CLASSPATH");
      const char *ld = getenv("LD_LIBRARY_PATH");
      fprintf(stderr, "FATAL: misconfiguration problem, cannot connect to hdfs - here's your environment\n");
      fprintf(stderr, "LD_LIBRARY_PATH=%s\n",ld == NULL ? "NULL" : ld);
      fprintf(stderr, "CLASSPATH=%s\n",cp == NULL ? "NULL" : cp);
      syslog(LOG_ERR, "FATAL: misconfiguration problem, cannot connect to hdfs - here's your environment\n");
      syslog(LOG_ERR, "LD_LIBRARY_PATH=%s\n",ld == NULL ? "NULL" : ld);
      syslog(LOG_ERR, "CLASSPATH=%s\n",cp == NULL ? "NULL" : cp);
      exit(0);
    }  
    temp = NULL;
  }

  int ret = fuse_main(args.argc, args.argv, &dfs_oper, NULL);

  if (ret) printf("\n");

  /** free arguments */
  fuse_opt_free_args(&args);

  return ret;
}
