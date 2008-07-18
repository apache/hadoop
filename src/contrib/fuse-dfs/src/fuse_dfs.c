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

// Constants
//
static const int default_id       = 99; // nobody  - not configurable since soon uids in dfs, yeah!
static const size_t rd_buf_size   = 128 * 1024;
static const int blksize = 512;
static const size_t rd_cache_buf_size = 10*1024*1024;//how much of reads to buffer here

/** options for fuse_opt.h */
struct options {
  char* server;
  int port;
  int debug;
  int nowrites;
  int no_trash;
}options;


typedef struct dfs_fh_struct {
  hdfsFile hdfsFH;
  char *buf;
  tSize sizeBuffer;  //what is the size of the buffer we have
  off_t startOffset; //where the buffer starts in the file
} dfs_fh;

#include <stddef.h>

/** macro to define options */
#define DFSFS_OPT_KEY(t, p, v) { t, offsetof(struct options, p), v }

/** keys for FUSE_OPT_ options */
static void print_usage(const char *pname)
{
  fprintf(stdout,"USAGE: %s [--debug] [--help] [--version] [--nowrites] [--notrash] --server=<hadoop_servername> --port=<hadoop_port> <mntpoint> [fuse options]\n",pname);
  fprintf(stdout,"NOTE: a useful fuse option is -o allow_others and -o default_permissions\n");
  fprintf(stdout,"NOTE: optimizations include -o entry_timeout=500 -o attr_timeout=500\n");
  fprintf(stdout,"NOTE: debugging option for fuse is -debug\n");
}


#define OPTIMIZED_READS 1

enum
  {
    KEY_VERSION,
    KEY_HELP,
  };

static struct fuse_opt dfs_opts[] =
  {
    DFSFS_OPT_KEY("--server=%s", server, 0),
    DFSFS_OPT_KEY("--port=%d", port, 0),
    DFSFS_OPT_KEY("--debug", debug, 1),
    DFSFS_OPT_KEY("--nowrites", nowrites, 1),
    DFSFS_OPT_KEY("--notrash", no_trash, 1),

    FUSE_OPT_KEY("-v",             KEY_VERSION),
    FUSE_OPT_KEY("--version",      KEY_VERSION),
    FUSE_OPT_KEY("-h",             KEY_HELP),
    FUSE_OPT_KEY("--help",         KEY_HELP),
    FUSE_OPT_END
  };

static const char *program;

int dfs_options(void *data, const char *arg, int key,  struct fuse_args *outargs)
{

  if (key == KEY_VERSION) {
    fprintf(stdout,"%s %s\n",program,_FUSE_DFS_VERSION);
    exit(0);
  } else if (key == KEY_HELP) {
    print_usage(program);
    exit(0);
  } else {
    // try and see if the arg is a URI for DFS
    int tmp_port;
    char tmp_server[1024];

    if (!sscanf(arg,"dfs://%1024[a-zA-Z0-9_.-]:%d",tmp_server,&tmp_port)) {
      printf("didn't recognize %s\n",arg);
      fuse_opt_add_arg(outargs,arg);
    } else {
      options.port = tmp_port;
      options.server = strdup(tmp_server);
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
  int nowrites;
  int no_trash;

  // todo:
  // total hack city - use this to strip off the dfs url from the filenames
  // that the dfs API is now providing in 0.14.5
  // Will do a better job of fixing this once I am back from vacation
  //
  char dfs_uri[1024];
  int dfs_uri_len;
} dfs_context;


//
// Start of read-only functions
//

static int dfs_getattr(const char *path, struct stat *st)
{
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

  // initialize the stat structure
  memset(st, 0, sizeof(struct stat));

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

  // set stat metadata
  st->st_size     = (info[0].mKind == kObjectKindDirectory) ? 4096 : info[0].mSize;
  st->st_blksize  = blksize;
  st->st_blocks   =  ceil(st->st_size/st->st_blksize);
  st->st_mode     = (info[0].mKind == kObjectKindDirectory) ? (S_IFDIR | 0777) :  (S_IFREG | 0666);
  st->st_uid      = default_id;
  st->st_gid      = default_id;
  st->st_atime    = info[0].mLastMod;
  st->st_mtime    = info[0].mLastMod;
  st->st_ctime    = info[0].mLastMod;

  // free the info pointer
  hdfsFreeFileInfo(info,1);

  return 0;
}

static int dfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
  (void) offset;
  (void) fi;

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);
  assert(buf);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  int path_len = strlen(path);

  // call dfs to read the dir
  int numEntries = 0;
  hdfsFileInfo *info = hdfsListDirectory(dfs->fs,path,&numEntries);

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
    memset(&st, 0, sizeof(struct stat));

    // set to 0 to indicate not supported for directory because we cannot (efficiently) get this info for every subdirectory
    st.st_nlink = (info[i].mKind == kObjectKindDirectory) ? 0 : 1;

    // setup stat size and acl meta data
    st.st_size    = info[i].mSize;
    st.st_blksize = 512;
    st.st_blocks  =  ceil(st.st_size/st.st_blksize);
    st.st_mode    = (info[i].mKind == kObjectKindDirectory) ? (S_IFDIR | 0777) :  (S_IFREG | 0666);
    st.st_uid     = default_id;
    st.st_gid     = default_id;
    st.st_atime   = info[i].mLastMod;
    st.st_mtime   = info[i].mLastMod;
    st.st_ctime   = info[i].mLastMod;

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

static int dfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(dfs);
  assert(path);
  assert(buf);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }


#ifdef OPTIMIZED_READS
  dfs_fh *fh = (dfs_fh*)fi->fh;
  //fprintf(stderr, "Cache bounds for %s: %llu -> %llu (%d bytes). Check for offset %llu\n", path, fh->startOffset, fh->startOffset + fh->sizeBuffer, fh->sizeBuffer, offset);
  if (fh->sizeBuffer == 0  || offset < fh->startOffset || offset > (fh->startOffset + fh->sizeBuffer)  )
  {
    // do the actual read
    //fprintf (stderr,"Reading %s from HDFS, offset %llu, amount %d\n", path, offset, rd_cache_buf_size);
    const tSize num_read = hdfsPread(dfs->fs, fh->hdfsFH, offset, fh->buf, rd_cache_buf_size);
    if (num_read < 0) {
      syslog(LOG_ERR, "Read error - pread failed for %s with return code %d %s:%d", path, num_read, __FILE__, __LINE__);
      hdfsDisconnect(dfs->fs);
      dfs->fs = NULL;
      return -EIO;
    }
    fh->sizeBuffer = num_read;
    fh->startOffset = offset;
    //fprintf (stderr,"Read %d bytes of %s from HDFS\n", num_read, path);
  }

  char* local_buf = fh->buf;
  const tSize cacheLookupOffset = offset - fh->startOffset;
  local_buf += cacheLookupOffset;
  //fprintf(stderr,"FUSE requested %d bytes of %s for offset %d in file\n", size, path, offset);
  const tSize amount = cacheLookupOffset + size > fh->sizeBuffer
    ?  fh->sizeBuffer - cacheLookupOffset
    : size;
  //fprintf(stderr,"Reading %s from cache, %d bytes from position %d\n", path, amount, cacheLookupOffset);
  //fprintf(stderr,"Cache status for %s: %d bytes cached from offset %llu\n", path, fh->sizeBuffer, fh->startOffset);
  memcpy(buf, local_buf, amount);
  //fprintf(stderr,"Read %s from cache, %d bytes from position %d\n", path, amount, cacheLookupOffset);
  //fprintf(stderr,"Cache status for %s: %d bytes cached from offset %llu\n", path, fh->sizeBuffer, fh->startOffset);
  return amount;

#else
  // NULL means either file doesn't exist or maybe IO error - i.e., the dfs_open must have failed
  if (NULL == (void*)fi->fh) {
    // should never happen
    return  -EIO;
  }
  syslog(LOG_DEBUG,"buffer size=%d\n",(int)size);

  // do the actual read
  const tSize num_read = hdfsPread(dfs->fs, (hdfsFile)fi->fh, offset, buf, size);

  // handle errors
  if (num_read < 0) {
    syslog(LOG_ERR, "Read error - pread failed for %s with return code %d %s:%d", path, num_read, __FILE__, __LINE__);
    hdfsDisconnect(dfs->fs);
    dfs->fs = NULL;
    return -EIO;
  }
  return num_read;
#endif

}

static int dfs_statfs(const char *path, struct statvfs *st)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(st);
  assert(dfs);

  // init the stat structure
  memset(st,0,sizeof(struct statvfs));

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  const long cap   = hdfsGetCapacity(dfs->fs);
  const long used  = hdfsGetUsed(dfs->fs);
  const long bsize = hdfsGetDefaultBlockSize(dfs->fs);

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
  st->f_frsize  =  st->f_bsize;
  st->f_blocks  =  cap/st->f_bsize;
  st->f_bfree   =  (cap-used)/st->f_bsize;
  st->f_bavail  =  st->f_bfree;
  st->f_files   =  1000;
  st->f_ffree   =  500;
  st->f_favail  =  500;
  st->f_fsid    =  1023;
  st->f_flag    =  ST_RDONLY | ST_NOSUID;
  st->f_namemax =  1023;

  return 0;
}

static int dfs_access(const char *path, int mask)
{
  // no permissions on dfs, always a success
  return 0;
}

//
// The remainder are write functionality and therefore not implemented right now
//


static char **protectedpaths;


static int dfs_mkdir(const char *path, mode_t mode)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  assert('/' == *path);

  int i ;
  for (i = 0; protectedpaths[i]; i++) {
    if (strcmp(path, protectedpaths[i]) == 0) {
      syslog(LOG_ERR,"ERROR: hdfs trying to create the directory: %s", path);
      return -EACCES;
    }
  }


  if (dfs->nowrites || hdfsCreateDirectory(dfs->fs, path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to create directory %s",path);
    return -EIO;
  }

  return 0;

}

static int dfs_rename(const char *from, const char *to)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(from);
  assert(to);
  assert(dfs);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  assert('/' == *from);
  assert('/' == *to);

  int i ;
  for (i = 0; protectedpaths[i] != NULL; i++) {
    if (strcmp(from, protectedpaths[i]) == 0) {
      syslog(LOG_ERR,"ERROR: hdfs trying to rename directories %s to %s",from,to);
      return -EACCES;
    }
    if (strcmp(to, protectedpaths[i]) == 0) {
      syslog(LOG_ERR,"ERROR: hdfs trying to rename directories %s to %s",from,to);
      return -EACCES;
    }
  }

  if (dfs->nowrites ||  hdfsRename(dfs->fs, from, to)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to rename %s to %s",from, to);
    return -EIO;
  }
  return 0;

}


static int dfs_rmdir(const char *path)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  assert('/' == *path);

  int i ;
  for (i = 0; protectedpaths[i]; i++) {
    if (strcmp(path, protectedpaths[i]) == 0) {
      syslog(LOG_ERR,"ERROR: hdfs trying to delete the directory: %s ",path);
      return -EACCES;
    }
  }

  int numEntries = 0;
  hdfsFileInfo *info = hdfsListDirectory(dfs->fs,path,&numEntries);

  // free the info pointers
  hdfsFreeFileInfo(info,numEntries);

  if (numEntries) {
    return -ENOTEMPTY;
  }



  // since these commands go through the programmatic hadoop API, there is no
  // trash feature. So, force it here.
  // But make sure the person isn't deleting from Trash itself :)
  // NOTE: /Trash is in protectedpaths so they cannot delete all of trash
  if (!dfs->no_trash && strncmp(path, "/Trash", strlen("/Trash")) != 0) {

    char target[4096];
    char dir[4096];
    int status;

    {
      // find the directory and full targets in Trash

      sprintf(target, "/Trash/Current%s",path);

      // strip off the actual file or directory name from the fullpath
      char *name = rindex(path, '/');
      assert(name);
      *name = 0;

      // use that path to ensure the directory exists in the Trash dir
      // prepend Trash to the directory
      sprintf(dir,"/Trash/Current%s/",path);

      // repair the path not used again but in case the caller expects it.
      *name = '/';
    }

    // if the directory doesn't already exist in the Trash
    // then we go through with the rename
    if ( hdfsExists(dfs->fs, target) != 0) { // 0 means it exists. weird
      // make the directory to put it in in the Trash
      if ((status = dfs_mkdir(dir,0)) != 0) {
        return status;
      }

      // do the rename
      return dfs_rename(path,target);

    }
    // if the directory exists in the Trash, then we don't bother doing the rename
    // and just delete the existing one by falling though.
  }

  if (dfs->nowrites ||  hdfsDelete(dfs->fs, path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to delete the directory %s",path);
    return -EIO;
  }
  return 0;
}


static int dfs_unlink(const char *path)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  assert('/' == *path);

  int i ;
  for (i = 0; protectedpaths[i]; i++) {
    if (strcmp(path, protectedpaths[i]) == 0) {
      syslog(LOG_ERR,"ERROR: hdfs trying to delete the directory: %s ",path);
      return -EACCES;
    }
  }



  // since these commands go through the programmatic hadoop API, there is no
  // trash feature. So, force it here.
  // But make sure the person isn't deleting from Trash itself :)
  // NOTE: /Trash is in protectedpaths so they cannot delete all of trash
  if (!dfs->no_trash && strncmp(path, "/Trash", strlen("/Trash")) != 0) {

    char target[4096];
    char dir[4096];
    int status;

    {
      // find the directory and full targets in Trash

      sprintf(target, "/Trash/Current%s",path);

      // strip off the actual file or directory name from the fullpath
      char *name = rindex(path, '/');
      assert(name);
      *name = 0;

      // use that path to ensure the directory exists in the Trash dir
      // prepend Trash to the directory
      sprintf(dir,"/Trash/Current%s/",path);

      // repair the path not used again but in case the caller expects it.
      *name = '/';
    }

    // if this is a file and it's already got a copy in the Trash, to be conservative, we
    // don't do the delete.
    if (hdfsExists(dfs->fs, target) == 0) {
      syslog(LOG_ERR,"ERROR: hdfs trying to delete a file that was already deleted so cannot back it to Trash: %s",target);
      return -EIO;
    }

    // make the directory to put it in in the Trash
    if ((status = dfs_mkdir(dir,0)) != 0) {
      return status;
    }

    // do the rename
    return dfs_rename(path,target);
  }

  if (dfs->nowrites ||  hdfsDelete(dfs->fs, path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to delete the file %s",path);
    return -EIO;
  }
  return 0;

}

static int dfs_chmod(const char *path, mode_t mode)
{
  (void)path;
  (void)mode;
  return -ENOTSUP;
}

static int dfs_chown(const char *path, uid_t uid, gid_t gid)
{
  (void)path;
  (void)uid;
  (void)gid;
  return -ENOTSUP;
}

//static int dfs_truncate(const char *path, off_t size)
//{
//  (void)path;
//  (void)size;
//  return -ENOTSUP;
//}

long tempfh = 0;

static int dfs_open(const char *path, struct fuse_file_info *fi)
{
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;


  // check params and the context var
  assert(path);
  assert('/' == *path);
  assert(dfs);

  int ret = 0;

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  // 0x8000 is always passed in and hadoop doesn't like it, so killing it here
  // bugbug figure out what this flag is and report problem to Hadoop JIRA
  int flags = (fi->flags & 0x7FFF);


#ifdef OPTIMIZED_READS
  // retrieve dfs specific data
  dfs_fh *fh = (dfs_fh*)malloc(sizeof (dfs_fh));
  fi->fh = (uint64_t)fh;
  fh->hdfsFH = (hdfsFile)hdfsOpenFile(dfs->fs, path, flags,  0, 3, 0);
  fh->buf = (char*)malloc(rd_cache_buf_size*sizeof (char));

  fh->startOffset = 0;
  fh->sizeBuffer = 0;

  if (0 == fh->hdfsFH) {
    syslog(LOG_ERR, "ERROR: could not open file %s dfs %s:%d\n", path,__FILE__, __LINE__);
    ret = -EIO;
  }
#else
  //  fprintf(stderr,"hdfsOpenFile being called %s,%o\n",path,flags);

  // bugbug should stop O_RDWR flag here.


  // bugbug when fix  https://issues.apache.org/jira/browse/HADOOP-3723 can remove the below code
  if (flags & O_WRONLY) {
    flags = O_WRONLY;

  }

  if (flags & O_RDWR) {
    // NOTE - should not normally be checking policy in the middleman, but the handling of Unix flags in DFS is not
    // consistent right now. 2008-07-16
    syslog(LOG_ERR, "ERROR: trying to open a file with O_RDWR and DFS does not support that %s dfs %s:%d\n", path,__FILE__, __LINE__);
    return -EIO;
  }

  //  fprintf(stderr,"hdfsOpenFile being called %s,%o\n",path,flags);

  // retrieve dfs specific data
  fi->fh = (uint64_t)hdfsOpenFile(dfs->fs, path, flags,  0, 3, 0);

  if (0 == fi->fh) {
    syslog(LOG_ERR, "ERROR: could not open file %s dfs %s:%d\n", path,__FILE__, __LINE__);
    ret = -EIO;
  }

#endif

  return ret;
}

static int dfs_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }
#ifdef OPTIMIZED_READS

  dfs_fh *fh = (dfs_fh*)fi->fh;
  hdfsFile file_handle = (hdfsFile)fh->hdfsFH;

#else
  hdfsFile file_handle = (hdfsFile)fi->fh;

  if (NULL == file_handle) {
    syslog(LOG_ERR, "ERROR: fuse problem - no file_handle for %s %s:%d\n",path, __FILE__, __LINE__);
    return -EIO;
  }
#endif

  syslog(LOG_DEBUG,"hdfsTell(dfs,%ld)\n",(long)file_handle);
  tOffset cur_offset = hdfsTell(dfs->fs, file_handle);

  if (cur_offset != offset) {
    syslog(LOG_ERR, "ERROR: user trying to random access write to a file %d!=%d for %s %s:%d\n",(int)cur_offset, (int)offset,path, __FILE__, __LINE__);
    return -EIO;
  }


  tSize length = hdfsWrite(dfs->fs, file_handle, buf, size);

  if(length <= 0) {
    syslog(LOG_ERR, "ERROR: fuse problem - could not write all the bytes for %s %d!=%d%s:%d\n",path,length,(int)size, __FILE__, __LINE__);
    return -EIO;
  }

  if (length != size) {
    syslog(LOG_ERR, "WARN: fuse problem - could not write all the bytes for %s %d!=%d%s:%d\n",path,length,(int)size, __FILE__, __LINE__);
  }

  return length;

}

int dfs_release (const char *path, struct fuse_file_info *fi) {

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);
  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (NULL == (void*)fi->fh) {
    return  0;
  }

#ifdef OPTIMIZED_READS
  dfs_fh *fh = (dfs_fh*)fi->fh;
  hdfsFile file_handle = (hdfsFile)fh->hdfsFH;
  free(fh->buf);
  free(fh);
#else
  hdfsFile file_handle = (hdfsFile)fi->fh;
#endif

  if (NULL == file_handle) {
    return 0;
 }

  if (hdfsCloseFile(dfs->fs, file_handle) != 0) {
    syslog(LOG_ERR, "ERROR: dfs problem - could not close file_handle(%ld) for %s %s:%d\n",(long)file_handle,path, __FILE__, __LINE__);
    //    fprintf(stderr, "ERROR: dfs problem - could not close file_handle(%ld) for %s %s:%d\n",(long)file_handle,path, __FILE__, __LINE__);
    return -EIO;
  }

  fi->fh = (uint64_t)0;
  return 0;
}

static int dfs_mknod(const char *path, mode_t mode, dev_t rdev) {
  syslog(LOG_DEBUG,"in dfs_mknod");
  return 0;
}

static int dfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  fi->flags |= mode;
  return dfs_open(path, fi);
}

int dfs_flush(const char *path, struct fuse_file_info *fi) {

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);


  // if not connected, try to connect and fail out if we can't.
  if (NULL == dfs->fs && NULL == (dfs->fs = hdfsConnect(dfs->nn_hostname,dfs->nn_port))) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if (NULL == (void*)fi->fh) {
    return  0;
  }

  // note that fuse calls flush on RO files too and hdfs does not like that and will return an error
  if(fi->flags & O_WRONLY) {

#ifdef OPTIMIZED_READS
    dfs_fh *fh = (dfs_fh*)fi->fh;
    hdfsFile file_handle = (hdfsFile)fh->hdfsFH;
#else
    hdfsFile file_handle = (hdfsFile)fi->fh;
#endif

    if (hdfsFlush(dfs->fs, file_handle) != 0) {
      syslog(LOG_ERR, "ERROR: dfs problem - could not flush file_handle(%x) for %s %s:%d\n",(long)file_handle,path, __FILE__, __LINE__);
      return -EIO;
    }
  }

  return 0;
}


void dfs_setattr(struct stat *attr, int to_set, struct fuse_file_info *fi)
{

}

void dfs_destroy (void *ptr)
{
  dfs_context *dfs = (dfs_context*)ptr;
  hdfsDisconnect(dfs->fs);
  dfs->fs = NULL;
}


// Hacked up function to basically do:
//  protectedpaths = split(PROTECTED_PATHS,',');

static void init_protectedpaths() {
  // PROTECTED_PATHS should be a #defined value from autoconf
  // set it with configure --with-protectedpaths=/,/user,/user/foo
  // note , seped with no other spaces and no quotes around it
  char *tmp = PROTECTED_PATHS;

  assert(tmp);

  // handle degenerate case up front.
  if (0 == *tmp) {
    protectedpaths = (char**)malloc(sizeof(char*));
    protectedpaths[0] = NULL;
    return;
  }

  int i = 0;
  while (tmp && (NULL != (tmp = index(tmp,',')))) {
    tmp++; // pass the ,
    i++;
  }
  i++; // for the last entry
  i++; // for the final NULL
  protectedpaths = (char**)malloc(sizeof(char*)*i);
  printf("i=%d\n",i);
  tmp = PROTECTED_PATHS;
  int j  = 0;
  while (NULL != tmp && j < i) {
    int length;
    char *eos = index(tmp,',');
    if (NULL != eos) {
      length = eos - tmp; // length of this value
    } else {
      length = strlen(tmp);
    }
    protectedpaths[j] = (char*)malloc(sizeof(char)*length+1);
    strncpy(protectedpaths[j], tmp, length);
    protectedpaths[j][length] = '\0';
    if (eos) {
      tmp = eos + 1;
    } else {
      tmp = NULL;
    }
    j++;
  }
  protectedpaths[j] = NULL;
  /*
  j  = 0;
  while (protectedpaths[j]) {
    printf("protectedpaths[%d]=%s\n",j,protectedpaths[j]);
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
  dfs->nowrites              = options.nowrites;
  dfs->no_trash              = options.no_trash;

  bzero(dfs->dfs_uri,0);
  sprintf(dfs->dfs_uri,"dfs://%s:%d/",dfs->nn_hostname,dfs->nn_port);
  dfs->dfs_uri_len = strlen(dfs->dfs_uri);

  // use ERR level to ensure it makes it into the log.
  syslog(LOG_ERR, "mounting %s", dfs->dfs_uri);

  init_protectedpaths();

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
  .statfs	= dfs_statfs,
  .mkdir	= dfs_mkdir,
  .rmdir	= dfs_rmdir,
  .rename	= dfs_rename,
  .unlink       = dfs_unlink,
  .release      = dfs_release,
  .create       = dfs_create,
  .write	= dfs_write,
  .flush        = dfs_flush,
  //.xsetattr      = dfs_setattr,
    .mknod        = dfs_mknod,
  .chmod	= dfs_chmod,
  .chown	= dfs_chown,
  //  .truncate	= dfs_truncate,
};


int main(int argc, char *argv[])
{

  umask(0);

  program = argv[0];
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

  /* clear structure that holds our options */
  memset(&options, 0, sizeof(struct options));

  if (fuse_opt_parse(&args, &options, dfs_opts, dfs_options) == -1)
    /** error parsing options */
    return -1;


  if (options.server == NULL || options.port == 0) {
    print_usage(argv[0]);
    exit(0);
  }


  int ret = fuse_main(args.argc, args.argv, &dfs_oper, NULL);

  if (ret) printf("\n");

  /** free arguments */
  fuse_opt_free_args(&args);

  return ret;
}
