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
#ifndef LIBHDFSPP_HDFS_HDFSEXT
#define LIBHDFSPP_HDFS_HDFSEXT

#include <hdfspp/log.h>

/* get typdefs and #defines from libhdfs' hdfs.h to stay consistent */
#include <hdfs/hdfs.h>

/**
 *  Note: The #defines below are copied directly from libhdfs'
 *  hdfs.h.  LIBHDFS_EXTERNAL gets explicitly #undefed at the
 *  end of the file so it must be redefined here.
 **/

#ifdef WIN32
    #ifdef LIBHDFS_DLL_EXPORT
        #define LIBHDFS_EXTERNAL __declspec(dllexport)
    #elif LIBHDFS_DLL_IMPORT
        #define LIBHDFS_EXTERNAL __declspec(dllimport)
    #else
        #define LIBHDFS_EXTERNAL
    #endif
#else
    #ifdef LIBHDFS_DLL_EXPORT
        #define LIBHDFS_EXTERNAL __attribute__((visibility("default")))
    #elif LIBHDFS_DLL_IMPORT
        #define LIBHDFS_EXTERNAL __attribute__((visibility("default")))
    #else
        #define LIBHDFS_EXTERNAL
    #endif
#endif


/**
 * Keep C bindings that are libhdfs++ specific in here.
 **/

#ifdef __cplusplus
extern "C" {
#endif

/**
 *  Reads the last error, if any, that happened in this thread
 *  into the user supplied buffer.
 *  @param buf  A chunk of memory with room for the error string.
 *  @param len  Size of the buffer, if the message is longer than
 *              len len-1 bytes of the message will be copied.
 *  @return     0 on successful read of the last error, -1 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsGetLastError(char *buf, int len);


/**
 *  Cancels operations being made by the FileHandle.
 *  Note: Cancel cannot be reversed.  This is intended
 *  to be used before hdfsClose to avoid waiting for
 *  operations to complete.
 **/
LIBHDFS_EXTERNAL
int hdfsCancel(hdfsFS fs, hdfsFile file);

/**
 * Create an HDFS builder, using the configuration XML files from the indicated
 * directory.  If the directory does not exist, or contains no configuration
 * XML files, a Builder using all default values will be returned.
 *
 * @return The HDFS builder, or NULL on error.
 */
struct hdfsBuilder *hdfsNewBuilderFromDirectory(const char * configDirectory);


/**
 * Get a configuration string from the settings currently read into the builder.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will be set to NULL if the
 *                 key isn't found.  You must free this string with
 *                 hdfsConfStrFree.
 *
 * @return         0 on success; -1 otherwise.
 *                 Failure to find the key is not an error.
 */
LIBHDFS_EXTERNAL
int hdfsBuilderConfGetStr(struct hdfsBuilder *bld, const char *key,
                          char **val);

/**
 * Get a configuration integer from the settings currently read into the builder.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will NOT be changed if the
 *                 key isn't found.
 *
 * @return         0 on success; -1 otherwise.
 *                 Failure to find the key is not an error.
 */
LIBHDFS_EXTERNAL
int hdfsBuilderConfGetInt(struct hdfsBuilder *bld, const char *key, int32_t *val);


/**
 * Get a configuration long from the settings currently read into the builder.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will NOT be changed if the
 *                 key isn't found.
 *
 * @return         0 on success; -1 otherwise.
 *                 Failure to find the key is not an error.
 */
LIBHDFS_EXTERNAL
int hdfsBuilderConfGetLong(struct hdfsBuilder *bld, const char *key, int64_t *val);

struct hdfsDNInfo {
  const char *    ip_address;
  const char *    hostname;
  const char *    network_location;
  int             xfer_port;
  int             info_port;
  int             IPC_port;
  int             info_secure_port;
};

struct hdfsBlockInfo {
    uint64_t            start_offset;
    uint64_t            num_bytes;

    size_t              num_locations;
    struct hdfsDNInfo * locations;
};

struct hdfsBlockLocations
{
    uint64_t               fileLength;
    int                    isLastBlockComplete;
    int                    isUnderConstruction;

    size_t                 num_blocks;
    struct hdfsBlockInfo * blocks;
};

/**
 * Returns the block information and data nodes associated with a particular file.
 *
 * The hdfsBlockLocations structure will have zero or more hdfsBlockInfo elements,
 * which will have zero or more ip_addr elements indicating which datanodes have
 * each block.
 *
 * @param fs         A connected hdfs instance
 * @param path       Path of the file to query
 * @param locations  The address of an output pointer to contain the block information.
 *                   On success, this pointer must be later freed with hdfsFreeBlockLocations.
 *
 * @return         0 on success; -1 otherwise.
 *                 If the file does not exist, -1 will be returned and errno will be set.
 */
LIBHDFS_EXTERNAL
int hdfsGetBlockLocations(hdfsFS fs, const char *path, struct hdfsBlockLocations ** locations);

/**
 * Frees up an hdfsBlockLocations pointer allocated by hdfsGetBlockLocations.
 *
 * @param locations    The previously-populated pointer allocated by hdfsGetBlockLocations
 * @return             0 on success, -1 on error
 */
LIBHDFS_EXTERNAL
int hdfsFreeBlockLocations(struct hdfsBlockLocations * locations);




/**
 *  Client can supply a C style function pointer to be invoked any time something
 *  is logged.  Unlike the C++ logger this will not filter by level or component,
 *  it is up to the consumer to throw away messages they don't want.
 *
 *  Note: The callback provided must be reentrant, the library does not guarentee
 *  that there won't be concurrent calls.
 *  Note: Callback does not own the LogData struct.  If the client would like to
 *  keep one around use hdfsCopyLogData/hdfsFreeLogData.
 **/
LIBHDFS_EXTERNAL
void hdfsSetLogFunction(void (*hook)(LogData*));

/**
 *  Create a copy of the LogData object passed in and return a pointer to it.
 *  Returns null if it was unable to copy/
 **/
LIBHDFS_EXTERNAL
LogData *hdfsCopyLogData(const LogData*);

/**
 *  Client must call this to dispose of the LogData created by hdfsCopyLogData.
 **/
LIBHDFS_EXTERNAL
void hdfsFreeLogData(LogData*);

/**
 * Enable loggind functionality for a component.
 * Return -1 on failure, 0 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsEnableLoggingForComponent(int component);

/**
 * Disable logging functionality for a component.
 * Return -1 on failure, 0 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsDisableLoggingForComponent(int component);

/**
 * Set level between trace and error.
 * Return -1 on failure, 0 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsSetLoggingLevel(int component);

/*
 * Supported event names.  These names will stay consistent in libhdfs callbacks.
 *
 * Other events not listed here may be seen, but they are not stable and
 * should not be counted on.
 */
extern const char * FS_NN_CONNECT_EVENT;
extern const char * FS_NN_READ_EVENT;
extern const char * FS_NN_WRITE_EVENT;

extern const char * FILE_DN_CONNECT_EVENT;
extern const char * FILE_DN_READ_EVENT;
extern const char * FILE_DN_WRITE_EVENT;


#define LIBHDFSPP_EVENT_OK (0)
#define DEBUG_SIMULATE_ERROR (-1)

typedef int (*libhdfspp_fs_event_callback)(const char * event, const char * cluster,
                                           int64_t value, int64_t cookie);
typedef int (*libhdfspp_file_event_callback)(const char * event,
                                             const char * cluster,
                                             const char * file,
                                             int64_t value, int64_t cookie);

/**
 * Registers a callback for the next filesystem connect operation the current
 * thread executes.
 *
 *  @param handler A function pointer.  Taken as a void* and internally
 *                 cast into the appropriate type.
 *  @param cookie  An opaque value that will be passed into the handler; can
 *                 be used to correlate the handler with some object in the
 *                 consumer's space.
 **/
LIBHDFS_EXTERNAL
int hdfsPreAttachFSMonitor(libhdfspp_fs_event_callback handler, int64_t cookie);


/**
 * Registers a callback for the next file open operation the current thread
 * executes.
 *
 *  @param fs      The filesystem
 *  @param handler A function pointer.  Taken as a void* and internally
 *                 cast into the appropriate type.
 *  @param cookie  An opaque value that will be passed into the handler; can
 *                 be used to correlate the handler with some object in the
 *                 consumer's space.
 **/
LIBHDFS_EXTERNAL
int hdfsPreAttachFileMonitor(libhdfspp_file_event_callback handler, int64_t cookie);


/**
 * Finds file name on the file system. hdfsFreeFileInfo should be called to deallocate memory.
 *
 *  @param fs         The filesystem (required)
 *  @param path       Path at which to begin search, can have wild cards  (must be non-blank)
 *  @param name       Name to find, can have wild cards                   (must be non-blank)
 *  @param numEntries Set to the number of files/directories in the result.
 *  @return           Returns a dynamically-allocated array of hdfsFileInfo
 *                    objects; NULL on error or empty result.
 *                    errno is set to non-zero on error or zero on success.
 **/
LIBHDFS_EXTERNAL
hdfsFileInfo * hdfsFind(hdfsFS fs, const char* path, const char* name, uint32_t * numEntries);


/*****************************************************************************
 *                    HDFS SNAPSHOT FUNCTIONS
 ****************************************************************************/

/**
 * Creates a snapshot of a snapshottable directory specified by path
 *
 *  @param fs      The filesystem (required)
 *  @param path    Path to the directory to be snapshotted (must be non-blank)
 *  @param name    Name to be given to the created snapshot (may be NULL)
 *  @return        0 on success, corresponding errno on failure
 **/
LIBHDFS_EXTERNAL
int hdfsCreateSnapshot(hdfsFS fs, const char* path, const char* name);

/**
 * Deletes the directory snapshot specified by path and name
 *
 *  @param fs      The filesystem (required)
 *  @param path    Path to the snapshotted directory (must be non-blank)
 *  @param name    Name of the snapshot to be deleted (must be non-blank)
 *  @return        0 on success, corresponding errno on failure
 **/
LIBHDFS_EXTERNAL
int hdfsDeleteSnapshot(hdfsFS fs, const char* path, const char* name);

/**
 * Renames the directory snapshot specified by path from old_name to new_name
 *
 *  @param fs         The filesystem (required)
 *  @param path       Path to the snapshotted directory (must be non-blank)
 *  @param old_name   Current name of the snapshot (must be non-blank)
 *  @param new_name   New name of the snapshot (must be non-blank)
 *  @return           0 on success, corresponding errno on failure
 **/
int hdfsRenameSnapshot(hdfsFS fs, const char* path, const char* old_name, const char* new_name);

/**
 * Allows snapshots to be made on the specified directory
 *
 *  @param fs      The filesystem (required)
 *  @param path    Path to the directory to be made snapshottable (must be non-blank)
 *  @return        0 on success, corresponding errno on failure
 **/
LIBHDFS_EXTERNAL
int hdfsAllowSnapshot(hdfsFS fs, const char* path);

/**
 * Disallows snapshots to be made on the specified directory
 *
 *  @param fs      The filesystem (required)
 *  @param path    Path to the directory to be made non-snapshottable (must be non-blank)
 *  @return        0 on success, corresponding errno on failure
 **/
LIBHDFS_EXTERNAL
int hdfsDisallowSnapshot(hdfsFS fs, const char* path);

/**
 * Create a FileSystem based on the builder but don't connect
 * @param bld     Used to populate config options in the same manner as hdfsBuilderConnect.
 *                Does not free builder.
 **/
LIBHDFS_EXTERNAL
hdfsFS hdfsAllocateFileSystem(struct hdfsBuilder *bld);

/**
 * Connect a FileSystem created with hdfsAllocateFileSystem
 * @param fs      A disconnected FS created with hdfsAllocateFileSystem
 * @param bld     The same or exact copy of the builder used for Allocate, we still need a few fields.
 *                Does not free builder.
 * @return        0 on success, corresponding errno on failure
 **/
LIBHDFS_EXTERNAL
int hdfsConnectAllocated(hdfsFS fs, struct hdfsBuilder *bld);

/**
 * Cancel a pending connection on a FileSystem
 * @param fs      A fs in the process of connecting using hdfsConnectAllocated in another thread.
 * @return        0 on success, corresponding errno on failure
 **/
LIBHDFS_EXTERNAL
int hdfsCancelPendingConnection(hdfsFS fs);


#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif
