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
 **/

LIBHDFS_EXTERNAL
void hdfsGetLastError(char *buf, int len);


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
 * @return         0 on success; nonzero error code otherwise.
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
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
LIBHDFS_EXTERNAL
int hdfsBuilderConfGetInt(struct hdfsBuilder *bld, const char *key, int32_t *val);


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
 * Return 1 on failure, 0 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsEnableLoggingForComponent(int component);

/**
 * Disable logging functionality for a component.
 * Return 1 on failure, 0 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsDisableLoggingForComponent(int component);

/**
 * Set level between trace and error.
 * Return 1 on failure, 0 otherwise.
 **/
LIBHDFS_EXTERNAL
int hdfsSetLoggingLevel(int component);


#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif
