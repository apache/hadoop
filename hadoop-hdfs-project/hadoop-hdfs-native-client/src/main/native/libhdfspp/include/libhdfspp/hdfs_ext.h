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

extern "C" {
/**
 *  Reads the last error, if any, that happened in this thread
 *  into the user supplied buffer.
 *  @param buf  A chunk of memory with room for the error string.
 *  @param len  Size of the buffer, if the message is longer than
 *              len len-1 bytes of the message will be copied.
 **/

LIBHDFS_EXTERNAL
void hdfsGetLastError(char *buf, int len);


} /* end extern "C" */
#endif
