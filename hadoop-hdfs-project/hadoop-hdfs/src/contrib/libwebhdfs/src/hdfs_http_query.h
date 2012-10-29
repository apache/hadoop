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


#ifndef _HDFS_HTTP_QUERY_H_
#define _HDFS_HTTP_QUERY_H_

#include <unistd.h> /* for size_t */
#include <inttypes.h> /* for int16_t */

/**
 * Create the URL for a MKDIR request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the dir to create
 * @param user User name
 * @param url Holding the generated URL for MKDIR request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForMKDIR(const char *host, int nnPort,
                      const char *path, const char *user,
                      char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a MKDIR (with mode) request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the dir to create
 * @param mode Mode of MKDIR
 * @param user User name
 * @param url Holding the generated URL for MKDIR request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForMKDIRwithMode(const char *host, int nnPort, const char *path,
                              int mode, const char *user,
                              char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a RENAME request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param srcpath Source path
 * @param dstpath Destination path
 * @param user User name
 * @param url Holding the generated URL for RENAME request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForRENAME(const char *host, int nnPort, const char *srcpath,
                       const char *dstpath, const char *user,
                       char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a CHMOD request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Target path
 * @param mode New mode for the file
 * @param user User name
 * @param url Holding the generated URL for CHMOD request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForCHMOD(const char *host, int nnPort, const char *path,
                      int mode, const char *user,
                      char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a GETFILESTATUS request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the target file
 * @param user User name
 * @param url Holding the generated URL for GETFILESTATUS request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForGetFileStatus(const char *host, int nnPort,
                              const char *path, const char *user,
                              char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a LISTSTATUS request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the directory for listing
 * @param user User name
 * @param url Holding the generated URL for LISTSTATUS request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForLS(const char *host, int nnPort,
                   const char *path, const char *user,
                   char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a DELETE request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the file to be deletected
 * @param recursive Whether or not to delete in a recursive way
 * @param user User name
 * @param url Holding the generated URL for DELETE request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForDELETE(const char *host, int nnPort, const char *path,
                       int recursive, const char *user,
                       char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a CHOWN request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the target
 * @param owner New owner
 * @param group New group
 * @param user User name
 * @param url Holding the generated URL for CHOWN request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForCHOWN(const char *host, int nnPort, const char *path,
                      const char *owner, const char *group, const char *user,
                      char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a OPEN/READ request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the file to read
 * @param user User name
 * @param offset Offset for reading (the start position for this read)
 * @param length Length of the file to read
 * @param url Holding the generated URL for OPEN/READ request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForOPEN(const char *host, int nnPort, const char *path,
                     const char *user, size_t offset, size_t length,
                     char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a UTIMES (update time) request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the file for updating time
 * @param mTime Modified time to set
 * @param aTime Access time to set
 * @param user User name
 * @param url Holding the generated URL for UTIMES request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForUTIMES(const char *host, int nnPort, const char *path,
                       long unsigned mTime, long unsigned aTime,
                       const char *user,
                       char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a WRITE/CREATE request (sent to NameNode)
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the dir to create
 * @param user User name
 * @param replication Number of replication of the file
 * @param blockSize Size of the block for the file
 * @param url Holding the generated URL for WRITE request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForNnWRITE(const char *host, int nnPort, const char *path,
                        const char *user, int16_t replication, size_t blockSize,
                        char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for an APPEND request (sent to NameNode)
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the file for appending
 * @param user User name
 * @param url Holding the generated URL for APPEND request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForNnAPPEND(const char *host, int nnPort,
                         const char *path, const char *user,
                         char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a SETREPLICATION request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the target file
 * @param replication New replication number
 * @param user User name
 * @param url Holding the generated URL for SETREPLICATION request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForSETREPLICATION(const char *host, int nnPort, const char *path,
                               int16_t replication, const char *user,
                               char **url) __attribute__ ((warn_unused_result));

/**
 * Create the URL for a GET_BLOCK_LOCATIONS request
 *
 * @param host The hostname of the NameNode
 * @param nnPort Port of the NameNode
 * @param path Path of the target file
 * @param offset The offset in the file
 * @param length Length of the file content
 * @param user User name
 * @param url Holding the generated URL for GET_BLOCK_LOCATIONS request
 * @return 0 on success and non-zero value on errors
 */
int createUrlForGetBlockLocations(const char *host, int nnPort,
                            const char *path, size_t offset,
                            size_t length, const char *user,
                            char **url) __attribute__ ((warn_unused_result));


#endif  //_HDFS_HTTP_QUERY_H_
