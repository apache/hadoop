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

#ifndef HADOOP_NATIVE_CORE_FS_H
#define HADOOP_NATIVE_CORE_FS_H

#include "fs/hdfs.h"

#include <inttypes.h>

struct hadoop_err;
struct hadoop_uri;
struct hconf;

/**
 * fs.h
 *
 * This is the __private__ API for native Hadoop filesystems.  (The public API
 * is in hdfs.h.) Native Hadoop filesystems such as JniFS or NDFS implement the
 * APIs in this file to provide a uniform experience to users.
 *
 * The general pattern here is:
 * 1. The client makes a call to libhdfs API
 * 2. fs.c locates the appropriate function in hadoop_fs_ops and calls it.
 * 3. Some filesystem-specific code implements the operation.
 *
 * In C, it is always safe to typecast a structure to the type of the first
 * element.  This allows fs.c to treat hdfsFS instances as if they were
 * instances of struct hadoop_file_base.  Other structures with "base" in the
 * name are intended to be used similarly.  This functionality is similar in
 * many ways to how "base classes" operate in Java.  The derived class contains
 * all the elements of the base class, plus some more.
 *
 * The structure definitions in this file are private, and users of this library
 * will not be able to access them.  This file will not be packaged or
 * distributed... only hdfs.h will.  Thus, it is safe to change any of the APIs
 * or types in this file without creating compatibility problems.
 */

/**
 * Hadoop filesystem types.
 */
enum hadoop_fs_ty {
    HADOOP_FS_TY_JNI = 0,
    HADOOP_FS_TY_NDFS = 1,
    HADOOP_FS_TY_NUM,
};

/**
 * Base data for Hadoop files.
 */
struct hadoop_file_base {
    // The type of filesystem this file was created by.
    enum hadoop_fs_ty ty;
};

/**
 * Base data for Hadoop FileSystem objects.
 */
struct hadoop_fs_base {
    // The type of this filesystem.
    enum hadoop_fs_ty ty;
};

/**
 * Base data for Hadoop Zero-Copy Read objects.
 */
struct hadoopRzOptions {
    // The name of the ByteBufferPool class we should use when doing a zero-copy
    // read.
    char *pool_name;

    // Non-zero to always skip checksums.
    int skip_checksums;

    // If non-null, this callback will be invoked to tear down the cached data
    // inside this options structure during hadoopRzOptionsFree.
    void (*cache_teardown_cb)(void *);

    // The cached data inside this options structure. 
    void *cache;
};

/**
 * Base data for Hadoop Zero-Copy Read buffers.
 */
struct hadoop_rz_buffer_base {
    // The base address the client can start reading at.
    void *ptr;

    // The maximum valid length of this buffer.
    int32_t length;
};

struct hdfsBuilderConfOpt {
    struct hdfsBuilderConfOpt *next;
    const char *key;
    const char *val;
};

/**
 * A builder used to create Hadoop filesystem instances.
 */
struct hdfsBuilder {
    const char *nn;
    uint16_t port;
    const char *kerbTicketCachePath;
    const char *userName;
    struct hdfsBuilderConfOpt *opts;
    struct hconf *hconf;
    struct hadoop_uri *uri;
};

/**
 * Operations which a libhadoopfs filesystem must implement.
 */
struct hadoop_fs_ops {
    const char * const name;
    int (*file_is_open_for_read)(struct hdfsFile_internal *file);
    int (*file_is_open_for_write)(struct hdfsFile_internal * file);
    int (*get_read_statistics)(struct hdfsFile_internal *file, 
            struct hdfsReadStatistics **stats);
    struct hadoop_err *(*connect)(struct hdfsBuilder *bld,
                                  struct hdfs_internal **fs);
    int (*disconnect)(struct hdfs_internal *fs);
    struct hdfsFile_internal *(*open)(struct hdfs_internal *fs,
            const char* uri, int flags, int bufferSize, short replication,
            int32_t blocksize);
    int (*close)(struct hdfs_internal *fs, struct hdfsFile_internal *file);
    int (*exists)(struct hdfs_internal *fs, const char *uri);
    int (*seek)(struct hdfs_internal *fs, struct hdfsFile_internal *file, 
            int64_t desiredPos);
    int64_t (*tell)(struct hdfs_internal *fs, struct hdfsFile_internal *file);
    int32_t (*read)(struct hdfs_internal *fs, struct hdfsFile_internal *file,
            void* buffer, int32_t length);
    int32_t (*pread)(struct hdfs_internal *fs, struct hdfsFile_internal *file,
            int64_t position, void *buffer, int32_t length);
    int32_t (*write)(struct hdfs_internal *fs, struct hdfsFile_internal *file,
            const void* buffer, int32_t length);
    int (*flush)(struct hdfs_internal *fs, struct hdfsFile_internal *file);
    int (*hflush)(struct hdfs_internal *fs, struct hdfsFile_internal *file);
    int (*hsync)(struct hdfs_internal *fs, struct hdfsFile_internal *file);
    int (*available)(struct hdfs_internal * fs, struct hdfsFile_internal *file);
    int (*copy)(struct hdfs_internal *srcFS, const char *src,
            struct hdfs_internal *dstFS, const char *dst);
    int (*move)(struct hdfs_internal *srcFS, const char *src,
            struct hdfs_internal *dstFS, const char *dst);
    int (*unlink)(struct hdfs_internal *fs, const char *path, int recursive);
    int (*rename)(struct hdfs_internal *fs, const char *old_uri,
            const char* new_uri);
    char* (*get_working_directory)(struct hdfs_internal *fs, char *buffer,
            size_t bufferSize);
    int (*set_working_directory)(struct hdfs_internal *fs, const char* uri);
    int (*mkdir)(struct hdfs_internal *fs, const char* uri);
    int (*set_replication)(struct hdfs_internal *fs, const char* uri,
            int16_t replication);
    hdfsFileInfo *(*list_directory)(struct hdfs_internal *fs,
            const char* uri, int *numEntries);
    hdfsFileInfo *(*get_path_info)(struct hdfs_internal *fs, const char* uri);
    hdfsFileInfo *(*stat)(struct hdfs_internal *fs, const char* uri);
    void (*free_file_info)(hdfsFileInfo *, int numEntries);
    char*** (*get_hosts)(struct hdfs_internal *fs, const char* uri, 
            int64_t start, int64_t length);
    int64_t (*get_default_block_size)(struct hdfs_internal *fs);
    int64_t (*get_default_block_size_at_path)(struct hdfs_internal *fs,
            const char *uri);
    int64_t (*get_capacity)(struct hdfs_internal *fs);
    int64_t (*get_used)(struct hdfs_internal *fs);
    int (*chown)(struct hdfs_internal *fs, const char *uri, const char *owner,
            const char *group);
    int (*chmod)(struct hdfs_internal *fs, const char* uri, short mode);
    int (*utime)(struct hdfs_internal *fs, const char* uri,
            int64_t mtime, int64_t atime);
    struct hadoopRzBuffer* (*read_zero)(struct hdfsFile_internal *file,
                struct hadoopRzOptions *opts, int32_t maxLength);
    void (*rz_buffer_free)(struct hdfsFile_internal *file,
                        struct hadoopRzBuffer *buffer);

    // For testing
    int (*file_uses_direct_read)(struct hdfsFile_internal *fs);
    void (*file_disable_direct_read)(struct hdfsFile_internal *file);
};

#endif

// vim: ts=4:sw=4:et
