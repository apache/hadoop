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

#ifndef HADOOP_CORE_NDFS_FILE_H
#define HADOOP_CORE_NDFS_FILE_H

#include "fs/hdfs.h"

#include <stdint.h>

int ndfs_file_is_open_for_read(hdfsFile bfile);
int ndfs_file_is_open_for_write(hdfsFile bfile);
int ndfs_file_get_read_statistics(hdfsFile bfile,
            struct hdfsReadStatistics **out);
hdfsFile ndfs_open_file(hdfsFS bfs, const char* uri, int flags, 
                      int buffer_size, short replication, tSize block_size);
int ndfs_close_file(hdfsFS fs, hdfsFile bfile);
int ndfs_seek(hdfsFS bfs, hdfsFile bfile, tOffset desiredPos);
int64_t ndfs_tell(hdfsFS bfs, hdfsFile bfile);
tSize ndfs_read(hdfsFS bfs, hdfsFile bfile, void *buffer, tSize length);
tSize ndfs_pread(hdfsFS bfs, hdfsFile bfile, tOffset position,
                 void* buffer, tSize length);
tSize ndfs_write(hdfsFS bfs, hdfsFile bfile, const void* buffer, tSize length);
int ndfs_flush(hdfsFS bfs, hdfsFile bfile);
int ndfs_hflush(hdfsFS bfs, hdfsFile bfile);
int ndfs_hsync(hdfsFS bfs, hdfsFile bfile);
int ndfs_available(hdfsFS bfs, hdfsFile bfile);
struct hadoopRzBuffer* ndfs_read_zero(hdfsFile bfile,
            struct hadoopRzOptions *opts, int32_t maxLength);
void ndfs_rz_buffer_free(hdfsFile bfile, struct hadoopRzBuffer *buffer);
int ndfs_file_uses_direct_read(hdfsFile bfile);
void ndfs_file_disable_direct_read(hdfsFile bfile);
char*** ndfs_get_hosts(hdfsFS bfs, const char* path,
                       tOffset start, tOffset length);

#endif

// vim: ts=4:sw=4:tw=79:et
