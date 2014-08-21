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

#include "fs/fs.h"
#include "ndfs/file.h"
#include "ndfs/meta.h"

const struct hadoop_fs_ops g_ndfs_ops = {
    .name = "ndfs",
    .file_is_open_for_read = ndfs_file_is_open_for_read,
    .file_is_open_for_write = ndfs_file_is_open_for_write,
    .get_read_statistics = ndfs_file_get_read_statistics,
    .connect = ndfs_connect,
    .disconnect = ndfs_disconnect,
    .open = ndfs_open_file,
    .close = ndfs_close_file,
    .exists = ndfs_file_exists,
    .seek = ndfs_seek,
    .tell = ndfs_tell,
    .read = ndfs_read,
    .pread = ndfs_pread,
    .write = ndfs_write,
    .flush = ndfs_flush,
    .hflush = ndfs_hflush,
    .hsync = ndfs_hsync,
    .available = ndfs_available,
    .copy = NULL,
    .move = NULL,
    .unlink = ndfs_unlink,
    .rename = ndfs_rename,
    .get_working_directory = ndfs_get_working_directory,
    .set_working_directory = ndfs_set_working_directory,
    .mkdir = ndfs_mkdir,
    .set_replication = ndfs_set_replication,
    .list_directory = ndfs_list_directory,
    .get_path_info = ndfs_get_path_info,
    .get_hosts = ndfs_get_hosts,
    .get_default_block_size = ndfs_get_default_block_size,
    .get_default_block_size_at_path = ndfs_get_default_block_size_at_path,
    .get_capacity = ndfs_get_capacity,
    .get_used = ndfs_get_used,
    .chown = ndfs_chown,
    .chmod = ndfs_chmod,
    .utime = ndfs_utime,
    .read_zero = ndfs_read_zero,
    .rz_buffer_free = ndfs_rz_buffer_free,

    // test
    .file_uses_direct_read = ndfs_file_uses_direct_read,
    .file_disable_direct_read = ndfs_file_disable_direct_read,
};

// vim: ts=4:sw=4:tw=79:et
