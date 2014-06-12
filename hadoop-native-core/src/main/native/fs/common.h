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

#ifndef HADOOP_NATIVE_CORE_FS_COMMON_H
#define HADOOP_NATIVE_CORE_FS_COMMON_H

struct file_info;
struct hadoop_err;
struct hdfsBuilder;

/**
 * Release the memory used inside an hdfsFileInfo structure.
 * Does not free the structure itself.
 *
 * @param hdfsFileInfo          The hdfsFileInfo structure.
 */
void release_file_info_entry(struct file_info *hdfsFileInfo);

/**
 * Sets errno and logs a message appropriately on encountering a Hadoop error.
 *
 * @param err                   The hadoop error, or NULL if there is none.
 *
 * @return                      -1 on error; 0 otherwise.  Errno will be set on
 *                              error.
 */
int hadoopfs_errno_and_retcode(struct hadoop_err *err);

/**
 * Sets errno and logs a message appropriately on encountering a Hadoop error.
 *
 * @param err                   The hadoop error, or NULL if there is none.
 * @param ptr                   The pointer to return if err is NULL.
 *
 * @return                      NULL on error; ptr otherwise.  Errno will be set
 *                              on error.
 */
void *hadoopfs_errno_and_retptr(struct hadoop_err *err, void *ptr);

#endif

// vim: ts=4:sw=4:et
