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

#ifndef LIBHDFS_HDFS_TEST_H
#define LIBHDFS_HDFS_TEST_H

struct hdfsFile_internal;

/**
 * Some functions that are visible only for testing.
 *
 * This header is not meant to be exported or used outside of the libhdfs unit
 * tests.
 */

#ifdef __cplusplus
extern  "C" {
#endif
    /**
     * Determine if a file is using the "direct read" optimization.
     *
     * @param file     The HDFS file
     * @return         1 if the file is using the direct read optimization,
     *                 0 otherwise.
     */
    int hdfsFileUsesDirectRead(struct hdfsFile_internal *file);

    /**
     * Disable the direct read optimization for a file.
     *
     * This is mainly provided for unit testing purposes.
     *
     * @param file     The HDFS file
     */
    void hdfsFileDisableDirectRead(struct hdfsFile_internal *file);

    /**
     * Disable domain socket security checks.
     *
     * @param          0 if domain socket security was disabled;
     *                 -1 if not.
     */
    int hdfsDisableDomainSocketSecurity(void); 

#ifdef __cplusplus
}
#endif

#endif
