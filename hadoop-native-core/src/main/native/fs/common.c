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

#include "common/hadoop_err.h"
#include "common/string.h"
#include "common/uri.h"
#include "common/user.h"
#include "fs/fs.h"
#include "fs/hdfs.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uriparser/Uri.h>

void release_file_info_entry(hdfsFileInfo *hdfsFileInfo)
{
    free(hdfsFileInfo->mName);
    free(hdfsFileInfo->mOwner);
    free(hdfsFileInfo->mGroup);
    memset(&hdfsFileInfo, 0, sizeof(hdfsFileInfo));
}

int hadoopfs_errno_and_retcode(struct hadoop_err *err)
{
    if (err) {
        fprintf(stderr, "%s\n", hadoop_err_msg(err));
        errno = hadoop_err_code(err);
        hadoop_err_free(err);
        return -1;
    }
    return 0;
}

void *hadoopfs_errno_and_retptr(struct hadoop_err *err, void *ptr)
{
    if (err) {
        fprintf(stderr, "%s\n", hadoop_err_msg(err));
        hadoop_err_free(err);
        return NULL;
    }
    return ptr;
}

// vim: ts=4:sw=4:et
