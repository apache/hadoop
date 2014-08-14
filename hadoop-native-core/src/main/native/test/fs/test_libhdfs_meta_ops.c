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

#include "fs/hdfs.h"
#include "test/native_mini_dfs.h"
#include "test/test.h"

#include <errno.h>
#include <inttypes.h>
#include <semaphore.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/** Test performing metadata operations via libhdfs. */

static void print_file_info(const hdfsFileInfo *info)
{
    printf("file_info(mName=%s): mOwner=%s, mGroup=%s, "
           "mPermissions=%04o\n", info->mName, info->mOwner,
           info->mGroup, info->mPermissions);
}

int main(void)
{
    hdfsFileInfo *infos, *info;
    struct hdfsBuilder *hdfs_bld = NULL;
    hdfsFS fs = NULL;
    struct NativeMiniDfsCluster* dfs_cluster = NULL;
    struct NativeMiniDfsConf dfs_conf = {
        .doFormat = 1,
    };
    const char *nn_uri;
    int i, num_entries;

    nn_uri = getenv("NAMENODE_URI");
    if (!nn_uri) {
        dfs_cluster = nmdCreate(&dfs_conf);
        EXPECT_NONNULL(dfs_cluster);
        EXPECT_INT_ZERO(nmdWaitClusterUp(dfs_cluster));
    }
    hdfs_bld = hdfsNewBuilder();
    if (nn_uri) {
        hdfsBuilderSetNameNode(hdfs_bld, nn_uri);
        EXPECT_INT_ZERO(hdfsBuilderConfSetStr(hdfs_bld,
                "default.native.handler", "ndfs"));
    } else {
        hdfsBuilderSetNameNode(hdfs_bld, "localhost");
        hdfsBuilderSetNameNodePort(hdfs_bld, nmdGetNameNodePort(dfs_cluster));
    }
    EXPECT_NONNULL(hdfs_bld);
    fs = hdfsBuilderConnect(hdfs_bld);
    EXPECT_NONNULL(fs);
    hdfsDelete(fs, "/abc", 1);
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/1"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/2"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/3"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/alpha"));
    EXPECT_INT_ZERO(hdfsCopy(fs, "/abc", fs, "/abc-2"));
    EXPECT_INT_ZERO(hdfsDelete(fs, "/abc", 1));
    hdfsDelete(fs, "/abc", 1);
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/1"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/2"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/3"));
    EXPECT_INT_ZERO(hdfsCreateDirectory(fs, "/abc/alpha"));
    infos = hdfsListDirectory(fs, "/abc", &num_entries);
    EXPECT_NONNULL(infos);
    EXPECT_INT_EQ(4, num_entries);
    for (i = 0; i < num_entries; i++) {
        print_file_info(&infos[i]);
    }
    hdfsFreeFileInfo(infos, num_entries);
    info = hdfsGetPathInfo(fs, "/abc");
    EXPECT_NONNULL(info);
    EXPECT_INT_ZERO(info->mReplication);
    EXPECT_INT_ZERO(info->mBlockSize);
    EXPECT_INT_EQ(kObjectKindDirectory, info->mKind);
    print_file_info(info);
    hdfsFreeFileInfo(info, 1);
    EXPECT_INT_ZERO(hdfsDisconnect(fs));
    if (dfs_cluster) {
        EXPECT_INT_ZERO(nmdShutdown(dfs_cluster));
        nmdFree(dfs_cluster);
    }
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
