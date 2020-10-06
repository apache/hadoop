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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.test.Whitebox;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestViewDistributedFileSystem extends TestDistributedFileSystem{
  @Override
  HdfsConfiguration getTestConfiguration() {
    HdfsConfiguration conf = super.getTestConfiguration();
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    return conf;
  }

  @Override
  public void testStatistics() throws IOException {
    FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        ViewDistributedFileSystem.class).reset();
    @SuppressWarnings("unchecked")
    ThreadLocal<FileSystem.Statistics.StatisticsData> data =
        (ThreadLocal<FileSystem.Statistics.StatisticsData>) Whitebox
            .getInternalState(FileSystem
                .getStatistics(HdfsConstants.HDFS_URI_SCHEME,
                    ViewDistributedFileSystem.class), "threadData");
    data.set(null);
    super.testStatistics();
  }

  @Test
  public void testOpenWithPathHandle() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fileSys = cluster.getFileSystem();
      Path openTestPath = new Path("/testOpen");
      fileSys.create(openTestPath).close();
      PathHandle pathHandle =
          fileSys.getPathHandle(fileSys.getFileStatus(openTestPath));
      fileSys.open(pathHandle, 1024).close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Override
  public void testEmptyDelegationToken() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      URI defaultUri =
          URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
      ConfigUtil.addLinkFallback(conf, defaultUri.getHost(), defaultUri);
      try (FileSystem fileSys = FileSystem.get(conf)) {
        fileSys.getDelegationToken("");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
