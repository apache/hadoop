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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;

public class TestDFSStripedOutputStreamUpdatePipeline {

  @Test
  public void testDFSStripedOutputStreamUpdatePipeline() throws Exception {

    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, 1 * 1024 * 1024);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(5).build()) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      // Create a file with EC policy
      Path dir = new Path("/test");
      dfs.mkdirs(dir);
      dfs.enableErasureCodingPolicy("RS-3-2-1024k");
      dfs.setErasureCodingPolicy(dir, "RS-3-2-1024k");
      Path filePath = new Path("/test/file");
      FSDataOutputStream out = dfs.create(filePath);
      try {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
          out.write(i);
          if (i == 1024 * 1024 * 5) {
            cluster.stopDataNode(0);
            cluster.stopDataNode(1);
            cluster.stopDataNode(2);
          }
        }
      } catch(Exception e) {
        dfs.delete(filePath, true);
      } finally {
        // The close should be success, shouldn't get stuck.
        IOUtils.closeStream(out);
      }
    }
  }

  /**
   * Test writing ec file hang when applying the second block group occurs
   * an addBlock exception (e.g. quota exception).
   */
  @Test(timeout = 90000)
  public void testECWriteHangWhenAddBlockWithException() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1 * 1024 * 1024);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build()) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      // Create a file with EC policy
      Path dir = new Path("/test");
      dfs.mkdirs(dir);
      dfs.enableErasureCodingPolicy("XOR-2-1-1024k");
      dfs.setErasureCodingPolicy(dir, "XOR-2-1-1024k");
      Path filePath = new Path("/test/file");
      FSDataOutputStream out = dfs.create(filePath);
      for (int i = 0; i < 1024 * 1024 * 2; i++) {
        out.write(i);
      }
      dfs.setQuota(dir, 5, 0);
      try {
        for (int i = 0; i < 1024 * 1024 * 2; i++) {
          out.write(i);
        }
      } catch (Exception e) {
        dfs.delete(filePath, true);
      } finally {
        // The close should be success, shouldn't get stuck.
        IOUtils.closeStream(out);
      }
    }
  }
}
