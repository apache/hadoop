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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECT_ENABLED_KEY;
import static org.junit.Assert.assertTrue;

/**
 * These tests make sure that DFSClient excludes writing data to a DN properly
 * in case of errors.
 */
public class TestDFSClientDetectDeadNodes {

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setUp() {
    cluster = null;
    conf = new HdfsConfiguration();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 60000000)
  public void testDetectDeadNodeInBackground() throws IOException {
    conf.setBoolean(DFS_CLIENT_DEAD_NODE_DETECT_ENABLED_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index = 0; index < bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    // Remove three DNs,
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);

    FSDataInputStream in = fs.open(filePath);
    try {
      try {
        in.read();
      } catch (BlockMissingException e) {
      }

      DFSInputStream din = (DFSInputStream) in.getWrappedStream();
      assertTrue(din.getDfsClient().getDeadNodes(din).size() == 3);
    } finally {
      in.close();
    }
  }

  @Test(timeout = 60000000)
  public void testDeadNodeMultipleDFSInputStream() throws IOException {
    conf.setBoolean(DFS_CLIENT_DEAD_NODE_DETECT_ENABLED_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    ThreadUtil.sleepAtLeastIgnoreInterrupts(10 * 1000L);

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testNodeBecomeDead");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index = 0; index < bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to DN (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    FSDataInputStream in1 = fs.open(filePath);
    DFSInputStream din1 = (DFSInputStream) in1.getWrappedStream();
    cluster.stopDataNode(0);

    FSDataInputStream in2 = fs.open(filePath);
    try {
      try {
        in1.read();
      } catch (BlockMissingException e) {
      }

      DFSInputStream din2 = (DFSInputStream) in1.getWrappedStream();
      assertTrue(din1.getDfsClient().getDeadNodes(din1).size() == 1);
      assertTrue(din2.getDfsClient().getDeadNodes(din2).size() == 1);
    } finally {
      in1.close();
      in2.close();
    }
  }
}
