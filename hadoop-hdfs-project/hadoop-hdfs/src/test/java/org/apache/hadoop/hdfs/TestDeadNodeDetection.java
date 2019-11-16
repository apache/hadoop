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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Tests for dead node detection in DFSClient.
 */
public class TestDeadNodeDetection {

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

  @Test
  public void testDeadNodeDetectionInBackground() throws IOException {
    conf.setBoolean(DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDetectDeadNodeInBackground");

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
    DFSInputStream din = null;
    DFSClient dfsClient = null;
    try {
      try {
        in.read();
      } catch (BlockMissingException e) {
      }

      din = (DFSInputStream) in.getWrappedStream();
      dfsClient = din.getDFSClient();
      assertEquals(3, dfsClient.getDeadNodes(din).size());
      assertEquals(3, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    } finally {
      in.close();
      fs.delete(new Path("/testDetectDeadNodeInBackground"), true);
      // check the dead node again here, the dead node is expected be removed
      assertEquals(0, dfsClient.getDeadNodes(din).size());
      assertEquals(0, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    }
  }

  @Test
  public void testDeadNodeDetectionInMultipleDFSInputStream()
      throws IOException {
    conf.setBoolean(DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY, true);
    // We'll be using a 512 bytes block size just for tests
    // so making sure the checksum bytes too match it.
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDeadNodeMultipleDFSInputStream");

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

    String datanodeUuid = cluster.getDataNodes().get(0).getDatanodeUuid();
    FSDataInputStream in1 = fs.open(filePath);
    DFSInputStream din1 = (DFSInputStream) in1.getWrappedStream();
    DFSClient dfsClient1 = din1.getDFSClient();
    cluster.stopDataNode(0);

    FSDataInputStream in2 = fs.open(filePath);
    DFSInputStream din2 = null;
    DFSClient dfsClient2 = null;
    try {
      try {
        in1.read();
      } catch (BlockMissingException e) {
      }

      din2 = (DFSInputStream) in1.getWrappedStream();
      dfsClient2 = din2.getDFSClient();
      assertEquals(1, dfsClient1.getDeadNodes(din1).size());
      assertEquals(1, dfsClient2.getDeadNodes(din2).size());
      assertEquals(1, dfsClient1.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      assertEquals(1, dfsClient2.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      // check the dn uuid of dead node to see if its expected dead node
      assertEquals(datanodeUuid,
          ((DatanodeInfo) dfsClient1.getClientContext().getDeadNodeDetector()
              .clearAndGetDetectedDeadNodes().toArray()[0]).getDatanodeUuid());
      assertEquals(datanodeUuid,
          ((DatanodeInfo) dfsClient2.getClientContext().getDeadNodeDetector()
              .clearAndGetDetectedDeadNodes().toArray()[0]).getDatanodeUuid());
    } finally {
      in1.close();
      in2.close();
      fs.delete(new Path("/testDeadNodeMultipleDFSInputStream"), true);
      // check the dead node again here, the dead node is expected be removed
      assertEquals(0, dfsClient1.getDeadNodes(din1).size());
      assertEquals(0, dfsClient2.getDeadNodes(din2).size());
      assertEquals(0, dfsClient1.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      assertEquals(0, dfsClient2.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    }
  }
}
