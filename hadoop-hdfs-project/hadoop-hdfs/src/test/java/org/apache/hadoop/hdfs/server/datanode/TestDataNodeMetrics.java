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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.Test;

public class TestDataNodeMetrics {

  @Test
  public void testDataNodeMetrics() throws Exception {
    Configuration conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      final long LONG_FILE_LEN = Integer.MAX_VALUE+1L; 
      DFSTestUtil.createFile(fs, new Path("/tmp.txt"),
          LONG_FILE_LEN, (short)1, 1L);
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      assertCounter("BytesWritten", LONG_FILE_LEN, rb);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testSendDataPacket() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      // Create and read a 1 byte file
      Path tmpfile = new Path("/tmp.txt");
      DFSTestUtil.createFile(fs, tmpfile,
          (long)1, (short)1, 1L);
      DFSTestUtil.readFile(fs, tmpfile);
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());

      // Expect 2 packets, 1 for the 1 byte read, 1 for the empty packet
      // signaling the end of the block
      assertCounter("SendDataPacketTransferNanosNumOps", (long)2, rb);
      assertCounter("SendDataPacketBlockedOnNetworkNanosNumOps", (long)2, rb);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testFlushMetric() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();

      Path testFile = new Path("/testFlushNanosMetric.txt");
      DFSTestUtil.createFile(fs, testFile, 1, (short)1, new Random().nextLong());

      List<DataNode> datanodes = cluster.getDataNodes();
      DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder dnMetrics = getMetrics(datanode.getMetrics().name());
      // Expect 2 flushes, 1 for the flush that occurs after writing, 1 that occurs
      // on closing the data and metadata files.
      assertCounter("FlushNanosNumOps", 2L, dnMetrics);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testRoundTripAckMetric() throws Exception {
    final int DATANODE_COUNT = 2;

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_COUNT).build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();

      Path testFile = new Path("/testRoundTripAckMetric.txt");
      DFSTestUtil.createFile(fs, testFile, 1, (short)DATANODE_COUNT,
          new Random().nextLong());

      boolean foundNonzeroPacketAckNumOps = false;
      for (DataNode datanode : cluster.getDataNodes()) {
        MetricsRecordBuilder dnMetrics = getMetrics(datanode.getMetrics().name());
        if (getLongCounter("PacketAckRoundTripTimeNanosNumOps", dnMetrics) > 0) {
          foundNonzeroPacketAckNumOps = true;
        }
      }
      assertTrue(
          "Expected at least one datanode to have reported PacketAckRoundTripTimeNanos metric",
          foundNonzeroPacketAckNumOps);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
