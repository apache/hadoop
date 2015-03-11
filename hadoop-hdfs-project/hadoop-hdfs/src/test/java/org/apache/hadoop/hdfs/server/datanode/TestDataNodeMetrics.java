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
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class TestDataNodeMetrics {
  private static final Log LOG = LogFactory.getLog(TestDataNodeMetrics.class);

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
      assertTrue("Expected non-zero number of incremental block reports",
          getLongCounter("IncrementalBlockReportsNumOps", rb) > 0);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testSendDataPacketMetrics() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int interval = 1;
    conf.set(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);
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
      // Wait for at least 1 rollover
      Thread.sleep((interval + 1) * 1000);
      // Check that the sendPacket percentiles rolled to non-zero values
      String sec = interval + "s";
      assertQuantileGauges("SendDataPacketBlockedOnNetworkNanos" + sec, rb);
      assertQuantileGauges("SendDataPacketTransferNanos" + sec, rb);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testReceivePacketMetrics() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int interval = 1;
    conf.set(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      Path testFile = new Path("/testFlushNanosMetric.txt");
      FSDataOutputStream fout = fs.create(testFile);
      fout.write(new byte[1]);
      fout.hsync();
      fout.close();
      List<DataNode> datanodes = cluster.getDataNodes();
      DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder dnMetrics = getMetrics(datanode.getMetrics().name());
      // Expect two flushes, 1 for the flush that occurs after writing, 
      // 1 that occurs on closing the data and metadata files.
      assertCounter("FlushNanosNumOps", 2L, dnMetrics);
      // Expect two syncs, one from the hsync, one on close.
      assertCounter("FsyncNanosNumOps", 2L, dnMetrics);
      // Wait for at least 1 rollover
      Thread.sleep((interval + 1) * 1000);
      // Check the receivePacket percentiles that should be non-zero
      String sec = interval + "s";
      assertQuantileGauges("FlushNanos" + sec, dnMetrics);
      assertQuantileGauges("FsyncNanos" + sec, dnMetrics);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /**
   * Tests that round-trip acks in a datanode write pipeline are correctly 
   * measured. 
   */
  @Test
  public void testRoundTripAckMetric() throws Exception {
    final int datanodeCount = 2;
    final int interval = 1;
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        datanodeCount).build();
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      // Open a file and get the head of the pipeline
      Path testFile = new Path("/testRoundTripAckMetric.txt");
      FSDataOutputStream fsout = fs.create(testFile, (short) datanodeCount);
      DFSOutputStream dout = (DFSOutputStream) fsout.getWrappedStream();
      // Slow down the writes to catch the write pipeline
      dout.setChunksPerPacket(5);
      dout.setArtificialSlowdown(3000);
      fsout.write(new byte[10000]);
      DatanodeInfo[] pipeline = null;
      int count = 0;
      while (pipeline == null && count < 5) {
        pipeline = dout.getPipeline();
        System.out.println("Waiting for pipeline to be created.");
        Thread.sleep(1000);
        count++;
      }
      // Get the head node that should be receiving downstream acks
      DatanodeInfo headInfo = pipeline[0];
      DataNode headNode = null;
      for (DataNode datanode : cluster.getDataNodes()) {
        if (datanode.getDatanodeId().equals(headInfo)) {
          headNode = datanode;
          break;
        }
      }
      assertNotNull("Could not find the head of the datanode write pipeline", 
          headNode);
      // Close the file and wait for the metrics to rollover
      Thread.sleep((interval + 1) * 1000);
      // Check the ack was received
      MetricsRecordBuilder dnMetrics = getMetrics(headNode.getMetrics()
          .name());
      assertTrue("Expected non-zero number of acks", 
          getLongCounter("PacketAckRoundTripTimeNanosNumOps", dnMetrics) > 0);
      assertQuantileGauges("PacketAckRoundTripTimeNanos" + interval
          + "s", dnMetrics);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=60000)
  public void testTimeoutMetric() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final Path path = new Path("/test");

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

    final List<FSDataOutputStream> streams = Lists.newArrayList();
    try {
      final FSDataOutputStream out =
          cluster.getFileSystem().create(path, (short) 2);
      final DataNodeFaultInjector injector = Mockito.mock
          (DataNodeFaultInjector.class);
      Mockito.doThrow(new IOException("mock IOException")).
          when(injector).
          writeBlockAfterFlush();
      DataNodeFaultInjector.instance = injector;
      streams.add(out);
      out.writeBytes("old gs data\n");
      out.hflush();

      /* Test the metric. */
      final MetricsRecordBuilder dnMetrics =
          getMetrics(cluster.getDataNodes().get(0).getMetrics().name());
      assertCounter("DatanodeNetworkErrors", 1L, dnMetrics);

      /* Test JMX datanode network counts. */
      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mxbeanName =
          new ObjectName("Hadoop:service=DataNode,name=DataNodeInfo");
      final Object dnc =
          mbs.getAttribute(mxbeanName, "DatanodeNetworkCounts");
      final String allDnc = dnc.toString();
      assertTrue("expected to see loopback address",
          allDnc.indexOf("127.0.0.1") >= 0);
      assertTrue("expected to see networkErrors",
          allDnc.indexOf("networkErrors") >= 0);
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      if (cluster != null) {
        cluster.shutdown();
      }
      DataNodeFaultInjector.instance = new DataNodeFaultInjector();
    }
  }

  /**
   * This function ensures that writing causes TotalWritetime to increment
   * and reading causes totalReadTime to move.
   * @throws Exception
   */
  @Test
  public void testDataNodeTimeSpend() throws Exception {
    Configuration conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      final long LONG_FILE_LEN = 1024 * 1024 * 10;

      long startWriteValue = getLongCounter("TotalWriteTime", rb);
      long startReadValue = getLongCounter("TotalReadTime", rb);

      for (int x =0; x < 50; x++) {
        DFSTestUtil.createFile(fs, new Path("/time.txt."+ x),
                LONG_FILE_LEN, (short) 1, Time.monotonicNow());
      }

      for (int x =0; x < 50; x++) {
        String s = DFSTestUtil.readFile(fs, new Path("/time.txt." + x));
      }

      MetricsRecordBuilder rbNew = getMetrics(datanode.getMetrics().name());
      long endWriteValue = getLongCounter("TotalWriteTime", rbNew);
      long endReadValue = getLongCounter("TotalReadTime", rbNew);

      assertTrue(endReadValue > startReadValue);
      assertTrue(endWriteValue > startWriteValue);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
