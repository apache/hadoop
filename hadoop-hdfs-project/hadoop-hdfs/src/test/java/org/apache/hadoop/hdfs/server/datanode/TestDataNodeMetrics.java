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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertInverseQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.function.Supplier;

import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.util.Lists;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

@NotThreadSafe
public class TestDataNodeMetrics {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataNodeMetrics.class);

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

  @Test
  public void testReceivePacketSlowMetrics() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int interval = 1;
    conf.setInt(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY, interval);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      final DataNodeFaultInjector injector =
          Mockito.mock(DataNodeFaultInjector.class);
      Answer answer = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock)
            throws Throwable {
          // make the op taking longer time
          Thread.sleep(1000);
          return null;
        }
      };
      Mockito.doAnswer(answer).when(injector).
          stopSendingPacketDownstream(Mockito.anyString());
      Mockito.doAnswer(answer).when(injector).delayWriteToOsCache();
      Mockito.doAnswer(answer).when(injector).delayWriteToDisk();
      DataNodeFaultInjector.set(injector);
      Path testFile = new Path("/testFlushNanosMetric.txt");
      FSDataOutputStream fout = fs.create(testFile);
      DFSOutputStream dout = (DFSOutputStream) fout.getWrappedStream();
      fout.write(new byte[1]);
      fout.hsync();
      DatanodeInfo[] pipeline = dout.getPipeline();
      fout.close();
      dout.close();
      DatanodeInfo headDatanodeInfo = pipeline[0];
      List<DataNode> datanodes = cluster.getDataNodes();
      DataNode headNode = datanodes.stream().filter(d -> d.getDatanodeId().equals(headDatanodeInfo))
          .findFirst().orElseGet(null);
      assertNotNull("Could not find the head of the datanode write pipeline",
          headNode);
      MetricsRecordBuilder dnMetrics = getMetrics(headNode.getMetrics().name());
      assertTrue("More than 1 packet received",
          getLongCounter("PacketsReceived", dnMetrics) > 1L);
      assertTrue("More than 1 slow packet to mirror",
          getLongCounter("PacketsSlowWriteToMirror", dnMetrics) > 1L);
      assertCounter("PacketsSlowWriteToDisk", 1L, dnMetrics);
      assertCounter("PacketsSlowWriteToOsCache", 0L, dnMetrics);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  /**
   * HDFS-15242: This function ensures that writing causes some metrics
   * of FSDatasetImpl to increment.
   */
  @Test
  public void testFsDatasetMetrics() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      String bpid = cluster.getNameNode().getNamesystem().getBlockPoolId();
      List<DataNode> datanodes = cluster.getDataNodes();
      DataNode datanode = datanodes.get(0);

      // Verify both of metrics set to 0 when initialize.
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      assertCounter("CreateRbwOpNumOps", 0L, rb);
      assertCounter("CreateTemporaryOpNumOps", 0L, rb);
      assertCounter("FinalizeBlockOpNumOps", 0L, rb);

      // Write into a file to trigger DN metrics.
      DistributedFileSystem fs = cluster.getFileSystem();
      Path testFile = new Path("/testBlockMetrics.txt");
      FSDataOutputStream fout = fs.create(testFile);
      fout.write(new byte[1]);
      fout.hsync();
      fout.close();

      // Create temporary block file to trigger DN metrics.
      final ExtendedBlock block = new ExtendedBlock(bpid, 1, 1, 2001);
      datanode.data.createTemporary(StorageType.DEFAULT, null, block, false);

      // Verify both of metrics value has updated after do some operations.
      rb = getMetrics(datanode.getMetrics().name());
      assertCounter("CreateRbwOpNumOps", 1L, rb);
      assertCounter("CreateTemporaryOpNumOps", 1L, rb);
      assertCounter("FinalizeBlockOpNumOps", 1L, rb);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
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
    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    try {
      final FSDataOutputStream out =
          cluster.getFileSystem().create(path, (short) 2);
      final DataNodeFaultInjector injector = Mockito.mock
          (DataNodeFaultInjector.class);
      Mockito.doThrow(new IOException("mock IOException")).
          when(injector).
          writeBlockAfterFlush();
      DataNodeFaultInjector.set(injector);
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
      IOUtils.cleanupWithLogger(LOG, streams.toArray(new Closeable[0]));
      if (cluster != null) {
        cluster.shutdown();
      }
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  /**
   * This function ensures that writing causes TotalWritetime to increment
   * and reading causes totalReadTime to move.
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testDataNodeTimeSpend() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY, "" + 60);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      final FileSystem fs = cluster.getFileSystem();
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      final DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      final long LONG_FILE_LEN = 1024 * 1024 * 10;

      final long startWriteValue = getLongCounter("TotalWriteTime", rb);
      final long startReadValue = getLongCounter("TotalReadTime", rb);
      assertCounter("ReadTransferRateNumOps", 0L, rb);
      final AtomicInteger x = new AtomicInteger(0);

      // Lets Metric system update latest metrics
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          x.getAndIncrement();
          try {
            DFSTestUtil.createFile(fs, new Path("/time.txt." + x.get()),
                LONG_FILE_LEN, (short) 1, Time.monotonicNow());
            DFSTestUtil.readFile(fs, new Path("/time.txt." + x.get()));
            fs.delete(new Path("/time.txt." + x.get()), true);
          } catch (IOException ioe) {
            LOG.error("Caught IOException while ingesting DN metrics", ioe);
            return false;
          }
          MetricsRecordBuilder rbNew = getMetrics(datanode.getMetrics().name());
          final long endWriteValue = getLongCounter("TotalWriteTime", rbNew);
          final long endReadValue = getLongCounter("TotalReadTime", rbNew);
          assertCounter("ReadTransferRateNumOps", 1L, rbNew);
          assertInverseQuantileGauges("ReadTransferRate60s", rbNew, "Rate");
          return endWriteValue > startWriteValue
              && endReadValue > startReadValue;
        }
      }, 30, 60000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDatanodeBlocksReplicatedMetric() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);

      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      long blocksReplicated = getLongCounter("BlocksReplicated", rb);
      assertEquals("No blocks replicated yet", 0, blocksReplicated);

      Path path = new Path("/counter.txt");
      DFSTestUtil.createFile(fs, path, 1024, (short) 2, Time.monotonicNow());
      cluster.startDataNodes(conf, 1, true, StartupOption.REGULAR, null);
      ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fs, path);
      DFSTestUtil.waitForReplication(cluster, firstBlock, 1, 2, 0);

      MetricsRecordBuilder rbNew = getMetrics(datanode.getMetrics().name());
      blocksReplicated = getLongCounter("BlocksReplicated", rbNew);
      assertEquals("blocks replicated counter incremented", 1, blocksReplicated);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDatanodeActiveXceiversCount() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);

      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      long dataNodeActiveXceiversCount = MetricsAsserts.getIntGauge(
              "DataNodeActiveXceiversCount", rb);
      assertEquals(dataNodeActiveXceiversCount, 0);

      Path path = new Path("/counter.txt");
      DFSTestUtil.createFile(fs, path, 204800000, (short) 3, Time
              .monotonicNow());

      MetricsRecordBuilder rbNew = getMetrics(datanode.getMetrics().name());
      dataNodeActiveXceiversCount = MetricsAsserts.getIntGauge(
              "DataNodeActiveXceiversCount", rbNew);
      assertTrue(dataNodeActiveXceiversCount >= 0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDataNodeMXBeanActiveThreadCount() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();
    Path p = new Path("/testfile");

    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(1, datanodes.size());
      DataNode datanode = datanodes.get(0);

      // create a xceiver thread for write
      FSDataOutputStream os = fs.create(p);
      for (int i = 0; i < 1024; i++) {
        os.write("testdatastr".getBytes());
      }
      os.hsync();
      // create a xceiver thread for read
      InputStream is = fs.open(p);
      is.read(new byte[16], 0, 4);

      int threadCount = datanode.threadGroup.activeCount();
      assertTrue(threadCount > 0);
      Thread[] threads = new Thread[threadCount];
      datanode.threadGroup.enumerate(threads);
      int xceiverCount = 0;
      int responderCount = 0;
      int recoveryWorkerCount = 0;
      for (Thread t : threads) {
        if (t.getName().contains("DataXceiver for client")) {
          xceiverCount++;
        } else if (t.getName().contains("PacketResponder")) {
          responderCount++;
        }
      }
      assertEquals(2, xceiverCount);
      assertEquals(1, responderCount);
      assertEquals(0, recoveryWorkerCount); //not easy to produce
      assertEquals(xceiverCount, datanode.getXceiverCount());
      assertEquals(xceiverCount + responderCount + recoveryWorkerCount,
          datanode.getActiveTransferThreadCount());

      is.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDNShouldNotDeleteBlockONTooManyOpenFiles()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 1);
    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    final DataNodeFaultInjector injector =
        Mockito.mock(DataNodeFaultInjector.class);
    try {
      // wait until the cluster is up
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path p = new Path("/testShouldThrowTMP");
      DFSTestUtil.writeFile(fs, p, new String("testdata"));
      //Before DN throws too many open files
      verifyBlockLocations(fs, p, 1);
      Mockito.doThrow(new FileNotFoundException("Too many open files")).
          when(injector).
          throwTooManyOpenFiles();
      DataNodeFaultInjector.set(injector);
      ExtendedBlock b =
          fs.getClient().getLocatedBlocks(p.toString(), 0).get(0).getBlock();
      try {
        new BlockSender(b, 0, -1, false, true, true,
                cluster.getDataNodes().get(0), null,
                CachingStrategy.newDefaultStrategy());
        fail("Must throw FileNotFoundException");
      } catch (FileNotFoundException fe) {
        assertTrue("Should throw too many open files",
                fe.getMessage().contains("Too many open files"));
      }
      cluster.triggerHeartbeats(); // IBR delete ack
      //After DN throws too many open files
      assertTrue(cluster.getDataNodes().get(0).getFSDataset().isValidBlock(b));
      verifyBlockLocations(fs, p, 1);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  private void verifyBlockLocations(DistributedFileSystem fs, Path p,
      int expected) throws IOException, TimeoutException, InterruptedException {
    final LocatedBlock lb =
        fs.getClient().getLocatedBlocks(p.toString(), 0).get(0);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return lb.getLocations().length == expected;
      }
    }, 1000, 6000);
  }

  @Test
  public void testNNRpcMetricsWithNonHA() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // setting heartbeat interval to 1 hour to prevent bpServiceActor sends
    // heartbeat periodically to NN during running test case, and bpServiceActor
    // only sends heartbeat once after startup
    conf.setTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY, 1, TimeUnit.HOURS);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DataNode dn = cluster.getDataNodes().get(0);
    MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());
    assertCounter("HeartbeatsNumOps", 1L, rb);
  }
  @Test(timeout = 60000)
  public void testSlowMetrics() throws Exception {
    DataNodeFaultInjector dnFaultInjector = new DataNodeFaultInjector() {
      @Override public void delay() {
        try {
          Thread.sleep(310);
        } catch (InterruptedException e) {
        }
      }
    };
    DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector.set(dnFaultInjector);

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      final FileSystem fs = cluster.getFileSystem();
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 3);
      final DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      final long longFileLen = 10;
      final long startFlushOrSyncValue =
          getLongCounter("SlowFlushOrSyncCount", rb);
      final long startAckToUpstreamValue =
          getLongCounter("SlowAckToUpstreamCount", rb);
      final AtomicInteger x = new AtomicInteger(0);

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          x.getAndIncrement();
          try {
            DFSTestUtil
                .createFile(fs, new Path("/time.txt." + x.get()), longFileLen,
                    (short) 3, Time.monotonicNow());
          } catch (IOException ioe) {
            LOG.error("Caught IOException while ingesting DN metrics", ioe);
            return false;
          }
          MetricsRecordBuilder rbNew = getMetrics(datanode.getMetrics().name());
          final long endFlushOrSyncValue = getLongCounter("SlowFlushOrSyncCount", rbNew);
          final long endAckToUpstreamValue = getLongCounter("SlowAckToUpstreamCount", rbNew);
          return endFlushOrSyncValue > startFlushOrSyncValue
              && endAckToUpstreamValue > startAckToUpstreamValue;
        }
      }, 30, 30000);
    } finally {
      DataNodeFaultInjector.set(oldDnInjector);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNNRpcMetricsWithHA() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // setting heartbeat interval to 1 hour to prevent bpServiceActor sends
    // heartbeat periodically to NN during running test case, and bpServiceActor
    // only sends heartbeat once after startup
    conf.setTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY, 1, TimeUnit.HOURS);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(
        MiniDFSNNTopology.simpleHATopology()).build();
    cluster.waitActive();
    DataNode dn = cluster.getDataNodes().get(0);
    cluster.transitionToActive(0);
    MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());
    assertCounter("HeartbeatsForminidfs-ns-nn1NumOps", 1L, rb);
    assertCounter("HeartbeatsForminidfs-ns-nn2NumOps", 1L, rb);
    assertCounter("HeartbeatsNumOps", 2L, rb);
  }

  @Test
  public void testNNRpcMetricsWithFederation() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // setting heartbeat interval to 1 hour to prevent bpServiceActor sends
    // heartbeat periodically to NN during running test case, and bpServiceActor
    // only sends heartbeat once after startup
    conf.setTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY, 1, TimeUnit.HOURS);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(
        MiniDFSNNTopology.simpleFederatedTopology("ns1,ns2")).build();
    cluster.waitActive();
    DataNode dn = cluster.getDataNodes().get(0);
    MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());
    assertCounter("HeartbeatsForns1NumOps", 1L, rb);
    assertCounter("HeartbeatsForns2NumOps", 1L, rb);
    assertCounter("HeartbeatsNumOps", 2L, rb);
  }

  @Test
  public void testNNRpcMetricsWithFederationAndHA() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // setting heartbeat interval to 1 hour to prevent bpServiceActor sends
    // heartbeat periodically to NN during running test case, and bpServiceActor
    // only sends heartbeat once after startup
    conf.setTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY, 1, TimeUnit.HOURS);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(
        MiniDFSNNTopology.simpleHAFederatedTopology(2)).build();
    cluster.waitActive();
    DataNode dn = cluster.getDataNodes().get(0);
    MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());

    assertCounter("HeartbeatsForns0-nn0NumOps", 1L, rb);
    assertCounter("HeartbeatsForns0-nn1NumOps", 1L, rb);
    assertCounter("HeartbeatsForns1-nn0NumOps", 1L, rb);
    assertCounter("HeartbeatsForns1-nn1NumOps", 1L, rb);
    assertCounter("HeartbeatsNumOps", 4L, rb);
  }

  @Test
  public void testNodeLocalMetrics() throws Exception {
    Assume.assumeTrue(null == DomainSocket.getLoadingFailureReason());
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
            "testNodeLocalMetrics._PORT.sock").getAbsolutePath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path testFile = new Path("/testNodeLocalMetrics.txt");
      DFSTestUtil.createFile(fs, testFile, 10L, (short)1, 1L);
      DFSTestUtil.readFile(fs, testFile);
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(1, datanodes.size());

      DataNode datanode = datanodes.get(0);
      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());

      // Write related metrics
      assertCounter("WritesFromLocalClient", 1L, rb);
      // Read related metrics
      assertCounter("ReadsFromLocalClient", 1L, rb);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
