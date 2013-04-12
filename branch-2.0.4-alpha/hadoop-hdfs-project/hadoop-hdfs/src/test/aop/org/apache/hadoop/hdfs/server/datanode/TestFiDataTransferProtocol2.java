/*
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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.DataTransferTestUtil.CountdownDoosAction;
import org.apache.hadoop.fi.DataTransferTestUtil.CountdownOomAction;
import org.apache.hadoop.fi.DataTransferTestUtil.CountdownSleepAction;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.fi.DataTransferTestUtil.SleepAction;
import org.apache.hadoop.fi.DataTransferTestUtil.VerificationAction;
import org.apache.hadoop.fi.FiTestUtil;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/** Test DataTransferProtocol with fault injection. */
public class TestFiDataTransferProtocol2 {
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1L * (1L << 20);
  static final int PACKET_SIZE = 1024;
  static final int MIN_N_PACKET = 3;
  static final int MAX_N_PACKET = 10;

  static final int MAX_SLEEP = 1000;

  static final Configuration conf = new Configuration();
  static {
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
  }

  static final byte[] bytes = new byte[MAX_N_PACKET * PACKET_SIZE];
  static final byte[] toRead = new byte[MAX_N_PACKET * PACKET_SIZE];

  static private FSDataOutputStream createFile(FileSystem fs, Path p
      ) throws IOException {
    return fs.create(p, true, fs.getConf()
        .getInt(IO_FILE_BUFFER_SIZE_KEY, 4096), REPLICATION, BLOCKSIZE);
  }

  {
    ((Log4JLogger) BlockReceiver.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataTransferProtocol.LOG).getLogger().setLevel(Level.ALL);
  }
  /**
   * 1. create files with dfs
   * 2. write MIN_N_PACKET to MAX_N_PACKET packets
   * 3. close file
   * 4. open the same file
   * 5. read the bytes and compare results
   */
  private static void writeSeveralPackets(String methodName) throws IOException {
    final Random r = FiTestUtil.RANDOM.get();
    final int nPackets = FiTestUtil.nextRandomInt(MIN_N_PACKET, MAX_N_PACKET + 1);
    final int lastPacketSize = FiTestUtil.nextRandomInt(1, PACKET_SIZE + 1);
    final int size = (nPackets - 1)*PACKET_SIZE + lastPacketSize;

    FiTestUtil.LOG.info("size=" + size + ", nPackets=" + nPackets
        + ", lastPacketSize=" + lastPacketSize);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf
        ).numDataNodes(REPLICATION + 2).build();
    final FileSystem dfs = cluster.getFileSystem();
    try {
      final Path p = new Path("/" + methodName + "/foo");
      final FSDataOutputStream out = createFile(dfs, p);

      final long seed = r.nextLong();
      final Random ran = new Random(seed);
      ran.nextBytes(bytes);
      out.write(bytes, 0, size);
      out.close();

      final FSDataInputStream in = dfs.open(p);
      int totalRead = 0;
      int nRead = 0;
      while ((nRead = in.read(toRead, totalRead, size - totalRead)) > 0) {
        totalRead += nRead;
      }
      Assert.assertEquals("Cannot read file.", size, totalRead);
      for (int i = 0; i < size; i++) {
        Assert.assertTrue("File content differ.", bytes[i] == toRead[i]);
      }
    }
    finally {
      dfs.close();
      cluster.shutdown();
    }
  }

  private static void initSlowDatanodeTest(DataTransferTest t, SleepAction a)
      throws IOException {
    t.fiCallReceivePacket.set(a);
    t.fiReceiverOpWriteBlock.set(a);
    t.fiStatusRead.set(a);
  }

  private void runTest17_19(String methodName, int dnIndex)
      throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    initSlowDatanodeTest(t, new SleepAction(methodName, 0, 0, MAX_SLEEP));
    initSlowDatanodeTest(t, new SleepAction(methodName, 1, 0, MAX_SLEEP));
    initSlowDatanodeTest(t, new SleepAction(methodName, 2, 0, MAX_SLEEP));
    t.fiCallWritePacketToDisk.set(new CountdownDoosAction(methodName, dnIndex, 3));
    t.fiPipelineErrorAfterInit.set(new VerificationAction(methodName, dnIndex));
    writeSeveralPackets(methodName);
    Assert.assertTrue(t.isSuccess());
  }

  private void runTest29_30(String methodName, int dnIndex) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    initSlowDatanodeTest(t, new SleepAction(methodName, 0, 0, MAX_SLEEP));
    initSlowDatanodeTest(t, new SleepAction(methodName, 1, 0, MAX_SLEEP));
    initSlowDatanodeTest(t, new SleepAction(methodName, 2, 0, MAX_SLEEP));
    t.fiAfterDownstreamStatusRead.set(new CountdownOomAction(methodName, dnIndex, 3));
    t.fiPipelineErrorAfterInit.set(new VerificationAction(methodName, dnIndex));
    writeSeveralPackets(methodName);
    Assert.assertTrue(t.isSuccess());
  }
  
  private void runTest34_35(String methodName, int dnIndex) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiAfterDownstreamStatusRead.set(new CountdownSleepAction(methodName, dnIndex, 0, 3));
    t.fiPipelineErrorAfterInit.set(new VerificationAction(methodName, dnIndex));
    writeSeveralPackets(methodName);
    Assert.assertTrue(t.isSuccess());
  }
  /**
   * Streaming:
   * Randomize datanode speed, write several packets,
   * DN0 throws a DiskOutOfSpaceError when it writes the third packet to disk.
   * Client gets an IOException and determines DN0 bad.
   */
  @Test
  public void pipeline_Fi_17() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest17_19(methodName, 0);
  }
  
  /**
   * Streaming:
   * Randomize datanode speed, write several packets,
   * DN1 throws a DiskOutOfSpaceError when it writes the third packet to disk.
   * Client gets an IOException and determines DN1 bad.
   */
  @Test
  public void pipeline_Fi_18() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest17_19(methodName, 1);
  }
  
  /**
   * Streaming:
   * Randomize datanode speed, write several packets,
   * DN2 throws a DiskOutOfSpaceError when it writes the third packet to disk.
   * Client gets an IOException and determines DN2 bad.
   */
  @Test
  public void pipeline_Fi_19() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest17_19(methodName, 2);
  }
  
  /**
   * Streaming: Client writes several packets with DN0 very slow. Client
   * finishes write successfully.
   */
  @Test
  public void pipeline_Fi_20() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    initSlowDatanodeTest(t, new SleepAction(methodName, 0, MAX_SLEEP));
    writeSeveralPackets(methodName);
  }

  /**
   * Streaming: Client writes several packets with DN1 very slow. Client
   * finishes write successfully.
   */
  @Test
  public void pipeline_Fi_21() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    initSlowDatanodeTest(t, new SleepAction(methodName, 1, MAX_SLEEP));
    writeSeveralPackets(methodName);
  }
  
  /**
   * Streaming: Client writes several packets with DN2 very slow. Client
   * finishes write successfully.
   */
  @Test
  public void pipeline_Fi_22() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    initSlowDatanodeTest(t, new SleepAction(methodName, 2, MAX_SLEEP));
    writeSeveralPackets(methodName);
  }
  
  /**
   * Streaming: Randomize datanode speed, write several packets, DN1 throws a
   * OutOfMemoryException when it receives the ack of the third packet from DN2.
   * Client gets an IOException and determines DN1 bad.
   */
  @Test
  public void pipeline_Fi_29() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest29_30(methodName, 1);
  }

  /**
   * Streaming: Randomize datanode speed, write several packets, DN0 throws a
   * OutOfMemoryException when it receives the ack of the third packet from DN1.
   * Client gets an IOException and determines DN0 bad.
   */
  @Test
  public void pipeline_Fi_30() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest29_30(methodName, 0);
  }
  
  /**
   * Streaming: Write several packets, DN1 never responses when it receives the
   * ack of the third packet from DN2. Client gets an IOException and determines
   * DN1 bad.
   */
  @Test
  public void pipeline_Fi_34() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest34_35(methodName, 1);
  }

  /**
   * Streaming: Write several packets, DN0 never responses when it receives the
   * ack of the third packet from DN1. Client gets an IOException and determines
   * DN0 bad.
   */
  @Test
  public void pipeline_Fi_35() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runTest34_35(methodName, 0);
  }
}
