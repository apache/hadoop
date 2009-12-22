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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.FiTestUtil;
import org.apache.hadoop.fi.DataTransferTestUtil.DataNodeAction;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.fi.DataTransferTestUtil.DatanodeMarkingAction;
import org.apache.hadoop.fi.DataTransferTestUtil.DoosAction;
import org.apache.hadoop.fi.DataTransferTestUtil.IoeAction;
import org.apache.hadoop.fi.DataTransferTestUtil.OomAction;
import org.apache.hadoop.fi.DataTransferTestUtil.SleepAction;
import org.apache.hadoop.fi.DataTransferTestUtil.VerificationAction;
import org.apache.hadoop.fi.FiTestUtil.Action;
import org.apache.hadoop.fi.FiTestUtil.ConstraintSatisfactionAction;
import org.apache.hadoop.fi.FiTestUtil.MarkerConstraint;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.Assert;
import org.junit.Test;

/** Test DataTransferProtocol with fault injection. */
public class TestFiDataTransferProtocol {
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1L * (1L << 20);

  static final Configuration conf = new HdfsConfiguration();
  static {
    conf.setInt("dfs.datanode.handler.count", 1);
    conf.setInt("dfs.replication", REPLICATION);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
  }

  static private FSDataOutputStream createFile(FileSystem fs, Path p
      ) throws IOException {
    return fs.create(p, true, fs.getConf().getInt("io.file.buffer.size", 4096),
        REPLICATION, BLOCKSIZE);
  }

  /**
   * 1. create files with dfs
   * 2. write 1 byte
   * 3. close file
   * 4. open the same file
   * 5. read the 1 byte and compare results
   */
  private static void write1byte(String methodName) throws IOException {
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, REPLICATION, true,
        null);
    final FileSystem dfs = cluster.getFileSystem();
    try {
      final Path p = new Path("/" + methodName + "/foo");
      final FSDataOutputStream out = createFile(dfs, p);
      out.write(1);
      out.close();
      
      final FSDataInputStream in = dfs.open(p);
      final int b = in.read();
      in.close();
      Assert.assertEquals(1, b);
    }
    finally {
      dfs.close();
      cluster.shutdown();
    }
  }

  private static void runSlowDatanodeTest(String methodName, SleepAction a
                  ) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest)DataTransferTestUtil.initTest();
    t.fiCallReceivePacket.set(a);
    t.fiReceiverOpWriteBlock.set(a);
    t.fiStatusRead.set(a);
    write1byte(methodName);
  }
  
  private static void runReceiverOpWriteBlockTest(String methodName,
      int errorIndex, Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiReceiverOpWriteBlock.set(a);
    t.fiPipelineInitErrorNonAppend.set(new VerificationAction(methodName,
        errorIndex));
    write1byte(methodName);
    Assert.assertTrue(t.isSuccess());
  }
  
  private static void runStatusReadTest(String methodName, int errorIndex,
      Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiStatusRead.set(a);
    t.fiPipelineInitErrorNonAppend.set(new VerificationAction(methodName,
        errorIndex));
    write1byte(methodName);
    Assert.assertTrue(t.isSuccess());
  }

  private static void runCallWritePacketToDisk(String methodName,
      int errorIndex, Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest)DataTransferTestUtil.initTest();
    t.fiCallWritePacketToDisk.set(a);
    t.fiPipelineErrorAfterInit.set(new VerificationAction(methodName, errorIndex));
    write1byte(methodName);
    Assert.assertTrue(t.isSuccess());
  }

  /**
   * Pipeline setup:
   * DN0 never responses after received setup request from client.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_01() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runReceiverOpWriteBlockTest(methodName, 0, new SleepAction(methodName, 0, 0));
  }

  /**
   * Pipeline setup:
   * DN1 never responses after received setup request from client.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_02() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runReceiverOpWriteBlockTest(methodName, 1, new SleepAction(methodName, 1, 0));
  }

  /**
   * Pipeline setup:
   * DN2 never responses after received setup request from client.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_03() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runReceiverOpWriteBlockTest(methodName, 2, new SleepAction(methodName, 2, 0));
  }

  /**
   * Pipeline setup, DN1 never responses after received setup ack from DN2.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_04() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runStatusReadTest(methodName, 1, new SleepAction(methodName, 1, 0));
  }

  /**
   * Pipeline setup, DN0 never responses after received setup ack from DN1.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_05() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runStatusReadTest(methodName, 0, new SleepAction(methodName, 0, 0));
  }

  /**
   * Pipeline setup with DN0 very slow but it won't lead to timeout.
   * Client finishes setup successfully.
   */
  @Test
  public void pipeline_Fi_06() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runSlowDatanodeTest(methodName, new SleepAction(methodName, 0, 3000));
  }

  /**
   * Pipeline setup with DN1 very slow but it won't lead to timeout.
   * Client finishes setup successfully.
   */
  @Test
  public void pipeline_Fi_07() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runSlowDatanodeTest(methodName, new SleepAction(methodName, 1, 3000));
  }

  /**
   * Pipeline setup with DN2 very slow but it won't lead to timeout.
   * Client finishes setup successfully.
   */
  @Test
  public void pipeline_Fi_08() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runSlowDatanodeTest(methodName, new SleepAction(methodName, 2, 3000));
  }

  /**
   * Pipeline setup, DN0 throws an OutOfMemoryException right after it
   * received a setup request from client.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_09() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runReceiverOpWriteBlockTest(methodName, 0, new OomAction(methodName, 0));
  }

  /**
   * Pipeline setup, DN1 throws an OutOfMemoryException right after it
   * received a setup request from DN0.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_10() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runReceiverOpWriteBlockTest(methodName, 1, new OomAction(methodName, 1));
  }

  /**
   * Pipeline setup, DN2 throws an OutOfMemoryException right after it
   * received a setup request from DN1.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_11() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runReceiverOpWriteBlockTest(methodName, 2, new OomAction(methodName, 2));
  }

  /**
   * Pipeline setup, DN1 throws an OutOfMemoryException right after it
   * received a setup ack from DN2.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_12() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runStatusReadTest(methodName, 1, new OomAction(methodName, 1));
  }

  /**
   * Pipeline setup, DN0 throws an OutOfMemoryException right after it
   * received a setup ack from DN1.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_13() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runStatusReadTest(methodName, 0, new OomAction(methodName, 0));
  }

  /**
   * Streaming: Write a packet, DN0 throws a DiskOutOfSpaceError
   * when it writes the data to disk.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_14() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runCallWritePacketToDisk(methodName, 0, new DoosAction(methodName, 0));
  }

  /**
   * Streaming: Write a packet, DN1 throws a DiskOutOfSpaceError
   * when it writes the data to disk.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_15() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runCallWritePacketToDisk(methodName, 1, new DoosAction(methodName, 1));
  }
  
  /**
   * Streaming: Write a packet, DN2 throws a DiskOutOfSpaceError
   * when it writes the data to disk.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_16() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runCallWritePacketToDisk(methodName, 2, new DoosAction(methodName, 2));
  }

  private static void runPipelineCloseTest(String methodName,
      Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiPipelineClose.set(a);
    write1byte(methodName);
  }

  private static void run41_43(String name, int i) throws IOException {
    runPipelineCloseTest(name, new SleepAction(name, i, 3000));
  }

  private static void runPipelineCloseAck(String name, int i, DataNodeAction a
      ) throws IOException {
    FiTestUtil.LOG.info("Running " + name + " ...");
    final DataTransferTest t = (DataTransferTest)DataTransferTestUtil.initTest();
    final MarkerConstraint marker = new MarkerConstraint(name);
    t.fiPipelineClose.set(new DatanodeMarkingAction(name, i, marker));
    t.fiPipelineAck.set(new ConstraintSatisfactionAction<DatanodeID, IOException>(a, marker));
    write1byte(name);
  }

  private static void run39_40(String name, int i) throws IOException {
    runPipelineCloseAck(name, i, new SleepAction(name, i, 0));
  }

  /**
   * Pipeline close:
   * DN1 never responses after received close ack DN2.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_39() throws IOException {
    run39_40(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close:
   * DN0 never responses after received close ack DN1.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_40() throws IOException {
    run39_40(FiTestUtil.getMethodName(), 0);
  }
  
  /**
   * Pipeline close with DN0 very slow but it won't lead to timeout.
   * Client finishes close successfully.
   */
  @Test
  public void pipeline_Fi_41() throws IOException {
    run41_43(FiTestUtil.getMethodName(), 0);
  }

  /**
   * Pipeline close with DN1 very slow but it won't lead to timeout.
   * Client finishes close successfully.
   */
  @Test
  public void pipeline_Fi_42() throws IOException {
    run41_43(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close with DN2 very slow but it won't lead to timeout.
   * Client finishes close successfully.
   */
  @Test
  public void pipeline_Fi_43() throws IOException {
    run41_43(FiTestUtil.getMethodName(), 2);
  }

  /**
   * Pipeline close:
   * DN0 throws an OutOfMemoryException
   * right after it received a close request from client.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_44() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new OomAction(methodName, 0));
  }

  /**
   * Pipeline close:
   * DN1 throws an OutOfMemoryException
   * right after it received a close request from client.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_45() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new OomAction(methodName, 1));
  }

  /**
   * Pipeline close:
   * DN2 throws an OutOfMemoryException
   * right after it received a close request from client.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_46() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new OomAction(methodName, 2));
  }

  private static void run47_48(String name, int i) throws IOException {
    runPipelineCloseAck(name, i, new OomAction(name, i));
  }

  /**
   * Pipeline close:
   * DN1 throws an OutOfMemoryException right after
   * it received a close ack from DN2.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_47() throws IOException {
    run47_48(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close:
   * DN0 throws an OutOfMemoryException right after
   * it received a close ack from DN1.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_48() throws IOException {
    run47_48(FiTestUtil.getMethodName(), 0);
  }

  private static void runBlockFileCloseTest(String methodName,
      Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiBlockFileClose.set(a);
    write1byte(methodName);
  }

  private static void run49_51(String name, int i) throws IOException {
    runBlockFileCloseTest(name, new IoeAction(name, i, "DISK ERROR"));
  }

  /**
   * Pipeline close:
   * DN0 throws a disk error exception when it is closing the block file.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_49() throws IOException {
    run49_51(FiTestUtil.getMethodName(), 0);
  }


  /**
   * Pipeline close:
   * DN1 throws a disk error exception when it is closing the block file.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_50() throws IOException {
    run49_51(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close:
   * DN2 throws a disk error exception when it is closing the block file.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_51() throws IOException {
    run49_51(FiTestUtil.getMethodName(), 2);
  }
}