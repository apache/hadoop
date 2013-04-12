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
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.fi.DataTransferTestUtil.DoosAction;
import org.apache.hadoop.fi.DataTransferTestUtil.OomAction;
import org.apache.hadoop.fi.DataTransferTestUtil.SleepAction;
import org.apache.hadoop.fi.DataTransferTestUtil.VerificationAction;
import org.apache.hadoop.fi.FiTestUtil;
import org.apache.hadoop.fi.FiTestUtil.Action;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/** Test DataTransferProtocol with fault injection. */
public class TestFiDataTransferProtocol {
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1L * (1L << 20);

  static final Configuration conf = new HdfsConfiguration();
  static {
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
  }

  static private FSDataOutputStream createFile(FileSystem fs, Path p
      ) throws IOException {
    return fs.create(p, true,
        fs.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
            4096), REPLICATION, BLOCKSIZE);
  }

  {
    ((Log4JLogger)DataTransferProtocol.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * 1. create files with dfs
   * 2. write 1 byte
   * 3. close file
   * 4. open the same file
   * 5. read the 1 byte and compare results
   */
  static void write1byte(String methodName) throws IOException {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf
        ).numDataNodes(REPLICATION + 1).build();
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
}
