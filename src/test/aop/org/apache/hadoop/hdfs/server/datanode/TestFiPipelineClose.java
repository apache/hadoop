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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.FiTestUtil;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.fi.DataTransferTestUtil.SleepAction;
import org.apache.hadoop.fi.FiTestUtil.Action;
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
public class TestFiPipelineClose {
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

   private static void runPipelineCloseTest(String methodName,
      Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiPipelineClose.set(a);
    write1byte(methodName);
  }

  /**
   * Pipeline close:
   * DN0 never responses after received close request from client.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_36() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new SleepAction(methodName, 0, 0));
  }

  /**
   * Pipeline close:
   * DN1 never responses after received close request from client.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_37() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new SleepAction(methodName, 1, 0));
  }

  /**
   * Pipeline close:
   * DN2 never responses after received close request from client.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_38() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new SleepAction(methodName, 2, 0));
  }
}
