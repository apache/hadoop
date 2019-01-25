
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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the FileStatus API.
 */
public class TestClientMetrics {
  {
    GenericTestUtils.setLogLevel(ProtobufRpcEngine.LOG,
        org.slf4j.event.Level.DEBUG);
  }

  private static final long SEED = 0xDEADBEEFL;
  private static final int BLOCKSIZE = 8192;
  private static final int FILESIZE = 16384;
  private static final String RPC_DETAILED_METRICS =
      "RpcDetailedActivityForPort";
  /** Dummy port -1 is used by the client. */
  private final int portNum = -1;

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static FileContext fc;
  private static DFSClient dfsClient;
  private static Path file1;

  @BeforeClass
  public static void testSetUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 2);
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
    fc = FileContext.getFileContext(cluster.getURI(0), conf);
    dfsClient = new DFSClient(DFSUtilClient.getNNAddress(conf), conf);
    file1 = new Path("filestatus.dat");
    DFSTestUtil.createFile(fs, file1, FILESIZE, FILESIZE, BLOCKSIZE, (short) 1,
        SEED);
  }

  @AfterClass
  public static void testTearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Test for getting the metrics on the client. */
  @Test
  public void testGetMetrics() throws IOException {
    final Logger log = LoggerFactory.getLogger(ProtobufRpcEngine.class);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(log);

    /** Check that / exists */
    Path path = new Path("/");
    assertTrue("/ should be a directory",
               fs.getFileStatus(path).isDirectory());
    ContractTestUtils.assertNotErasureCoded(fs, path);

    /** Make sure getFileInfo returns null for files which do not exist */
    HdfsFileStatus fileInfo = dfsClient.getFileInfo("/noSuchFile");
    assertEquals("Non-existant file should result in null", null, fileInfo);

    Path path1 = new Path("/name1");
    Path path2 = new Path("/name1/name2");
    assertTrue(fs.mkdirs(path1));
    String metricsName = RPC_DETAILED_METRICS + portNum;
    FSDataOutputStream out = fs.create(path2, false);

    out.close();
    fileInfo = dfsClient.getFileInfo(path1.toString());
    assertEquals(1, fileInfo.getChildrenNum());
    fileInfo = dfsClient.getFileInfo(path2.toString());
    assertEquals(0, fileInfo.getChildrenNum());

    String output = logCapturer.getOutput();
    assertTrue("Unexpected output in: " + output,
        output.contains("MkdirsNumOps = 1"));
    assertTrue("Unexpected output in: " + output,
        output.contains("CreateNumOps = 1"));
    assertTrue("Unexpected output in: " + output,
        output.contains("GetFileInfoNumOps = 5"));
    assertCounter("CreateNumOps", 1L, getMetrics(metricsName));
    assertCounter("MkdirsNumOps", 1L, getMetrics(metricsName));
    assertCounter("GetFileInfoNumOps", 5L, getMetrics(metricsName));

  }
}

