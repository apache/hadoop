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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class TestSaslDataTransfer extends SaslDataTransferTestCase {

  private static final int BLOCK_SIZE = 4096;
  private static final int BUFFER_SIZE= 1024;
  private static final int NUM_BLOCKS = 3;
  private static final Path PATH  = new Path("/file1");
  private static final short REPLICATION = 3;

  private MiniDFSCluster cluster;
  private FileSystem fs;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(60000);

  @After
  public void shutdown() {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAuthentication() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    doTest(clientConf);
  }

  @Test
  public void testIntegrity() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "integrity");
    doTest(clientConf);
  }

  @Test
  public void testPrivacy() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "privacy");
    doTest(clientConf);
  }

  @Test
  public void testClientAndServerDoNotHaveCommonQop() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig("privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    exception.expect(IOException.class);
    exception.expectMessage("could only be replicated to 0 nodes");
    doTest(clientConf);
  }

  @Test
  public void testServerSaslNoClientSasl() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    // Set short retry timeouts so this test runs faster
    clusterConf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "");

    LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
        LogFactory.getLog(DataNode.class));
    try {
      doTest(clientConf);
      Assert.fail("Should fail if SASL data transfer protection is not " +
          "configured or not supported in client");
    } catch (IOException e) {
      GenericTestUtils.assertMatches(e.getMessage(), 
          "could only be replicated to 0 nodes");
    } finally {
      logs.stopCapturing();
    }

    GenericTestUtils.assertMatches(logs.getOutput(),
        "Failed to read expected SASL data transfer protection " +
        "handshake from client at");
  }

  @Test
  public void testDataNodeAbortsIfNoSasl() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig("");
    exception.expect(RuntimeException.class);
    exception.expectMessage("Cannot start secure DataNode");
    startCluster(clusterConf);
  }

  @Test
  public void testDataNodeAbortsIfNotHttpsOnly() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig("authentication");
    clusterConf.set(DFS_HTTP_POLICY_KEY,
      HttpConfig.Policy.HTTP_AND_HTTPS.name());
    exception.expect(RuntimeException.class);
    exception.expectMessage("Cannot start secure DataNode");
    startCluster(clusterConf);
  }

  @Test
  public void testNoSaslAndSecurePortsIgnored() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig("");
    clusterConf.setBoolean(IGNORE_SECURE_PORTS_FOR_TESTING_KEY, true);
    startCluster(clusterConf);
    doTest(clusterConf);
  }

  /**
   * Tests DataTransferProtocol with the given client configuration.
   *
   * @param conf client configuration
   * @throws IOException if there is an I/O error
   */
  private void doTest(HdfsConfiguration conf) throws IOException {
    fs = FileSystem.get(cluster.getURI(), conf);
    FileSystemTestHelper.createFile(fs, PATH, NUM_BLOCKS, BLOCK_SIZE);
    assertArrayEquals(FileSystemTestHelper.getFileData(NUM_BLOCKS, BLOCK_SIZE),
      DFSTestUtil.readFile(fs, PATH).getBytes("UTF-8"));
    BlockLocation[] blockLocations = fs.getFileBlockLocations(PATH, 0,
      Long.MAX_VALUE);
    assertNotNull(blockLocations);
    assertEquals(NUM_BLOCKS, blockLocations.length);
    for (BlockLocation blockLocation: blockLocations) {
      assertNotNull(blockLocation.getHosts());
      assertEquals(3, blockLocation.getHosts().length);
    }
  }

  /**
   * Starts a cluster with the given configuration.
   *
   * @param conf cluster configuration
   * @throws IOException if there is an I/O error
   */
  private void startCluster(HdfsConfiguration conf) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }
}
