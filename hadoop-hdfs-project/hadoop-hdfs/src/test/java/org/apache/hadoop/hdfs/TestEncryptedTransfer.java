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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TestEncryptedTransfer {
  {
    LogManager.getLogger(SaslDataTransferServer.class).setLevel(Level.DEBUG);
    LogManager.getLogger(DataTransferSaslUtil.class).setLevel(Level.DEBUG);
  }
  
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{null});
    params.add(new Object[]{"org.apache.hadoop.hdfs.TestEncryptedTransfer$TestTrustedChannelResolver"});
    return params;
  }
  
  private static final Log LOG = LogFactory.getLog(TestEncryptedTransfer.class);
  
  private static final String PLAIN_TEXT = "this is very secret plain text";
  private static final Path TEST_PATH = new Path("/non-encrypted-file");
  
  private void setEncryptionConfigKeys(Configuration conf) {
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    if (resolverClazz != null){
      conf.set(DFSConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, resolverClazz);
    }
  }
  
  // Unset DFS_ENCRYPT_DATA_TRANSFER_KEY and DFS_DATA_ENCRYPTION_ALGORITHM_KEY
  // on the client side to ensure that clients will detect this setting
  // automatically from the NN.
  private static FileSystem getFileSystem(Configuration conf) throws IOException {
    Configuration localConf = new Configuration(conf);
    localConf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, false);
    localConf.unset(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    return FileSystem.get(localConf);
  }
  
  String resolverClazz;
  public TestEncryptedTransfer(String resolverClazz){
    this.resolverClazz = resolverClazz;
  }

  @Test
  public void testEncryptedRead() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
      fs.close();
      cluster.shutdown();
      
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();
      
      fs = getFileSystem(conf);
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(SaslDataTransferServer.class));
      LogCapturer logs1 = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(DataTransferSaslUtil.class));
      try {
        assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
        assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      } finally {
        logs.stopCapturing();
        logs1.stopCapturing();
      }
      
      fs.close();
      
      if (resolverClazz == null) {
        // Test client and server negotiate cipher option
        GenericTestUtils.assertDoesNotMatch(logs.getOutput(),
            "Server using cipher suite");
        // Check the IOStreamPair
        GenericTestUtils.assertDoesNotMatch(logs1.getOutput(),
            "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testEncryptedReadWithRC4() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
      fs.close();
      cluster.shutdown();
      
      setEncryptionConfigKeys(conf);
      // It'll use 3DES by default, but we set it to rc4 here.
      conf.set(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY, "rc4");
      
      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();
      
      fs = getFileSystem(conf);
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(SaslDataTransferServer.class));
      LogCapturer logs1 = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(DataTransferSaslUtil.class));
      try {
        assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
        assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      } finally {
        logs.stopCapturing();
        logs1.stopCapturing();
      }

      fs.close();

      if (resolverClazz == null) {
        // Test client and server negotiate cipher option
        GenericTestUtils.assertDoesNotMatch(logs.getOutput(),
            "Server using cipher suite");
        // Check the IOStreamPair
        GenericTestUtils.assertDoesNotMatch(logs1.getOutput(),
            "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testEncryptedReadWithAES() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.set(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY,
          "AES/CTR/NoPadding");
      cluster = new MiniDFSCluster.Builder(conf).build();

      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
      fs.close();
      cluster.shutdown();

      setEncryptionConfigKeys(conf);

      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();

      fs = getFileSystem(conf);
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(SaslDataTransferServer.class));
      LogCapturer logs1 = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(DataTransferSaslUtil.class));
      try {
        assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
        assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      } finally {
        logs.stopCapturing();
        logs1.stopCapturing();
      }

      fs.close();

      if (resolverClazz == null) {
        // Test client and server negotiate cipher option
        GenericTestUtils.assertMatches(logs.getOutput(),
            "Server using cipher suite");
        // Check the IOStreamPair
        GenericTestUtils.assertMatches(logs1.getOutput(),
            "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEncryptedReadAfterNameNodeRestart() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
      fs.close();
      cluster.shutdown();
      
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();
      
      fs = getFileSystem(conf);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      fs.close();
      
      cluster.restartNameNode();
      fs = getFileSystem(conf);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testClientThatDoesNotSupportEncryption() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      // Set short retry timeouts so this test runs faster
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      fs.close();
      cluster.shutdown();
      
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();
      
      
      fs = getFileSystem(conf);
      DFSClient client = DFSClientAdapter.getDFSClient((DistributedFileSystem) fs);
      DFSClient spyClient = Mockito.spy(client);
      Mockito.doReturn(false).when(spyClient).shouldEncryptData();
      DFSClientAdapter.setDFSClient((DistributedFileSystem) fs, spyClient);
      
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(DataNode.class));
      try {
        assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
        if (resolverClazz != null && !resolverClazz.endsWith("TestTrustedChannelResolver")){
          fail("Should not have been able to read without encryption enabled.");
        }
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains("Could not obtain block:",
            ioe);
      } finally {
        logs.stopCapturing();
      }
      fs.close();
      
      if (resolverClazz == null) {
        GenericTestUtils.assertMatches(logs.getOutput(),
        "Failed to read expected encryption handshake from client at");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testLongLivedReadClientAfterRestart() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
      fs.close();
      cluster.shutdown();
      
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();
      
      fs = getFileSystem(conf);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      
      // Restart the NN and DN, after which the client's encryption key will no
      // longer be valid.
      cluster.restartNameNode();
      assertTrue(cluster.restartDataNode(0));
      
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      
      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testLongLivedWriteClientAfterRestart() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      setEncryptionConfigKeys(conf);
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      
      // Restart the NN and DN, after which the client's encryption key will no
      // longer be valid.
      cluster.restartNameNode();
      assertTrue(cluster.restartDataNodes());
      cluster.waitActive();
      
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT + PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      
      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testLongLivedClient() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
      fs.close();
      cluster.shutdown();
      
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .manageDataDfsDirs(false)
          .manageNameDfsDirs(false)
          .format(false)
          .startupOption(StartupOption.REGULAR)
          .build();
      
      BlockTokenSecretManager btsm = cluster.getNamesystem().getBlockManager()
          .getBlockTokenSecretManager();
      btsm.setKeyUpdateIntervalForTesting(2 * 1000);
      btsm.setTokenLifetime(2 * 1000);
      btsm.clearAllKeysForTesting();
      
      fs = getFileSystem(conf);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      
      // Sleep for 15 seconds, after which the encryption key will no longer be
      // valid. It needs to be a few multiples of the block token lifetime,
      // since several block tokens are valid at any given time (the current
      // and the last two, by default.)
      LOG.info("Sleeping so that encryption keys expire...");
      Thread.sleep(15 * 1000);
      LOG.info("Done sleeping.");
      
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
      
      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testEncryptedWriteWithOneDn() throws IOException {
    testEncryptedWrite(1);
  }
  
  @Test
  public void testEncryptedWriteWithTwoDns() throws IOException {
    testEncryptedWrite(2);
  }
  
  @Test
  public void testEncryptedWriteWithMultipleDns() throws IOException {
    testEncryptedWrite(10);
  }
  
  private void testEncryptedWrite(int numDns) throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDns).build();
      
      FileSystem fs = getFileSystem(conf);
      
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(SaslDataTransferServer.class));
      LogCapturer logs1 = GenericTestUtils.LogCapturer.captureLogs(
          LogFactory.getLog(DataTransferSaslUtil.class));
      try {
        writeTestDataToFile(fs);
      } finally {
        logs.stopCapturing();
        logs1.stopCapturing();
      }
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      fs.close();
      
      if (resolverClazz == null) {
        // Test client and server negotiate cipher option
        GenericTestUtils.assertDoesNotMatch(logs.getOutput(),
            "Server using cipher suite");
        // Check the IOStreamPair
        GenericTestUtils.assertDoesNotMatch(logs1.getOutput(),
            "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testEncryptedAppend() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      setEncryptionConfigKeys(conf);
      
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      
      FileSystem fs = getFileSystem(conf);
      
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT + PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      
      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testEncryptedAppendRequiringBlockTransfer() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      setEncryptionConfigKeys(conf);
      
      // start up 4 DNs
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      
      FileSystem fs = getFileSystem(conf);
      
      // Create a file with replication 3, so its block is on 3 / 4 DNs.
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      
      // Shut down one of the DNs holding a block replica.
      FSDataInputStream in = fs.open(TEST_PATH);
      List<LocatedBlock> locatedBlocks = DFSTestUtil.getAllBlocks(in);
      in.close();
      assertEquals(1, locatedBlocks.size());
      assertEquals(3, locatedBlocks.get(0).getLocations().length);
      DataNode dn = cluster.getDataNode(locatedBlocks.get(0).getLocations()[0].getIpcPort());
      dn.shutdown();
      
      // Reopen the file for append, which will need to add another DN to the
      // pipeline and in doing so trigger a block transfer.
      writeTestDataToFile(fs);
      assertEquals(PLAIN_TEXT + PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      
      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private static void writeTestDataToFile(FileSystem fs) throws IOException {
    OutputStream out = null;
    if (!fs.exists(TEST_PATH)) {
      out = fs.create(TEST_PATH);
    } else {
      out = fs.append(TEST_PATH);
    }
    out.write(PLAIN_TEXT.getBytes());
    out.close();
  }
  
  static class TestTrustedChannelResolver extends TrustedChannelResolver {
    
    public boolean isTrusted(){
      return true;
    }

    public boolean isTrusted(InetAddress peerAddress){
      return true;
    }
  }
  
}
