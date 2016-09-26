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
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
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

  @Rule
  public Timeout timeout = new Timeout(300000);
  
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

  private MiniDFSCluster cluster = null;
  private Configuration conf = null;
  private FileSystem fs = null;
  
  private void setEncryptionConfigKeys() {
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    if (resolverClazz != null){
      conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, resolverClazz);
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

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
  }

  @After
  public void teardown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private FileChecksum writeUnencryptedAndThenRestartEncryptedCluster()
      throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).build();

    fs = getFileSystem(conf);
    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
    FileChecksum checksum = fs.getFileChecksum(TEST_PATH);
    fs.close();
    cluster.shutdown();

    setEncryptionConfigKeys();

    cluster = new MiniDFSCluster.Builder(conf)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .format(false)
        .startupOption(StartupOption.REGULAR)
        .build();

    fs = getFileSystem(conf);
    return checksum;
  }

  private void testEncryptedRead(String algorithm, String cipherSuite,
      boolean matchLog, boolean readAfterRestart)
      throws IOException {
    // set encryption algorithm and cipher suites, but don't enable transfer
    // encryption yet.
    conf.set(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY, algorithm);
    conf.set(HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY,
        cipherSuite);

    FileChecksum checksum = writeUnencryptedAndThenRestartEncryptedCluster();

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

    if (resolverClazz == null) {
      if (matchLog) {
        // Test client and server negotiate cipher option
        GenericTestUtils
            .assertMatches(logs.getOutput(), "Server using cipher suite");
        // Check the IOStreamPair
        GenericTestUtils.assertMatches(logs1.getOutput(),
            "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
      } else {
        // Test client and server negotiate cipher option
        GenericTestUtils
            .assertDoesNotMatch(logs.getOutput(), "Server using cipher suite");
        // Check the IOStreamPair
        GenericTestUtils.assertDoesNotMatch(logs1.getOutput(),
            "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
      }
    }

    if (readAfterRestart) {
      cluster.restartNameNode();
      fs = getFileSystem(conf);
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
    }
  }

  @Test
  public void testEncryptedReadDefaultAlgorithmCipherSuite()
      throws IOException {
    testEncryptedRead("", "", false, false);
  }

  @Test
  public void testEncryptedReadWithRC4() throws IOException {
    testEncryptedRead("rc4", "", false, false);
  }

  @Test
  public void testEncryptedReadWithAES() throws IOException {
    testEncryptedRead("", "AES/CTR/NoPadding", true, false);
  }

  @Test
  public void testEncryptedReadAfterNameNodeRestart() throws IOException {
    testEncryptedRead("", "", false, true);
  }

  @Test
  public void testClientThatDoesNotSupportEncryption() throws IOException {
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);

    writeUnencryptedAndThenRestartEncryptedCluster();

    DFSClient client = DFSClientAdapter.getDFSClient((DistributedFileSystem)fs);
    DFSClient spyClient = Mockito.spy(client);
    Mockito.doReturn(false).when(spyClient).shouldEncryptData();
    DFSClientAdapter.setDFSClient((DistributedFileSystem) fs, spyClient);

    LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
        LogFactory.getLog(DataNode.class));
    try {
      assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
      if (resolverClazz != null &&
          !resolverClazz.endsWith("TestTrustedChannelResolver")){
        fail("Should not have been able to read without encryption enabled.");
      }
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Could not obtain block:",
          ioe);
    } finally {
      logs.stopCapturing();
    }

    if (resolverClazz == null) {
      GenericTestUtils.assertMatches(logs.getOutput(),
          "Failed to read expected encryption handshake from client at");
    }
  }

  @Test
  public void testLongLivedReadClientAfterRestart() throws IOException {
    FileChecksum checksum = writeUnencryptedAndThenRestartEncryptedCluster();

    assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
    assertEquals(checksum, fs.getFileChecksum(TEST_PATH));

    // Restart the NN and DN, after which the client's encryption key will no
    // longer be valid.
    cluster.restartNameNode();
    assertTrue(cluster.restartDataNode(0));

    assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
    assertEquals(checksum, fs.getFileChecksum(TEST_PATH));
  }

  @Test
  public void testLongLivedWriteClientAfterRestart() throws IOException {
    setEncryptionConfigKeys();
    cluster = new MiniDFSCluster.Builder(conf).build();

    fs = getFileSystem(conf);

    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));

    // Restart the NN and DN, after which the client's encryption key will no
    // longer be valid.
    cluster.restartNameNode();
    assertTrue(cluster.restartDataNodes());
    cluster.waitActive();

    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT + PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
  }
  
  @Test
  public void testLongLivedClient() throws IOException, InterruptedException {
    FileChecksum checksum = writeUnencryptedAndThenRestartEncryptedCluster();

    BlockTokenSecretManager btsm = cluster.getNamesystem().getBlockManager()
        .getBlockTokenSecretManager();
    btsm.setKeyUpdateIntervalForTesting(2 * 1000);
    btsm.setTokenLifetime(2 * 1000);
    btsm.clearAllKeysForTesting();

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
  }

  @Test
  public void testLongLivedClientPipelineRecovery()
      throws IOException, InterruptedException, TimeoutException {
    if (resolverClazz != null) {
      // TestTrustedChannelResolver does not use encryption keys.
      return;
    }
    // use 4 datanodes to make sure that after 1 data node is stopped,
    // client only retries establishing pipeline with the 4th node.
    int numDataNodes = 4;
    // do not consider load factor when selecting a data node
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    setEncryptionConfigKeys();

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes)
        .build();

    fs = getFileSystem(conf);
    DFSClient client = DFSClientAdapter.getDFSClient((DistributedFileSystem)fs);
    DFSClient spyClient = Mockito.spy(client);
    DFSClientAdapter.setDFSClient((DistributedFileSystem) fs, spyClient);
    writeTestDataToFile(fs);

    BlockTokenSecretManager btsm = cluster.getNamesystem().getBlockManager()
        .getBlockTokenSecretManager();
    // Reduce key update interval and token life for testing.
    btsm.setKeyUpdateIntervalForTesting(2 * 1000);
    btsm.setTokenLifetime(2 * 1000);
    btsm.clearAllKeysForTesting();

    // Wait until the encryption key becomes invalid.
    LOG.info("Wait until encryption keys become invalid...");

    DataEncryptionKey encryptionKey = spyClient.getEncryptionKey();
    List<DataNode> dataNodes = cluster.getDataNodes();
    for (DataNode dn: dataNodes) {
      GenericTestUtils.waitFor(
          new Supplier<Boolean>() {
            @Override
            public Boolean get() {
              return !dn.getBlockPoolTokenSecretManager().
                  get(encryptionKey.blockPoolId)
                  .hasKey(encryptionKey.keyId);
            }
          }, 100, 30*1000
      );
    }
    LOG.info("The encryption key is invalid on all nodes now.");
    try(FSDataOutputStream out = fs.append(TEST_PATH)) {
      DFSOutputStream dfstream = (DFSOutputStream) out.getWrappedStream();
      // shut down the first datanode in the pipeline.
      DatanodeInfo[] targets = dfstream.getPipeline();
      cluster.stopDataNode(targets[0].getXferAddr());
      // write data to induce pipeline recovery
      out.write(PLAIN_TEXT.getBytes());
      out.hflush();
      assertFalse("The first datanode in the pipeline was not replaced.",
          Arrays.asList(dfstream.getPipeline()).contains(targets[0]));
    }
    // verify that InvalidEncryptionKeyException is handled properly
    Mockito.verify(spyClient, times(1)).clearDataEncryptionKey();
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
    setEncryptionConfigKeys();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDns).build();

    fs = getFileSystem(conf);

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

    if (resolverClazz == null) {
      // Test client and server negotiate cipher option
      GenericTestUtils.assertDoesNotMatch(logs.getOutput(),
          "Server using cipher suite");
      // Check the IOStreamPair
      GenericTestUtils.assertDoesNotMatch(logs1.getOutput(),
          "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream.");
    }
  }

  @Test
  public void testEncryptedAppend() throws IOException {
    setEncryptionConfigKeys();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();

    fs = getFileSystem(conf);

    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));

    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT + PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
  }

  @Test
  public void testEncryptedAppendRequiringBlockTransfer() throws IOException {
    setEncryptionConfigKeys();

    // start up 4 DNs
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();

    fs = getFileSystem(conf);

    // Create a file with replication 3, so its block is on 3 / 4 DNs.
    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));

    // Shut down one of the DNs holding a block replica.
    FSDataInputStream in = fs.open(TEST_PATH);
    List<LocatedBlock> locatedBlocks = DFSTestUtil.getAllBlocks(in);
    in.close();
    assertEquals(1, locatedBlocks.size());
    assertEquals(3, locatedBlocks.get(0).getLocations().length);
    DataNode dn = cluster.getDataNode(
        locatedBlocks.get(0).getLocations()[0].getIpcPort());
    dn.shutdown();

    // Reopen the file for append, which will need to add another DN to the
    // pipeline and in doing so trigger a block transfer.
    writeTestDataToFile(fs);
    assertEquals(PLAIN_TEXT + PLAIN_TEXT, DFSTestUtil.readFile(fs, TEST_PATH));
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
