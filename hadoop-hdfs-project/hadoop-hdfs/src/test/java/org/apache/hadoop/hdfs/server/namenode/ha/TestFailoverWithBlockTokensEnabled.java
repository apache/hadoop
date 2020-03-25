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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFailoverWithBlockTokensEnabled {
  
  private static final Path TEST_PATH = new Path("/test-path");
  private static final String TEST_DATA = "very important text";
  private static final int numNNs = 3;

  private Configuration conf;
  private MiniDFSCluster cluster;

  @Before
  public void startCluster() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology(numNNs))
        .numDataNodes(1)
        .build();
  }
  
  @After
  public void shutDownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void ensureSerialNumbersNeverOverlap() {
    BlockTokenSecretManager btsm1 = cluster.getNamesystem(0).getBlockManager()
        .getBlockTokenSecretManager();
    BlockTokenSecretManager btsm2 = cluster.getNamesystem(1).getBlockManager()
        .getBlockTokenSecretManager();
    BlockTokenSecretManager btsm3 = cluster.getNamesystem(2).getBlockManager()
        .getBlockTokenSecretManager();

    setAndCheckSerialNumber(0, btsm1, btsm2, btsm3);
    setAndCheckSerialNumber(Integer.MAX_VALUE, btsm1, btsm2, btsm3);
    setAndCheckSerialNumber(Integer.MAX_VALUE / 2, btsm1, btsm2, btsm3);
    setAndCheckSerialNumber(Integer.MAX_VALUE / 3, btsm1, btsm2, btsm3);
    setAndCheckSerialNumber(Integer.MAX_VALUE / 171717,
        btsm1, btsm2, btsm3);
  }

  private void setAndCheckSerialNumber(int serialNumber, BlockTokenSecretManager... btsms) {
    for (BlockTokenSecretManager btsm : btsms) {
      btsm.setSerialNo(serialNumber);
    }

    for (int i = 0; i < btsms.length; i++) {
      for (int j = 0; j < btsms.length; j++) {
        if (j == i) {
          continue;
        }
        int first = btsms[i].getSerialNoForTesting();
        int second = btsms[j].getSerialNoForTesting();
        assertFalse("Overlap found for set serial number (" + serialNumber + ") is " + i + ": "
            + first + " == " + j + ": " + second, first == second);
      }
    }
  }
  
  @Test
  public void ensureInvalidBlockTokensAreRejected() throws IOException,
      URISyntaxException {
    cluster.transitionToActive(0);
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    
    DFSTestUtil.writeFile(fs, TEST_PATH, TEST_DATA);
    assertEquals(TEST_DATA, DFSTestUtil.readFile(fs, TEST_PATH));
    
    DFSClient dfsClient = DFSClientAdapter.getDFSClient((DistributedFileSystem) fs);
    DFSClient spyDfsClient = Mockito.spy(dfsClient);
    Mockito.doAnswer(
        new Answer<LocatedBlocks>() {
          @Override
          public LocatedBlocks answer(InvocationOnMock arg0) throws Throwable {
            LocatedBlocks locatedBlocks = (LocatedBlocks)arg0.callRealMethod();
            for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
              Token<BlockTokenIdentifier> token = lb.getBlockToken();
              BlockTokenIdentifier id = lb.getBlockToken().decodeIdentifier();
              // This will make the token invalid, since the password
              // won't match anymore
              id.setExpiryDate(Time.now() + 10);
              Token<BlockTokenIdentifier> newToken =
                  new Token<BlockTokenIdentifier>(id.getBytes(),
                      token.getPassword(), token.getKind(), token.getService());
              lb.setBlockToken(newToken);
            }
            return locatedBlocks;
          }
        }).when(spyDfsClient).getLocatedBlocks(Mockito.anyString(),
            Mockito.anyLong(), Mockito.anyLong());
    DFSClientAdapter.setDFSClient((DistributedFileSystem)fs, spyDfsClient);
    
    try {
      assertEquals(TEST_DATA, DFSTestUtil.readFile(fs, TEST_PATH));
      fail("Shouldn't have been able to read a file with invalid block tokens");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Could not obtain block", ioe);
    }
  }
  
  @Test
  public void testFailoverAfterRegistration() throws IOException,
      URISyntaxException {
    writeUsingBothNameNodes();
  }
  
  @Test
  public void TestFailoverAfterAccessKeyUpdate() throws IOException,
      URISyntaxException, InterruptedException {
    lowerKeyUpdateIntervalAndClearKeys(cluster);
    // Sleep 10s to guarantee DNs heartbeat and get new keys.
    Thread.sleep(10 * 1000);
    writeUsingBothNameNodes();
  }
  
  private void writeUsingBothNameNodes() throws ServiceFailedException,
      IOException, URISyntaxException {
    cluster.transitionToActive(0);
    
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    DFSTestUtil.writeFile(fs, TEST_PATH, TEST_DATA);
    
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);
    
    fs.delete(TEST_PATH, false);
    DFSTestUtil.writeFile(fs, TEST_PATH, TEST_DATA);
  }
  
  private static void lowerKeyUpdateIntervalAndClearKeys(MiniDFSCluster cluster) {
    lowerKeyUpdateIntervalAndClearKeys(cluster.getNamesystem(0));
    lowerKeyUpdateIntervalAndClearKeys(cluster.getNamesystem(1));
    for (DataNode dn : cluster.getDataNodes()) {
      dn.clearAllBlockSecretKeys();
    }
  }
  
  private static void lowerKeyUpdateIntervalAndClearKeys(FSNamesystem namesystem) {
    BlockTokenSecretManager btsm = namesystem.getBlockManager()
        .getBlockTokenSecretManager();
    btsm.setKeyUpdateIntervalForTesting(2 * 1000);
    btsm.setTokenLifetime(2 * 1000);
    btsm.clearAllKeysForTesting();
  }
  
}
