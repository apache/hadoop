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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test cases that the HA configuration is reasonably validated and
 * interpreted in various places. These should be proper unit tests
 * which don't start daemons.
 */
public class TestHAConfiguration {

  private final FSNamesystem fsn = Mockito.mock(FSNamesystem.class);

  @Test
  public void testCheckpointerValidityChecks() throws Exception {
    try {
      Configuration conf = new Configuration();
      new StandbyCheckpointer(conf, fsn);
      fail("Bad config did not throw an error");
    } catch (IllegalArgumentException iae) {
      GenericTestUtils.assertExceptionContains(
          "Invalid URI for NameNode address", iae);
    }
  }

  private Configuration getHAConf(String nsId, String ... hosts) {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsId);
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");

    String[] nnids = new String[hosts.length];
    for (int i = 0; i < hosts.length; i++) {
      String nnid = "nn" + (i + 1);
      nnids[i] = nnid;
      conf.set(DFSUtil.addKeySuffixes(
              DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, nnid),
          hosts[i] + ":12345");
    }

    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, nsId),
        Joiner.on(',').join(nnids));
    return conf;
  }

  @Test
  public void testGetOtherNNHttpAddress() throws IOException {
    // Use non-local addresses to avoid host address matching
    Configuration conf = getHAConf("ns1", "1.2.3.1", "1.2.3.2");
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, "ns1");

    // This is done by the NN before the StandbyCheckpointer is created
    NameNode.initializeGenericKeys(conf, "ns1", "nn1");

    // Since we didn't configure the HTTP address, and the default is
    // 0.0.0.0, it should substitute the address from the RPC configuration
    // above.
    StandbyCheckpointer checkpointer = new StandbyCheckpointer(conf, fsn);
    assertAddressMatches("1.2.3.2", checkpointer.getActiveNNAddresses().get(0));

    //test when there are three NNs
    // Use non-local addresses to avoid host address matching
    conf = getHAConf("ns1", "1.2.3.1", "1.2.3.2", "1.2.3.3");

    // This is done by the NN before the StandbyCheckpointer is created
    NameNode.initializeGenericKeys(conf, "ns1", "nn1");

    checkpointer = new StandbyCheckpointer(conf, fsn);
    assertEquals("Got an unexpected number of possible active NNs", 2, checkpointer
        .getActiveNNAddresses().size());
    assertEquals(new URL("http", "1.2.3.2", DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT, ""),
        checkpointer.getActiveNNAddresses().get(0));
    assertAddressMatches("1.2.3.2", checkpointer.getActiveNNAddresses().get(0));
    assertAddressMatches("1.2.3.3", checkpointer.getActiveNNAddresses().get(1));
  }

  private void assertAddressMatches(String address, URL url) throws MalformedURLException {
    assertEquals(new URL("http", address, DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT, ""), url);
  }

  /**
   * Tests that the namenode edits dirs and shared edits dirs are gotten with
   * duplicates removed
   */
  @Test
  public void testHAUniqueEditDirs() throws IOException {
    Configuration conf = new Configuration();

    conf.set(DFS_NAMENODE_EDITS_DIR_KEY, "file://edits/dir, "
        + "file://edits/shared/dir"); // overlapping
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY, "file://edits/shared/dir");

    // getNamespaceEditsDirs removes duplicates across edits and shared.edits
    Collection<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(conf);
    assertEquals(2, editsDirs.size());
  }
  
  /**
   * Test that the 2NN does not start if given a config with HA NNs.
   */
  @Test
  public void testSecondaryNameNodeDoesNotStart() throws IOException {
    // Note we're not explicitly setting the nameservice Id in the
    // config as it is not required to be set and we want to test
    // that we can determine if HA is enabled when the nameservice Id
    // is not explicitly defined.
    Configuration conf = getHAConf("ns1", "1.2.3.1", "1.2.3.2");
    try {
      new SecondaryNameNode(conf);
      fail("Created a 2NN with an HA config");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Cannot use SecondaryNameNode in an HA cluster", ioe);
    }
  }

  @Test
  public void testGetOtherNNGenericConf() throws IOException {
    String nsId = "ns1";
    String host1 = "1.2.3.1";
    String host2 = "1.2.3.2";
    Configuration conf = getHAConf(nsId, host1, host2);
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nsId, "nn1"),
        host1 + ":54321");
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, "ns1");
    NameNode.initializeGenericKeys(conf, "ns1", "nn1");
    List<Configuration> others = HAUtil.getConfForOtherNodes(conf);
    Configuration nn2Conf = others.get(0);
    assertEquals(nn2Conf.get(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY),"nn2");
    assertTrue(!conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)
        .equals(nn2Conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)));
    assertNull(nn2Conf.get(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY));
  }
}
