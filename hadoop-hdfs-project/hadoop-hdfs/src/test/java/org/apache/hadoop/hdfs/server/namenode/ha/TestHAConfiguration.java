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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
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

  private Configuration getHAConf(String nsId, String host1, String host2) {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsId);    
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, nsId),
        "nn1,nn2");    
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, "nn1"),
        host1 + ":12345");
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, "nn2"),
        host2 + ":12345");
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
    assertEquals(new URL("http", "1.2.3.2",
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT, ""),
        checkpointer.getActiveNNAddress());
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
}
