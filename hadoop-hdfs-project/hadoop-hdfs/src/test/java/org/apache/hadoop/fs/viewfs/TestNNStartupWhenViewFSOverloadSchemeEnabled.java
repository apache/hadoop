/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the NN startup is successful with ViewFSOverloadScheme.
 */
public class TestNNStartupWhenViewFSOverloadSchemeEnabled {
  private MiniDFSCluster cluster;
  private static final String FS_IMPL_PATTERN_KEY = "fs.%s.impl";
  private static final String HDFS_SCHEME = "hdfs";
  private static final Configuration CONF = new Configuration();

  @BeforeClass
  public static void setUp() {
    CONF.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    CONF.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    CONF.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    CONF.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFileSystemOverloadScheme.class.getName());
    CONF.set(String
        .format(FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
            HDFS_SCHEME), DistributedFileSystem.class.getName());
    // By default trash interval is 0. To trigger TrashEmptier, let's set it to
    // >0 value.
    CONF.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 100);
  }

  /**
   * Tests that the HA mode NameNode startup is successful when
   * ViewFSOverloadScheme configured.
   */
  @Test(timeout = 30000)
  public void testHANameNodeAndDataNodeStartup() throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(1)
        .waitSafeMode(false).build();
    cluster.waitActive();
    cluster.transitionToActive(0);
  }

  /**
   * Tests that the NameNode startup is successful when ViewFSOverloadScheme
   * configured.
   */
  @Test(timeout = 30000)
  public void testNameNodeAndDataNodeStartup() throws Exception {
    cluster =
        new MiniDFSCluster.Builder(CONF).numDataNodes(1).waitSafeMode(false)
            .build();
    cluster.waitActive();
  }

  @After
  public void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
