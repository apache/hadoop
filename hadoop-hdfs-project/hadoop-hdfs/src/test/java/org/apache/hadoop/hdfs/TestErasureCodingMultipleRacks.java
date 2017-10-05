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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyRackFaultTolerant;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Test erasure coding block placement with skewed # nodes per rack.
 */
public class TestErasureCodingMultipleRacks {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestErasureCodingMultipleRacks.class);

  static {
    GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockPlacementPolicyDefault.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockPlacementPolicyRackFaultTolerant.LOG,
        Level.DEBUG);
    GenericTestUtils.setLogLevel(NetworkTopology.LOG, Level.DEBUG);
  }

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public ErasureCodingPolicy getPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  private MiniDFSCluster cluster;
  private ErasureCodingPolicy ecPolicy;
  private Configuration conf;
  private DistributedFileSystem dfs;

  @Before
  public void setup() throws Exception {
    ecPolicy = getPolicy();
    final int dataUnits = ecPolicy.getNumDataUnits();
    final int parityUnits = ecPolicy.getNumParityUnits();
    final int numDatanodes = dataUnits + parityUnits;
    final int numRacks = 2;
    final String[] racks = new String[numDatanodes];
    for (int i = 0; i < numRacks; i++) {
      racks[i] = "/rack" + i;
    }
    for (int i = numRacks; i < numDatanodes; i++) {
      racks[i] = "/rack" + (numRacks - 1);
    }
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .racks(racks)
        .build();
    dfs = cluster.getFileSystem();
    cluster.waitActive();
    dfs.setErasureCodingPolicy(new Path("/"), ecPolicy.getName());
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSkewedRack() throws Exception {
    final int filesize = ecPolicy.getNumDataUnits() * ecPolicy
        .getCellSize();
    byte[] contents = new byte[filesize];

    for (int i = 0; i < 10; i++) {
      final Path path = new Path("/testfile" + i);
      LOG.info("Writing file " + path);
      DFSTestUtil.writeFile(dfs, path, contents);
    }
  }
}
