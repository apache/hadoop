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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyRackFaultTolerantGDA;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBlockPlacementPolicyRackFaultTolerantGDA {

  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;

  public Map<String, Map<String, Integer>> defineLinkCosts(int[][] costs) {
    Map<String, Map<String, Integer>> linkCosts =
      new HashMap<String, Map<String, Integer>>();

    for (int i = 0; i < costs.length; i++) {
      String rack_i = "/rack" + i;
      linkCosts.put(rack_i, new HashMap<String, Integer>());
      Map<String, Integer> costs_i = linkCosts.get(rack_i);
      for (int j = 0; j < costs.length; j++) {
        String rack_j = "/rack" + j;
        costs_i.put(rack_j, costs[i][j]);
      }
    }
    return linkCosts;
  }

  public void createLinkCostScript(File src, int[][] costs) throws IOException {
    Map<String, Map<String, Integer>> linkCosts = defineLinkCosts(costs);
    PrintWriter writer = new PrintWriter(src);
    writer.println("#!/bin/bash");
    try {
      for (String rack1 : linkCosts.keySet()) {
        Map<String, Integer> otherRacks = linkCosts.get(rack1);
        for (String rack2 : otherRacks.keySet()) {
          int cost = otherRacks.get(rack2);
          writer.format(
              "if [[ \"$1\" == \"%s\" && \"$2\" == \"%s\" ]]; then echo \"%d\"; fi\n",
              rack1, rack2, cost);
        }
      }
    } finally {
      writer.close();
    }
  }

  private MiniDFSCluster testGDASetup(int numRacks,
      int numHostsPerRack, int[][] costs) throws URISyntaxException, IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final ArrayList<String> rackList = new ArrayList<String>();
    final ArrayList<String> hostList = new ArrayList<String>();
    for (int i = 0; i < numRacks; i++) {
      for (int j = 0; j < numHostsPerRack; j++) {
        rackList.add("/rack" + i);
        hostList.add("/host" + i + j);
      }
    }

    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyRackFaultTolerantGDA.class,
        BlockPlacementPolicy.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);

    // Setup link cost script.
    String scriptFileName = "/" +
        Shell.appendScriptExtension("custom-link-script");
    assertEquals(true, scriptFileName != null && !scriptFileName.isEmpty());
    URL shellScript = getClass().getResource(scriptFileName);
    Path resourcePath = Paths.get(shellScript.toURI());
    FileUtil.setExecutable(resourcePath.toFile(), true);
    FileUtil.setWritable(resourcePath.toFile(), true);

    // Manually update the script with link cost logic
    createLinkCostScript(resourcePath.toFile(), costs);
    conf.set(DFSConfigKeys.NET_LINK_SCRIPT_FILE_NAME_KEY,
      resourcePath.toString());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(hostList.size())
        .racks(rackList.toArray(new String[rackList.size()]))
        .hosts(hostList.toArray(new String[hostList.size()]))
        .build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyEC", null,
        FsPermission.getDefault());

    return cluster;
  }

  private void doTestGDA(String filename, int numRacks, short[][] testSuite,
      int[][] expectedReplication, int[][] expectedAdditionalReplication,
      String clientMachine) throws Exception {
    // Test 5 files
    int fileCount = 0;
    for (int i = 0; i < 5; i++) {
      int idx = 0;
      for (short[] testCase : testSuite) {
        short replication = testCase[0];
        short additionalReplication = testCase[1];
        String src = "/" + filename + (fileCount++);
        // Create the file with client machine
        HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
            clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
            replication, DEFAULT_BLOCK_SIZE, null, false);

        //test chooseTarget for new file
        LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
            null, null, fileStatus.getFileId(), null, null);
        testBlockLocations(replication, locatedBlock,
                           expectedReplication, idx, numRacks);

        //test chooseTarget for existing file.
        LocatedBlock additionalLocatedBlock =
            nameNodeRpc.getAdditionalDatanode(src, fileStatus.getFileId(),
                locatedBlock.getBlock(), locatedBlock.getLocations(),
                locatedBlock.getStorageIDs(), new DatanodeInfo[0],
                additionalReplication, clientMachine);
        testBlockLocations(replication + additionalReplication,
                           additionalLocatedBlock,
                           expectedAdditionalReplication, idx, numRacks);
        idx++;
      }
    }
  }

  @Test
  public void doTestGDASimple1() throws Exception {
    // Simple GDA testcase. 3 racks, 2 hosts per rack
    // Rack0, Rack1 are in the same DC. Rack2 is in another DC
    int numRacks = 3;
    int numHostsPerRack = 2;
    int[][] costs = {
      {0, 1, 10},
      {1, 0, 10},
      {10, 10, 0},
    };
    // Setup
    MiniDFSCluster cluster = testGDASetup(numRacks, numHostsPerRack, costs);

    String clientMachine = "/host00";
    short[][] testSuite = {
        {3,2}, {3,1}, {4,1}, {4,2},
    };
    int[][] expectedReplication = {
      {2, 1, 0},
      {2, 1, 0},
      {2, 2, 0},
      {2, 2, 0},
    };
    int[][] expectedAdditionalReplication = {
      {2, 2, 1},
      {2, 2, 0},
      {2, 2, 1},
      {2, 2, 2},
    };
    doTestGDA("testfile", numRacks, testSuite, expectedReplication,
        expectedAdditionalReplication, clientMachine);

    // Cleanup
    cluster.shutdown();
  }

  @Test
  public void doTestGDASimple2() throws Exception {
    // Simple GDA testcase. 4 racks, 2 hosts per rack
    // Rack0, Rack1 are in one DC. Rack2, Rack3 are in another DC
    int numRacks = 4;
    int numHostsPerRack = 2;
    int[][] costs = {
      {0, 1, 10, 10},
      {1, 0, 10, 10},
      {10, 10, 0, 1},
      {10, 10, 1, 0},
    };
    // Setup
    MiniDFSCluster cluster = testGDASetup(numRacks, numHostsPerRack, costs);

    // Test when client is on a host on Rack0. It should try to stick to Rack0
    // and Rack1 as much as possible
    String clientMachine1 = "/host00";
    // XXX-kbavishi Can't pick testcases where there is a chance of
    // randomization kicking in. How do we fix this?
    short[][] testSuite = {
      {2,2}, {3,1}, {4,4},
    };
    int[][] expectedReplication1 = {
      {2, 0, 0, 0},
      {2, 1, 0, 0},
      {2, 2, 0, 0},
    };
    int[][] expectedAdditionalReplication1 = {
      {2, 2, 0, 0},
      {2, 2, 0, 0},
      {2, 2, 2, 2},
    };
    doTestGDA("testfile1", numRacks, testSuite, expectedReplication1,
        expectedAdditionalReplication1, clientMachine1);

    // Test when client is on a host on Rack2. It should try to stick to Rack2
    // and Rack3 as much as possible
    String clientMachine2 = "/host20";
    int[][] expectedReplication2 = {
      {0, 0, 2, 0},
      {0, 0, 2, 1},
      {0, 0, 2, 2},
    };
    int[][] expectedAdditionalReplication2 = {
      {0, 0, 2, 2},
      {0, 0, 2, 2},
      {2, 2, 2, 2},
    };
    doTestGDA("testfile2", numRacks, testSuite, expectedReplication2,
        expectedAdditionalReplication2, clientMachine2);

    // Cleanup
    cluster.shutdown();
  }

  private void testBlockLocations(int replication, LocatedBlock locatedBlock,
                                  int[][] expected, int idx, int numRacks) {
    assertEquals(replication, locatedBlock.getLocations().length);

    HashMap<String, Integer> racksCount = new HashMap<String, Integer>();
    for (DatanodeInfo node : locatedBlock.getLocations()) {
      addToRacksCount(node.getNetworkLocation(), racksCount);
    }

    for (int i = 0; i < numRacks; i++) {
      assertEquals(expected[idx][i], getRacksCount(racksCount, "/rack" + i));
    }
  }

  private void addToRacksCount(String rack, HashMap<String, Integer> racksCount) {
    Integer count = racksCount.get(rack);
    if (count == null) {
      racksCount.put(rack, 1);
    } else {
      racksCount.put(rack, count + 1);
    }
  }
  private int getRacksCount(HashMap<String, Integer> racksCount, String rack) {
    if (racksCount.containsKey(rack)) {
      return racksCount.get(rack);
    } else {
      return 0;
    }
  }
}
