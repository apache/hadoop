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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterNamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Used to test the functionality of {@link RouterAsyncNamenodeProtocol}.
 */
public class TestRouterAsyncNamenodeProtocol extends RouterAsyncProtocolTestBase {

  private RouterAsyncNamenodeProtocol asyncNamenodeProtocol;
  private RouterNamenodeProtocol namenodeProtocol;

  @Before
  public void setup() throws Exception {
    asyncNamenodeProtocol = new RouterAsyncNamenodeProtocol(getRouterAsyncRpcServer());
    namenodeProtocol = new RouterNamenodeProtocol(getRouterRpcServer());
  }

  @Test
  public void getBlocks() throws Exception {
    DatanodeInfo[] dns = getRouter().getClient()
        .getNamenode().getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);

    DatanodeInfo dn0 = dns[0];
    asyncNamenodeProtocol.getBlocks(dn0, 1024, 0, 0,
        null);
    BlocksWithLocations asyncRouterBlockLocations = syncReturn(BlocksWithLocations.class);
    assertNotNull(asyncRouterBlockLocations);

    BlocksWithLocations syncRouterBlockLocations = namenodeProtocol.getBlocks(dn0, 1024,
        0, 0, null);

    BlockWithLocations[] asyncRouterBlocks = asyncRouterBlockLocations.getBlocks();
    BlockWithLocations[] syncRouterBlocks = syncRouterBlockLocations.getBlocks();

    assertEquals(asyncRouterBlocks.length, syncRouterBlocks.length);
    for (int i = 0; i < syncRouterBlocks.length; i++) {
      assertEquals(
          asyncRouterBlocks[i].getBlock().getBlockId(),
          syncRouterBlocks[i].getBlock().getBlockId());
    }
  }

  @Test
  public void getBlockKeys() throws Exception {
    asyncNamenodeProtocol.getBlockKeys();
    ExportedBlockKeys asyncBlockKeys = syncReturn(ExportedBlockKeys.class);
    assertNotNull(asyncBlockKeys);

    ExportedBlockKeys syncBlockKeys = namenodeProtocol.getBlockKeys();
    compareBlockKeys(asyncBlockKeys, syncBlockKeys);
  }

  @Test
  public void getTransactionID() throws Exception {
    asyncNamenodeProtocol.getTransactionID();
    long asyncTransactionID = syncReturn(Long.class);
    assertNotNull(asyncTransactionID);

    long transactionID = namenodeProtocol.getTransactionID();
    assertEquals(asyncTransactionID, transactionID);
  }

  @Test
  public void getMostRecentCheckpointTxId() throws Exception {
    asyncNamenodeProtocol.getMostRecentCheckpointTxId();
    long asyncMostRecentCheckpointTxId = syncReturn(Long.class);
    assertNotNull(asyncMostRecentCheckpointTxId);

    long mostRecentCheckpointTxId = namenodeProtocol.getMostRecentCheckpointTxId();
    assertEquals(asyncMostRecentCheckpointTxId, mostRecentCheckpointTxId);
  }

  @Test
  public void versionRequest() throws Exception {
    asyncNamenodeProtocol.versionRequest();
    NamespaceInfo asyncNamespaceInfo = syncReturn(NamespaceInfo.class);
    assertNotNull(asyncNamespaceInfo);
    NamespaceInfo syncNamespaceInfo = namenodeProtocol.versionRequest();
    compareVersion(asyncNamespaceInfo, syncNamespaceInfo);
  }

  private void compareBlockKeys(
      ExportedBlockKeys blockKeys, ExportedBlockKeys otherBlockKeys) {
    assertEquals(blockKeys.getCurrentKey(), otherBlockKeys.getCurrentKey());
    assertEquals(blockKeys.getKeyUpdateInterval(), otherBlockKeys.getKeyUpdateInterval());
    assertEquals(blockKeys.getTokenLifetime(), otherBlockKeys.getTokenLifetime());
  }

  private void compareVersion(NamespaceInfo version, NamespaceInfo otherVersion) {
    assertEquals(version.getBlockPoolID(), otherVersion.getBlockPoolID());
    assertEquals(version.getNamespaceID(), otherVersion.getNamespaceID());
    assertEquals(version.getClusterID(), otherVersion.getClusterID());
    assertEquals(version.getLayoutVersion(), otherVersion.getLayoutVersion());
    assertEquals(version.getCTime(), otherVersion.getCTime());
  }
}