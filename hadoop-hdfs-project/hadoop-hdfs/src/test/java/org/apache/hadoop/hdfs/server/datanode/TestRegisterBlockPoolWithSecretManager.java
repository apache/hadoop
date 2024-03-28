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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.test.GenericTestUtils;

public class TestRegisterBlockPoolWithSecretManager {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
  }

  private static BPOfferService bpOfferServiceForMiniCluster(Configuration conf)
      throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DataNode dn = cluster.getDataNodes().get(0);
    List<BPOfferService> allBpOs = dn.getAllBpOs();
    return allBpOs.get(0);
  }

  @Test
  public void testBothBpNoTokens() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);
    BPOfferService service = bpOfferServiceForMiniCluster(conf);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    DatanodeRegistration bpRegistration = actor.getBpRegistration();

    // Simulate a registration from two NameNodes, neither using block tokens
    service.registrationSucceeded(actor, bpRegistration);
    service.registrationSucceeded(actor, bpRegistration);
  }

  @Test
  public void testBothBpWithTokens() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    BPOfferService service = bpOfferServiceForMiniCluster(conf);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    DatanodeRegistration bpRegistration = actor.getBpRegistration();

    // Simulate a registration from two NameNodes, both using block tokens
    service.registrationSucceeded(actor, bpRegistration);
    service.registrationSucceeded(actor, bpRegistration);
  }

  @Test
  public void testFirstBpWithTokens() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    BPOfferService service = bpOfferServiceForMiniCluster(conf);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    DatanodeRegistration bpRegistration = actor.getBpRegistration();
    exception.expect(RuntimeException.class);
    exception.expectMessage("Inconsistent configuration of block access tokens");

    // Simulate a registration from two NameNodes, just the first one using block tokens
    service.registrationSucceeded(actor, bpRegistration);
    bpRegistration.setExportedKeys(new ExportedBlockKeys());
    service.registrationSucceeded(actor, bpRegistration);
  }

  @Test
  public void testSecondBpWithTokens() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    BPOfferService service = bpOfferServiceForMiniCluster(conf);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    DatanodeRegistration originalBpRegistration = actor.getBpRegistration();
    DatanodeRegistration noTokensBpRegistration =
        new DatanodeRegistration(originalBpRegistration.getDatanodeUuid(), originalBpRegistration);
    exception.expect(RuntimeException.class);
    exception.expectMessage("Inconsistent configuration of block access tokens");

    // Simulate a registration from two NameNodes, just the second one using block tokens
    noTokensBpRegistration.setExportedKeys(new ExportedBlockKeys());
    service.registrationSucceeded(actor, noTokensBpRegistration);
    service.registrationSucceeded(actor, originalBpRegistration);
  }

  @Test
  public void testFirstBpWithTokensMigrationMode() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_BLOCK_ACCESS_TOKEN_UNSAFE_ALLOWED_NOT_REQUIRED_KEY,
        true);
    BPOfferService service = bpOfferServiceForMiniCluster(conf);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    DatanodeRegistration bpRegistration = actor.getBpRegistration();

    GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
        LoggerFactory.getLogger(DataNode.class));
    try {
      // Simulate a registration from two NameNodes, just the first one using block tokens
      service.registrationSucceeded(actor, bpRegistration);
      bpRegistration.setExportedKeys(new ExportedBlockKeys());
      service.registrationSucceeded(actor, bpRegistration);
    } finally {
      logs.stopCapturing();
    }
    GenericTestUtils.assertMatches(logs.getOutput(),
        "Block access token migration enabled - ignoring blockTokenEnabled");
  }

  @Test
  public void testSecondBpWithTokensMigrationMode() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_BLOCK_ACCESS_TOKEN_UNSAFE_ALLOWED_NOT_REQUIRED_KEY,
        true);
    BPOfferService service = bpOfferServiceForMiniCluster(conf);
    BPServiceActor actor = service.getBPServiceActors().get(0);
    DatanodeRegistration originalBpRegistration = actor.getBpRegistration();
    DatanodeRegistration noTokensBpRegistration =
        new DatanodeRegistration(originalBpRegistration.getDatanodeUuid(), originalBpRegistration);

    GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(
        LoggerFactory.getLogger(DataNode.class));
    try {
      // Simulate a registration from two NameNodes, just the second one using block tokens
      noTokensBpRegistration.setExportedKeys(new ExportedBlockKeys());
      service.registrationSucceeded(actor, noTokensBpRegistration);
      service.registrationSucceeded(actor, originalBpRegistration);
    } finally {
      logs.stopCapturing();
    }
    GenericTestUtils.assertMatches(logs.getOutput(),
        "Block access token migration enabled - ignoring blockTokenEnabled");
  }
}
