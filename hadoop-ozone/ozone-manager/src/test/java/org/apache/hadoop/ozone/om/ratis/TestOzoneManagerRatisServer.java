/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMNodeDetails;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;

/**
 * Test OM Ratis server.
 */
public class TestOzoneManagerRatisServer {

  private OzoneConfiguration conf;
  private OzoneManagerRatisServer omRatisServer;
  private OzoneManagerRatisClient omRatisClient;
  private String omID;
  private String clientId = UUID.randomUUID().toString();
  private static final long LEADER_ELECTION_TIMEOUT = 500L;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    omID = UUID.randomUUID().toString();
    final String path = GenericTestUtils.getTempPath(omID);
    Path metaDirPath = Paths.get(path, "om-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    conf.setTimeDuration(
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    OMNodeDetails omNodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(omID)
        .setOMServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT)
        .build();
    // Starts a single node Ratis server
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(conf, null,
      omNodeDetails, Collections.emptyList());
    omRatisServer.start();
    omRatisClient = OzoneManagerRatisClient.newOzoneManagerRatisClient(omID,
        omRatisServer.getRaftGroup(), conf);
    omRatisClient.connect();
  }

  @After
  public void shutdown() {
    if (omRatisServer != null) {
      omRatisServer.stop();
    }
    if (omRatisClient != null) {
      omRatisClient.close();
    }
  }

  /**
   * Start a OM Ratis Server and checks its state.
   */
  @Test
  public void testStartOMRatisServer() throws Exception {
    Assert.assertEquals("Ratis Server should be in running state",
        LifeCycle.State.RUNNING, omRatisServer.getServerState());
  }

  /**
   * Test that all of {@link OzoneManagerProtocolProtos.Type} enum values are
   * categorized in {@link OmUtils#isReadOnly(OMRequest)}.
   */
  @Test
  public void testIsReadOnlyCapturesAllCmdTypeEnums() throws Exception {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(OmUtils.class));
    OzoneManagerProtocolProtos.Type[] cmdTypes =
        OzoneManagerProtocolProtos.Type.values();

    for (OzoneManagerProtocolProtos.Type cmdtype : cmdTypes) {
      OMRequest request = OMRequest.newBuilder()
          .setCmdType(cmdtype)
          .setClientId(clientId)
          .build();
      OmUtils.isReadOnly(request);
      assertFalse(cmdtype + "is not categorized in OmUtils#isReadyOnly",
          logCapturer.getOutput().contains("CmdType " + cmdtype +" is not " +
              "categorized as readOnly or not."));
      logCapturer.clearOutput();
    }
  }

  @Test
  public void verifyRaftGroupIdGenerationWithDefaultOmServiceId() throws
      Exception {
    UUID uuid = UUID.nameUUIDFromBytes(OzoneConsts.OM_SERVICE_ID_DEFAULT
        .getBytes());
    RaftGroupId raftGroupId = omRatisServer.getRaftGroup().getGroupId();
    Assert.assertEquals(uuid, raftGroupId.getUuid());
    Assert.assertEquals(raftGroupId.toByteString().size(), 16);
  }

  @Test
  public void verifyRaftGroupIdGenerationWithCustomOmServiceId() throws
      Exception {
    String customOmServiceId = "omSIdCustom123";
    OzoneConfiguration newConf = new OzoneConfiguration();
    String newOmId = UUID.randomUUID().toString();
    String path = GenericTestUtils.getTempPath(newOmId);
    Path metaDirPath = Paths.get(path, "om-meta");
    newConf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    newConf.setTimeDuration(
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = 9873;
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    OMNodeDetails omNodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(newOmId)
        .setOMServiceId(customOmServiceId)
        .build();
    // Starts a single node Ratis server
    OzoneManagerRatisServer newOmRatisServer = OzoneManagerRatisServer
        .newOMRatisServer(newConf, null,
            omNodeDetails, Collections.emptyList());
    newOmRatisServer.start();
    OzoneManagerRatisClient newOmRatisClient = OzoneManagerRatisClient
        .newOzoneManagerRatisClient(
            newOmId,
            newOmRatisServer.getRaftGroup(), newConf);
    newOmRatisClient.connect();

    UUID uuid = UUID.nameUUIDFromBytes(customOmServiceId.getBytes());
    RaftGroupId raftGroupId = newOmRatisServer.getRaftGroup().getGroupId();
    Assert.assertEquals(uuid, raftGroupId.getUuid());
    Assert.assertEquals(raftGroupId.toByteString().size(), 16);
  }
}
