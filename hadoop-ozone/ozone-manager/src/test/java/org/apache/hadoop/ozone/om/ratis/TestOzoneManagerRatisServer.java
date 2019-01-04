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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.test.GenericTestUtils;
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
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(omID,
        InetAddress.getLocalHost(), conf);
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
   * Submit any request to OM Ratis server and check that the dummy response
   * message is received.
   * TODO: Once state machine is implemented, submitting a request to Ratis
   * server should result in a valid response.
   */
  @Test
  public void testSubmitRatisRequest() throws Exception {
    // Wait for leader election
    Thread.sleep(LEADER_ELECTION_TIMEOUT * 2);

    OMRequest request = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
        .setClientId(clientId)
        .build();

    OMResponse response = omRatisClient.sendCommand(request);

    // Since the state machine is not implemented yet, we should get the
    // configured dummy message from Ratis.
    Assert.assertEquals(false, response.getSuccess());
    Assert.assertTrue(response.getMessage().contains("Dummy response from " +
        "Ratis server for command type: " +
        OzoneManagerProtocolProtos.Type.CreateVolume));
    Assert.assertEquals(false, response.hasCreateVolumeResponse());
  }

  /**
   * Test that all of {@link OzoneManagerProtocolProtos.Type} enum values are
   * categorized in {@link OmUtils#isReadOnly(OMRequest)}.
   */
  @Test
  public void testIsReadOnlyCapturesAllCmdTypeEnums() throws Exception {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(OmUtils.class));
    String clientId = UUID.randomUUID().toString();
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
}
