/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests ozone containers via secure grpc/netty.
 */
@RunWith(Parameterized.class)
public class TestSecureOzoneContainer {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestSecureOzoneContainer.class);
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SecurityConfig secConfig;
  private Boolean requireBlockToken;
  private Boolean hasBlockToken;
  private Boolean blockTokeExpired;
  private CertificateClientTestImpl caClient;
  private OzoneBlockTokenSecretManager secretManager;


  public TestSecureOzoneContainer(Boolean requireBlockToken,
      Boolean hasBlockToken, Boolean blockTokenExpired) {
    this.requireBlockToken = requireBlockToken;
    this.hasBlockToken = hasBlockToken;
    this.blockTokeExpired = blockTokenExpired;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> blockTokenOptions() {
    return Arrays.asList(new Object[][] {
        {true, true, false},
        {true, true, true},
        {true, false, false},
        {false, true, false},
        {false, false, false}});
  }

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    String ozoneMetaPath =
        GenericTestUtils.getTempPath("ozoneMeta");
    conf.set(OZONE_METADATA_DIRS, ozoneMetaPath);
    secConfig = new SecurityConfig(conf);
    caClient = new CertificateClientTestImpl(conf);
    secretManager = new OzoneBlockTokenSecretManager(new SecurityConfig(conf),
        60 * 60 * 24, caClient.getCertificate().
        getSerialNumber().toString());
  }

  @Test
  public void testCreateOzoneContainer() throws Exception {
    LOG.info("Test case: requireBlockToken: {} hasBlockToken: {} " +
        "blockTokenExpired: {}.", requireBlockToken, hasBlockToken,
        blockTokeExpired);
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED,
        requireBlockToken);

    long containerID = ContainerTestHelper.getTestContainerID();
    OzoneContainer container = null;
    System.out.println(System.getProperties().getProperty("java.library.path"));
    try {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline();
      conf.set(HDDS_DATANODE_DIR_KEY, tempFolder.getRoot().getPath());
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT, pipeline
          .getFirstNode().getPort(DatanodeDetails.Port.Name.STANDALONE)
          .getValue());
      conf.setBoolean(
          OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);

      DatanodeDetails dn = TestUtils.randomDatanodeDetails();
      container = new OzoneContainer(dn, conf, getContext(dn), caClient);
      //Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          "user1",  new String[] {"usergroup"});
      long expiryDate = (blockTokeExpired) ?
          Time.now() - 60 * 60 * 2 : Time.now() + 60 * 60 * 24;

      OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier(
          "testUser", "cid:lud:bcsid",
          EnumSet.allOf(AccessModeProto.class),
          expiryDate, "1234", 128L);

      int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
      if (port == 0) {
        port = secConfig.getConfiguration().getInt(OzoneConfigKeys
                .DFS_CONTAINER_IPC_PORT, DFS_CONTAINER_IPC_PORT_DEFAULT);
      }
      secretManager.start(caClient);
      Token<OzoneBlockTokenIdentifier> token = secretManager.generateToken(
          "123", EnumSet.allOf(AccessModeProto.class), RandomUtils.nextLong());
      if (hasBlockToken) {
        ugi.addToken(token);
      }

      ugi.doAs((PrivilegedAction<Void>) () -> {
        try {
          XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
          client.connect(token.encodeToUrlString());
          if (hasBlockToken) {
            createContainerForTesting(client, containerID, token);
          } else {
            createContainerForTesting(client, containerID, null);
          }

        } catch (Exception e) {
          if (requireBlockToken && hasBlockToken && !blockTokeExpired) {
            LOG.error("Unexpected error. ", e);
            fail("Client with BlockToken should succeed when block token is" +
                " required.");
          }
          if (requireBlockToken && hasBlockToken && blockTokeExpired) {
            assertTrue("Receive expected exception",
                e instanceof SCMSecurityException);
          }
          if (requireBlockToken && !hasBlockToken) {
            assertTrue("Receive expected exception", e instanceof
                IOException);
          }
        }
        return null;
      });
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  public static void createContainerForTesting(XceiverClientSpi client,
      long containerID, Token token) throws Exception {
    // Create container
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerSecureRequest(
            containerID, client.getPipeline(), token);
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
  }

  private StateContext getContext(DatanodeDetails datanodeDetails) {
    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    return context;
  }
}