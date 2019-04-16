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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

/**
 * Tests ozone containers via secure grpc/netty.
 */
@RunWith(Parameterized.class)
@Ignore("TODO:HDDS-1157")
public class TestOzoneContainerWithTLS {
  private final static Logger LOG = LoggerFactory.getLogger(
      TestOzoneContainerWithTLS.class);
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SecurityConfig secConfig;
  private Boolean requireMutualTls;

  public TestOzoneContainerWithTLS(Boolean requireMutualTls) {
    this.requireMutualTls = requireMutualTls;

  }

  @Parameterized.Parameters
  public static Collection<Object[]> encryptionOptions() {
    return Arrays.asList(new Object[][] {
        {true},
        {false}
    });
  }

  private void copyResource(String inputResourceName, File outputFile) throws
      IOException {
    InputStream is = ThreadUtil.getResourceAsStream(inputResourceName);
    try (OutputStream os = new FileOutputStream(outputFile)) {
      IOUtils.copy(is, os);
    } finally {
      IOUtils.closeQuietly(is);
    }
  }

  @Before
  public void setup() throws IOException{
    conf = new OzoneConfiguration();
    String ozoneMetaPath =
        GenericTestUtils.getTempPath("ozoneMeta");
    File ozoneMetaFile = new File(ozoneMetaPath);
    conf.set(OZONE_METADATA_DIRS, ozoneMetaPath);

    FileUtil.fullyDelete(ozoneMetaFile);
    String keyDirName = conf.get(HDDS_KEY_DIR_NAME,
        HDDS_KEY_DIR_NAME_DEFAULT);

    File ozoneKeyDir = new File(ozoneMetaFile, keyDirName);
    ozoneKeyDir.mkdirs();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_ENABLED, true);

    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT, true);
    secConfig = new SecurityConfig(conf);

    copyResource("ssl/ca.crt", secConfig.getTrustStoreFile());
    copyResource("ssl/server.pem", secConfig.getServerPrivateKeyFile());
    copyResource("ssl/client.pem", secConfig.getClientPrivateKeyFile());
    copyResource("ssl/client.crt", secConfig.getClientCertChainFile());
    copyResource("ssl/server.crt", secConfig.getServerCertChainFile());
  }

  @Test
  public void testCreateOzoneContainer() throws Exception {
    LOG.info("testCreateOzoneContainer with Mutual TLS: {}",
        requireMutualTls);
    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_MUTUAL_TLS_REQUIRED,
        requireMutualTls);

    long containerID = ContainerTestHelper.getTestContainerID();
    OzoneContainer container = null;
    System.out.println(System.getProperties().getProperty("java.library.path"));
    DatanodeDetails dn = TestUtils.randomDatanodeDetails();
    try {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline();
      conf.set(HDDS_DATANODE_DIR_KEY, tempFolder.getRoot().getPath());
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode().getPort(DatanodeDetails.Port.Name.STANDALONE)
              .getValue());
      conf.setBoolean(
          OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);

      container = new OzoneContainer(dn, conf, getContext(dn), null);
      //Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
      client.connect();
      createContainerForTesting(client, containerID);
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  public static void createContainerForTesting(XceiverClientSpi client,
      long containerID) throws Exception {
    // Create container
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerRequest(
            containerID, client.getPipeline());
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
