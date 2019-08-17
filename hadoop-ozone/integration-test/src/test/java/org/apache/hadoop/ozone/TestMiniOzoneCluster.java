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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.test.TestGenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * Test cases for mini ozone cluster.
 */
public class TestMiniOzoneCluster {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  private final static File TEST_ROOT = TestGenericTestUtils.getTestDir();
  private final static File WRITE_TMP = new File(TEST_ROOT, "write");
  private final static File READ_TMP = new File(TEST_ROOT, "read");

  @BeforeClass
  public static void setup() {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, TEST_ROOT.toString());
    conf.setBoolean(DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    WRITE_TMP.mkdirs();
    READ_TMP.mkdirs();
    WRITE_TMP.deleteOnExit();
    READ_TMP.deleteOnExit();
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testStartMultipleDatanodes() throws Exception {
    final int numberOfNodes = 3;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numberOfNodes)
        .build();
    cluster.waitForClusterToBeReady();
    List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
    assertEquals(numberOfNodes, datanodes.size());
    for(HddsDatanodeService dn : datanodes) {
      // Create a single member pipe line
      List<DatanodeDetails> dns = new ArrayList<>();
      dns.add(dn.getDatanodeDetails());
      Pipeline pipeline = Pipeline.newBuilder()
          .setState(Pipeline.PipelineState.OPEN)
          .setId(PipelineID.randomId())
          .setType(HddsProtos.ReplicationType.STAND_ALONE)
          .setFactor(HddsProtos.ReplicationFactor.ONE)
          .setNodes(dns)
          .build();

      // Verify client is able to connect to the container
      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf)){
        client.connect();
        assertTrue(client.isConnected(pipeline.getFirstNode()));
      }
    }
  }

  @Test
  public void testDatanodeIDPersistent() throws Exception {
    // Generate IDs for testing
    DatanodeDetails id1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails id2 = TestUtils.randomDatanodeDetails();
    DatanodeDetails id3 = TestUtils.randomDatanodeDetails();
    id1.setPort(DatanodeDetails.newPort(Port.Name.STANDALONE, 1));
    id2.setPort(DatanodeDetails.newPort(Port.Name.STANDALONE, 2));
    id3.setPort(DatanodeDetails.newPort(Port.Name.STANDALONE, 3));

    // Add certificate serial  id.
    String certSerialId = "" + RandomUtils.nextLong();
    id1.setCertSerialId(certSerialId);

    // Write a single ID to the file and read it out
    File validIdsFile = new File(WRITE_TMP, "valid-values.id");
    validIdsFile.delete();
    ContainerUtils.writeDatanodeDetailsTo(id1, validIdsFile);
    // Validate using yaml parser
    Yaml yaml = new Yaml();
    try {
      yaml.load(new FileReader(validIdsFile));
    } catch (Exception e) {
      Assert.fail("Failed parsing datanode id yaml.");
    }
    DatanodeDetails validId = ContainerUtils.readDatanodeDetailsFrom(
        validIdsFile);

    assertEquals(validId.getCertSerialId(), certSerialId);
    assertEquals(id1, validId);
    assertEquals(id1.getProtoBufMessage(), validId.getProtoBufMessage());

    // Read should return an empty value if file doesn't exist
    File nonExistFile = new File(READ_TMP, "non_exist.id");
    nonExistFile.delete();
    try {
      ContainerUtils.readDatanodeDetailsFrom(nonExistFile);
      Assert.fail();
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    // Read should fail if the file is malformed
    File malformedFile = new File(READ_TMP, "malformed.id");
    createMalformedIDFile(malformedFile);
    try {
      ContainerUtils.readDatanodeDetailsFrom(malformedFile);
      fail("Read a malformed ID file should fail");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    // Test upgrade scenario - protobuf file instead of yaml
    File protoFile = new File(WRITE_TMP, "valid-proto.id");
    try (FileOutputStream out = new FileOutputStream(protoFile)) {
      HddsProtos.DatanodeDetailsProto proto = id1.getProtoBufMessage();
      proto.writeTo(out);
    }
    validId = ContainerUtils.readDatanodeDetailsFrom(protoFile);
    assertEquals(validId.getCertSerialId(), certSerialId);
    assertEquals(id1, validId);
    assertEquals(id1.getProtoBufMessage(), validId.getProtoBufMessage());
  }

  @Test
  public void testContainerRandomPort() throws IOException {
    Configuration ozoneConf = SCMTestUtils.getConf();
    File testDir = PathUtils.getTestDir(TestOzoneContainer.class);
    ozoneConf.set(DFS_DATANODE_DATA_DIR_KEY, testDir.getAbsolutePath());
    ozoneConf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        TEST_ROOT.toString());

    // Each instance of SM will create an ozone container
    // that bounds to a random port.
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, true);
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    List<DatanodeStateMachine> stateMachines = new ArrayList<>();
    try {

      for (int i = 0; i < 3; i++) {
        stateMachines.add(new DatanodeStateMachine(
            TestUtils.randomDatanodeDetails(), ozoneConf, null, null));
      }

      //we need to start all the servers to get the fix ports
      for (DatanodeStateMachine dsm : stateMachines) {
        dsm.getContainer().getReadChannel().start();
        dsm.getContainer().getWriteChannel().start();

      }

      for (DatanodeStateMachine dsm : stateMachines) {
        dsm.getContainer().getWriteChannel().stop();
        dsm.getContainer().getReadChannel().stop();

      }

      //after the start the real port numbers should be available AND unique
      HashSet<Integer> ports = new HashSet<Integer>();
      for (DatanodeStateMachine dsm : stateMachines) {
        int readPort = dsm.getContainer().getReadChannel().getIPCPort();

        assertNotEquals("Port number of the service is not updated", 0,
            readPort);

        assertTrue("Port of datanode service is conflicted with other server.",
            ports.add(readPort));

        int writePort = dsm.getContainer().getWriteChannel().getIPCPort();

        assertNotEquals("Port number of the service is not updated", 0,
            writePort);
        assertTrue("Port of datanode service is conflicted with other server.",
            ports.add(writePort));
      }

    } finally {
      for (DatanodeStateMachine dsm : stateMachines) {
        dsm.close();
      }
    }

    // Turn off the random port flag and test again
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);
    try (
        DatanodeStateMachine sm1 = new DatanodeStateMachine(
            TestUtils.randomDatanodeDetails(), ozoneConf,  null, null);
        DatanodeStateMachine sm2 = new DatanodeStateMachine(
            TestUtils.randomDatanodeDetails(), ozoneConf,  null, null);
        DatanodeStateMachine sm3 = new DatanodeStateMachine(
            TestUtils.randomDatanodeDetails(), ozoneConf,  null, null);
    ) {
      HashSet<Integer> ports = new HashSet<Integer>();
      assertTrue(ports.add(sm1.getContainer().getReadChannel().getIPCPort()));
      assertFalse(ports.add(sm2.getContainer().getReadChannel().getIPCPort()));
      assertFalse(ports.add(sm3.getContainer().getReadChannel().getIPCPort()));
      assertEquals(ports.iterator().next().intValue(),
          conf.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
              OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT));
    }
  }

  private void createMalformedIDFile(File malformedFile)
      throws IOException{
    malformedFile.delete();
    DatanodeDetails id = TestUtils.randomDatanodeDetails();
    ContainerUtils.writeDatanodeDetailsTo(id, malformedFile);

    FileOutputStream out = new FileOutputStream(malformedFile);
    out.write("malformed".getBytes());
    out.close();
  }

  /**
   * Test that a DN can register with SCM even if it was started before the SCM.
   * @throws Exception
   */
  @Test (timeout = 300_000)
  public void testDNstartAfterSCM() throws Exception {
    // Start a cluster with 1 DN
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();

    // Stop the SCM
    StorageContainerManager scm = cluster.getStorageContainerManager();
    scm.stop();

    // Restart DN
    cluster.restartHddsDatanode(0, false);

    // DN should be in GETVERSION state till the SCM is restarted.
    // Check DN endpoint state for 20 seconds
    DatanodeStateMachine dnStateMachine = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine();
    for (int i = 0; i < 20; i++) {
      for (EndpointStateMachine endpoint :
          dnStateMachine.getConnectionManager().getValues()) {
        Assert.assertEquals(
            EndpointStateMachine.EndPointStates.GETVERSION,
            endpoint.getState());
      }
      Thread.sleep(1000);
    }

    // DN should successfully register with the SCM after SCM is restarted.
    // Restart the SCM
    cluster.restartStorageContainerManager(true);
    // Wait for DN to register
    cluster.waitForClusterToBeReady();
    // DN should be in HEARTBEAT state after registering with the SCM
    for (EndpointStateMachine endpoint :
        dnStateMachine.getConnectionManager().getValues()) {
      Assert.assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT,
          endpoint.getState());
    }
  }
}
