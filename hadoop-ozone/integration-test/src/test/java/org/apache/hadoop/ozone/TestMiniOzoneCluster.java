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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.XceiverClient;
import org.apache.hadoop.scm.container.common.helpers.PipelineChannel;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.test.TestGenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.*;

/**
 * Test cases for mini ozone cluster.
 */
public class TestMiniOzoneCluster {

  private static MiniOzoneClassicCluster cluster;
  private static OzoneConfiguration conf;

  private final static File TEST_ROOT = TestGenericTestUtils.getTestDir();
  private final static File WRITE_TMP = new File(TEST_ROOT, "write");
  private final static File READ_TMP = new File(TEST_ROOT, "read");

  @BeforeClass
  public static void setup() {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        TEST_ROOT.toString());
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
      cluster.close();
    }
  }

  @Test(timeout = 30000)
  public void testStartMultipleDatanodes() throws Exception {
    final int numberOfNodes = 3;
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .numDataNodes(numberOfNodes)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .build();

    // make sure datanode.id file is correct
    File idPath = new File(
        conf.get(ScmConfigKeys.OZONE_SCM_DATANODE_ID));
    assertTrue(idPath.exists());
    List<DatanodeID> ids = ContainerUtils.readDatanodeIDsFrom(idPath);
    assertEquals(numberOfNodes, ids.size());

    List<DataNode> datanodes = cluster.getDataNodes();
    assertEquals(datanodes.size(), numberOfNodes);
    for(DataNode dn : datanodes) {
      // Each datanode ID should match an entry in the ID file
      assertTrue("Datanode ID not found in ID file",
          ids.contains(dn.getDatanodeId()));

      // Create a single member pipe line
      String containerName = OzoneUtils.getRequestID();
      DatanodeID dnId = dn.getDatanodeId();
      final PipelineChannel pipelineChannel =
          new PipelineChannel(dnId.getDatanodeUuid(),
              HdslProtos.LifeCycleState.OPEN,
              HdslProtos.ReplicationType.STAND_ALONE,
              HdslProtos.ReplicationFactor.ONE, "test");
      pipelineChannel.addMember(dnId);
      Pipeline pipeline = new Pipeline(containerName, pipelineChannel);

      // Verify client is able to connect to the container
      try (XceiverClient client = new XceiverClient(pipeline, conf)){
        client.connect();
        assertTrue(client.isConnected());
      }
    }
  }

  @Test
  public void testDatanodeIDPersistent() throws Exception {
    // Generate IDs for testing
    DatanodeID id1 = DFSTestUtil.getLocalDatanodeID(1);
    DatanodeID id2 = DFSTestUtil.getLocalDatanodeID(2);
    DatanodeID id3 = DFSTestUtil.getLocalDatanodeID(3);
    id1.setContainerPort(1);
    id2.setContainerPort(2);
    id3.setContainerPort(3);

    // Write a single ID to the file and read it out
    File validIdsFile = new File(WRITE_TMP, "valid-values.id");
    validIdsFile.delete();
    ContainerUtils.writeDatanodeIDTo(id1, validIdsFile);
    List<DatanodeID> validIds = ContainerUtils
        .readDatanodeIDsFrom(validIdsFile);
    assertEquals(1, validIds.size());
    DatanodeID id11 = validIds.iterator().next();
    assertEquals(id11, id1);
    assertEquals(id11.getProtoBufMessage(), id1.getProtoBufMessage());

    // Write should avoid duplicate entries
    File noDupIDFile = new File(WRITE_TMP, "no-dup-values.id");
    noDupIDFile.delete();
    ContainerUtils.writeDatanodeIDTo(id1, noDupIDFile);
    ContainerUtils.writeDatanodeIDTo(id1, noDupIDFile);
    ContainerUtils.writeDatanodeIDTo(id1, noDupIDFile);
    ContainerUtils.writeDatanodeIDTo(id2, noDupIDFile);
    ContainerUtils.writeDatanodeIDTo(id3, noDupIDFile);

    List<DatanodeID> noDupIDs =ContainerUtils
        .readDatanodeIDsFrom(noDupIDFile);
    assertEquals(3, noDupIDs.size());
    assertTrue(noDupIDs.contains(id1));
    assertTrue(noDupIDs.contains(id2));
    assertTrue(noDupIDs.contains(id3));

    // Write should fail if unable to create file or directory
    File invalidPath = new File(WRITE_TMP, "an/invalid/path");
    try {
      ContainerUtils.writeDatanodeIDTo(id1, invalidPath);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(e instanceof IOException);
    }

    // Read should return an empty value if file doesn't exist
    File nonExistFile = new File(READ_TMP, "non_exist.id");
    nonExistFile.delete();
    List<DatanodeID> emptyIDs =
        ContainerUtils.readDatanodeIDsFrom(nonExistFile);
    assertTrue(emptyIDs.isEmpty());

    // Read should fail if the file is malformed
    File malformedFile = new File(READ_TMP, "malformed.id");
    createMalformedIDFile(malformedFile);
    try {
      ContainerUtils.readDatanodeIDsFrom(malformedFile);
      fail("Read a malformed ID file should fail");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }

  @Test
  public void testContainerRandomPort() throws IOException {
    Configuration ozoneConf = SCMTestUtils.getConf();
    File testDir = PathUtils.getTestDir(TestOzoneContainer.class);
    ozoneConf.set(DFS_DATANODE_DATA_DIR_KEY, testDir.getAbsolutePath());
    ozoneConf.set(OZONE_METADATA_DIRS,
        TEST_ROOT.toString());

    // Each instance of SM will create an ozone container
    // that bounds to a random port.
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, true);
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    try (
        DatanodeStateMachine sm1 = new DatanodeStateMachine(
            DFSTestUtil.getLocalDatanodeID(), ozoneConf);
        DatanodeStateMachine sm2 = new DatanodeStateMachine(
            DFSTestUtil.getLocalDatanodeID(), ozoneConf);
        DatanodeStateMachine sm3 = new DatanodeStateMachine(
            DFSTestUtil.getLocalDatanodeID(), ozoneConf);
    ) {
      HashSet<Integer> ports = new HashSet<Integer>();
      assertTrue(ports.add(sm1.getContainer().getContainerServerPort()));
      assertTrue(ports.add(sm2.getContainer().getContainerServerPort()));
      assertTrue(ports.add(sm3.getContainer().getContainerServerPort()));

      // Assert that ratis is also on a different port.
      assertTrue(ports.add(sm1.getContainer().getRatisContainerServerPort()));
      assertTrue(ports.add(sm2.getContainer().getRatisContainerServerPort()));
      assertTrue(ports.add(sm3.getContainer().getRatisContainerServerPort()));


    }

    // Turn off the random port flag and test again
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);
    try (
        DatanodeStateMachine sm1 = new DatanodeStateMachine(
            DFSTestUtil.getLocalDatanodeID(), ozoneConf);
        DatanodeStateMachine sm2 = new DatanodeStateMachine(
            DFSTestUtil.getLocalDatanodeID(), ozoneConf);
        DatanodeStateMachine sm3 = new DatanodeStateMachine(
            DFSTestUtil.getLocalDatanodeID(), ozoneConf);
    ) {
      HashSet<Integer> ports = new HashSet<Integer>();
      assertTrue(ports.add(sm1.getContainer().getContainerServerPort()));
      assertFalse(ports.add(sm2.getContainer().getContainerServerPort()));
      assertFalse(ports.add(sm3.getContainer().getContainerServerPort()));
      assertEquals(ports.iterator().next().intValue(),
          conf.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
              OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT));
    }
  }

  private void createMalformedIDFile(File malformedFile)
      throws IOException{
    malformedFile.delete();
    DatanodeID id1 = DFSTestUtil.getLocalDatanodeID(1);
    ContainerUtils.writeDatanodeIDTo(id1, malformedFile);

    FileOutputStream out = new FileOutputStream(malformedFile);
    out.write("malformed".getBytes());
    out.close();
  }
}
