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
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.chillmode.SCMChillModeManager;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestStorageContainerManagerHelper;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
public class TestScmChillMode {

  private final static Logger LOG = LoggerFactory
      .getLogger(TestScmChillMode.class);
  private static MiniOzoneCluster cluster = null;
  private static MiniOzoneCluster.Builder builder = null;
  private static OzoneConfiguration conf;
  private static OzoneManager om;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;


  @Rule
  public Timeout timeout = new Timeout(1000 * 200);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    builder = MiniOzoneCluster.newBuilder(conf)
        .setHbInterval(1000)
        .setHbProcessorInterval(500)
        .setStartDataNodes(false);
    cluster = builder.build();
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    om = cluster.getOzoneManager();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      try {
        cluster.shutdown();
      } catch (Exception e) {
        // do nothing.
      }
    }
  }

  @Test(timeout = 300_000)
  public void testChillModeOperations() throws Exception {
    // Create {numKeys} random names keys.
    TestStorageContainerManagerHelper helper =
        new TestStorageContainerManagerHelper(cluster, conf);
    Map<String, OmKeyInfo> keyLocations = helper.createKeys(100, 4096);
    final List<ContainerInfo> containers = cluster
        .getStorageContainerManager().getContainerManager().getContainers();
    GenericTestUtils.waitFor(() -> containers.size() >= 3, 100, 1000);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(1000)
        .build();
    OmVolumeArgs volArgs = new OmVolumeArgs.Builder()
        .setAdminName(adminName)
        .setCreationTime(Time.monotonicNow())
        .setQuotaInBytes(10000)
        .setVolume(volumeName)
        .setOwnerName(userName)
        .build();
    OmBucketInfo bucketInfo = new OmBucketInfo.Builder()
        .setBucketName(bucketName)
        .setIsVersionEnabled(false)
        .setVolumeName(volumeName)
        .build();
    om.createVolume(volArgs);
    om.createBucket(bucketInfo);
    om.openKey(keyArgs);
    //om.commitKey(keyArgs, 1);

    cluster.stop();

    try {
      cluster = builder.build();
    } catch (IOException e) {
      fail("failed");
    }


    StorageContainerManager scm;

    scm = cluster.getStorageContainerManager();
    Assert.assertTrue(scm.isInChillMode());

    om = cluster.getOzoneManager();

// As cluster is restarted with out datanodes restart
    LambdaTestUtils.intercept(IOException.class,
        "ChillModePrecheck failed for allocateBlock",
        () -> om.openKey(keyArgs));
  }

  /**
   * Tests inChillMode & forceExitChillMode api calls.
   */
  @Test(timeout = 300_000)
  public void testIsScmInChillModeAndForceExit() throws Exception {
    // Test 1: SCM should be out of chill mode.
    Assert.assertFalse(storageContainerLocationClient.inChillMode());
    cluster.stop();
    // Restart the cluster with same metadata dir.

    try {
      cluster = builder.build();
    } catch (IOException e) {
      Assert.fail("Cluster startup failed.");
    }

    // Test 2: Scm should be in chill mode as datanodes are not started yet.
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    Assert.assertTrue(storageContainerLocationClient.inChillMode());
    // Force scm out of chill mode.
    cluster.getStorageContainerManager().getClientProtocolServer()
        .forceExitChillMode();
    // Test 3: SCM should be out of chill mode.
    GenericTestUtils.waitFor(() -> {
      try {
        return !cluster.getStorageContainerManager().getClientProtocolServer()
            .inChillMode();
      } catch (IOException e) {
        Assert.fail("Cluster");
        return false;
      }
    }, 10, 1000 * 5);

  }

  @Test(timeout = 300_000)
  public void testSCMChillMode() throws Exception {
    // Test1: Test chill mode  when there are no containers in system.
    cluster.stop();

    try {
      cluster = builder.build();
    } catch (IOException e) {
      Assert.fail("Cluster startup failed.");
    }
    assertTrue(cluster.getStorageContainerManager().isInChillMode());
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    assertFalse(cluster.getStorageContainerManager().isInChillMode());

    // Test2: Test chill mode  when containers are there in system.
    // Create {numKeys} random names keys.
    TestStorageContainerManagerHelper helper =
        new TestStorageContainerManagerHelper(cluster, conf);
    Map<String, OmKeyInfo> keyLocations = helper.createKeys(100 * 2, 4096);
    final List<ContainerInfo> containers = cluster
        .getStorageContainerManager().getContainerManager().getContainers();
    GenericTestUtils.waitFor(() -> containers.size() >= 3, 100, 1000 * 30);

    // Removing some container to keep them open.
    containers.remove(0);
    containers.remove(0);

    // Close remaining containers
    SCMContainerManager mapping = (SCMContainerManager) cluster
        .getStorageContainerManager().getContainerManager();
    containers.forEach(c -> {
      try {
        mapping.updateContainerState(c.containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
        mapping.updateContainerState(c.containerID(),
            LifeCycleEvent.CLOSE);
      } catch (IOException e) {
        LOG.info("Failed to change state of open containers.", e);
      }
    });
    cluster.stop();

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(SCMChillModeManager.getLogger());
    logCapturer.clearOutput();

    try {
      cluster = builder.build();
    } catch (IOException ex) {
      fail("failed");
    }

    StorageContainerManager scm;

    scm = cluster.getStorageContainerManager();
    assertTrue(scm.isInChillMode());
    assertFalse(logCapturer.getOutput().contains("SCM exiting chill mode."));
    assertTrue(scm.getCurrentContainerThreshold() == 0);
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      dn.start(null);
    }
    GenericTestUtils
        .waitFor(() -> scm.getCurrentContainerThreshold() == 1.0, 100, 20000);

    double chillModeCutoff = conf
        .getDouble(HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT,
            HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT_DEFAULT);
    assertTrue(scm.getCurrentContainerThreshold() >= chillModeCutoff);
    assertTrue(logCapturer.getOutput().contains("SCM exiting chill mode."));
    assertFalse(scm.isInChillMode());
  }

  @Test(timeout = 300_000)
  public void testSCMChillModeRestrictedOp() throws Exception {
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
        OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB);
    cluster.stop();
    cluster = builder.build();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    assertTrue(scm.isInChillMode());

    LambdaTestUtils.intercept(SCMException.class,
        "ChillModePrecheck failed for allocateContainer", () -> {
          scm.getClientProtocolServer()
              .allocateContainer(ReplicationType.STAND_ALONE,
                  ReplicationFactor.ONE, "");
        });

    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();
    assertFalse(scm.isInChillMode());

    TestStorageContainerManagerHelper helper =
        new TestStorageContainerManagerHelper(cluster, conf);
    helper.createKeys(10, 4096);
    SCMClientProtocolServer clientProtocolServer = cluster
        .getStorageContainerManager().getClientProtocolServer();
    assertFalse((scm.getClientProtocolServer()).getChillModeStatus());
    final List<ContainerInfo> containers = scm.getContainerManager()
        .getContainers();
    scm.getEventQueue().fireEvent(SCMEvents.CHILL_MODE_STATUS,
        new SCMChillModeManager.ChillModeStatus(true));
    GenericTestUtils.waitFor(() -> {
      return clientProtocolServer.getChillModeStatus();
    }, 50, 1000 * 30);
    assertTrue(clientProtocolServer.getChillModeStatus());

    LambdaTestUtils.intercept(SCMException.class,
        "Open container " + containers.get(0).getContainerID() + " "
            + "doesn't have enough replicas to service this operation in Chill"
            + " mode.", () -> clientProtocolServer
            .getContainerWithPipeline(containers.get(0).getContainerID()));
  }

  @Test(timeout = 300_000)
  public void testSCMChillModeDisabled() throws Exception {
    cluster.stop();

    // If chill mode is disabled, cluster should not be in chill mode even if
    // min number of datanodes are not started.
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED, false);
    conf.setInt(HddsConfigKeys.HDDS_SCM_CHILLMODE_MIN_DATANODE, 3);
    builder = MiniOzoneCluster.newBuilder(conf)
        .setHbInterval(1000)
        .setHbProcessorInterval(500)
        .setNumDatanodes(1);
    cluster = builder.build();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    assertFalse(scm.isInChillMode());

    // Even on SCM restart, cluster should be out of chill mode immediately.
    cluster.restartStorageContainerManager();
    assertFalse(scm.isInChillMode());
  }
}
