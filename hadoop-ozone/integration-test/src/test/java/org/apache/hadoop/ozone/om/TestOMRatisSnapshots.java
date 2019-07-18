/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.TestOzoneManagerHA.createKey;

/**
 * Tests the Ratis snaphsots feature in OM.
 */
public class TestOMRatisSnapshots {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private int numOfOMs = 3;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(500_000);

  /**
   * Create a MiniOzoneCluster for testing. The cluster initially has one
   * inactive OM. So at the start of the cluster, there will be 2 active and 1
   * inactive OM.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfActiveOMs(2)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInstallSnapshot() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = objectStore.getClientProxy().getOMProxyProvider()
        .getCurrentProxyOMNodeId();
    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getOMNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getOMNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName);
    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    long leaderOMappliedLogIndex =
        leaderRatisServer.getStateMachineLastAppliedIndex();
    leaderOM.getOmRatisServer().getStateMachineLastAppliedIndex();

    List<String> keys = new ArrayList<>();
    while (leaderOMappliedLogIndex < 2000) {
      keys.add(createKey(ozoneBucket));
      leaderOMappliedLogIndex =
          leaderRatisServer.getStateMachineLastAppliedIndex();
    }

    // Get the latest db checkpoint from the leader OM.
    long leaderOMSnaphsotIndex = leaderOM.saveRatisSnapshot(true);
    DBCheckpoint leaderDbCheckpoint =
        leaderOM.getMetadataManager().getStore().getCheckpoint(false);

    // Start the inactive OM
    cluster.startInactiveOM(followerNodeId);

    // The recently started OM should be lagging behind the leader OM.
    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getStateMachineLastAppliedIndex();
    Assert.assertTrue(
        followerOMLastAppliedIndex < leaderOMSnaphsotIndex);

    // Install leader OM's db checkpoint on the lagging OM.
    followerOM.getOmRatisServer().getOmStateMachine().pause();
    followerOM.getMetadataManager().getStore().close();
    followerOM.replaceOMDBWithCheckpoint(
        leaderOMSnaphsotIndex, leaderDbCheckpoint.getCheckpointLocation());

    // Reload the follower OM with new DB checkpoint from the leader OM.
    followerOM.reloadOMState(leaderOMSnaphsotIndex);
    followerOM.getOmRatisServer().getOmStateMachine().unpause(
        leaderOMSnaphsotIndex);

    // After the new checkpoint is loaded and state machine is unpaused, the
    // follower OM lastAppliedIndex must match the snapshot index of the
    // checkpoint.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getStateMachineLastAppliedIndex();
    Assert.assertEquals(leaderOMSnaphsotIndex, followerOMLastAppliedIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    Assert.assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    Assert.assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      Assert.assertNotNull(followerOMMetaMngr.getKeyTable().get(
          followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
  }
}
