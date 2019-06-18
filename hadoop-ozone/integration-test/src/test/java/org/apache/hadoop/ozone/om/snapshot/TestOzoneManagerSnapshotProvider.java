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

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.UUID;

/**
 * Test OM's snapshot provider service.
 */
public class TestOzoneManagerSnapshotProvider {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private int numOfOMs = 3;

  @Rule
  public Timeout timeout = new Timeout(300_000);

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    conf.setBoolean(OMConfigKeys.OZONE_OM_HTTP_ENABLED_KEY, true);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
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
  public void testDownloadCheckpoint() throws Exception {
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

    String leaderOMNodeId = objectStore.getClientProxy().getOMProxyProvider()
        .getCurrentProxyOMNodeId();
    OzoneManager ozoneManager = cluster.getOzoneManager(leaderOMNodeId);

    // Get a follower OM
    String followerNodeId = ozoneManager.getPeerNodes().get(0).getOMNodeId();
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Download latest checkpoint from leader OM to follower OM
    DBCheckpoint omSnapshot = followerOM.getOmSnapshotProvider()
        .getOzoneManagerDBSnapshot(leaderOMNodeId);

    long leaderSnapshotIndex = ozoneManager.loadRatisSnapshotIndex();
    long downloadedSnapshotIndex = omSnapshot.getRatisSnapshotIndex();

    // The snapshot index downloaded from leader OM should match the ratis
    // snapshot index on the leader OM
    Assert.assertEquals("The snapshot index downloaded from leader OM does " +
        "not match its ratis snapshot index",
        leaderSnapshotIndex, downloadedSnapshotIndex);
  }
}