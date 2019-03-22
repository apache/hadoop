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

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.
    HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.
    HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
    HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
    OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests the validity BCSID of a container.
 */
public class TestBCSID {

  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    String path = GenericTestUtils
        .getTempPath(TestBCSID.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).setHbInterval(200)
            .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "bcsid";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBCSID() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    key.write("ratis".getBytes());
    key.close();

    // get the name of a valid container.
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).
        setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName("ratis")
        .setRefreshPipeline(true)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    Assert.assertEquals(1, keyLocationInfos.size());
    OmKeyLocationInfo omKeyLocationInfo = keyLocationInfos.get(0);

    long blockCommitSequenceId =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerReport().getBlockCommitSequenceId();
    Assert.assertTrue(blockCommitSequenceId > 0);

    // make sure the persisted block Id in OM is same as that seen in the
    // container report to be reported to SCM.
    Assert.assertEquals(blockCommitSequenceId,
        omKeyLocationInfo.getBlockCommitSequenceId());

    // verify that on restarting the datanode, it reloads the BCSID correctly.
    cluster.restartHddsDatanode(0, true);
    Assert.assertEquals(blockCommitSequenceId,
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerReport().getBlockCommitSequenceId());
  }
}