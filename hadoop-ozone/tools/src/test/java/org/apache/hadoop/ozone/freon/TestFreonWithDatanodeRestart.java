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

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests Freon with Datanode restarts.
 */
public class TestFreonWithDatanodeRestart {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 5, TimeUnit.SECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, 1,
        TimeUnit.SECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, 1,
        TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT, 5,
        TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
      .setHbProcessorInterval(1000)
      .setHbInterval(1000)
      .setNumDatanodes(3)
      .build();
    cluster.waitForClusterToBeReady();
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

  @Ignore
  // Pipeline close is not happening now, this requires HDDS-801 and
  // pipeline teardown logic in place. Enable this once those things are in
  // place
  @Test
  public void testRestart() throws Exception {
    startFreon();
    cluster.restartHddsDatanode(0, true);
    startFreon();
  }

  private void startFreon() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(1);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(1);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setKeySize(20971520);
    randomKeyGenerator.setValidateWrites(true);
    randomKeyGenerator.call();
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
  }
}
