/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public class TestRandomKeyGenerator {

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
    conf.set(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, "5000ms");
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
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

  @Test
  public void defaultTest() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(2);
    randomKeyGenerator.setNumOfBuckets(5);
    randomKeyGenerator.setNumOfKeys(10);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.call();
    Assert.assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  public void multiThread() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(10);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(10);
    randomKeyGenerator.setNumOfThreads(10);
    randomKeyGenerator.setKeySize(10240);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.call();
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  public void ratisTest3() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(10);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(10);
    randomKeyGenerator.setNumOfThreads(10);
    randomKeyGenerator.setKeySize(10240);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.call();
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  public void bigFileThan2GB() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(1);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(1);
    randomKeyGenerator.setNumOfThreads(1);
    randomKeyGenerator.setKeySize(10L + Integer.MAX_VALUE);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setValidateWrites(true);
    randomKeyGenerator.call();
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(1, randomKeyGenerator.getSuccessfulValidationCount());
  }

  @Test
  public void fileWithSizeZero() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(1);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(1);
    randomKeyGenerator.setNumOfThreads(1);
    randomKeyGenerator.setKeySize(0);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setValidateWrites(true);
    randomKeyGenerator.call();
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(1, randomKeyGenerator.getSuccessfulValidationCount());
  }

  @Test
  public void testThreadPoolSize() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(1);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(1);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setNumOfThreads(10);
    randomKeyGenerator.call();
    Assert.assertEquals(10, randomKeyGenerator.getThreadPoolSize());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
  }
}
