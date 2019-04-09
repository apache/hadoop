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

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests Freon, with MiniOzoneCluster and validate data.
 */
public abstract class TestDataValidate {

  private static MiniOzoneCluster cluster = null;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    conf.set(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, "5000ms");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void ratisTestLargeKey() throws Exception {
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

  @Test
  public void standaloneTestLargeKey() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(1);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(1);
    randomKeyGenerator.setKeySize(20971520);
    randomKeyGenerator.setValidateWrites(true);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.call();
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
  }

  @Test
  public void validateWriteTest() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator((OzoneConfiguration) cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(2);
    randomKeyGenerator.setNumOfBuckets(5);
    randomKeyGenerator.setNumOfKeys(10);
    randomKeyGenerator.setValidateWrites(true);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.call();
    Assert.assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertTrue(randomKeyGenerator.getValidateWrites());
    Assert.assertNotEquals(0, randomKeyGenerator.getTotalKeysValidated());
    Assert.assertNotEquals(0, randomKeyGenerator
        .getSuccessfulValidationCount());
    Assert.assertEquals(0, randomKeyGenerator
        .getUnsuccessfulValidationCount());
  }
}
