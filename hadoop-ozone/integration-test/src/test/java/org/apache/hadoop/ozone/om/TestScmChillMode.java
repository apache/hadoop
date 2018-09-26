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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestStorageContainerManagerHelper;
import org.apache.hadoop.ozone.om.exceptions.OMException;
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

import static org.junit.Assert.fail;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
public class TestScmChillMode {

  private static MiniOzoneCluster cluster = null;
  private static MiniOzoneCluster.Builder builder = null;
  private static OzoneConfiguration conf;
  private static OzoneManager om;


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
  public void testChillModeOperations() throws Exception {
    final AtomicReference<MiniOzoneCluster> miniCluster =
        new AtomicReference<>();

    try {
      // Create {numKeys} random names keys.
      TestStorageContainerManagerHelper helper =
          new TestStorageContainerManagerHelper(cluster, conf);
      Map<String, OmKeyInfo> keyLocations = helper.createKeys(100, 4096);
      final List<ContainerInfo> containers = cluster
          .getStorageContainerManager()
          .getScmContainerManager().getStateManager().getAllContainers();
      GenericTestUtils.waitFor(() -> {
        return containers.size() > 10;
      }, 100, 1000);

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

      new Thread(() -> {
        try {
          miniCluster.set(builder.build());
        } catch (IOException e) {
          fail("failed");
        }
      }).start();

      StorageContainerManager scm;
      GenericTestUtils.waitFor(() -> {
        return miniCluster.get() != null;
      }, 100, 1000 * 3);

      scm = miniCluster.get().getStorageContainerManager();
      Assert.assertTrue(scm.isInChillMode());

      om = miniCluster.get().getOzoneManager();

      LambdaTestUtils.intercept(OMException.class,
          "ChillModePrecheck failed for allocateBlock",
          () -> om.openKey(keyArgs));

    } finally {
      if (miniCluster.get() != null) {
        try {
          miniCluster.get().shutdown();
        } catch (Exception e) {
          // do nothing.
        }
      }
    }
  }
}
