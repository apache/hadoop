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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.ksm;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.io.IOException;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Test for KSM metrics.
 */
public class TestKSMMetrcis {
  private MiniOzoneCluster cluster;
  private KeySpaceManager ksmManager;

  /**
   * The exception used for testing failure metrics.
   */
  private IOException exception = new IOException();

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    ksmManager = cluster.getKeySpaceManager();
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
  public void testVolumeOps() throws IOException {
    VolumeManager volumeManager = (VolumeManager) Whitebox
        .getInternalState(ksmManager, "volumeManager");
    VolumeManager mockVm = Mockito.spy(volumeManager);

    Mockito.doNothing().when(mockVm).createVolume(null);
    Mockito.doNothing().when(mockVm).deleteVolume(null);
    Mockito.doReturn(null).when(mockVm).getVolumeInfo(null);
    Mockito.doReturn(true).when(mockVm).checkVolumeAccess(null, null);
    Mockito.doNothing().when(mockVm).setOwner(null, null);
    Mockito.doReturn(null).when(mockVm).listVolumes(null, null, null, 0);

    Whitebox.setInternalState(ksmManager, "volumeManager", mockVm);
    doVolumeOps();

    MetricsRecordBuilder ksmMetrics = getMetrics("KSMMetrics");
    assertCounter("NumVolumeOps", 6L, ksmMetrics);
    assertCounter("NumVolumeCreates", 1L, ksmMetrics);
    assertCounter("NumVolumeUpdates", 1L, ksmMetrics);
    assertCounter("NumVolumeInfos", 1L, ksmMetrics);
    assertCounter("NumVolumeCheckAccesses", 1L, ksmMetrics);
    assertCounter("NumVolumeDeletes", 1L, ksmMetrics);
    assertCounter("NumVolumeLists", 1L, ksmMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockVm).createVolume(null);
    Mockito.doThrow(exception).when(mockVm).deleteVolume(null);
    Mockito.doThrow(exception).when(mockVm).getVolumeInfo(null);
    Mockito.doThrow(exception).when(mockVm).checkVolumeAccess(null, null);
    Mockito.doThrow(exception).when(mockVm).setOwner(null, null);
    Mockito.doThrow(exception).when(mockVm).listVolumes(null, null, null, 0);

    Whitebox.setInternalState(ksmManager, "volumeManager", mockVm);
    doVolumeOps();

    ksmMetrics = getMetrics("KSMMetrics");
    assertCounter("NumVolumeOps", 12L, ksmMetrics);
    assertCounter("NumVolumeCreates", 2L, ksmMetrics);
    assertCounter("NumVolumeUpdates", 2L, ksmMetrics);
    assertCounter("NumVolumeInfos", 2L, ksmMetrics);
    assertCounter("NumVolumeCheckAccesses", 2L, ksmMetrics);
    assertCounter("NumVolumeDeletes", 2L, ksmMetrics);
    assertCounter("NumVolumeLists", 2L, ksmMetrics);

    assertCounter("NumVolumeCreateFails", 1L, ksmMetrics);
    assertCounter("NumVolumeUpdateFails", 1L, ksmMetrics);
    assertCounter("NumVolumeInfoFails", 1L, ksmMetrics);
    assertCounter("NumVolumeCheckAccessFails", 1L, ksmMetrics);
    assertCounter("NumVolumeDeleteFails", 1L, ksmMetrics);
    assertCounter("NumVolumeListFails", 1L, ksmMetrics);
  }

  @Test
  public void testBucketOps() throws IOException {
    BucketManager bucketManager = (BucketManager) Whitebox
        .getInternalState(ksmManager, "bucketManager");
    BucketManager mockBm = Mockito.spy(bucketManager);

    Mockito.doNothing().when(mockBm).createBucket(null);
    Mockito.doNothing().when(mockBm).deleteBucket(null, null);
    Mockito.doReturn(null).when(mockBm).getBucketInfo(null, null);
    Mockito.doNothing().when(mockBm).setBucketProperty(null);
    Mockito.doReturn(null).when(mockBm).listBuckets(null, null, null, 0);

    Whitebox.setInternalState(ksmManager, "bucketManager", mockBm);
    doBucketOps();

    MetricsRecordBuilder ksmMetrics = getMetrics("KSMMetrics");
    assertCounter("NumBucketOps", 5L, ksmMetrics);
    assertCounter("NumBucketCreates", 1L, ksmMetrics);
    assertCounter("NumBucketUpdates", 1L, ksmMetrics);
    assertCounter("NumBucketInfos", 1L, ksmMetrics);
    assertCounter("NumBucketDeletes", 1L, ksmMetrics);
    assertCounter("NumBucketLists", 1L, ksmMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockBm).createBucket(null);
    Mockito.doThrow(exception).when(mockBm).deleteBucket(null, null);
    Mockito.doThrow(exception).when(mockBm).getBucketInfo(null, null);
    Mockito.doThrow(exception).when(mockBm).setBucketProperty(null);
    Mockito.doThrow(exception).when(mockBm).listBuckets(null, null, null, 0);

    Whitebox.setInternalState(ksmManager, "bucketManager", mockBm);
    doBucketOps();

    ksmMetrics = getMetrics("KSMMetrics");
    assertCounter("NumBucketOps", 10L, ksmMetrics);
    assertCounter("NumBucketCreates", 2L, ksmMetrics);
    assertCounter("NumBucketUpdates", 2L, ksmMetrics);
    assertCounter("NumBucketInfos", 2L, ksmMetrics);
    assertCounter("NumBucketDeletes", 2L, ksmMetrics);
    assertCounter("NumBucketLists", 2L, ksmMetrics);

    assertCounter("NumBucketCreateFails", 1L, ksmMetrics);
    assertCounter("NumBucketUpdateFails", 1L, ksmMetrics);
    assertCounter("NumBucketInfoFails", 1L, ksmMetrics);
    assertCounter("NumBucketDeleteFails", 1L, ksmMetrics);
    assertCounter("NumBucketListFails", 1L, ksmMetrics);
  }

  @Test
  public void testKeyOps() throws IOException {
    KeyManager bucketManager = (KeyManager) Whitebox
        .getInternalState(ksmManager, "keyManager");
    KeyManager mockKm = Mockito.spy(bucketManager);

    Mockito.doReturn(null).when(mockKm).openKey(null);
    Mockito.doNothing().when(mockKm).deleteKey(null);
    Mockito.doReturn(null).when(mockKm).lookupKey(null);
    Mockito.doReturn(null).when(mockKm).listKeys(null, null, null, null, 0);

    Whitebox.setInternalState(ksmManager, "keyManager", mockKm);
    doKeyOps();

    MetricsRecordBuilder ksmMetrics = getMetrics("KSMMetrics");
    assertCounter("NumKeyOps", 4L, ksmMetrics);
    assertCounter("NumKeyAllocate", 1L, ksmMetrics);
    assertCounter("NumKeyLookup", 1L, ksmMetrics);
    assertCounter("NumKeyDeletes", 1L, ksmMetrics);
    assertCounter("NumKeyLists", 1L, ksmMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockKm).openKey(null);
    Mockito.doThrow(exception).when(mockKm).deleteKey(null);
    Mockito.doThrow(exception).when(mockKm).lookupKey(null);
    Mockito.doThrow(exception).when(mockKm).listKeys(
        null, null, null, null, 0);

    Whitebox.setInternalState(ksmManager, "keyManager", mockKm);
    doKeyOps();

    ksmMetrics = getMetrics("KSMMetrics");
    assertCounter("NumKeyOps", 8L, ksmMetrics);
    assertCounter("NumKeyAllocate", 2L, ksmMetrics);
    assertCounter("NumKeyLookup", 2L, ksmMetrics);
    assertCounter("NumKeyDeletes", 2L, ksmMetrics);
    assertCounter("NumKeyLists", 2L, ksmMetrics);

    assertCounter("NumKeyAllocateFails", 1L, ksmMetrics);
    assertCounter("NumKeyLookupFails", 1L, ksmMetrics);
    assertCounter("NumKeyDeleteFails", 1L, ksmMetrics);
    assertCounter("NumKeyListFails", 1L, ksmMetrics);
  }

  /**
   * Test volume operations with ignoring thrown exception.
   */
  private void doVolumeOps() {
    try {
      ksmManager.createVolume(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.deleteVolume(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.getVolumeInfo(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.checkVolumeAccess(null, null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.setOwner(null, null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.listAllVolumes(null, null, 0);
    } catch (IOException ignored) {
    }
  }

  /**
   * Test bucket operations with ignoring thrown exception.
   */
  private void doBucketOps() {
    try {
      ksmManager.createBucket(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.deleteBucket(null, null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.getBucketInfo(null, null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.setBucketProperty(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.listBuckets(null, null, null, 0);
    } catch (IOException ignored) {
    }
  }

  /**
   * Test key operations with ignoring thrown exception.
   */
  private void doKeyOps() {
    try {
      ksmManager.openKey(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.deleteKey(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.lookupKey(null);
    } catch (IOException ignored) {
    }

    try {
      ksmManager.listKeys(null, null, null, null, 0);
    } catch (IOException ignored) {
    }
  }
}
