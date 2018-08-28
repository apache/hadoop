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
package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.io.IOException;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for OM metrics.
 */
@SuppressWarnings("deprecation")
public class TestOmMetrics {
  private MiniOzoneCluster cluster;
  private OzoneManager ozoneManager;

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
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
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
    VolumeManager volumeManager =
        (VolumeManager) org.apache.hadoop.test.Whitebox
            .getInternalState(ozoneManager, "volumeManager");
    VolumeManager mockVm = Mockito.spy(volumeManager);

    Mockito.doNothing().when(mockVm).createVolume(null);
    Mockito.doNothing().when(mockVm).deleteVolume(null);
    Mockito.doReturn(null).when(mockVm).getVolumeInfo(null);
    Mockito.doReturn(true).when(mockVm).checkVolumeAccess(null, null);
    Mockito.doNothing().when(mockVm).setOwner(null, null);
    Mockito.doReturn(null).when(mockVm).listVolumes(null, null, null, 0);

    org.apache.hadoop.test.Whitebox.setInternalState(
        ozoneManager, "volumeManager", mockVm);
    doVolumeOps();

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumVolumeOps", 6L, omMetrics);
    assertCounter("NumVolumeCreates", 1L, omMetrics);
    assertCounter("NumVolumeUpdates", 1L, omMetrics);
    assertCounter("NumVolumeInfos", 1L, omMetrics);
    assertCounter("NumVolumeCheckAccesses", 1L, omMetrics);
    assertCounter("NumVolumeDeletes", 1L, omMetrics);
    assertCounter("NumVolumeLists", 1L, omMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockVm).createVolume(null);
    Mockito.doThrow(exception).when(mockVm).deleteVolume(null);
    Mockito.doThrow(exception).when(mockVm).getVolumeInfo(null);
    Mockito.doThrow(exception).when(mockVm).checkVolumeAccess(null, null);
    Mockito.doThrow(exception).when(mockVm).setOwner(null, null);
    Mockito.doThrow(exception).when(mockVm).listVolumes(null, null, null, 0);

    org.apache.hadoop.test.Whitebox.setInternalState(ozoneManager,
        "volumeManager", mockVm);
    doVolumeOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumVolumeOps", 12L, omMetrics);
    assertCounter("NumVolumeCreates", 2L, omMetrics);
    assertCounter("NumVolumeUpdates", 2L, omMetrics);
    assertCounter("NumVolumeInfos", 2L, omMetrics);
    assertCounter("NumVolumeCheckAccesses", 2L, omMetrics);
    assertCounter("NumVolumeDeletes", 2L, omMetrics);
    assertCounter("NumVolumeLists", 2L, omMetrics);

    assertCounter("NumVolumeCreateFails", 1L, omMetrics);
    assertCounter("NumVolumeUpdateFails", 1L, omMetrics);
    assertCounter("NumVolumeInfoFails", 1L, omMetrics);
    assertCounter("NumVolumeCheckAccessFails", 1L, omMetrics);
    assertCounter("NumVolumeDeleteFails", 1L, omMetrics);
    assertCounter("NumVolumeListFails", 1L, omMetrics);
  }

  @Test
  public void testBucketOps() throws IOException {
    BucketManager bucketManager =
        (BucketManager) org.apache.hadoop.test.Whitebox
            .getInternalState(ozoneManager, "bucketManager");
    BucketManager mockBm = Mockito.spy(bucketManager);

    Mockito.doNothing().when(mockBm).createBucket(null);
    Mockito.doNothing().when(mockBm).deleteBucket(null, null);
    Mockito.doReturn(null).when(mockBm).getBucketInfo(null, null);
    Mockito.doNothing().when(mockBm).setBucketProperty(null);
    Mockito.doReturn(null).when(mockBm).listBuckets(null, null, null, 0);

    org.apache.hadoop.test.Whitebox.setInternalState(
        ozoneManager, "bucketManager", mockBm);
    doBucketOps();

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 5L, omMetrics);
    assertCounter("NumBucketCreates", 1L, omMetrics);
    assertCounter("NumBucketUpdates", 1L, omMetrics);
    assertCounter("NumBucketInfos", 1L, omMetrics);
    assertCounter("NumBucketDeletes", 1L, omMetrics);
    assertCounter("NumBucketLists", 1L, omMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockBm).createBucket(null);
    Mockito.doThrow(exception).when(mockBm).deleteBucket(null, null);
    Mockito.doThrow(exception).when(mockBm).getBucketInfo(null, null);
    Mockito.doThrow(exception).when(mockBm).setBucketProperty(null);
    Mockito.doThrow(exception).when(mockBm).listBuckets(null, null, null, 0);

    org.apache.hadoop.test.Whitebox.setInternalState(
        ozoneManager, "bucketManager", mockBm);
    doBucketOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 10L, omMetrics);
    assertCounter("NumBucketCreates", 2L, omMetrics);
    assertCounter("NumBucketUpdates", 2L, omMetrics);
    assertCounter("NumBucketInfos", 2L, omMetrics);
    assertCounter("NumBucketDeletes", 2L, omMetrics);
    assertCounter("NumBucketLists", 2L, omMetrics);

    assertCounter("NumBucketCreateFails", 1L, omMetrics);
    assertCounter("NumBucketUpdateFails", 1L, omMetrics);
    assertCounter("NumBucketInfoFails", 1L, omMetrics);
    assertCounter("NumBucketDeleteFails", 1L, omMetrics);
    assertCounter("NumBucketListFails", 1L, omMetrics);
  }

  @Test
  public void testKeyOps() throws IOException {
    KeyManager bucketManager = (KeyManager) org.apache.hadoop.test.Whitebox
        .getInternalState(ozoneManager, "keyManager");
    KeyManager mockKm = Mockito.spy(bucketManager);

    Mockito.doReturn(null).when(mockKm).openKey(null);
    Mockito.doNothing().when(mockKm).deleteKey(null);
    Mockito.doReturn(null).when(mockKm).lookupKey(null);
    Mockito.doReturn(null).when(mockKm).listKeys(null, null, null, null, 0);

    org.apache.hadoop.test.Whitebox.setInternalState(
        ozoneManager, "keyManager", mockKm);
    doKeyOps();

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 4L, omMetrics);
    assertCounter("NumKeyAllocate", 1L, omMetrics);
    assertCounter("NumKeyLookup", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 1L, omMetrics);
    assertCounter("NumKeyLists", 1L, omMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockKm).openKey(null);
    Mockito.doThrow(exception).when(mockKm).deleteKey(null);
    Mockito.doThrow(exception).when(mockKm).lookupKey(null);
    Mockito.doThrow(exception).when(mockKm).listKeys(
        null, null, null, null, 0);

    org.apache.hadoop.test.Whitebox.setInternalState(
        ozoneManager, "keyManager", mockKm);
    doKeyOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 8L, omMetrics);
    assertCounter("NumKeyAllocate", 2L, omMetrics);
    assertCounter("NumKeyLookup", 2L, omMetrics);
    assertCounter("NumKeyDeletes", 2L, omMetrics);
    assertCounter("NumKeyLists", 2L, omMetrics);

    assertCounter("NumKeyAllocateFails", 1L, omMetrics);
    assertCounter("NumKeyLookupFails", 1L, omMetrics);
    assertCounter("NumKeyDeleteFails", 1L, omMetrics);
    assertCounter("NumKeyListFails", 1L, omMetrics);
  }

  /**
   * Test volume operations with ignoring thrown exception.
   */
  private void doVolumeOps() {
    try {
      ozoneManager.createVolume(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.deleteVolume(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.getVolumeInfo(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.checkVolumeAccess(null, null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.setOwner(null, null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listAllVolumes(null, null, 0);
    } catch (IOException ignored) {
    }
  }

  /**
   * Test bucket operations with ignoring thrown exception.
   */
  private void doBucketOps() {
    try {
      ozoneManager.createBucket(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.deleteBucket(null, null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.getBucketInfo(null, null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.setBucketProperty(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listBuckets(null, null, null, 0);
    } catch (IOException ignored) {
    }
  }

  /**
   * Test key operations with ignoring thrown exception.
   */
  private void doKeyOps() {
    try {
      ozoneManager.openKey(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.deleteKey(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.lookupKey(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listKeys(null, null, null, null, 0);
    } catch (IOException ignored) {
    }
  }
}
