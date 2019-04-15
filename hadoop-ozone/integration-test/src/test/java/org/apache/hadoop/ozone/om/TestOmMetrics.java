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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.MetricsAsserts;
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
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
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
        (VolumeManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "volumeManager");
    VolumeManager mockVm = Mockito.spy(volumeManager);

    Mockito.doReturn(null).when(mockVm).createVolume(null);
    Mockito.doReturn(null).when(mockVm).deleteVolume(null);
    Mockito.doReturn(null).when(mockVm).getVolumeInfo(null);
    Mockito.doReturn(true).when(mockVm).checkVolumeAccess(null, null);
    Mockito.doReturn(null).when(mockVm).setOwner(null, null);
    Mockito.doReturn(null).when(mockVm).listVolumes(null, null, null, 0);

    HddsWhiteboxTestUtils.setInternalState(
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
    assertCounter("NumVolumes", 0L, omMetrics);

    ozoneManager.createVolume(null);
    ozoneManager.createVolume(null);
    ozoneManager.createVolume(null);
    ozoneManager.deleteVolume(null);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumVolumes", 2L, omMetrics);


    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockVm).createVolume(null);
    Mockito.doThrow(exception).when(mockVm).deleteVolume(null);
    Mockito.doThrow(exception).when(mockVm).getVolumeInfo(null);
    Mockito.doThrow(exception).when(mockVm).checkVolumeAccess(null, null);
    Mockito.doThrow(exception).when(mockVm).setOwner(null, null);
    Mockito.doThrow(exception).when(mockVm).listVolumes(null, null, null, 0);

    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "volumeManager", mockVm);
    doVolumeOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumVolumeOps", 16L, omMetrics);
    assertCounter("NumVolumeCreates", 5L, omMetrics);
    assertCounter("NumVolumeUpdates", 2L, omMetrics);
    assertCounter("NumVolumeInfos", 2L, omMetrics);
    assertCounter("NumVolumeCheckAccesses", 2L, omMetrics);
    assertCounter("NumVolumeDeletes", 3L, omMetrics);
    assertCounter("NumVolumeLists", 2L, omMetrics);

    assertCounter("NumVolumeCreateFails", 1L, omMetrics);
    assertCounter("NumVolumeUpdateFails", 1L, omMetrics);
    assertCounter("NumVolumeInfoFails", 1L, omMetrics);
    assertCounter("NumVolumeCheckAccessFails", 1L, omMetrics);
    assertCounter("NumVolumeDeleteFails", 1L, omMetrics);
    assertCounter("NumVolumeListFails", 1L, omMetrics);

    // As last call for volumesOps does not increment numVolumes as those are
    // failed.
    assertCounter("NumVolumes", 2L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumVolumes", 2L, omMetrics);


  }

  @Test
  public void testBucketOps() throws IOException {
    BucketManager bucketManager =
        (BucketManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "bucketManager");
    BucketManager mockBm = Mockito.spy(bucketManager);

    S3BucketManager s3BucketManager =
        (S3BucketManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "s3BucketManager");
    S3BucketManager mockS3Bm = Mockito.spy(s3BucketManager);

    Mockito.doNothing().when(mockS3Bm).createS3Bucket("random", "random");
    Mockito.doNothing().when(mockS3Bm).deleteS3Bucket("random");
    Mockito.doReturn(true).when(mockS3Bm).createOzoneVolumeIfNeeded(null);

    Mockito.doReturn(null).when(mockBm).createBucket(null);
    Mockito.doReturn(null).when(mockBm).createBucket(null);
    Mockito.doNothing().when(mockBm).deleteBucket(null, null);
    Mockito.doReturn(null).when(mockBm).getBucketInfo(null, null);
    Mockito.doReturn(null).when(mockBm).setBucketProperty(null);
    Mockito.doReturn(null).when(mockBm).listBuckets(null, null, null, 0);

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);
    doBucketOps();

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 5L, omMetrics);
    assertCounter("NumBucketCreates", 1L, omMetrics);
    assertCounter("NumBucketUpdates", 1L, omMetrics);
    assertCounter("NumBucketInfos", 1L, omMetrics);
    assertCounter("NumBucketDeletes", 1L, omMetrics);
    assertCounter("NumBucketLists", 1L, omMetrics);
    assertCounter("NumBuckets", 0L, omMetrics);

    ozoneManager.createBucket(null);
    ozoneManager.createBucket(null);
    ozoneManager.createBucket(null);
    ozoneManager.deleteBucket(null, null);

    //Taking already existing value, as the same metrics is used over all the
    // test cases.
    long numVolumesOps = MetricsAsserts.getLongCounter("NumVolumeOps",
        omMetrics);
    long numVolumes = MetricsAsserts.getLongCounter("NumVolumes",
        omMetrics);
    long numVolumeCreates = MetricsAsserts.getLongCounter("NumVolumeCreates",
        omMetrics);

    ozoneManager.createS3Bucket("random", "random");
    ozoneManager.createS3Bucket("random1", "random1");
    ozoneManager.createS3Bucket("random2", "random2");
    ozoneManager.deleteS3Bucket("random");

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBuckets", 4L, omMetrics);

    assertCounter("NumVolumeOps", numVolumesOps + 3, omMetrics);
    assertCounter("NumVolumeCreates", numVolumeCreates + 3, omMetrics);
    assertCounter("NumVolumes", numVolumes + 3, omMetrics);



    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockBm).createBucket(null);
    Mockito.doThrow(exception).when(mockBm).deleteBucket(null, null);
    Mockito.doThrow(exception).when(mockBm).getBucketInfo(null, null);
    Mockito.doThrow(exception).when(mockBm).setBucketProperty(null);
    Mockito.doThrow(exception).when(mockBm).listBuckets(null, null, null, 0);

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);
    doBucketOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 18L, omMetrics);
    assertCounter("NumBucketCreates", 8L, omMetrics);
    assertCounter("NumBucketUpdates", 2L, omMetrics);
    assertCounter("NumBucketInfos", 2L, omMetrics);
    assertCounter("NumBucketDeletes", 4L, omMetrics);
    assertCounter("NumBucketLists", 2L, omMetrics);

    assertCounter("NumBucketCreateFails", 1L, omMetrics);
    assertCounter("NumBucketUpdateFails", 1L, omMetrics);
    assertCounter("NumBucketInfoFails", 1L, omMetrics);
    assertCounter("NumBucketDeleteFails", 1L, omMetrics);
    assertCounter("NumBucketListFails", 1L, omMetrics);

    assertCounter("NumBuckets", 4L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumBuckets", 4L, omMetrics);
  }

  @Test
  public void testKeyOps() throws IOException {
    KeyManager keyManager = (KeyManager) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    KeyManager mockKm = Mockito.spy(keyManager);

    Mockito.doReturn(null).when(mockKm).openKey(null);
    Mockito.doNothing().when(mockKm).deleteKey(null);
    Mockito.doReturn(null).when(mockKm).lookupKey(null);
    Mockito.doReturn(null).when(mockKm).listKeys(null, null, null, null, 0);
    Mockito.doNothing().when(mockKm).commitKey(any(OmKeyArgs.class), anyLong());
    Mockito.doReturn(null).when(mockKm).initiateMultipartUpload(
        any(OmKeyArgs.class));

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);
    doKeyOps();

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 6L, omMetrics);
    assertCounter("NumKeyAllocate", 1L, omMetrics);
    assertCounter("NumKeyLookup", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 1L, omMetrics);
    assertCounter("NumKeyLists", 1L, omMetrics);
    assertCounter("NumKeys", 0L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 1L, omMetrics);


    ozoneManager.openKey(null);
    ozoneManager.commitKey(createKeyArgs(), 0);
    ozoneManager.openKey(null);
    ozoneManager.commitKey(createKeyArgs(), 0);
    ozoneManager.openKey(null);
    ozoneManager.commitKey(createKeyArgs(), 0);
    ozoneManager.deleteKey(null);


    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 2L, omMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockKm).openKey(null);
    Mockito.doThrow(exception).when(mockKm).deleteKey(null);
    Mockito.doThrow(exception).when(mockKm).lookupKey(null);
    Mockito.doThrow(exception).when(mockKm).listKeys(
        null, null, null, null, 0);
    Mockito.doThrow(exception).when(mockKm).commitKey(any(OmKeyArgs.class),
        anyLong());
    Mockito.doThrow(exception).when(mockKm).initiateMultipartUpload(
        any(OmKeyArgs.class));

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);
    doKeyOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 19L, omMetrics);
    assertCounter("NumKeyAllocate", 5L, omMetrics);
    assertCounter("NumKeyLookup", 2L, omMetrics);
    assertCounter("NumKeyDeletes", 3L, omMetrics);
    assertCounter("NumKeyLists", 2L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 2L, omMetrics);

    assertCounter("NumKeyAllocateFails", 1L, omMetrics);
    assertCounter("NumKeyLookupFails", 1L, omMetrics);
    assertCounter("NumKeyDeleteFails", 1L, omMetrics);
    assertCounter("NumKeyListFails", 1L, omMetrics);
    assertCounter("NumInitiateMultipartUploadFails", 1L, omMetrics);


    assertCounter("NumKeys", 2L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumKeys", 2L, omMetrics);

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

    try {
      ozoneManager.commitKey(createKeyArgs(), 0);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.initiateMultipartUpload(null);
    } catch (IOException ignored) {
    }

  }

  private OmKeyArgs createKeyArgs() {
    OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(new ContainerBlockID(1, 1))).build();
    keyLocationInfo.setCreateVersion(0);
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    omKeyLocationInfoList.add(keyLocationInfo);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setLocationInfoList(
        omKeyLocationInfoList).build();
    return keyArgs;
  }
}
