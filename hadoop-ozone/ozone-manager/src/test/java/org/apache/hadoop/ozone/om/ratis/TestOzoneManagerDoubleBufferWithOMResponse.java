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

package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.fail;

/**
 * This class tests OzoneManagerDouble Buffer with actual OMResponse classes.
 */
public class TestOzoneManagerDoubleBufferWithOMResponse {

  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private final AtomicLong trxId = new AtomicLong(0);
  private OzoneManagerRatisSnapshot ozoneManagerRatisSnapshot;
  private volatile long lastAppliedIndex;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException  {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager =
        new OmMetadataManagerImpl(configuration);
    ozoneManagerRatisSnapshot = index -> {
      lastAppliedIndex = index;
    };
    doubleBuffer = new OzoneManagerDoubleBuffer(omMetadataManager,
        ozoneManagerRatisSnapshot);
  }

  @After
  public void stop() {
    doubleBuffer.stop();
  }

  /**
   * This tests OzoneManagerDoubleBuffer implementation. It calls
   * testDoubleBuffer with number of iterations to do transactions and
   * number of buckets to be created in each iteration. It then
   * verifies OM DB entries count is matching with total number of
   * transactions or not.
   * @throws Exception
   */
  @Test(timeout = 300_000)
  public void testDoubleBuffer() throws Exception {
    // This test checks whether count in tables are correct or not.
    testDoubleBuffer(1, 10);
    testDoubleBuffer(10, 100);
    testDoubleBuffer(100, 100);
    testDoubleBuffer(1000, 1000);
  }

  /**
   * This test first creates a volume, and then does a mix of transactions
   * like create/delete buckets and add them to double buffer. Then it
   * verifies OM DB entries are matching with actual responses added to
   * double buffer or not.
   * @throws Exception
   */
  @Test
  public void testDoubleBufferWithMixOfTransactions() throws Exception {
    // This test checks count, data in table is correct or not.
    Queue< OMBucketCreateResponse > bucketQueue =
        new ConcurrentLinkedQueue<>();
    Queue< OMBucketDeleteResponse > deleteBucketQueue =
        new ConcurrentLinkedQueue<>();

    String volumeName = UUID.randomUUID().toString();
    OMVolumeCreateResponse omVolumeCreateResponse = createVolume(volumeName);
    doubleBuffer.add(omVolumeCreateResponse, trxId.incrementAndGet());


    int bucketCount = 10;

    doMixTransactions(volumeName, 10, deleteBucketQueue, bucketQueue);

    // As for every 2 transactions of create bucket we add deleted bucket.
    final int deleteCount = 5;

    // We are doing +1 for volume transaction.
    GenericTestUtils.waitFor(() ->
        doubleBuffer.getFlushedTransactionCount() ==
            (bucketCount + deleteCount + 1), 100, 120000);

    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getVolumeTable()) == 1);

    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getBucketTable()) == 5);

    // Now after this in our DB we should have 5 buckets and one volume

    checkVolume(volumeName, omVolumeCreateResponse);

    checkCreateBuckets(bucketQueue);

    checkDeletedBuckets(deleteBucketQueue);

    // Check lastAppliedIndex is updated correctly or not.
    Assert.assertEquals(bucketCount + deleteCount + 1, lastAppliedIndex);
  }

  /**
   * This test first creates a volume, and then does a mix of transactions
   * like create/delete buckets in parallel and add to double buffer. Then it
   * verifies OM DB entries are matching with actual responses added to
   * double buffer or not.
   * @throws Exception
   */
  @Test
  public void testDoubleBufferWithMixOfTransactionsParallel() throws Exception {
    // This test checks count, data in table is correct or not.

    Queue< OMBucketCreateResponse > bucketQueue =
        new ConcurrentLinkedQueue<>();
    Queue< OMBucketDeleteResponse > deleteBucketQueue =
        new ConcurrentLinkedQueue<>();

    String volumeName1 = UUID.randomUUID().toString();
    OMVolumeCreateResponse omVolumeCreateResponse1 =
        createVolume(volumeName1);

    String volumeName2 = UUID.randomUUID().toString();
    OMVolumeCreateResponse omVolumeCreateResponse2 =
        createVolume(volumeName2);

    doubleBuffer.add(omVolumeCreateResponse1, trxId.incrementAndGet());

    doubleBuffer.add(omVolumeCreateResponse2, trxId.incrementAndGet());

    Daemon daemon1 = new Daemon(() -> doMixTransactions(volumeName1, 10,
        deleteBucketQueue, bucketQueue));
    Daemon daemon2 = new Daemon(() -> doMixTransactions(volumeName2, 10,
        deleteBucketQueue, bucketQueue));

    daemon1.start();
    daemon2.start();

    int bucketCount = 20;

      // As for every 2 transactions of create bucket we add deleted bucket.
    final int deleteCount = 10;

    // We are doing +1 for volume transaction.
    GenericTestUtils.waitFor(() -> doubleBuffer.getFlushedTransactionCount()
            == (bucketCount + deleteCount + 2), 100, 120000);

    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getVolumeTable()) == 2);

    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getBucketTable()) == 10);

    // Now after this in our DB we should have 5 buckets and one volume


    checkVolume(volumeName1, omVolumeCreateResponse1);
    checkVolume(volumeName2, omVolumeCreateResponse2);

    checkCreateBuckets(bucketQueue);

    checkDeletedBuckets(deleteBucketQueue);

    // Check lastAppliedIndex is updated correctly or not.
    Assert.assertEquals(bucketCount + deleteCount + 2, lastAppliedIndex);
  }

  /**
   * This method add's a mix of createBucket/DeleteBucket responses to double
   * buffer. Total number of responses added is specified by bucketCount.
   * @param volumeName
   * @param bucketCount
   * @param deleteBucketQueue
   * @param bucketQueue
   */
  private void doMixTransactions(String volumeName, int bucketCount,
      Queue<OMBucketDeleteResponse> deleteBucketQueue,
      Queue<OMBucketCreateResponse> bucketQueue) {
    for (int i=0; i < bucketCount; i++) {
      String bucketName = UUID.randomUUID().toString();
      OMBucketCreateResponse omBucketCreateResponse = createBucket(volumeName,
          bucketName);
      doubleBuffer.add(omBucketCreateResponse, trxId.incrementAndGet());
      // For every 2 transactions have a deleted bucket.
      if (i % 2 == 0) {
        OMBucketDeleteResponse omBucketDeleteResponse =
            deleteBucket(volumeName, bucketName);
        doubleBuffer.add(omBucketDeleteResponse, trxId.incrementAndGet());
        deleteBucketQueue.add(omBucketDeleteResponse);
      } else {
        bucketQueue.add(omBucketCreateResponse);
      }
    }
  }

  /**
   * Verifies volume table data is matching with actual response added to
   * double buffer.
   * @param volumeName
   * @param omVolumeCreateResponse
   * @throws Exception
   */
  private void checkVolume(String volumeName,
      OMVolumeCreateResponse omVolumeCreateResponse) throws Exception {
    OmVolumeArgs tableVolumeArgs = omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName));
    Assert.assertTrue(tableVolumeArgs != null);

    OmVolumeArgs omVolumeArgs = omVolumeCreateResponse.getOmVolumeArgs();

    Assert.assertEquals(omVolumeArgs.getVolume(), tableVolumeArgs.getVolume());
    Assert.assertEquals(omVolumeArgs.getAdminName(),
        tableVolumeArgs.getAdminName());
    Assert.assertEquals(omVolumeArgs.getOwnerName(),
        tableVolumeArgs.getOwnerName());
    Assert.assertEquals(omVolumeArgs.getCreationTime(),
        tableVolumeArgs.getCreationTime());
  }

  /**
   * Verifies bucket table data is matching with actual response added to
   * double buffer.
   * @param bucketQueue
   */
  private void checkCreateBuckets(Queue<OMBucketCreateResponse> bucketQueue) {
    bucketQueue.forEach((omBucketCreateResponse) -> {
      OmBucketInfo omBucketInfo = omBucketCreateResponse.getOmBucketInfo();
      String bucket = omBucketInfo.getBucketName();
      OmBucketInfo tableBucketInfo = null;
      try {
        tableBucketInfo =
            omMetadataManager.getBucketTable().get(
                omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
                    bucket));
      } catch (IOException ex) {
        fail("testDoubleBufferWithMixOfTransactions failed");
      }
      Assert.assertNotNull(tableBucketInfo);

      Assert.assertEquals(omBucketInfo.getVolumeName(),
          tableBucketInfo.getVolumeName());
      Assert.assertEquals(omBucketInfo.getBucketName(),
          tableBucketInfo.getBucketName());
      Assert.assertEquals(omBucketInfo.getCreationTime(),
          tableBucketInfo.getCreationTime());
    });
  }

  /**
   * Verifies deleted bucket responses added to double buffer are actually
   * removed from the OM DB or not.
   * @param deleteBucketQueue
   */
  private void checkDeletedBuckets(Queue<OMBucketDeleteResponse>
      deleteBucketQueue) {
    deleteBucketQueue.forEach((omBucketDeleteResponse -> {
      try {
        Assert.assertNull(omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(
                omBucketDeleteResponse.getVolumeName(),
                omBucketDeleteResponse.getBucketName())));
      } catch (IOException ex) {
        fail("testDoubleBufferWithMixOfTransactions failed");
      }
    }));
  }

  /**
   * Create bucketCount number of createBucket responses for each iteration.
   * All these iterations are run in parallel. Then verify OM DB has correct
   * number of entries or not.
   * @param iterations
   * @param bucketCount
   * @throws Exception
   */
  public void testDoubleBuffer(int iterations, int bucketCount)
      throws Exception {
    try {
      // Reset transaction id.
      trxId.set(0);
      // Calling setup and stop here because this method is called from a
      // single test multiple times.
      setup();
      for (int i = 0; i < iterations; i++) {
        Daemon d1 = new Daemon(() ->
            doTransactions(UUID.randomUUID().toString(), bucketCount));
        d1.start();
      }

      // We are doing +1 for volume transaction.
      long expectedTransactions = (bucketCount + 1) * iterations;
      GenericTestUtils.waitFor(() -> lastAppliedIndex == expectedTransactions,
          100, 120000);

      Assert.assertEquals(expectedTransactions,
          doubleBuffer.getFlushedTransactionCount()
      );

      Assert.assertEquals(iterations,
          omMetadataManager.countRowsInTable(omMetadataManager.getVolumeTable())
      );

      Assert.assertEquals(bucketCount * iterations,
          omMetadataManager.countRowsInTable(omMetadataManager.getBucketTable())
      );

      Assert.assertTrue(doubleBuffer.getFlushIterations() > 0);
    } finally {
      stop();
    }
  }

  /**
   * This method adds bucketCount number of createBucket responses to double
   * buffer.
   * @param volumeName
   * @param bucketCount
   */
  public void doTransactions(String volumeName, int bucketCount) {
    doubleBuffer.add(createVolume(volumeName), trxId.incrementAndGet());
    for (int i=0; i< bucketCount; i++) {
      doubleBuffer.add(createBucket(volumeName, UUID.randomUUID().toString()),
          trxId.incrementAndGet());
      // For every 100 buckets creation adding 100ms delay

      if (i % 100 == 0) {
        try {
          Thread.sleep(100);
        } catch (Exception ex) {

        }
      }
    }
  }

  /**
   * Create OMVolumeCreateResponse for specified volume.
   * @param volumeName
   * @return OMVolumeCreateResponse
   */
  private OMVolumeCreateResponse createVolume(String volumeName) {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder()
            .setAdminName(UUID.randomUUID().toString())
            .setOwnerName(UUID.randomUUID().toString())
            .setVolume(volumeName)
            .setCreationTime(Time.now()).build();

    VolumeList volumeList = VolumeList.newBuilder()
        .addVolumeNames(volumeName).build();
    return new OMVolumeCreateResponse(omVolumeArgs, volumeList,
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateVolumeResponse(CreateVolumeResponse.newBuilder().build())
            .build());
  }

  /**
   * Create OMBucketCreateResponse for specified volume and bucket.
   * @param volumeName
   * @param bucketName
   * @return OMBucketCreateResponse
   */
  private OMBucketCreateResponse createBucket(String volumeName,
      String bucketName) {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();
    return new OMBucketCreateResponse(omBucketInfo, OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCreateBucketResponse(CreateBucketResponse.newBuilder().build())
        .build());
  }

  /**
   * Create OMBucketDeleteResponse for specified volume and bucket.
   * @param volumeName
   * @param bucketName
   * @return OMBucketDeleteResponse
   */
  private OMBucketDeleteResponse deleteBucket(String volumeName,
      String bucketName) {
    return new OMBucketDeleteResponse(volumeName, bucketName,
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setDeleteBucketResponse(DeleteBucketResponse.newBuilder().build())
            .build());
  }


}

