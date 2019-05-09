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

import org.apache.hadoop.ozone.om.response.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.utils.db.BatchOperation;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;



import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.fail;

/**
 * This class tests OzoneManagerDouble Buffer.
 */
public class TestOzoneManagerDoubleBuffer {

  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private AtomicLong trxId = new AtomicLong(0);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private void setup() throws IOException  {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager =
        new OmMetadataManagerImpl(configuration);
    doubleBuffer = new OzoneManagerDoubleBuffer(omMetadataManager);
  }

  private void stop() {
    doubleBuffer.stop();
  }

  @Test(timeout = 300_000)
  public void testDoubleBufferWithDummyResponse() throws Exception {
    try {
      setup();
      String volumeName = UUID.randomUUID().toString();
      int bucketCount = 100;
      for (int i=0; i < bucketCount; i++) {
        doubleBuffer.add(createDummyBucketResponse(volumeName,
            UUID.randomUUID().toString()), trxId.incrementAndGet());
      }
      GenericTestUtils.waitFor(() ->
              doubleBuffer.getFlushedTransactionCount() == bucketCount, 100,
          120000);
      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getBucketTable()) == (bucketCount));
      Assert.assertTrue(doubleBuffer.getFlushIterations() > 0);
    } finally {
      stop();
    }
  }


  @Test(timeout = 300_000)
  public void testDoubleBuffer() throws Exception {
    // This test checks whether count in tables are correct or not.
    testDoubleBuffer(1, 10);
    testDoubleBuffer(10, 100);
    testDoubleBuffer(100, 100);
    testDoubleBuffer(1000, 1000);
  }



  @Test
  public void testDoubleBufferWithMixOfTransactions() throws Exception {
    // This test checks count, data in table is correct or not.
    try {
      setup();

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
    } finally {
      stop();
    }
  }

  @Test
  public void testDoubleBufferWithMixOfTransactionsParallel() throws Exception {
    // This test checks count, data in table is correct or not.
    try {
      setup();

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
              == (bucketCount + deleteCount + 2),
          100,
          120000);

      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getVolumeTable()) == 2);

      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getBucketTable()) == 10);

      // Now after this in our DB we should have 5 buckets and one volume


      checkVolume(volumeName1, omVolumeCreateResponse1);
      checkVolume(volumeName2, omVolumeCreateResponse2);

      checkCreateBuckets(bucketQueue);

      checkDeletedBuckets(deleteBucketQueue);
    } finally {
      stop();
    }
  }


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

  public void testDoubleBuffer(int iterations, int bucketCount)
      throws Exception {
    try {
      setup();
      for (int i = 0; i < iterations; i++) {
        Daemon d1 = new Daemon(() ->
            doTransactions(UUID.randomUUID().toString(), bucketCount));
        d1.start();
      }

      // We are doing +1 for volume transaction.
      GenericTestUtils.waitFor(() ->
              doubleBuffer.getFlushedTransactionCount() ==
                  (bucketCount + 1) * iterations, 100,
          120000);

      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getVolumeTable()) == iterations);

      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getBucketTable()) == (bucketCount) * iterations);

      Assert.assertTrue(doubleBuffer.getFlushIterations() > 0);
    } finally {
      stop();
    }
  }


  public void doTransactions(String volumeName, int buckets) {
    doubleBuffer.add(createVolume(volumeName), trxId.incrementAndGet());
    for (int i=0; i< buckets; i++) {
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

  private OMVolumeCreateResponse createVolume(String volumeName) {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder()
            .setAdminName(UUID.randomUUID().toString())
            .setOwnerName(UUID.randomUUID().toString())
            .setVolume(volumeName)
            .setCreationTime(Time.now()).build();

    VolumeList volumeList = VolumeList.newBuilder()
        .addVolumeNames(volumeName).build();
    return new OMVolumeCreateResponse(omVolumeArgs, volumeList);
  }

  private OMBucketCreateResponse createBucket(String volumeName,
      String bucketName) {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();
    return new OMBucketCreateResponse(omBucketInfo);
  }

  private OMDummyCreateBucketResponse createDummyBucketResponse(
      String volumeName, String bucketName) {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();
    return new OMDummyCreateBucketResponse(omBucketInfo);
  }

  private OMBucketDeleteResponse deleteBucket(String volumeName,
      String bucketName) {
    return new OMBucketDeleteResponse(volumeName, bucketName);
  }

  /**
   * DummyCreatedBucket Response class used in testing.
   */
  public static class OMDummyCreateBucketResponse implements OMClientResponse {
    private final OmBucketInfo omBucketInfo;

    public OMDummyCreateBucketResponse(OmBucketInfo omBucketInfo) {
      this.omBucketInfo = omBucketInfo;
    }

    @Override
    public void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException {
      String dbBucketKey =
          omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
              omBucketInfo.getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          dbBucketKey, omBucketInfo);
    }

  }
}

