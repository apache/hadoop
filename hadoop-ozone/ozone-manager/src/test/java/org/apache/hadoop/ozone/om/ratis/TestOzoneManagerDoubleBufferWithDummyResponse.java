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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.om.ratis.metrics.OzoneManagerDoubleBufferMetrics;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.BatchOperation;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * This class tests OzoneManagerDoubleBuffer implementation with
 * dummy response class.
 */
public class TestOzoneManagerDoubleBufferWithDummyResponse {

  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private AtomicLong trxId = new AtomicLong(0);
  private OzoneManagerRatisSnapshot ozoneManagerRatisSnapshot;
  private long lastAppliedIndex;


  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
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
   * This tests add's 100 bucket creation responses to doubleBuffer, and
   * check OM DB bucket table has 100 entries or not. In addition checks
   * flushed transaction count is matching with expected count or not.
   * @throws Exception
   */
  @Test(timeout = 300_000)
  public void testDoubleBufferWithDummyResponse() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    int bucketCount = 100;
    OzoneManagerDoubleBufferMetrics ozoneManagerDoubleBufferMetrics =
        doubleBuffer.getOzoneManagerDoubleBufferMetrics();

    // As we have not flushed/added any transactions, all metrics should have
    // value zero.
    Assert.assertTrue(ozoneManagerDoubleBufferMetrics
        .getTotalNumOfFlushOperations() == 0);
    Assert.assertTrue(ozoneManagerDoubleBufferMetrics
        .getTotalNumOfFlushedTransactions() == 0);
    Assert.assertTrue(ozoneManagerDoubleBufferMetrics
        .getMaxNumberOfTransactionsFlushedInOneIteration() == 0);

    for (int i=0; i < bucketCount; i++) {
      doubleBuffer.add(createDummyBucketResponse(volumeName,
          UUID.randomUUID().toString()), trxId.incrementAndGet());
    }
    GenericTestUtils.waitFor(() ->
            doubleBuffer.getFlushedTransactionCount() == bucketCount, 100,
        60000);

    Assert.assertTrue(ozoneManagerDoubleBufferMetrics
        .getTotalNumOfFlushOperations() > 0);
    Assert.assertTrue(ozoneManagerDoubleBufferMetrics
        .getTotalNumOfFlushedTransactions() == bucketCount);
    Assert.assertTrue(ozoneManagerDoubleBufferMetrics
        .getMaxNumberOfTransactionsFlushedInOneIteration() > 0);
    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getBucketTable()) == (bucketCount));
    Assert.assertTrue(doubleBuffer.getFlushIterations() > 0);

    // Check lastAppliedIndex is updated correctly or not.
    Assert.assertEquals(bucketCount, lastAppliedIndex);
  }

  /**
   * Create DummyBucketCreate response.
   * @param volumeName
   * @param bucketName
   * @return OMDummyCreateBucketResponse
   */
  private OMDummyCreateBucketResponse createDummyBucketResponse(
      String volumeName, String bucketName) {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();
    return new OMDummyCreateBucketResponse(omBucketInfo,
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateBucketResponse(CreateBucketResponse.newBuilder().build())
            .build());
  }


  /**
   * DummyCreatedBucket Response class used in testing.
   */
  public static class OMDummyCreateBucketResponse extends OMClientResponse {
    private final OmBucketInfo omBucketInfo;

    public OMDummyCreateBucketResponse(OmBucketInfo omBucketInfo,
        OMResponse omResponse) {
      super(omResponse);
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
