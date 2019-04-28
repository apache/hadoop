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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests failure detection and handling in BlockOutputStream Class.
 */
public class TestBlockOutputStreamWithFailures {

  private static MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private ObjectStore objectStore;
  private int chunkSize;
  private int flushSize;
  private int maxFlushSize;
  private int blockSize;
  private String volumeName;
  private String bucketName;
  private String keyString;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;
    conf.set(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, "5000ms");
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE, "NONE");
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(7)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "testblockoutputstream";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
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
  public void testWatchForCommitWithCloseContainerException() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);

    // since its hitting the full bufferCondition, it will call watchForCommit
    // and completes atleast putBlock for first flushSize worth of data
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            <= pendingWriteChunkCount + 2);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 4 buffers allocated worth of chunk size

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(maxFlushSize,
        blockOutputStream.getTotalDataFlushedLength());

    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // ack'd by all servers right here
    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() >= flushSize);

    // watchForCommit will clean up atleast one entry from the map where each
    // entry corresponds to flushSize worth of data
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 1);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 8,
        metrics.getTotalOpCount());

    // flush is a sync call, all pending operations will complete
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    // flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 2);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    // Close the containers on the Datanode and write more data
    ContainerTestHelper.waitForContainerClose(key, cluster);
    // 4 writeChunks = maxFlushSize + 2 putBlocks  will be discarded here
    // once exception is hit
    key.write(data1);

    // As a part of handling the exception, 4 failed writeChunks  will be
    // rewritten plus one partial chunk plus two putBlocks for flushSize
    // and one flush for partial chunk
    key.flush();

    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof ContainerNotOpenException);

    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    // commitInfoMap will remain intact as there is no server failure
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 14,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 8,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 22,
        metrics.getTotalOpCount());
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  @Test
  public void testWatchForCommitDatanodeFailure() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    // since its hitting the full bufferCondition, it will call watchForCommit
    // and completes at least putBlock for first flushSize worth of data
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            <= pendingWriteChunkCount + 2);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 3 buffers allocated worth of chunk size

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    // since data written is still less than flushLength, flushLength will
    // still be 0.
    Assert.assertEquals(maxFlushSize,
        blockOutputStream.getTotalDataFlushedLength());

    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // ack'd by all servers right here
    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() >= flushSize);

    // watchForCommit will clean up atleast flushSize worth of data buffer
    // where each entry corresponds to flushSize worth of data
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 2);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 8,
        metrics.getTotalOpCount());

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    //  flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    Pipeline pipeline = raftClient.getPipeline();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));

    // again write data with more than max buffer limit. This will call
    // watchForCommit again. Since the commit will happen 2 way, the
    // commitInfoMap will get updated for servers which are alive
    key.write(data1);

    key.flush();
    Assert.assertEquals(2, raftClient.getCommitInfoMap().size());

    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert
        .assertEquals(blockSize, blockOutputStream.getTotalAckDataLength());
    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // in total, there are 8 full write chunks + 2 partial chunks written
    Assert.assertEquals(writeChunkCount + 10,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    // 4 flushes at flushSize boundaries + 2 flush for partial chunks
    Assert.assertEquals(putBlockCount + 6,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 16,
        metrics.getTotalOpCount());
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  @Test
  public void test2DatanodesFailure() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    // since its hitting the full bufferCondition, it will call watchForCommit
    // and completes atleast putBlock for first flushSize worth of data
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            <= pendingWriteChunkCount + 2);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 3 buffers allocated worth of chunk size

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(maxFlushSize,
        blockOutputStream.getTotalDataFlushedLength());

    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // acked by all servers right here
    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() >= flushSize);

    // watchForCommit will clean up atleast one entry from the map where each
    // entry corresponds to flushSize worth of data
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 1);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 8,
        metrics.getTotalOpCount());

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    // flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 2);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    Pipeline pipeline = raftClient.getPipeline();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
    // again write data with more than max buffer limit. This will call
    // watchForCommit again. Since the commit will happen 2 way, the
    // commitInfoMap will get updated for servers which are alive

    // 4 writeChunks = maxFlushSize + 2 putBlocks  will be discarded here
    // once exception is hit
    key.write(data1);

    // As a part of handling the exception, 4 failed writeChunks  will be
    // rewritten plus one partial chunk plus two putBlocks for flushSize
    // and one flush for partial chunk
    key.flush();
    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof RaftRetryFailureException);
    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    // now close the stream, It will update the ack length after watchForCommit

    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    key.close();
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 14,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 8,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 22,
        metrics.getTotalOpCount());
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  public void testFailureWithPrimeSizedData() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    int dataLength = 167;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);

    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            == pendingWriteChunkCount + 1);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            == pendingPutBlockCount);
    Assert.assertEquals(writeChunkCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 1,
        metrics.getTotalOpCount());

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;


    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(0,
        blockOutputStream.getTotalDataFlushedLength());

    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() == 0);

    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 3,
        metrics.getTotalOpCount());

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    // flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    // Close the containers on the Datanode and write more data
    ContainerTestHelper.waitForContainerClose(key, cluster);
    key.write(data1);

    // As a part of handling the exception, 2 failed writeChunks  will be
    // rewritten plus 1 putBlocks for flush
    // and one flush for partial chunk
    key.flush();

    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof ContainerNotOpenException);
    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);

    // commitInfoMap will remain intact as there is no server failure
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 6,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 9,
        metrics.getTotalOpCount());
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 0);
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  @Test
  public void testExceptionDuringClose() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    int dataLength = 167;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);

    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            == pendingWriteChunkCount + 1);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            == pendingPutBlockCount);
    Assert.assertEquals(writeChunkCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 1,
        metrics.getTotalOpCount());

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;


    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(0,
        blockOutputStream.getTotalDataFlushedLength());

    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() == 0);

    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 3,
        metrics.getTotalOpCount());

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    // flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    // Close the containers on the Datanode and write more data
    ContainerTestHelper.waitForContainerClose(key, cluster);
    key.write(data1);

    // commitInfoMap will remain intact as there is no server failure
    Assert.assertEquals(3, raftClient.getCommitInfoMap().size());
    // now close the stream, It will hit exception
    key.close();

    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof ContainerNotOpenException);
    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 6,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 9,
        metrics.getTotalOpCount());
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 0);
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  @Test
  public void testWatchForCommitWithSingleNodeRatis() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 0, ReplicationFactor.ONE);
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);

    // since its hitting the full bufferCondition, it will call watchForCommit
    // and completes atleast putBlock for first flushSize worth of data
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            <= pendingWriteChunkCount + 2);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 4 buffers allocated worth of chunk size

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(maxFlushSize,
        blockOutputStream.getTotalDataFlushedLength());

    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // ack'd by all servers right here
    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() >= flushSize);

    // watchForCommit will clean up atleast one entry from the map where each
    // entry corresponds to flushSize worth of data
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 1);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 8,
        metrics.getTotalOpCount());

    // flush is a sync call, all pending operations will complete
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    // flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 2);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(1, raftClient.getCommitInfoMap().size());
    // Close the containers on the Datanode and write more data
    ContainerTestHelper.waitForContainerClose(key, cluster);
    // 4 writeChunks = maxFlushSize + 2 putBlocks  will be discarded here
    // once exception is hit
    key.write(data1);

    // As a part of handling the exception, 4 failed writeChunks  will be
    // rewritten plus one partial chunk plus two putBlocks for flushSize
    // and one flush for partial chunk
    key.flush();

    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof ContainerNotOpenException);
    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    // commitInfoMap will remain intact as there is no server failure
    Assert.assertEquals(1, raftClient.getCommitInfoMap().size());
    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 14,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 8,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 22,
        metrics.getTotalOpCount());
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  @Test
  public void testDatanodeFailureWithSingleNodeRatis() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 0, ReplicationFactor.ONE);
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    // since its hitting the full bufferCondition, it will call watchForCommit
    // and completes at least putBlock for first flushSize worth of data
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            <= pendingWriteChunkCount + 2);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 3 buffers allocated worth of chunk size

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(maxFlushSize,
        blockOutputStream.getTotalDataFlushedLength());

    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // ack'd by all servers right here
    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() >= flushSize);

    // watchForCommit will clean up atleast flushSize worth of data buffer
    // where each entry corresponds to flushSize worth of data
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 2);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 8,
        metrics.getTotalOpCount());

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    //  flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(1, raftClient.getCommitInfoMap().size());
    Pipeline pipeline = raftClient.getPipeline();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));

    // again write data with more than max buffer limit. This will call
    // watchForCommit again. No write will happen in the current block and
    // data will be rewritten to the next block.

    key.write(data1);

    key.flush();

    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof RaftRetryFailureException);
    Assert.assertEquals(1, raftClient.getCommitInfoMap().size());
    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // in total, there are 14 full write chunks, 5 before the failure injection,
    // 4 chunks after which we detect the failure and then 5 again on the next
    // block
    Assert.assertEquals(writeChunkCount + 14,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    // 3 flushes at flushSize boundaries before failure injection + 2
    // flush failed + 3 more flushes for the next block
    Assert.assertEquals(putBlockCount + 8,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 22,
        metrics.getTotalOpCount());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    cluster.restartHddsDatanode(pipeline.getNodes().get(0), true);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  @Test
  public void testDatanodeFailureWithPreAllocation() throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingWriteChunkCount =  metrics.getContainerOpsMetrics(
        ContainerProtos.Type.WriteChunk);
    long pendingPutBlockCount = metrics.getContainerOpsMetrics(
        ContainerProtos.Type.PutBlock);
    long totalOpCount = metrics.getTotalOpCount();
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 3 * blockSize,
            ReplicationFactor.ONE);
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    // since its hitting the full bufferCondition, it will call watchForCommit
    // and completes at least putBlock for first flushSize worth of data
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk)
            <= pendingWriteChunkCount + 2);
    Assert.assertTrue(
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 3);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 3 buffers allocated worth of chunk size

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(maxFlushSize,
        blockOutputStream.getTotalDataFlushedLength());

    // since data equals to maxBufferSize is written, this will be a blocking
    // call and hence will wait for atleast flushSize worth of data to get
    // ack'd by all servers right here
    Assert.assertTrue(blockOutputStream.getTotalAckDataLength() >= flushSize);

    // watchForCommit will clean up atleast flushSize worth of data buffer
    // where each entry corresponds to flushSize worth of data
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 2);

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 8,
        metrics.getTotalOpCount());

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(4, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    //  flush will make sure one more entry gets updated in the map
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() == 0);

    XceiverClientRatis raftClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Assert.assertEquals(1, raftClient.getCommitInfoMap().size());
    Pipeline pipeline = raftClient.getPipeline();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));

    // again write data with more than max buffer limit. This will call
    // watchForCommit again. No write will happen and

    key.write(data1);

    key.flush();

    Assert.assertTrue(keyOutputStream.checkForException(blockOutputStream
        .getIoException()) instanceof RaftRetryFailureException);

    // Make sure the retryCount is reset after the exception is handled
    Assert.assertTrue(keyOutputStream.getRetryCount() == 0);
    Assert.assertEquals(1, raftClient.getCommitInfoMap().size());

    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // in total, there are 14 full write chunks, 5 before the failure injection,
    // 4 chunks after which we detect the failure and then 5 again on the next
    // block
    Assert.assertEquals(writeChunkCount + 14,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));

    // 3 flushes at flushSize boundaries before failure injection + 2
    // flush failed + 3 more flushes for the next block
    Assert.assertEquals(putBlockCount + 8,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 22,
        metrics.getTotalOpCount());
    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    cluster.restartHddsDatanode(pipeline.getNodes().get(0), true);
    validateData(keyName, dataString.concat(dataString).getBytes());
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return createKey(keyName, type, size, ReplicationFactor.THREE);
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size, ReplicationFactor factor) throws Exception {
    return ContainerTestHelper
        .createKey(keyName, type, factor, size, objectStore, volumeName,
            bucketName);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    ContainerTestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }
}
