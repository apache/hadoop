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
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests BlockOutputStream class.
 */
public class TestBlockOutputStream {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static int chunkSize;
  private static int flushSize;
  private static int maxFlushSize;
  private static int blockSize;
  private static String volumeName;
  private static String bucketName;
  private static String keyString;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
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
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBufferCaching() throws Exception {
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
    int dataLength = 50;
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data less than a chunk size, the data will just sit
    // in the buffer, with only one buffer being allocated in the buffer pool

    Assert.assertEquals(1, blockOutputStream.getBufferPool().getSize());
    //Just the writtenDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    // no data will be flushed till now
    Assert.assertEquals(0, blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0, blockOutputStream.getTotalAckDataLength());
    Assert.assertEquals(pendingWriteChunkCount,
        XceiverClientManager.getXceiverClientMetrics()
            .getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        XceiverClientManager.getXceiverClientMetrics()
            .getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // commitIndex2FlushedData Map will be empty here
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().isEmpty());

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();

    // flush is a sync call, all pending operations will complete
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    // we have just written data less than a chunk size, the data will just sit
    // in the buffer, with only one buffer being allocated in the buffer pool

    Assert.assertEquals(1, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(0,
        blockOutputStream.getBufferPool().getBuffer(0).position());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());
    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    // flush ensures watchForCommit updates the total length acknowledged
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());

    Assert.assertEquals(1, keyOutputStream.getStreamEntries().size());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();

    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 2,
        metrics.getTotalOpCount());

    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  public void testFlushChunk() throws Exception {
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
    int dataLength = flushSize;
    // write data equal to 2 chunks
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    Assert.assertEquals(pendingWriteChunkCount + 2,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount + 1,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data equal flush Size = 2 chunks, at this time
    // buffer pool will have 2 buffers allocated worth of chunk size

    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0, blockOutputStream.getTotalAckDataLength());

    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();
    Assert.assertEquals(1, keyOutputStream.getStreamEntries().size());
    // flush is a sync call, all pending operations will complete
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    // Since the data in the buffer is already flushed, flush here will have
    // no impact on the counters and data structures

    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    // flush ensures watchForCommit updates the total length acknowledged
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
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
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  public void testMultiChunkWrite() throws Exception {
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
    int dataLength = chunkSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    Assert.assertEquals(pendingWriteChunkCount + 1,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data equal flush Size > 1 chunk, at this time
    // buffer pool will have 2 buffers allocated worth of chunk size

    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    // since data written is still less than flushLength, flushLength will
    // still be 0.
    Assert.assertEquals(0,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0, blockOutputStream.getTotalAckDataLength());

    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    // Now do a flush. This will flush the data and update the flush length and
    // the map.
    key.flush();
    Assert.assertEquals(writeChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));

    Assert.assertEquals(2, blockOutputStream.getBufferPool().getSize());
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(dataLength,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    // flush ensures watchForCommit updates the total length acknowledged
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());

    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
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
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  public void testMultiChunkWrite2() throws Exception {
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
    int dataLength = flushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(data1);
    Assert.assertEquals(pendingWriteChunkCount + 2,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount + 1,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data more than flush Size(2 chunks), at this time
    // buffer pool will have 3 buffers allocated worth of chunk size

    Assert.assertEquals(3, blockOutputStream.getBufferPool().getSize());
    // writtenDataLength as well flushedDataLength will be updated here
    Assert.assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

    Assert.assertEquals(flushSize,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0, blockOutputStream.getTotalAckDataLength());

    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    Assert.assertEquals(flushSize,
        blockOutputStream.getTotalDataFlushedLength());
    Assert.assertEquals(0,
        blockOutputStream.getCommitIndex2flushedDataMap().size());

    Assert.assertEquals(0, blockOutputStream.getTotalAckDataLength());
    Assert.assertEquals(1, keyOutputStream.getStreamEntries().size());
    key.close();
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 5,
        metrics.getTotalOpCount());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  public void testFullBufferCondition() throws Exception {
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
    int dataLength = maxFlushSize;
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
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;


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
    Assert.assertEquals(1, keyOutputStream.getStreamEntries().size());
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
    Assert.assertTrue(
        blockOutputStream.getCommitIndex2flushedDataMap().size() <= 1);

    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert.assertEquals(pendingWriteChunkCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(pendingPutBlockCount,
        metrics.getContainerOpsMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(writeChunkCount + 4,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    Assert.assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock));
    Assert.assertEquals(totalOpCount + 6,
        metrics.getTotalOpCount());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  public void testWriteWithExceedingMaxBufferLimit() throws Exception {
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
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream)key.getOutputStream();

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
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

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
    Assert.assertEquals(1, keyOutputStream.getStreamEntries().size());
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

    // now close the stream, It will update the ack length after watchForCommit
    key.close();
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
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
    Assert.assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    Assert.assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return ContainerTestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }
  private void validateData(String keyName, byte[] data) throws Exception {
    ContainerTestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }

}
