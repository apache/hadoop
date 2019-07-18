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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.*;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * This class verifies the watchForCommit Handling by xceiverClient.
 */
public class TestWatchForCommit {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private String keyString;
  private int chunkSize;
  private int flushSize;
  private int maxFlushSize;
  private int blockSize;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String containerOwner = "OZONE";

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  private void startCluster(OzoneConfiguration conf) throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY,
        1, TimeUnit.SECONDS);

    conf.setQuietMode(false);
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
    volumeName = "watchforcommithandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }


  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  @Test
  public void testWatchForCommitWithKeyWrite() throws Exception {
    // in this case, watch request should fail with RaftRetryFailureException
    // and will be captured in keyOutputStream and the failover will happen
    // to a different block
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, 20,
        TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 20);
    conf.setTimeDuration(
        OzoneConfigKeys.DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        1, TimeUnit.SECONDS);
    startCluster(conf);
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
    Assert.assertTrue(HddsClientUtils.checkForException(blockOutputStream
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
    Assert
        .assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    // make sure the bufferPool is empty
    Assert
        .assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    Assert.assertNull(blockOutputStream.getCommitIndex2flushedDataMap());
    validateData(keyName, data1);
    shutdown();
  }

  @Test
  public void testWatchForCommitWithSmallerTimeoutValue() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, 3,
        TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 20);
    startCluster(conf);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, containerOwner);
    XceiverClientSpi xceiverClient = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        xceiverClient.getPipeline());
    Pipeline pipeline = xceiverClient.getPipeline();
    XceiverClientReply reply = xceiverClient.sendCommandAsync(
        ContainerTestHelper.getCreateContainerRequest(
            container1.getContainerInfo().getContainerID(),
            xceiverClient.getPipeline()));
    reply.getResponse().get();
    long index = reply.getLogIndex();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
    try {
      // just watch for a log index which in not updated in the commitInfo Map
      // as well as there is no logIndex generate in Ratis.
      // The basic idea here is just to test if its throws an exception.
      xceiverClient
          .watchForCommit(index + new Random().nextInt(100) + 10, 3000);
      Assert.fail("expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(
          HddsClientUtils.checkForException(e) instanceof TimeoutException);
    }
    // After releasing the xceiverClient, this connection should be closed
    // and any container operations should fail
    clientManager.releaseClient(xceiverClient, false);
    shutdown();
  }

  @Test
  public void testWatchForCommitForRetryfailure() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT,
        100, TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 20);
    startCluster(conf);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, containerOwner);
    XceiverClientSpi xceiverClient = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        xceiverClient.getPipeline());
    Pipeline pipeline = xceiverClient.getPipeline();
    XceiverClientReply reply = xceiverClient.sendCommandAsync(
        ContainerTestHelper.getCreateContainerRequest(
            container1.getContainerInfo().getContainerID(),
            xceiverClient.getPipeline()));
    reply.getResponse().get();
    long index = reply.getLogIndex();
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
    // again write data with more than max buffer limit. This wi
    try {
      // just watch for a log index which in not updated in the commitInfo Map
      // as well as there is no logIndex generate in Ratis.
      // The basic idea here is just to test if its throws an exception.
      xceiverClient
          .watchForCommit(index + new Random().nextInt(100) + 10, 20000);
      Assert.fail("expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ExecutionException);
      // since the timeout value is quite long, the watch request will either
      // fail with NotReplicated exceptio, RetryFailureException or
      // RuntimeException
      Assert.assertFalse(HddsClientUtils
          .checkForException(e) instanceof TimeoutException);
    }
    clientManager.releaseClient(xceiverClient, false);
    shutdown();
  }

  @Test
  public void test2WayCommitForTimeoutException() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, 3,
        TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 20);
    startCluster(conf);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(XceiverClientRatis.LOG);
    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, containerOwner);
    XceiverClientSpi xceiverClient = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        xceiverClient.getPipeline());
    Pipeline pipeline = xceiverClient.getPipeline();
    XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
    XceiverClientReply reply = xceiverClient.sendCommandAsync(
        ContainerTestHelper.getCreateContainerRequest(
            container1.getContainerInfo().getContainerID(),
            xceiverClient.getPipeline()));
    reply.getResponse().get();
    Assert.assertEquals(3, ratisClient.getCommitInfoMap().size());
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    reply = xceiverClient.sendCommandAsync(ContainerTestHelper
        .getCloseContainer(pipeline,
            container1.getContainerInfo().getContainerID()));
    reply.getResponse().get();
    xceiverClient.watchForCommit(reply.getLogIndex(), 3000);

    // commitInfo Map will be reduced to 2 here
    Assert.assertEquals(2, ratisClient.getCommitInfoMap().size());
    clientManager.releaseClient(xceiverClient, false);
    Assert.assertTrue(logCapturer.getOutput().contains("3 way commit failed"));
    Assert.assertTrue(logCapturer.getOutput().contains("TimeoutException"));
    Assert
        .assertTrue(logCapturer.getOutput().contains("Committed by majority"));
    logCapturer.stopCapturing();
    shutdown();
  }

  @Test
  public void testWatchForCommitForGroupMismatchException() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, 20,
        TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 20);

    // mark the node stale early so that pipleline gets destroyed quickly
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    startCluster(conf);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(XceiverClientRatis.LOG);
    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, containerOwner);
    XceiverClientSpi xceiverClient = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        xceiverClient.getPipeline());
    Pipeline pipeline = xceiverClient.getPipeline();
    XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
    long containerId = container1.getContainerInfo().getContainerID();
    XceiverClientReply reply = xceiverClient.sendCommandAsync(
        ContainerTestHelper.getCreateContainerRequest(containerId,
            xceiverClient.getPipeline()));
    reply.getResponse().get();
    Assert.assertEquals(3, ratisClient.getCommitInfoMap().size());
    List<Pipeline> pipelineList = new ArrayList<>();
    pipelineList.add(pipeline);
    ContainerTestHelper.waitForPipelineClose(pipelineList, cluster);
    try {
      // just watch for a log index which in not updated in the commitInfo Map
      // as well as there is no logIndex generate in Ratis.
      // The basic idea here is just to test if its throws an exception.
      xceiverClient
          .watchForCommit(reply.getLogIndex() + new Random().nextInt(100) + 10,
              20000);
      Assert.fail("Expected exception not thrown");
    } catch(Exception e) {
      Assert.assertTrue(HddsClientUtils
          .checkForException(e) instanceof GroupMismatchException);
    }
    clientManager.releaseClient(xceiverClient, false);
    shutdown();
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
