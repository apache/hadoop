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
package org.apache.hadoop.ozone.genesis;

import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .WriteChunkRequestProto;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

/**
 * Benchmarks DatanodeDispatcher class.
 */
@State(Scope.Benchmark)
public class BenchMarkDatanodeDispatcher {

  private String baseDir;
  private String datanodeUuid;
  private HddsDispatcher dispatcher;
  private ByteString data;
  private Random random;
  private AtomicInteger containerCount;
  private AtomicInteger keyCount;
  private AtomicInteger chunkCount;

  private static final int INIT_CONTAINERS = 100;
  private static final int INIT_KEYS = 50;
  private static final int INIT_CHUNKS = 100;

  private List<Long> containers;
  private List<Long> keys;
  private List<String> chunks;

  @Setup(Level.Trial)
  public void initialize() throws IOException {
    datanodeUuid = UUID.randomUUID().toString();

    // 1 MB of data
    data = ByteString.copyFromUtf8(RandomStringUtils.randomAscii(1048576));
    random  = new Random();
    Configuration conf = new OzoneConfiguration();
    baseDir = System.getProperty("java.io.tmpdir") + File.separator +
        datanodeUuid;

    // data directory
    conf.set("dfs.datanode.data.dir", baseDir + File.separator + "data");

    ContainerSet containerSet = new ContainerSet();
    VolumeSet volumeSet = new VolumeSet(datanodeUuid, conf);
    StateContext context = new StateContext(
        conf, DatanodeStates.RUNNING, null);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf, context, containerSet, volumeSet, metrics));
    }
    dispatcher = new HddsDispatcher(conf, containerSet, volumeSet, handlers,
        context, metrics);
    dispatcher.init();

    containerCount = new AtomicInteger();
    keyCount = new AtomicInteger();
    chunkCount = new AtomicInteger();

    containers = new ArrayList<>();
    keys = new ArrayList<>();
    chunks = new ArrayList<>();

    // Create containers
    for (int x = 0; x < INIT_CONTAINERS; x++) {
      long containerID = HddsUtils.getUtcTime() + x;
      ContainerCommandRequestProto req = getCreateContainerCommand(containerID);
      dispatcher.dispatch(req, null);
      containers.add(containerID);
      containerCount.getAndIncrement();
    }

    for (int x = 0; x < INIT_KEYS; x++) {
      keys.add(HddsUtils.getUtcTime()+x);
    }

    for (int x = 0; x < INIT_CHUNKS; x++) {
      chunks.add("chunk-" + x);
    }

    // Add chunk and keys to the containers
    for (int x = 0; x < INIT_KEYS; x++) {
      String chunkName = chunks.get(x);
      chunkCount.getAndIncrement();
      long key = keys.get(x);
      keyCount.getAndIncrement();
      for (int y = 0; y < INIT_CONTAINERS; y++) {
        long containerID = containers.get(y);
        BlockID  blockID = new BlockID(containerID, key);
        dispatcher
            .dispatch(getPutBlockCommand(blockID, chunkName), null);
        dispatcher.dispatch(getWriteChunkCommand(blockID, chunkName), null);
      }
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    dispatcher.shutdown();
    FileUtils.deleteDirectory(new File(baseDir));
  }

  private ContainerCommandRequestProto getCreateContainerCommand(
      long containerID) {
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(datanodeUuid);
    request.setTraceID(containerID + "-trace");
    return request.build();
  }

  private ContainerCommandRequestProto getWriteChunkCommand(
      BlockID blockID, String chunkName) {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(getChunkInfo(blockID, chunkName))
        .setData(data);

    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder();
    request.setCmdType(ContainerProtos.Type.WriteChunk)
        .setContainerID(blockID.getContainerID())
        .setTraceID(getBlockTraceID(blockID))
        .setDatanodeUuid(datanodeUuid)
        .setWriteChunk(writeChunkRequest);
    return request.build();
  }

  private ContainerCommandRequestProto getReadChunkCommand(
      BlockID blockID, String chunkName) {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(getChunkInfo(blockID, chunkName));

    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder();
    request.setCmdType(ContainerProtos.Type.ReadChunk)
        .setContainerID(blockID.getContainerID())
        .setTraceID(getBlockTraceID(blockID))
        .setDatanodeUuid(datanodeUuid)
        .setReadChunk(readChunkRequest);
    return request.build();
  }

  private ContainerProtos.ChunkInfo getChunkInfo(
      BlockID blockID, String chunkName) {
    ContainerProtos.ChunkInfo.Builder builder =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(
                DigestUtils.md5Hex(chunkName)
                    + "_stream_" + blockID.getContainerID() + "_block_"
                    + blockID.getLocalID())
            .setOffset(0).setLen(data.size());
    return builder.build();
  }

  private ContainerCommandRequestProto getPutBlockCommand(
      BlockID blockID, String chunkKey) {
    PutBlockRequestProto.Builder putBlockRequest = PutBlockRequestProto
        .newBuilder()
        .setBlockData(getBlockData(blockID, chunkKey));

    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder();
    request.setCmdType(ContainerProtos.Type.PutBlock)
        .setContainerID(blockID.getContainerID())
        .setTraceID(getBlockTraceID(blockID))
        .setDatanodeUuid(datanodeUuid)
        .setPutBlock(putBlockRequest);
    return request.build();
  }

  private ContainerCommandRequestProto getGetBlockCommand(BlockID blockID) {
    GetBlockRequestProto.Builder readBlockRequest =
        GetBlockRequestProto.newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(ContainerProtos.Type.GetBlock)
        .setContainerID(blockID.getContainerID())
        .setTraceID(getBlockTraceID(blockID))
        .setDatanodeUuid(datanodeUuid)
        .setGetBlock(readBlockRequest);
    return request.build();
  }

  private ContainerProtos.BlockData getBlockData(
      BlockID blockID, String chunkKey) {
    ContainerProtos.BlockData.Builder builder =  ContainerProtos.BlockData
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .addChunks(getChunkInfo(blockID, chunkKey));
    return builder.build();
  }

  @Benchmark
  public void createContainer(BenchMarkDatanodeDispatcher bmdd) {
    long containerID = RandomUtils.nextLong();
    ContainerCommandRequestProto req = getCreateContainerCommand(containerID);
    bmdd.dispatcher.dispatch(req, null);
    bmdd.containers.add(containerID);
    bmdd.containerCount.getAndIncrement();
  }


  @Benchmark
  public void writeChunk(BenchMarkDatanodeDispatcher bmdd) {
    bmdd.dispatcher.dispatch(getWriteChunkCommand(
        getRandomBlockID(), getNewChunkToWrite()), null);
  }

  @Benchmark
  public void readChunk(BenchMarkDatanodeDispatcher bmdd) {
    BlockID blockID = getRandomBlockID();
    String chunkKey = getRandomChunkToRead();
    bmdd.dispatcher.dispatch(getReadChunkCommand(blockID, chunkKey), null);
  }

  @Benchmark
  public void putBlock(BenchMarkDatanodeDispatcher bmdd) {
    BlockID blockID = getRandomBlockID();
    String chunkKey = getNewChunkToWrite();
    bmdd.dispatcher.dispatch(getPutBlockCommand(blockID, chunkKey), null);
  }

  @Benchmark
  public void getBlock(BenchMarkDatanodeDispatcher bmdd) {
    BlockID blockID = getRandomBlockID();
    bmdd.dispatcher.dispatch(getGetBlockCommand(blockID), null);
  }

  // Chunks writes from benchmark only reaches certain containers
  // Use INIT_CHUNKS instead of updated counters to guarantee
  // key/chunks are readable.

  private BlockID getRandomBlockID() {
    return new BlockID(getRandomContainerID(), getRandomKeyID());
  }

  private long getRandomContainerID() {
    return containers.get(random.nextInt(INIT_CONTAINERS));
  }

  private long getRandomKeyID() {
    return keys.get(random.nextInt(INIT_KEYS));
  }

  private String getRandomChunkToRead() {
    return chunks.get(random.nextInt(INIT_CHUNKS));
  }

  private String getNewChunkToWrite() {
    return "chunk-" + chunkCount.getAndIncrement();
  }

  private String getBlockTraceID(BlockID blockID) {
    return blockID.getContainerID() + "-" + blockID.getLocalID() +"-trace";
  }
}
