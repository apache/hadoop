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

import com.google.protobuf.ByteString;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.Dispatcher;
import org.apache.hadoop.ozone.container.common.impl.KeyManagerImpl;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;

import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineChannel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ROOT_PREFIX;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.CreateContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.PutKeyRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.GetKeyRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.ContainerData;

import org.apache.hadoop.hdds.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;

@State(Scope.Benchmark)
public class BenchMarkDatanodeDispatcher {

  private String baseDir;
  private String datanodeUuid;
  private Dispatcher dispatcher;
  private PipelineChannel pipelineChannel;
  private ByteString data;
  private Random random;
  private AtomicInteger containerCount;
  private AtomicInteger keyCount;
  private AtomicInteger chunkCount;

  @Setup(Level.Trial)
  public void initialize() throws IOException {
    datanodeUuid = UUID.randomUUID().toString();
    pipelineChannel = new PipelineChannel("127.0.0.1",
        LifeCycleState.OPEN, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE, "SA-" + UUID.randomUUID());

    // 1 MB of data
    data = ByteString.copyFromUtf8(RandomStringUtils.randomAscii(1048576));
    random  = new Random();
    Configuration conf = new OzoneConfiguration();
    ContainerManager manager = new ContainerManagerImpl();
    baseDir = System.getProperty("java.io.tmpdir") + File.separator +
        datanodeUuid;

    // data directory
    conf.set("dfs.datanode.data.dir", baseDir + File.separator + "data");

    // metadata directory
    StorageLocation metadataDir = StorageLocation.parse(
        baseDir+ File.separator + CONTAINER_ROOT_PREFIX);
    List<StorageLocation> locations = Arrays.asList(metadataDir);

    manager
        .init(conf, locations, GenesisUtil.createDatanodeDetails(datanodeUuid));
    manager.setChunkManager(new ChunkManagerImpl(manager));
    manager.setKeyManager(new KeyManagerImpl(manager, conf));

    dispatcher = new Dispatcher(manager, conf);
    dispatcher.init();

    containerCount = new AtomicInteger();
    keyCount = new AtomicInteger();
    chunkCount = new AtomicInteger();

    // Create containers
    for (int x = 0; x < 100; x++) {
      String containerName = "container-" + containerCount.getAndIncrement();
      dispatcher.dispatch(getCreateContainerCommand(containerName));
    }
    // Add chunk and keys to the containers
    for (int x = 0; x < 50; x++) {
      String chunkName = "chunk-" + chunkCount.getAndIncrement();
      String keyName = "key-" + keyCount.getAndIncrement();
      for (int y = 0; y < 100; y++) {
        String containerName = "container-" + y;
        dispatcher.dispatch(getWriteChunkCommand(containerName, chunkName));
        dispatcher
            .dispatch(getPutKeyCommand(containerName, chunkName, keyName));
      }
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    dispatcher.shutdown();
    FileUtils.deleteDirectory(new File(baseDir));
  }

  private ContainerCommandRequestProto getCreateContainerCommand(
      String containerName) {
    CreateContainerRequestProto.Builder createRequest =
        CreateContainerRequestProto.newBuilder();
    createRequest.setPipeline(
        new Pipeline(containerName, pipelineChannel).getProtobufMessage());
    createRequest.setContainerData(
        ContainerData.newBuilder().setName(containerName).build());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setCreateContainer(createRequest);
    request.setDatanodeUuid(datanodeUuid);
    request.setTraceID(containerName + "-trace");
    return request.build();
  }

  private ContainerCommandRequestProto getWriteChunkCommand(
      String containerName, String key) {

    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setPipeline(
            new Pipeline(containerName, pipelineChannel).getProtobufMessage())
        .setKeyName(key)
        .setChunkData(getChunkInfo(containerName, key))
        .setData(data);

    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder();
    request.setCmdType(ContainerProtos.Type.WriteChunk)
        .setTraceID(containerName + "-" + key +"-trace")
        .setDatanodeUuid(datanodeUuid)
        .setWriteChunk(writeChunkRequest);
    return request.build();
  }

  private ContainerCommandRequestProto getReadChunkCommand(
      String containerName, String key) {
    ReadChunkRequestProto.Builder readChunkRequest = ReadChunkRequestProto
        .newBuilder()
        .setPipeline(
            new Pipeline(containerName, pipelineChannel).getProtobufMessage())
        .setKeyName(key)
        .setChunkData(getChunkInfo(containerName, key));
    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder();
    request.setCmdType(ContainerProtos.Type.ReadChunk)
        .setTraceID(containerName + "-" + key +"-trace")
        .setDatanodeUuid(datanodeUuid)
        .setReadChunk(readChunkRequest);
    return request.build();
  }

  private ContainerProtos.ChunkInfo getChunkInfo(
      String containerName, String key) {
    ContainerProtos.ChunkInfo.Builder builder =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(
                DigestUtils.md5Hex(key) + "_stream_" + containerName + "_chunk_"
                    + key)
            .setOffset(0).setLen(data.size());
    return builder.build();
  }

  private ContainerCommandRequestProto getPutKeyCommand(
      String containerName, String chunkKey, String key) {
    PutKeyRequestProto.Builder putKeyRequest = PutKeyRequestProto
        .newBuilder()
        .setPipeline(
            new Pipeline(containerName, pipelineChannel).getProtobufMessage())
        .setKeyData(getKeyData(containerName, chunkKey, key));
    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder();
    request.setCmdType(ContainerProtos.Type.PutKey)
        .setTraceID(containerName + "-" + key +"-trace")
        .setDatanodeUuid(datanodeUuid)
        .setPutKey(putKeyRequest);
    return request.build();
  }

  private ContainerCommandRequestProto getGetKeyCommand(
      String containerName, String chunkKey, String key) {
    GetKeyRequestProto.Builder readKeyRequest = GetKeyRequestProto.newBuilder()
        .setPipeline(
            new Pipeline(containerName, pipelineChannel).getProtobufMessage())
        .setKeyData(getKeyData(containerName, chunkKey, key));
    ContainerCommandRequestProto.Builder request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(ContainerProtos.Type.GetKey)
        .setTraceID(containerName + "-" + key +"-trace")
        .setDatanodeUuid(datanodeUuid)
        .setGetKey(readKeyRequest);
    return request.build();
  }

  private ContainerProtos.KeyData getKeyData(
      String containerName, String chunkKey, String key) {
    ContainerProtos.KeyData.Builder builder =  ContainerProtos.KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(key)
        .addChunks(getChunkInfo(containerName, chunkKey));
    return builder.build();
  }

  @Benchmark
  public void createContainer(BenchMarkDatanodeDispatcher bmdd) {
    bmdd.dispatcher.dispatch(getCreateContainerCommand(
        "container-" + containerCount.getAndIncrement()));
  }


  @Benchmark
  public void writeChunk(BenchMarkDatanodeDispatcher bmdd) {
    String containerName = "container-" + random.nextInt(containerCount.get());
    bmdd.dispatcher.dispatch(getWriteChunkCommand(
        containerName, "chunk-" + chunkCount.getAndIncrement()));
  }

  @Benchmark
  public void readChunk(BenchMarkDatanodeDispatcher bmdd) {
    String containerName = "container-" + random.nextInt(containerCount.get());
    String chunkKey = "chunk-" + random.nextInt(chunkCount.get());
    bmdd.dispatcher.dispatch(getReadChunkCommand(containerName, chunkKey));
  }

  @Benchmark
  public void putKey(BenchMarkDatanodeDispatcher bmdd) {
    String containerName = "container-" + random.nextInt(containerCount.get());
    String chunkKey = "chunk-" + random.nextInt(chunkCount.get());
    bmdd.dispatcher.dispatch(getPutKeyCommand(
        containerName, chunkKey, "key-" + keyCount.getAndIncrement()));
  }

  @Benchmark
  public void getKey(BenchMarkDatanodeDispatcher bmdd) {
    String containerName = "container-" + random.nextInt(containerCount.get());
    String chunkKey = "chunk-" + random.nextInt(chunkCount.get());
    String key = "key-" + random.nextInt(keyCount.get());
    bmdd.dispatcher.dispatch(getGetKeyCommand(containerName, chunkKey, key));
  }
}
