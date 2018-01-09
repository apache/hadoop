/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ShadedProtoUtil;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.BaseStateMachine;
import org.apache.ratis.statemachine.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;

/** A {@link org.apache.ratis.statemachine.StateMachine} for containers.  */
public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(
      ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage
      = new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;
  private final ThreadPoolExecutor writeChunkExecutor;
  private final ConcurrentHashMap<String, CompletableFuture<Message>>
                                                                writeChunkMap;

  ContainerStateMachine(ContainerDispatcher dispatcher,
      int numWriteChunkThreads) {
    this.dispatcher = dispatcher;
    writeChunkMap = new ConcurrentHashMap<>();
    writeChunkExecutor =
        new ThreadPoolExecutor(numWriteChunkThreads, numWriteChunkThreads,
            60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1024),
            new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void initialize(
      RaftPeerId id, RaftProperties properties, RaftStorage raftStorage)
      throws IOException {
    super.initialize(id, properties, raftStorage);
    storage.init(raftStorage);
    writeChunkExecutor.prestartAllCoreThreads();
    //  TODO handle snapshots

    // TODO: Add a flag that tells you that initialize has been called.
    // Check with Ratis if this feature is done in Ratis.
  }

  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    final ContainerCommandRequestProto proto =
        getRequestProto(request.getMessage().getContent());

    final SMLogEntryProto log;
    if (proto.getCmdType() == ContainerProtos.Type.WriteChunk) {
      final WriteChunkRequestProto write = proto.getWriteChunk();
      // create the state machine data proto
      final WriteChunkRequestProto dataWriteChunkProto =
          WriteChunkRequestProto
              .newBuilder(write)
              .setStage(ContainerProtos.Stage.WRITE_DATA)
              .build();
      ContainerCommandRequestProto dataContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(dataWriteChunkProto)
              .build();

      // create the log entry proto
      final WriteChunkRequestProto commitWriteChunkProto =
          WriteChunkRequestProto
              .newBuilder(write)
              .setStage(ContainerProtos.Stage.COMMIT_DATA)
              .build();
      ContainerCommandRequestProto commitContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(commitWriteChunkProto)
              .build();

      log = SMLogEntryProto.newBuilder()
          .setData(getShadedByteString(commitContainerCommandProto))
          .setStateMachineData(getShadedByteString(dataContainerCommandProto))
          .build();
    } else {
      log = SMLogEntryProto.newBuilder()
          .setData(request.getMessage().getContent())
          .build();
    }
    return new TransactionContext(this, request, log);
  }

  private ByteString getShadedByteString(ContainerCommandRequestProto proto) {
    return ShadedProtoUtil.asShadedByteString(proto.toByteArray());
  }

  private ContainerCommandRequestProto getRequestProto(ByteString request)
      throws InvalidProtocolBufferException {
    return ContainerCommandRequestProto.parseFrom(
        ShadedProtoUtil.asByteString(request));
  }

  private Message runCommand(ContainerCommandRequestProto requestProto) {
    LOG.trace("dispatch {}", requestProto);
    ContainerCommandResponseProto response = dispatcher.dispatch(requestProto);
    LOG.trace("response {}", response);
    return () -> ShadedProtoUtil.asShadedByteString(response.toByteArray());
  }

  @Override
  public CompletableFuture<Message> writeStateMachineData(LogEntryProto entry) {
    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(entry.getSmLogEntry().getStateMachineData());
      final WriteChunkRequestProto write = requestProto.getWriteChunk();
      Message raftClientReply = runCommand(requestProto);
      CompletableFuture<Message> future =
          CompletableFuture.completedFuture(raftClientReply);
      writeChunkMap.put(write.getChunkData().getChunkName(),future);
      return future;
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<RaftClientReply> query(RaftClientRequest request) {
    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(request.getMessage().getContent());
      RaftClientReply raftClientReply =
          new RaftClientReply(request, runCommand(requestProto));
      return CompletableFuture.completedFuture(raftClientReply);
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      ContainerCommandRequestProto requestProto =
          getRequestProto(trx.getSMLogEntry().getData());

      if (requestProto.getCmdType() == ContainerProtos.Type.WriteChunk) {
        WriteChunkRequestProto write = requestProto.getWriteChunk();
        CompletableFuture<Message> stateMachineFuture =
            writeChunkMap.remove(write.getChunkData().getChunkName());
        return stateMachineFuture
            .thenComposeAsync(v ->
                CompletableFuture.completedFuture(runCommand(requestProto)));
      } else {
        return CompletableFuture.completedFuture(runCommand(requestProto));
      }
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @Override
  public void close() throws IOException {
    writeChunkExecutor.shutdown();
  }
}
