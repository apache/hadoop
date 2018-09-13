/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.server;


import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .ContainerStateMachine;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.ProtoUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class tests ContainerStateMachine.
 */
public class TestContainerStateMachine {

  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private ThreadPoolExecutor executor =
      new ThreadPoolExecutor(4, 4, 100, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(1024),
          new ThreadPoolExecutor.CallerRunsPolicy());
  private ContainerStateMachine stateMachine =
      new ContainerStateMachine(new TestContainerDispatcher(), executor, null);


  @Test
  public void testCloseContainerSynchronization() throws Exception {
    Pipeline pipeline = ContainerTestHelper.createPipeline(3);
    long containerId = new Random().nextLong();

    //create container request
    RaftClientRequest createContainer = getRaftClientRequest(
        ContainerTestHelper.getCreateContainerRequest(containerId, pipeline));

    ContainerCommandRequestProto writeChunkProto = ContainerTestHelper
        .getWriteChunkRequest(pipeline, new BlockID(containerId, nextCallId()),
            1024);

    RaftClientRequest writeChunkRequest = getRaftClientRequest(writeChunkProto);

    // add putKey request
    ContainerCommandRequestProto putKeyProto = ContainerTestHelper
            .getPutKeyRequest(pipeline, writeChunkProto.getWriteChunk());
    RaftClientRequest putKeyRequest = getRaftClientRequest(putKeyProto);

    TransactionContext createContainerCtxt =
        startAndWriteStateMachineData(createContainer);
    // Start and Write into the StateMachine
    TransactionContext writeChunkcontext =
        startAndWriteStateMachineData(writeChunkRequest);

    TransactionContext putKeyContext =
        stateMachine.startTransaction(putKeyRequest);
    Assert.assertEquals(1, stateMachine.getStateMachineMap().size());
    Assert.assertNotNull(stateMachine.getCreateContainerFuture(containerId));
    Assert.assertEquals(1,
        stateMachine.getWriteChunkFutureMap().size());
    Assert.assertTrue(
        stateMachine.getCommitChunkFutureMap(containerId).isEmpty());

    //Add a closeContainerRequest
    RaftClientRequest closeRequest = getRaftClientRequest(
        ContainerTestHelper.getCloseContainer(pipeline, containerId));

    TransactionContext closeCtx = stateMachine.startTransaction(closeRequest);

    // Now apply all the transaction for the CreateContainer Command.
    // This will unblock writeChunks as well

    stateMachine.applyTransaction(createContainerCtxt);
    stateMachine.applyTransaction(writeChunkcontext);
    CompletableFuture<Message> putKeyFuture =
        stateMachine.applyTransaction(putKeyContext);
    waitForTransactionCompletion(putKeyFuture);
    // Make sure the putKey transaction complete
    Assert.assertTrue(putKeyFuture.isDone());

    // Execute the closeContainer. This should ensure all prior Write Type
    // container requests finish execution

    CompletableFuture<Message> closeFuture =
        stateMachine.applyTransaction(closeCtx);
    waitForTransactionCompletion(closeFuture);
    // Make sure the closeContainer transaction complete
    Assert.assertTrue(closeFuture.isDone());
    Assert.assertNull(stateMachine.getCreateContainerFuture(containerId));
    Assert.assertNull(stateMachine.getCommitChunkFutureMap(containerId));

  }

  private RaftClientRequest getRaftClientRequest(
      ContainerCommandRequestProto req) throws IOException {
    ClientId clientId = ClientId.randomId();
    return new RaftClientRequest(clientId,
        RatisHelper.toRaftPeerId(ContainerTestHelper.createDatanodeDetails()),
        RatisHelper.emptyRaftGroup().getGroupId(), nextCallId(), 0,
        Message.valueOf(req.toByteString()), RaftClientRequest
        .writeRequestType(RaftProtos.ReplicationLevel.MAJORITY));
  }

  private void waitForTransactionCompletion(
      CompletableFuture<Message> future) throws Exception {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService
        .invokeAll(Collections.singleton(future::get), 10,
            TimeUnit.SECONDS); // Timeout of 10 minutes.
    executorService.shutdown();
  }

  private TransactionContext startAndWriteStateMachineData(
      RaftClientRequest request) throws IOException {
    TransactionContext ctx = stateMachine.startTransaction(request);
    RaftProtos.LogEntryProto e = ProtoUtils
        .toLogEntryProto(ctx.getSMLogEntry(), request.getSeqNum(),
            request.getCallId(), ClientId.randomId(), request.getCallId());
    ctx.setLogEntry(e);
    stateMachine.writeStateMachineData(e);
    return ctx;
  }

  // ContainerDispatcher for test only purpose.
  private static class TestContainerDispatcher implements ContainerDispatcher {
    /**
     * Dispatches commands to container layer.
     *
     * @param msg - Command Request
     * @return Command Response
     */
    @Override
    public ContainerCommandResponseProto dispatch(
        ContainerCommandRequestProto msg) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void init() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void setScmId(String scmId) {
    }

    @Override
    public Handler getHandler(ContainerType containerType) {
      return null;
    }
  }
}