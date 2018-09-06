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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Client for the storageContainer protocol.
 */
public abstract class XceiverClientSpi implements Closeable {

  final private AtomicInteger referenceCount;
  private boolean isEvicted;

  XceiverClientSpi() {
    this.referenceCount = new AtomicInteger(0);
    this.isEvicted = false;
  }

  void incrementReference() {
    this.referenceCount.incrementAndGet();
  }

  void decrementReference() {
    this.referenceCount.decrementAndGet();
    cleanup();
  }

  void setEvicted() {
    isEvicted = true;
    cleanup();
  }

  // close the xceiverClient only if,
  // 1) there is no refcount on the client
  // 2) it has been evicted from the cache.
  private void cleanup() {
    if (referenceCount.get() == 0 && isEvicted) {
      close();
    }
  }

  @VisibleForTesting
  public int getRefcount() {
    return referenceCount.get();
  }

  /**
   * Connects to the leader in the pipeline.
   */
  public abstract void connect() throws Exception;

  @Override
  public abstract void close();

  /**
   * Returns the pipeline of machines that host the container used by this
   * client.
   *
   * @return pipeline of machines that host the container
   */
  public abstract Pipeline getPipeline();

  /**
   * Sends a given command to server and gets the reply back.
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  public abstract ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException;

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  public abstract CompletableFuture<ContainerCommandResponseProto>
      sendCommandAsync(ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException;

  /**
   * Create a pipeline.
   */
  public abstract void createPipeline() throws IOException;

  /**
   * Destroy a pipeline.
   * @throws IOException
   */
  public abstract void destroyPipeline() throws IOException;

  /**
   * Returns pipeline Type.
   *
   * @return - {Stand_Alone, Ratis or Chained}
   */
  public abstract HddsProtos.ReplicationType getPipelineType();
}
