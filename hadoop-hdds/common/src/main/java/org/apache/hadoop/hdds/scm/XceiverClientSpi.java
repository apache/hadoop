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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import com.google.common.annotations.VisibleForTesting;

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

  /**
   * Connects to the leader in the pipeline using encoded token. To be used
   * in a secure cluster.
   */
  public abstract void connect(String encodedToken) throws Exception;

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
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException {
    try {
      XceiverClientReply reply;
      reply = sendCommandAsync(request);
      ContainerCommandResponseProto responseProto = reply.getResponse().get();
      return responseProto;
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to command " + request, e);
    }
  }

  /**
   * Sends a given command to server and gets the reply back along with
   * the server associated info.
   * @param request Request
   * @param excludeDns list of servers on which the command won't be sent to.
   * @return Response to the command
   * @throws IOException
   */
  public XceiverClientReply sendCommand(
      ContainerCommandRequestProto request, List<DatanodeDetails> excludeDns)
      throws IOException {
    try {
      XceiverClientReply reply;
      reply = sendCommandAsync(request);
      reply.getResponse().get();
      return reply;
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to command " + request, e);
    }
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  public abstract XceiverClientReply
      sendCommandAsync(ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException;

  /**
   * Returns pipeline Type.
   *
   * @return - {Stand_Alone, Ratis or Chained}
   */
  public abstract HddsProtos.ReplicationType getPipelineType();

  /**
   * Check if an specfic commitIndex is replicated to majority/all servers.
   * @param index index to watch for
   * @param timeout timeout provided for the watch ipeartion to complete
   * @return reply containing the min commit index replicated to all or majority
   *         servers in case of a failure
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   * @throws IOException
   */
  public abstract XceiverClientReply watchForCommit(long index, long timeout)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException;

  /**
   * returns the min commit index replicated to all servers.
   * @return min commit index replicated to all servers.
   */
  public abstract long getReplicatedMinCommitIndex();
}
