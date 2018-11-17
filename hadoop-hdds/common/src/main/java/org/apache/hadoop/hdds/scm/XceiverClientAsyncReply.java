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


import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Async reply from XceiverClient.
 */
public class XceiverClientAsyncReply {

  private CompletableFuture<ContainerCommandResponseProto> response;
  private Long logIndex;
  private Collection<CommitInfo> commitInfos;

  public XceiverClientAsyncReply(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this(response, 0, null);
  }

  public XceiverClientAsyncReply(
      CompletableFuture<ContainerCommandResponseProto> response, long index,
      Collection<CommitInfo> commitInfos) {
    this.commitInfos = commitInfos;
    this.logIndex = index;
    this.response = response;
  }

  /**
   * A class having details about latest commitIndex for each server in the
   * Ratis pipeline. For Standalone pipeline, commitInfo will be null.
   */
  public static class CommitInfo {

    private final String server;

    private final Long commitIndex;

    public CommitInfo(String server, long commitIndex) {
      this.server = server;
      this.commitIndex = commitIndex;
    }

    public String getServer() {
      return server;
    }

    public long getCommitIndex() {
      return commitIndex;
    }
  }

  public Collection<CommitInfo> getCommitInfos() {
    return commitInfos;
  }

  public CompletableFuture<ContainerCommandResponseProto> getResponse() {
    return response;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public void setCommitInfos(Collection<CommitInfo> commitInfos) {
    this.commitInfos = commitInfos;
  }

  public void setLogIndex(Long logIndex) {
    this.logIndex = logIndex;
  }

  public void setResponse(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this.response = response;
  }
}
