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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Async reply from XceiverClient.
 */
public class XceiverClientReply {

  private CompletableFuture<ContainerCommandResponseProto> response;
  private Long logIndex;
  private UUID dnId;

  public XceiverClientReply(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this(response, null);
  }

  public XceiverClientReply(
      CompletableFuture<ContainerCommandResponseProto> response, UUID dnId) {
    this.logIndex = (long) 0;
    this.response = response;
    this.dnId = dnId;
  }

  public CompletableFuture<ContainerCommandResponseProto> getResponse() {
    return response;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public void setLogIndex(Long logIndex) {
    this.logIndex = logIndex;
  }

  public UUID getDatanode() {
    return dnId;
  }

  public void setDatanode(UUID datanodeId) {
    this.dnId = datanodeId;
  }

  public void setResponse(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this.response = response;
  }
}
