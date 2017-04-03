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

package org.apache.hadoop.scm;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.shaded.com.google.protobuf.ShadedProtoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An abstract implementation of {@link XceiverClientSpi} using Ratis.
 * The underlying RPC mechanism can be chosen via the constructor.
 */
public final  class XceiverClientRatis implements XceiverClientSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClientRatis.class);

  public static XceiverClientRatis newXceiverClientRatis(
      Pipeline pipeline, Configuration ozoneConf) {
    final String rpcType = ozoneConf.get(
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    return new XceiverClientRatis(pipeline,
        SupportedRpcType.valueOfIgnoreCase(rpcType));
  }

  private final Pipeline pipeline;
  private final RaftClient client;

  /** Constructs a client. */
  XceiverClientRatis(Pipeline pipeline, RpcType rpcType) {
    this.pipeline = pipeline;
    final List<RaftPeer> peers = pipeline.getMachines().stream()
        .map(dn -> dn.getXferAddr())
        .map(addr -> new RaftPeer(new RaftPeerId(addr), addr))
        .collect(Collectors.toList());

    final RaftProperties properties = new RaftProperties();
    final ClientFactory factory = ClientFactory.cast(rpcType.newFactory(
        properties, null));

    client = RaftClient.newBuilder()
        .setClientRpc(factory.newRaftClientRpc())
        .setServers(peers)
        .setLeaderId(new RaftPeerId(pipeline.getLeader().getXferAddr()))
        .setProperties(properties)
        .build();
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void connect() throws Exception {
    // do nothing.
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException {
    LOG.debug("sendCommand {}", request);
    final RaftClientReply reply = client.send(
        () -> ShadedProtoUtil.asShadedByteString(request.toByteArray()));
    LOG.debug("reply {}", reply);
    Preconditions.checkState(reply.isSuccess());
    return ContainerCommandResponseProto.parseFrom(
        ShadedProtoUtil.asByteString(reply.getMessage().getContent()));
  }
}
