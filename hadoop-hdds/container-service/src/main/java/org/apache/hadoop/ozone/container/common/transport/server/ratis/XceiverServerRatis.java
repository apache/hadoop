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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.shaded.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates a ratis server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerRatis implements XceiverServerSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverServerRatis.class);
  private static final AtomicLong callIdCounter = new AtomicLong();

  private static long nextCallId() {
    return callIdCounter.getAndIncrement() & Long.MAX_VALUE;
  }

  private final int port;
  private final RaftServer server;
  private ThreadPoolExecutor chunkExecutor;
  private ClientId clientId = ClientId.randomId();

  private XceiverServerRatis(DatanodeDetails dd, int port, String storageDir,
      ContainerDispatcher dispatcher, Configuration conf) throws IOException {

    final String rpcType = conf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    final int raftSegmentSize = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT);
    final int raftSegmentPreallocatedSize = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT);
    final int maxChunkSize = OzoneConfigKeys.DFS_CONTAINER_CHUNK_MAX_SIZE;
    final int numWriteChunkThreads = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_DEFAULT);
    TimeUnit timeUnit =
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration clientRequestTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    timeUnit = OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT
        .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration serverRequestTimeout =
        TimeDuration.valueOf(duration, timeUnit);

    Objects.requireNonNull(dd, "id == null");
    this.port = port;
    RaftProperties serverProperties =
        newRaftProperties(rpc, port, storageDir, maxChunkSize, raftSegmentSize,
            raftSegmentPreallocatedSize);
    setRequestTimeout(serverProperties, clientRequestTimeout,
        serverRequestTimeout);

    chunkExecutor =
        new ThreadPoolExecutor(numWriteChunkThreads, numWriteChunkThreads,
            100, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1024),
            new ThreadPoolExecutor.CallerRunsPolicy());
    ContainerStateMachine stateMachine =
        new ContainerStateMachine(dispatcher, chunkExecutor);
    this.server = RaftServer.newBuilder()
        .setServerId(RatisHelper.toRaftPeerId(dd))
        .setGroup(RatisHelper.emptyRaftGroup())
        .setProperties(serverProperties)
        .setStateMachine(stateMachine)
        .build();
  }

  private static void setRequestTimeout(RaftProperties serverProperties,
      TimeDuration clientRequestTimeout, TimeDuration serverRequestTimeout) {
    RaftClientConfigKeys.Rpc
        .setRequestTimeout(serverProperties, clientRequestTimeout);
    RaftServerConfigKeys.Rpc
        .setRequestTimeout(serverProperties, serverRequestTimeout);
  }

  private static RaftProperties newRaftProperties(
      RpcType rpc, int port, String storageDir, int scmChunkSize,
      int raftSegmentSize, int raftSegmentPreallocatedSize) {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.Log.Appender.setBatchEnabled(properties, true);
    RaftServerConfigKeys.Log.Appender.setBufferCapacity(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(properties,
        SizeInBytes.valueOf(scmChunkSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(raftSegmentSize));
    RaftServerConfigKeys.setStorageDir(properties, new File(storageDir));
    RaftConfigKeys.Rpc.setType(properties, rpc);

    RaftServerConfigKeys.Log.setMaxCachedSegmentNum(properties, 2);
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(scmChunkSize + raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        TimeDuration.valueOf(800, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }
    return properties;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails datanodeDetails, Configuration ozoneConf,
      ContainerDispatcher dispatcher) throws IOException {
    final String ratisDir = File.separator + "ratis";
    int localPort = ozoneConf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT_DEFAULT);
    String storageDir = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ozoneConf.get(OzoneConfigKeys
          .OZONE_METADATA_DIRS);
      Preconditions.checkNotNull(storageDir, "ozone.metadata.dirs " +
          "cannot be null, Please check your configs.");
      storageDir = storageDir.concat(ratisDir);
      LOG.warn("Storage directory for Ratis is not configured. Mapping Ratis " +
              "storage under {}. It is a good idea to map this to an SSD disk.",
          storageDir);
    }

    // Get an available port on current node and
    // use that as the container port
    if (ozoneConf.getBoolean(OzoneConfigKeys
            .DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT_DEFAULT)) {
      try (ServerSocket socket = new ServerSocket()) {
        socket.setReuseAddress(true);
        SocketAddress address = new InetSocketAddress(0);
        socket.bind(address);
        localPort = socket.getLocalPort();
        LOG.info("Found a free port for the server : {}", localPort);
        // If we have random local ports configured this means that it
        // probably running under MiniOzoneCluster. Ratis locks the storage
        // directories, so we need to pass different local directory for each
        // local instance. So we map ratis directories under datanode ID.
        storageDir =
            storageDir.concat(File.separator +
                datanodeDetails.getUuidString());
      } catch (IOException e) {
        LOG.error("Unable find a random free port for the server, "
            + "fallback to use default port {}", localPort, e);
      }
    }
    datanodeDetails.setPort(
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS, localPort));
    return new XceiverServerRatis(datanodeDetails, localPort, storageDir,
        dispatcher, ozoneConf);
  }

  @Override
  public void start() throws IOException {
    LOG.info("Starting {} {} at port {}", getClass().getSimpleName(),
        server.getId(), getIPCPort());
    chunkExecutor.prestartAllCoreThreads();
    server.start();
  }

  @Override
  public void stop() {
    try {
      chunkExecutor.shutdown();
      server.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getIPCPort() {
    return port;
  }

  /**
   * Returns the Replication type supported by this end-point.
   *
   * @return enum -- {Stand_Alone, Ratis, Chained}
   */
  @Override
  public HddsProtos.ReplicationType getServerType() {
    return HddsProtos.ReplicationType.RATIS;
  }

  @VisibleForTesting
  public RaftServer getServer() {
    return server;
  }

  private void processReply(RaftClientReply reply) {

    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. The request will be rejected
    // and will eventually be executed once the request comnes via the leader
    // node.
    NotLeaderException notLeaderException = reply.getNotLeaderException();
    if (notLeaderException != null) {
      LOG.info(reply.getNotLeaderException().getLocalizedMessage());
    }
    StateMachineException stateMachineException =
        reply.getStateMachineException();
    if (stateMachineException != null) {
      // In case the request could not be completed, StateMachine Exception
      // will be thrown. For now, Just log the message.
      // If the container could not be closed, SCM will come to know
      // via containerReports. CloseContainer should be re tried via SCM.
      LOG.error(stateMachineException.getLocalizedMessage());
    }
  }

  @Override
  public void submitRequest(
      ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID)
      throws IOException {
    // ReplicationLevel.ALL ensures the transactions corresponding to
    // the request here are applied on all the raft servers.
    RaftClientRequest raftClientRequest =
        createRaftClientRequest(request, pipelineID,
            RaftClientRequest.writeRequestType(ReplicationLevel.ALL));
    CompletableFuture<RaftClientReply> reply =
        server.submitClientRequestAsync(raftClientRequest);
    reply.thenAccept(this::processReply);
  }

  private RaftClientRequest createRaftClientRequest(
      ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID,
      RaftClientRequest.Type type) {
    return new RaftClientRequest(clientId, server.getId(),
        PipelineID.getFromProtobuf(pipelineID).getRaftGroupID(),
        nextCallId(),0, Message.valueOf(request.toByteString()), type);
  }
}