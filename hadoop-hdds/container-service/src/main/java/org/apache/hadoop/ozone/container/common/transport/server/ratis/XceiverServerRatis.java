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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;

import io.opentracing.Scope;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates a ratis server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerRatis extends XceiverServer {
  private static final Logger LOG = LoggerFactory
      .getLogger(XceiverServerRatis.class);
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private final int port;
  private final RaftServer server;
  private ThreadPoolExecutor chunkExecutor;
  private final List<ExecutorService> executors;
  private final ContainerDispatcher dispatcher;
  private ClientId clientId = ClientId.randomId();
  private final StateContext context;
  private final ReplicationLevel replicationLevel;
  private long nodeFailureTimeoutMs;
  private final long cacheEntryExpiryInteval;

  private XceiverServerRatis(DatanodeDetails dd, int port,
      ContainerDispatcher dispatcher, Configuration conf, StateContext
      context, GrpcTlsConfig tlsConfig, CertificateClient caClient)
      throws IOException {
    super(conf, caClient);
    Objects.requireNonNull(dd, "id == null");
    this.port = port;
    RaftProperties serverProperties = newRaftProperties(conf);
    final int numWriteChunkThreads = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_DEFAULT);
    chunkExecutor =
        new ThreadPoolExecutor(numWriteChunkThreads, numWriteChunkThreads,
            100, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1024),
            new ThreadPoolExecutor.CallerRunsPolicy());
    final int numContainerOpExecutors = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT);
    this.context = context;
    this.replicationLevel =
        conf.getEnum(OzoneConfigKeys.DFS_CONTAINER_RATIS_REPLICATION_LEVEL_KEY,
            OzoneConfigKeys.DFS_CONTAINER_RATIS_REPLICATION_LEVEL_DEFAULT);
    this.executors = new ArrayList<>();
    cacheEntryExpiryInteval = conf.getTimeDuration(OzoneConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL,
        OzoneConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.dispatcher = dispatcher;
    for (int i = 0; i < numContainerOpExecutors; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }

    RaftServer.Builder builder = RaftServer.newBuilder()
        .setServerId(RatisHelper.toRaftPeerId(dd))
        .setProperties(serverProperties)
        .setStateMachineRegistry(this::getStateMachine);
    if (tlsConfig != null) {
      builder.setParameters(GrpcFactory.newRaftParameters(tlsConfig));
    }
    this.server = builder.build();
  }

  private ContainerStateMachine getStateMachine(RaftGroupId gid) {
    return new ContainerStateMachine(gid, dispatcher, chunkExecutor, this,
        Collections.unmodifiableList(executors), cacheEntryExpiryInteval,
        getSecurityConfig().isBlockTokenEnabled(), getBlockTokenVerifier());
  }

  private RaftProperties newRaftProperties(Configuration conf) {
    final RaftProperties properties = new RaftProperties();

    // Set rpc type
    final RpcType rpc = setRpcType(conf, properties);

    // set raft segment size
    setRaftSegmentSize(conf, properties);

    // set raft segment pre-allocated size
    final int raftSegmentPreallocatedSize =
        setRaftSegmentPreallocatedSize(conf, properties);

    // Set max write buffer size, which is the scm chunk size
    final int maxChunkSize = setMaxWriteBuffer(properties);
    TimeUnit timeUnit;
    long duration;

    // set the configs enable and set the stateMachineData sync timeout
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
    timeUnit = OzoneConfigKeys.
        DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT.getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT,
        OzoneConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration dataSyncTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.Log.StateMachineData
        .setSyncTimeout(properties, dataSyncTimeout);

    // Set the server Request timeout
    setServerRequestTimeout(conf, properties);

    // set timeout for a retry cache entry
    setTimeoutForRetryCache(conf, properties);

    // Set the ratis leader election timeout
    setRatisLeaderElectionTimeout(conf, properties);

    // Set the maximum cache segments
    RaftServerConfigKeys.Log.setMaxCachedSegmentNum(properties, 2);

    // set the node failure timeout
    setNodeFailureTimeout(conf, properties);

    // Set the ratis storage directory
    String storageDir = HddsServerUtil.getOzoneDatanodeRatisDirectory(conf);
    RaftServerConfigKeys.setStorageDirs(properties,
        Collections.singletonList(new File(storageDir)));

    // For grpc set the maximum message size
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(maxChunkSize + raftSegmentPreallocatedSize));

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }

    long snapshotThreshold =
        conf.getLong(OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY,
            OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_DEFAULT);
    RaftServerConfigKeys.Snapshot.
      setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.
      setAutoTriggerThreshold(properties, snapshotThreshold);
    int logQueueNumElements =
        conf.getInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS,
            OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS_DEFAULT);
    final int logQueueByteLimit = (int) conf.getStorageSize(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setElementLimit(properties, logQueueNumElements);
    RaftServerConfigKeys.Log.setByteLimit(properties, logQueueByteLimit);

    int numSyncRetries = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES,
        OzoneConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES_DEFAULT);
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeoutRetry(properties,
        numSyncRetries);

    // Enable the StateMachineCaching
    RaftServerConfigKeys.Log.StateMachineData
        .setCachingEnabled(properties, true);
    return properties;
  }

  private void setNodeFailureTimeout(Configuration conf,
                                     RaftProperties properties) {
    TimeUnit timeUnit;
    long duration;
    timeUnit = OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT
        .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration nodeFailureTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.setLeaderElectionTimeout(properties,
        nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        nodeFailureTimeout);
    nodeFailureTimeoutMs = nodeFailureTimeout.toLong(TimeUnit.MILLISECONDS);
  }

  private void setRatisLeaderElectionTimeout(Configuration conf,
                                             RaftProperties properties) {
    long duration;
    TimeUnit leaderElectionMinTimeoutUnit =
        OzoneConfigKeys.
            DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.
            DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getDuration(), leaderElectionMinTimeoutUnit);
    final TimeDuration leaderElectionMinTimeout =
        TimeDuration.valueOf(duration, leaderElectionMinTimeoutUnit);
    RaftServerConfigKeys.Rpc
        .setTimeoutMin(properties, leaderElectionMinTimeout);
    long leaderElectionMaxTimeout =
        leaderElectionMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));
  }

  private void setTimeoutForRetryCache(Configuration conf,
                                       RaftProperties properties) {
    TimeUnit timeUnit;
    long duration;
    timeUnit =
        OzoneConfigKeys.DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration retryCacheTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.RetryCache
        .setExpiryTime(properties, retryCacheTimeout);
  }

  private void setServerRequestTimeout(Configuration conf,
                                       RaftProperties properties) {
    TimeUnit timeUnit;
    long duration;
    timeUnit = OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT
        .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration serverRequestTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.Rpc
        .setRequestTimeout(properties, serverRequestTimeout);
  }

  private int setMaxWriteBuffer(RaftProperties properties) {
    final int maxChunkSize = OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE;
    RaftServerConfigKeys.Log.setWriteBufferSize(properties,
        SizeInBytes.valueOf(maxChunkSize));
    return maxChunkSize;
  }

  private int setRaftSegmentPreallocatedSize(Configuration conf,
                                             RaftProperties properties) {
    final int raftSegmentPreallocatedSize = (int) conf.getStorageSize(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT,
        StorageUnit.BYTES);
    int logAppenderQueueNumElements = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS,
        OzoneConfigKeys
            .DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT);
    final int logAppenderQueueByteLimit = (int) conf.getStorageSize(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OzoneConfigKeys
            .DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.Appender
        .setBufferElementLimit(properties, logAppenderQueueNumElements);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(logAppenderQueueByteLimit));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    return raftSegmentPreallocatedSize;
  }

  private void setRaftSegmentSize(Configuration conf,
                                  RaftProperties properties) {
    final int raftSegmentSize = (int)conf.getStorageSize(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(raftSegmentSize));
  }

  private RpcType setRpcType(Configuration conf, RaftProperties properties) {
    final String rpcType = conf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    RaftConfigKeys.Rpc.setType(properties, rpc);
    return rpc;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails datanodeDetails, Configuration ozoneConf,
      ContainerDispatcher dispatcher, StateContext context,
      CertificateClient caClient) throws IOException {
    int localPort = ozoneConf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT_DEFAULT);

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
      } catch (IOException e) {
        LOG.error("Unable find a random free port for the server, "
            + "fallback to use default port {}", localPort, e);
      }
    }
    GrpcTlsConfig tlsConfig = RatisHelper.createTlsServerConfig(
          new SecurityConfig(ozoneConf));
    datanodeDetails.setPort(
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS, localPort));
    return new XceiverServerRatis(datanodeDetails, localPort,
        dispatcher, ozoneConf, context, tlsConfig, caClient);
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
      // shutdown server before the executors as while shutting down,
      // some of the tasks would be executed using the executors.
      server.close();
      chunkExecutor.shutdown();
      executors.forEach(ExecutorService::shutdown);
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

  private void processReply(RaftClientReply reply) throws IOException {
    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. The request will be rejected
    // and will eventually be executed once the request comes via the leader
    // node.
    NotLeaderException notLeaderException = reply.getNotLeaderException();
    if (notLeaderException != null) {
      throw notLeaderException;
    }
    StateMachineException stateMachineException =
        reply.getStateMachineException();
    if (stateMachineException != null) {
      throw stateMachineException;
    }
  }

  @Override
  public void submitRequest(ContainerCommandRequestProto request,
      HddsProtos.PipelineID pipelineID) throws IOException {
    super.submitRequest(request, pipelineID);
    RaftClientReply reply;
    try (Scope scope = TracingUtil
        .importAndCreateScope(
            "XceiverServerRatis." + request.getCmdType().name(),
            request.getTraceID())) {

      RaftClientRequest raftClientRequest =
          createRaftClientRequest(request, pipelineID,
              RaftClientRequest.writeRequestType());
      try {
        reply = server.submitClientRequestAsync(raftClientRequest).get();
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
      processReply(reply);
    }
  }

  private RaftClientRequest createRaftClientRequest(
      ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID,
      RaftClientRequest.Type type) {
    return new RaftClientRequest(clientId, server.getId(),
        RaftGroupId.valueOf(PipelineID.getFromProtobuf(pipelineID).getId()),
        nextCallId(), Message.valueOf(request.toByteString()), type,
        null);
  }

  private void handlePipelineFailure(RaftGroupId groupId,
      RoleInfoProto roleInfoProto) {
    String msg;
    UUID datanode = RatisHelper.toDatanodeId(roleInfoProto.getSelf());
    RaftPeerId id = RaftPeerId.valueOf(roleInfoProto.getSelf().getId());
    switch (roleInfoProto.getRole()) {
    case CANDIDATE:
      msg = datanode + " is in candidate state for " +
          roleInfoProto.getCandidateInfo().getLastLeaderElapsedTimeMs() + "ms";
      break;
    case LEADER:
      StringBuilder sb = new StringBuilder();
      sb.append(datanode).append(" has not seen follower/s");
      for (RaftProtos.ServerRpcProto follower : roleInfoProto.getLeaderInfo()
          .getFollowerInfoList()) {
        if (follower.getLastRpcElapsedTimeMs() > nodeFailureTimeoutMs) {
          sb.append(" ").append(RatisHelper.toDatanodeId(follower.getId()))
              .append(" for ").append(follower.getLastRpcElapsedTimeMs())
              .append("ms");
        }
      }
      msg = sb.toString();
      break;
    default:
      LOG.error("unknown state:" + roleInfoProto.getRole());
      throw new IllegalStateException("node" + id + " is in illegal role "
          + roleInfoProto.getRole());
    }

    PipelineID pipelineID = PipelineID.valueOf(groupId.getUuid());
    ClosePipelineInfo.Builder closePipelineInfo =
        ClosePipelineInfo.newBuilder()
            .setPipelineID(pipelineID.getProtobuf())
            .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED)
            .setDetailedReason(msg);

    PipelineAction action = PipelineAction.newBuilder()
        .setClosePipeline(closePipelineInfo)
        .setAction(PipelineAction.Action.CLOSE)
        .build();
    context.addPipelineActionIfAbsent(action);
    LOG.debug(
        "pipeline Action " + action.getAction() + "  on pipeline " + pipelineID
            + ".Reason : " + action.getClosePipeline().getDetailedReason());
  }

  @Override
  public boolean isExist(HddsProtos.PipelineID pipelineId) {
    for (RaftGroupId groupId : server.getGroupIds()) {
      if (PipelineID.valueOf(groupId.getUuid()).getProtobuf()
          .equals(pipelineId)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<PipelineReport> getPipelineReport() {
    try {
      Iterable<RaftGroupId> gids = server.getGroupIds();
      List<PipelineReport> reports = new ArrayList<>();
      for (RaftGroupId groupId : gids) {
        reports.add(PipelineReport.newBuilder()
            .setPipelineID(PipelineID.valueOf(groupId.getUuid()).getProtobuf())
            .build());
      }
      return reports;
    } catch (Exception e) {
      return null;
    }
  }

  @VisibleForTesting
  public List<PipelineID> getPipelineIds() {
    Iterable<RaftGroupId> gids = server.getGroupIds();
    List<PipelineID> pipelineIDs = new ArrayList<>();
    for (RaftGroupId groupId : gids) {
      pipelineIDs.add(PipelineID.valueOf(groupId.getUuid()));
      LOG.info("pipeline id {}", PipelineID.valueOf(groupId.getUuid()));
    }
    return pipelineIDs;
  }

  void handleNodeSlowness(RaftGroup group, RoleInfoProto roleInfoProto) {
    handlePipelineFailure(group.getGroupId(), roleInfoProto);
  }

  void handleNoLeader(RaftGroup group, RoleInfoProto roleInfoProto) {
    handlePipelineFailure(group.getGroupId(), roleInfoProto);
  }
}