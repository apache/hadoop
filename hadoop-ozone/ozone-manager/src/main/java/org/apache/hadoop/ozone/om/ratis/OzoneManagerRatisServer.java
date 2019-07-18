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

package org.apache.hadoop.ozone.om.ratis;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMNodeDetails;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.STATUS_CODE;

/**
 * Creates a Ratis server endpoint for OM.
 */
public final class OzoneManagerRatisServer {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerRatisServer.class);

  private final int port;
  private final InetSocketAddress omRatisAddress;
  private final RaftServer server;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;

  private final OzoneManager ozoneManager;
  private final OzoneManagerStateMachine omStateMachine;
  private final ClientId clientId = ClientId.randomId();

  private final ScheduledExecutorService scheduledRoleChecker;
  private long roleCheckInitialDelayMs = 1000; // 1 second default
  private long roleCheckIntervalMs;
  private ReentrantReadWriteLock roleCheckLock = new ReentrantReadWriteLock();
  private Optional<RaftPeerRole> cachedPeerRole = Optional.empty();
  private Optional<RaftPeerId> cachedLeaderPeerId = Optional.empty();

  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  /**
   * Submit request to Ratis server.
   * @param omRequest
   * @return OMResponse - response returned to the client.
   * @throws ServiceException
   */
  public OMResponse submitRequest(OMRequest omRequest) throws ServiceException {
    RaftClientRequest raftClientRequest =
        createWriteRaftClientRequest(omRequest);
    RaftClientReply raftClientReply;
    try {
      raftClientReply = server.submitClientRequestAsync(raftClientRequest)
          .get();
    } catch (Exception ex) {
      throw new ServiceException(ex.getMessage(), ex);
    }

    return processReply(omRequest, raftClientReply);
  }

  /**
   * Create Write RaftClient request from OMRequest.
   * @param omRequest
   * @return RaftClientRequest - Raft Client request which is submitted to
   * ratis server.
   */
  private RaftClientRequest createWriteRaftClientRequest(OMRequest omRequest) {
    return new RaftClientRequest(clientId, server.getId(), raftGroupId,
        nextCallId(),
        Message.valueOf(OMRatisHelper.convertRequestToByteString(omRequest)),
        RaftClientRequest.writeRequestType(), null);
  }

  /**
   * Process the raftClientReply and return OMResponse.
   * @param omRequest
   * @param reply
   * @return OMResponse - response which is returned to client.
   * @throws ServiceException
   */
  private OMResponse processReply(OMRequest omRequest, RaftClientReply reply)
      throws ServiceException {
    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. This can happen first time
    // when client is submitting request to OM.
    NotLeaderException notLeaderException = reply.getNotLeaderException();
    if (notLeaderException != null) {
      throw new ServiceException(notLeaderException);
    }
    StateMachineException stateMachineException =
        reply.getStateMachineException();
    if (stateMachineException != null) {
      OMResponse.Builder omResponse = OMResponse.newBuilder();
      omResponse.setCmdType(omRequest.getCmdType());
      omResponse.setSuccess(false);
      omResponse.setMessage(stateMachineException.getCause().getMessage());
      omResponse.setStatus(parseErrorStatus(
          stateMachineException.getCause().getMessage()));
      LOG.debug("Error while executing ratis request. " +
          "stateMachineException: ", stateMachineException);
      return omResponse.build();
    }

    try {
      return OMRatisHelper.getOMResponseFromRaftClientReply(reply);
    } catch (InvalidProtocolBufferException ex) {
      if (ex.getMessage() != null) {
        throw new ServiceException(ex.getMessage(), ex);
      } else {
        throw new ServiceException(ex);
      }
    }

    // TODO: Still need to handle RaftRetry failure exception and
    //  NotReplicated exception.
  }

  /**
   * Parse errorMessage received from the exception and convert to
   * {@link OzoneManagerProtocolProtos.Status}.
   * @param errorMessage
   * @return OzoneManagerProtocolProtos.Status
   */
  private OzoneManagerProtocolProtos.Status parseErrorStatus(
      String errorMessage) {
    if (errorMessage.contains(STATUS_CODE)) {
      String errorCode = errorMessage.substring(
          errorMessage.indexOf(STATUS_CODE) + STATUS_CODE.length());
      LOG.debug("Parsing error message for error code " +
          errorCode);
      return OzoneManagerProtocolProtos.Status.valueOf(errorCode.trim());
    } else {
      return OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
    }

  }


  /**
   * Returns an OM Ratis server.
   * @param conf configuration
   * @param om the OM instance starting the ratis server
   * @param raftGroupIdStr raft group id string
   * @param localRaftPeerId raft peer id of this Ratis server
   * @param addr address of the ratis server
   * @param raftPeers peer nodes in the raft ring
   * @throws IOException
   */
  private OzoneManagerRatisServer(Configuration conf,
      OzoneManager om,
      String raftGroupIdStr, RaftPeerId localRaftPeerId,
      InetSocketAddress addr, List<RaftPeer> raftPeers)
      throws IOException {
    this.ozoneManager = om;
    this.omRatisAddress = addr;
    this.port = addr.getPort();
    RaftProperties serverProperties = newRaftProperties(conf);

    this.raftPeerId = localRaftPeerId;
    this.raftGroupId = RaftGroupId.valueOf(
        getRaftGroupIdFromOmServiceId(raftGroupIdStr));
    this.raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);

    StringBuilder raftPeersStr = new StringBuilder();
    for (RaftPeer peer : raftPeers) {
      raftPeersStr.append(", ").append(peer.getAddress());
    }
    LOG.info("Instantiating OM Ratis server with GroupID: {} and " +
        "Raft Peers: {}", raftGroupIdStr, raftPeersStr.toString().substring(2));

    this.omStateMachine = getStateMachine();

    this.server = RaftServer.newBuilder()
        .setServerId(this.raftPeerId)
        .setGroup(this.raftGroup)
        .setProperties(serverProperties)
        .setStateMachine(omStateMachine)
        .build();

    // Run a scheduler to check and update the server role on the leader
    // periodically
    this.scheduledRoleChecker = Executors.newSingleThreadScheduledExecutor();
    this.scheduledRoleChecker.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        // Run this check only on the leader OM
        if (cachedPeerRole.isPresent() &&
            cachedPeerRole.get() == RaftPeerRole.LEADER) {
          updateServerRole();
        }
      }
    }, roleCheckInitialDelayMs, roleCheckIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Creates an instance of OzoneManagerRatisServer.
   */
  public static OzoneManagerRatisServer newOMRatisServer(
      Configuration ozoneConf, OzoneManager omProtocol,
      OMNodeDetails omNodeDetails, List<OMNodeDetails> peerNodes)
      throws IOException {

    // RaftGroupId is the omServiceId
    String omServiceId = omNodeDetails.getOMServiceId();

    String omNodeId = omNodeDetails.getOMNodeId();
    RaftPeerId localRaftPeerId = RaftPeerId.getRaftPeerId(omNodeId);

    InetSocketAddress ratisAddr = new InetSocketAddress(
        omNodeDetails.getAddress(), omNodeDetails.getRatisPort());

    RaftPeer localRaftPeer = new RaftPeer(localRaftPeerId, ratisAddr);

    List<RaftPeer> raftPeers = new ArrayList<>();
    // Add this Ratis server to the Ratis ring
    raftPeers.add(localRaftPeer);

    for (OMNodeDetails peerInfo : peerNodes) {
      String peerNodeId = peerInfo.getOMNodeId();
      InetSocketAddress peerRatisAddr = new InetSocketAddress(
          peerInfo.getAddress(), peerInfo.getRatisPort());
      RaftPeerId raftPeerId = RaftPeerId.valueOf(peerNodeId);
      RaftPeer raftPeer = new RaftPeer(raftPeerId, peerRatisAddr);

      // Add other OM nodes belonging to the same OM service to the Ratis ring
      raftPeers.add(raftPeer);
    }

    return new OzoneManagerRatisServer(ozoneConf, omProtocol, omServiceId,
        localRaftPeerId, ratisAddr, raftPeers);
  }

  public RaftGroup getRaftGroup() {
    return this.raftGroup;
  }

  /**
   * Initializes and returns OzoneManager StateMachine.
   */
  private OzoneManagerStateMachine getStateMachine() {
    return new OzoneManagerStateMachine(this);
  }

  @VisibleForTesting
  public OzoneManagerStateMachine getOmStateMachine() {
    return omStateMachine;
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  /**
   * Start the Ratis server.
   * @throws IOException
   */
  public void start() throws IOException {
    LOG.info("Starting {} {} at port {}", getClass().getSimpleName(),
        server.getId(), port);
    server.start();
  }

  public void stop() {
    try {
      server.close();
      omStateMachine.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  //TODO simplify it to make it shorter
  @SuppressWarnings("methodlength")
  private RaftProperties newRaftProperties(Configuration conf) {
    final RaftProperties properties = new RaftProperties();

    // Set RPC type
    final String rpcType = conf.get(
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    RaftConfigKeys.Rpc.setType(properties, rpc);

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }

    // Set Ratis storage directory
    String storageDir = OmUtils.getOMRatisDirectory(conf);
    RaftServerConfigKeys.setStorageDirs(properties,
        Collections.singletonList(new File(storageDir)));

    // Set RAFT segment size
    final int raftSegmentSize = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(raftSegmentSize));

    // Set RAFT segment pre-allocated size
    final int raftSegmentPreallocatedSize = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT,
        StorageUnit.BYTES);
    int logAppenderQueueNumElements = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT);
    final int logAppenderQueueByteLimit = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties,
        logAppenderQueueNumElements);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(logAppenderQueueByteLimit));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties,
        false);
    final int logPurgeGap = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP,
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP_DEFAULT);
    RaftServerConfigKeys.Log.setPurgeGap(properties, logPurgeGap);

    // For grpc set the maximum message size
    // TODO: calculate the optimal max message size
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(logAppenderQueueByteLimit));

    // Set the server request timeout
    TimeUnit serverRequestTimeoutUnit =
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT.getUnit();
    long serverRequestTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
            .getDuration(), serverRequestTimeoutUnit);
    final TimeDuration serverRequestTimeout = TimeDuration.valueOf(
        serverRequestTimeoutDuration, serverRequestTimeoutUnit);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties,
        serverRequestTimeout);

    // Set timeout for server retry cache entry
    TimeUnit retryCacheTimeoutUnit = OMConfigKeys
        .OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT.getUnit();
    long retryCacheTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT
            .getDuration(), retryCacheTimeoutUnit);
    final TimeDuration retryCacheTimeout = TimeDuration.valueOf(
        retryCacheTimeoutDuration, retryCacheTimeoutUnit);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties,
        retryCacheTimeout);

    // Set the server min and max timeout
    TimeUnit serverMinTimeoutUnit =
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT.getUnit();
    long serverMinTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT
            .getDuration(), serverMinTimeoutUnit);
    final TimeDuration serverMinTimeout = TimeDuration.valueOf(
        serverMinTimeoutDuration, serverMinTimeoutUnit);
    long serverMaxTimeoutDuration =
        serverMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    final TimeDuration serverMaxTimeout = TimeDuration.valueOf(
        serverMaxTimeoutDuration, serverMinTimeoutUnit);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        serverMaxTimeout);

    // Set the number of maximum cached segments
    RaftServerConfigKeys.Log.setMaxCachedSegmentNum(properties, 2);

    // Set the client request timeout
    TimeUnit clientRequestTimeoutUnit = OMConfigKeys
        .OZONE_OM_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT .getUnit();
    long clientRequestTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY,
        OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), clientRequestTimeoutUnit);
    final TimeDuration clientRequestTimeout = TimeDuration.valueOf(
        clientRequestTimeoutDuration, clientRequestTimeoutUnit);
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        clientRequestTimeout);

    // TODO: set max write buffer size

    // Set the ratis leader election timeout
    TimeUnit leaderElectionMinTimeoutUnit =
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long leaderElectionMinTimeoutduration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getDuration(), leaderElectionMinTimeoutUnit);
    final TimeDuration leaderElectionMinTimeout = TimeDuration.valueOf(
        leaderElectionMinTimeoutduration, leaderElectionMinTimeoutUnit);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        leaderElectionMinTimeout);
    long leaderElectionMaxTimeout = leaderElectionMinTimeout.toLong(
        TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));

    TimeUnit nodeFailureTimeoutUnit =
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long nodeFailureTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT
            .getDuration(), nodeFailureTimeoutUnit);
    final TimeDuration nodeFailureTimeout = TimeDuration.valueOf(
        nodeFailureTimeoutDuration, nodeFailureTimeoutUnit);
    RaftServerConfigKeys.setLeaderElectionTimeout(properties,
        nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        nodeFailureTimeout);

    TimeUnit roleCheckIntervalUnit =
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ROLE_CHECK_INTERVAL_DEFAULT
            .getUnit();
    long roleCheckIntervalDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ROLE_CHECK_INTERVAL_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ROLE_CHECK_INTERVAL_DEFAULT
            .getDuration(), nodeFailureTimeoutUnit);
    this.roleCheckIntervalMs = TimeDuration.valueOf(
        roleCheckIntervalDuration, roleCheckIntervalUnit)
        .toLong(TimeUnit.MILLISECONDS);
    this.roleCheckInitialDelayMs = leaderElectionMinTimeout
        .toLong(TimeUnit.MILLISECONDS);

    long snapshotAutoTriggerThreshold = conf.getLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_DEFAULT);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(
        properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
        properties, snapshotAutoTriggerThreshold);

    return properties;
  }

  /**
   * Check the cached leader status.
   * @return true if cached role is Leader, false otherwise.
   */
  private boolean checkCachedPeerRoleIsLeader() {
    this.roleCheckLock.readLock().lock();
    try {
      if (cachedPeerRole.isPresent() &&
          cachedPeerRole.get() == RaftPeerRole.LEADER) {
        return true;
      }
      return false;
    } finally {
      this.roleCheckLock.readLock().unlock();
    }
  }

  /**
   * Check if the current OM node is the leader node.
   * @return true if Leader, false otherwise.
   */
  public boolean isLeader() {
    if (checkCachedPeerRoleIsLeader()) {
      return true;
    }

    // Get the server role from ratis server and update the cached values.
    updateServerRole();

    // After updating the server role, check and return if leader or not.
    return checkCachedPeerRoleIsLeader();
  }

  /**
   * Get the suggested leader peer id.
   * @return RaftPeerId of the suggested leader node.
   */
  public Optional<RaftPeerId> getCachedLeaderPeerId() {
    this.roleCheckLock.readLock().lock();
    try {
      return cachedLeaderPeerId;
    } finally {
      this.roleCheckLock.readLock().unlock();
    }
  }

  /**
   * Get the gorup info (peer role and leader peer id) from Ratis server and
   * update the OM server role.
   */
  public void updateServerRole() {
    try {
      GroupInfoReply groupInfo = getGroupInfo();
      RoleInfoProto roleInfoProto = groupInfo.getRoleInfoProto();
      RaftPeerRole thisNodeRole = roleInfoProto.getRole();

      if (thisNodeRole.equals(RaftPeerRole.LEADER)) {
        setServerRole(thisNodeRole, raftPeerId);

      } else if (thisNodeRole.equals(RaftPeerRole.FOLLOWER)) {
        ByteString leaderNodeId = roleInfoProto.getFollowerInfo()
            .getLeaderInfo().getId().getId();
        // There may be a chance, here we get leaderNodeId as null. For
        // example, in 3 node OM Ratis, if 2 OM nodes are down, there will
        // be no leader.
        RaftPeerId leaderPeerId = null;
        if (leaderNodeId != null && !leaderNodeId.isEmpty()) {
          leaderPeerId = RaftPeerId.valueOf(leaderNodeId);
        }

        setServerRole(thisNodeRole, leaderPeerId);

      } else {
        setServerRole(thisNodeRole, null);

      }
    } catch (IOException e) {
      LOG.error("Failed to retrieve RaftPeerRole. Setting cached role to " +
          "{} and resetting leader info.", RaftPeerRole.UNRECOGNIZED, e);
      setServerRole(null, null);
    }
  }

  /**
   * Set the current server role and the leader peer id.
   */
  private void setServerRole(RaftPeerRole currentRole,
      RaftPeerId leaderPeerId) {
    this.roleCheckLock.writeLock().lock();
    try {
      this.cachedPeerRole = Optional.ofNullable(currentRole);
      this.cachedLeaderPeerId = Optional.ofNullable(leaderPeerId);
    } finally {
      this.roleCheckLock.writeLock().unlock();
    }
  }

  private GroupInfoReply getGroupInfo() throws IOException {
    GroupInfoRequest groupInfoRequest = new GroupInfoRequest(clientId,
        raftPeerId, raftGroupId, nextCallId());
    GroupInfoReply groupInfo = server.getGroupInfo(groupInfoRequest);
    return groupInfo;
  }

  public int getServerPort() {
    return port;
  }

  @VisibleForTesting
  public LifeCycle.State getServerState() {
    return server.getLifeCycleState();
  }

  @VisibleForTesting
  public RaftPeerId getRaftPeerId() {
    return this.raftPeerId;
  }

  private UUID getRaftGroupIdFromOmServiceId(String omServiceId) {
    return UUID.nameUUIDFromBytes(omServiceId.getBytes(StandardCharsets.UTF_8));
  }

  public long getStateMachineLastAppliedIndex() {
    return omStateMachine.getLastAppliedIndex();
  }
}
