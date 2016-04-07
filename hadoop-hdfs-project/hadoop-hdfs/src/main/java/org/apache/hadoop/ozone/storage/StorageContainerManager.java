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

package org.apache.hadoop.ozone.storage;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_BIND_HOST_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.BlockingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.CreateContainerRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Result;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClientManager;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.StringUtils;

/**
 * StorageContainerManager is the main entry point for the service that provides
 * information about which HDFS nodes host containers.
 *
 * The current implementation is a stub suitable to begin end-to-end testing of
 * Ozone service interactions.  DataNodes report to StorageContainerManager
 * using the existing heartbeat messages.  StorageContainerManager lazily
 * initializes a single storage container to be served by those DataNodes.
 * All subsequent requests for container locations will reply with that single
 * pipeline, using all registered nodes.
 *
 * This will evolve from a stub to a full-fledged implementation capable of
 * partitioning the keyspace across multiple containers, with appropriate
 * distribution across nodes.
 */
@InterfaceAudience.Private
public class StorageContainerManager
    implements DatanodeProtocol, StorageContainerLocationProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerManager.class);

  private final StorageContainerNameService ns;
  private final BlockManager blockManager;
  private final XceiverClientManager xceiverClientManager;
  private Pipeline singlePipeline;

  /** The RPC server that listens to requests from DataNodes. */
  private final RPC.Server serviceRpcServer;
  private final InetSocketAddress serviceRpcAddress;

  /** The RPC server that listens to requests from clients. */
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;

  /** The RPC server that listens to requests from nodes to find containers. */
  private final RPC.Server storageRpcServer;
  private final InetSocketAddress storageRpcAddress;

  /**
   * Creates a new StorageContainerManager.  Configuration will be updated with
   * information on the actual listening addresses used for RPC servers.
   *
   * @param conf configuration
   */
  public StorageContainerManager(OzoneConfiguration conf)
      throws IOException {
    ns = new StorageContainerNameService();
    boolean haEnabled = false;
    blockManager = new BlockManager(ns, haEnabled, conf);
    xceiverClientManager = new XceiverClientManager(conf);

    RPC.setProtocolEngine(conf, DatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService dnProtoPbService =
        DatanodeProtocolProtos
        .DatanodeProtocolService
        .newReflectiveBlockingService(
            new DatanodeProtocolServerSideTranslatorPB(this));

    InetSocketAddress serviceRpcAddr = NameNode.getServiceAddress(conf, false);
    serviceRpcServer = startRpcServer(conf, serviceRpcAddr,
        DatanodeProtocolPB.class, dnProtoPbService,
        DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY,
        DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
        DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
    serviceRpcAddress = updateListenAddress(conf,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, serviceRpcAddr, serviceRpcServer);
    LOG.info(buildRpcServerStartMessage("Service RPC server",
        serviceRpcAddress));

    InetSocketAddress rpcAddr = DFSUtilClient.getNNAddress(conf);
    clientRpcServer = startRpcServer(conf, rpcAddr,
        DatanodeProtocolPB.class, dnProtoPbService,
        DFS_NAMENODE_RPC_BIND_HOST_KEY,
        DFS_NAMENODE_HANDLER_COUNT_KEY,
        DFS_NAMENODE_HANDLER_COUNT_DEFAULT);
    clientRpcAddress = updateListenAddress(conf,
        DFS_NAMENODE_RPC_ADDRESS_KEY, rpcAddr, clientRpcServer);
    conf.set(FS_DEFAULT_NAME_KEY, DFSUtilClient.getNNUri(clientRpcAddress)
        .toString());
    LOG.info(buildRpcServerStartMessage("RPC server", clientRpcAddress));

    BlockingService storageProtoPbService =
        StorageContainerLocationProtocolProtos
        .StorageContainerLocationProtocolService
        .newReflectiveBlockingService(
            new StorageContainerLocationProtocolServerSideTranslatorPB(this));

    InetSocketAddress storageRpcAddr = NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_STORAGE_RPC_ADDRESS_KEY,
            DFS_STORAGE_RPC_ADDRESS_DEFAULT), -1, DFS_STORAGE_RPC_ADDRESS_KEY);

    storageRpcServer = startRpcServer(conf, storageRpcAddr,
        StorageContainerLocationProtocolPB.class, storageProtoPbService,
        DFS_STORAGE_RPC_BIND_HOST_KEY,
        DFS_STORAGE_HANDLER_COUNT_KEY,
        DFS_STORAGE_HANDLER_COUNT_DEFAULT);
    storageRpcAddress = updateListenAddress(conf,
        DFS_STORAGE_RPC_ADDRESS_KEY, storageRpcAddr, storageRpcServer);
    LOG.info(buildRpcServerStartMessage(
        "StorageContainerLocationProtocol RPC server", storageRpcAddress));
  }

  @Override
  public Set<LocatedContainer> getStorageContainerLocations(Set<String> keys)
      throws IOException {
    LOG.trace("getStorageContainerLocations keys = {}", keys);
    Pipeline pipeline = initSingleContainerPipeline();
    List<DatanodeDescriptor> liveNodes = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(liveNodes, null, false);
    if (liveNodes.isEmpty()) {
      throw new IOException("Storage container locations not found.");
    }
    Set<DatanodeInfo> locations =
        Sets.<DatanodeInfo>newLinkedHashSet(liveNodes);
    DatanodeInfo leader = liveNodes.get(0);
    Set<LocatedContainer> locatedContainers =
        Sets.newLinkedHashSetWithExpectedSize(keys.size());
    for (String key: keys) {
      locatedContainers.add(new LocatedContainer(key, key,
          pipeline.getContainerName(), locations, leader));
    }
    LOG.trace("getStorageContainerLocations keys = {}, locatedContainers = {}",
        keys, locatedContainers);
    return locatedContainers;
  }

  @Override
  public DatanodeRegistration registerDatanode(
      DatanodeRegistration registration) throws IOException {
    ns.writeLock();
    try {
      blockManager.getDatanodeManager().registerDatanode(registration);
    } finally {
      ns.writeUnlock();
    }
    return registration;
  }

  @Override
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
      StorageReport[] reports, long dnCacheCapacity, long dnCacheUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease) throws IOException {
    ns.readLock();
    try {
      long cacheCapacity = 0;
      long cacheUsed = 0;
      int maxTransfer = blockManager.getMaxReplicationStreams()
          - xmitsInProgress;
      DatanodeCommand[] cmds = blockManager.getDatanodeManager()
          .handleHeartbeat(registration, reports, blockManager.getBlockPoolId(),
              cacheCapacity, cacheUsed, xceiverCount, maxTransfer,
              failedVolumes, volumeFailureSummary);
      long txnId = 234;
      NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(
          HAServiceProtocol.HAServiceState.ACTIVE, txnId);
      RollingUpgradeInfo rollingUpgradeInfo = null;
      long blockReportLeaseId = requestFullBlockReportLease ?
          blockManager.requestBlockReportLeaseId(registration) : 0;
      return new HeartbeatResponse(cmds, haState, rollingUpgradeInfo,
          blockReportLeaseId);
    } finally {
      ns.readUnlock();
    }
  }

  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, StorageBlockReport[] reports, BlockReportContext context)
      throws IOException {
    for (int r = 0; r < reports.length; r++) {
      final BlockListAsLongs storageContainerList = reports[r].getBlocks();
      blockManager.processReport(registration, reports[r].getStorage(),
          storageContainerList, context, r == (reports.length - 1));
    }
    return null;
  }

  @Override
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds) throws IOException {
    // Centralized Cache Management is not supported
    return null;
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
      throws IOException {
    for(StorageReceivedDeletedBlocks r : rcvdAndDeletedBlocks) {
      ns.writeLock();
      try {
        blockManager.processIncrementalBlockReport(registration, r);
      } finally {
        ns.writeUnlock();
      }
    }
  }

  @Override
  public void errorReport(DatanodeRegistration registration,
      int errorCode, String msg) throws IOException {
    String dnName =
        (registration == null) ? "Unknown DataNode" : registration.toString();
    if (errorCode == DatanodeProtocol.NOTIFY) {
      LOG.info("Error report from " + dnName + ": " + msg);
      return;
    }
    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Disk error on " + dnName + ": " + msg);
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      LOG.warn("Fatal disk error on " + dnName + ": " + msg);
      blockManager.getDatanodeManager().removeDatanode(registration);
    } else {
      LOG.info("Error report from " + dnName + ": " + msg);
    }
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    ns.readLock();
    try {
      return new NamespaceInfo(1, "random", "random", 2,
          NodeType.STORAGE_CONTAINER_SERVICE);
    } finally {
      ns.readUnlock();
    }
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ns.writeLock();
    try {
      for (int i = 0; i < blocks.length; i++) {
        ExtendedBlock blk = blocks[i].getBlock();
        DatanodeInfo[] nodes = blocks[i].getLocations();
        String[] storageIDs = blocks[i].getStorageIDs();
        for (int j = 0; j < nodes.length; j++) {
          blockManager.findAndMarkBlockAsCorrupt(blk, nodes[j],
              storageIDs == null ? null: storageIDs[j],
              "client machine reported it");
        }
      }
    } finally {
      ns.writeUnlock();
    }
  }

  @Override
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets, String[] newtargetstorages)
      throws IOException {
    // Not needed for the purpose of object store
    throw new UnsupportedOperationException();
  }

  /**
   * Returns information on registered DataNodes.
   *
   * @param type DataNode type to report
   * @return registered DataNodes matching requested type
   */
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type) {
    ns.readLock();
    try {
      List<DatanodeDescriptor> results =
          blockManager.getDatanodeManager().getDatanodeListForReport(type);
      return results.toArray(new DatanodeInfo[results.size()]);
    } finally {
      ns.readUnlock();
    }
  }

  /**
   * Returns listen address of StorageContainerLocation RPC server.
   *
   * @return listen address of StorageContainerLocation RPC server
   */
  @VisibleForTesting
  public InetSocketAddress getStorageContainerLocationRpcAddress() {
    return storageRpcAddress;
  }

  /**
   * Start service.
   */
  public void start() {
    clientRpcServer.start();
    if (serviceRpcServer != null) {
      serviceRpcServer.start();
    }
    storageRpcServer.start();
  }

  /**
   * Stop service.
   */
  public void stop() {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
    if (serviceRpcServer != null) {
      serviceRpcServer.stop();
    }
    if (storageRpcServer != null) {
      storageRpcServer.stop();
    }
    IOUtils.closeStream(ns);
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      clientRpcServer.join();
      if (serviceRpcServer != null) {
        serviceRpcServer.join();
      }
      storageRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  /**
   * Lazily initializes a single container pipeline using all registered
   * DataNodes via a synchronous call to the container protocol.  This single
   * container pipeline will be reused for container requests for the lifetime
   * of this StorageContainerManager.
   *
   * @throws IOException if there is an I/O error
   */
  private synchronized Pipeline initSingleContainerPipeline()
      throws IOException {
    if (singlePipeline == null) {
      List<DatanodeDescriptor> liveNodes = new ArrayList<DatanodeDescriptor>();
      blockManager.getDatanodeManager().fetchDatanodes(liveNodes, null, false);
      if (liveNodes.isEmpty()) {
        throw new IOException("Storage container locations not found.");
      }
      Pipeline newPipeline = newPipelineFromNodes(liveNodes,
          UUID.randomUUID().toString());
      XceiverClient xceiverClient =
          xceiverClientManager.acquireClient(newPipeline);
      try {
        ContainerData containerData = ContainerData
            .newBuilder()
            .setName(newPipeline.getContainerName())
            .build();
        CreateContainerRequestProto createContainerRequest =
            CreateContainerRequestProto.newBuilder()
            .setPipeline(newPipeline.getProtobufMessage())
            .setContainerData(containerData)
            .build();
        ContainerCommandRequestProto request = ContainerCommandRequestProto
            .newBuilder()
            .setCmdType(Type.CreateContainer)
            .setCreateContainer(createContainerRequest)
            .build();
        ContainerCommandResponseProto response = xceiverClient.sendCommand(
            request);
        Result result = response.getResult();
        if (result != Result.SUCCESS) {
          throw new IOException(
              "Failed to initialize container due to result code: " + result);
        }
        singlePipeline = newPipeline;
      } finally {
        xceiverClientManager.releaseClient(xceiverClient);
      }
    }
    return singlePipeline;
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr RPC server listening address
   * @return server startup message
   */
  private static String buildRpcServerStartMessage(String description,
      InetSocketAddress addr) {
    return addr != null ? String.format("%s is listening at %s",
        description, NetUtils.getHostPortString(addr)) :
        String.format("%s not started", description);
  }

  /**
   * Translates a list of nodes, ordered such that the first is the leader, into
   * a corresponding {@link Pipeline} object.
   *
   * @param nodes list of nodes
   * @param containerName container name
   * @return pipeline corresponding to nodes
   */
  private static Pipeline newPipelineFromNodes(List<DatanodeDescriptor> nodes,
      String containerName) {
    String leaderId = nodes.get(0).getDatanodeUuid();
    Pipeline pipeline = new Pipeline(leaderId);
    for (DatanodeDescriptor node : nodes) {
      pipeline.addMember(node);
    }
    pipeline.setContainerName(containerName);
    return pipeline;
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param bindHostKey configuration key for setting explicit bind host.  If
   *     the property is not configured, then the bind host is taken from addr.
   * @param handlerCountKey configuration key for RPC server handler count
   * @param handlerCountDefault default RPC server handler count if unconfigured
   * @return RPC server, or null if addr is null
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      String bindHostKey, String handlerCountKey, int handlerCountDefault)
      throws IOException {
    if (addr == null) {
      return null;
    }
    String bindHost = conf.getTrimmed(bindHostKey);
    if (bindHost == null || bindHost.isEmpty()) {
      bindHost = addr.getHostName();
    }
    int numHandlers = conf.getInt(handlerCountKey, handlerCountDefault);
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(bindHost)
        .setPort(addr.getPort())
        .setNumHandlers(numHandlers)
        .setVerbose(false)
        .setSecretManager(null)
        .build();
    DFSUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }

  /**
   * After starting an RPC server, updates configuration with the actual
   * listening address of that server.  The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf configuration to update
   * @param rpcAddressKey configuration key for RPC server address
   * @param addr configured address
   * @param rpcServer started RPC server.  If null, then the server was not
   *     started, and this method is a no-op.
   */
  private static InetSocketAddress updateListenAddress(OzoneConfiguration conf,
      String rpcAddressKey, InetSocketAddress addr, RPC.Server rpcServer) {
    if (rpcServer == null) {
      return null;
    }
    InetSocketAddress listenAddr = rpcServer.getListenerAddress();
    InetSocketAddress updatedAddr = new InetSocketAddress(
        addr.getHostName(), listenAddr.getPort());
    conf.set(rpcAddressKey, NetUtils.getHostPortString(updatedAddr));
    return updatedAddr;
  }

  /**
   * Main entry point for starting StorageContainerManager.
   *
   * @param argv arguments
   * @throws IOException if startup fails due to I/O error
   */
  public static void main(String[] argv) throws IOException {
    StringUtils.startupShutdownMessage(
        StorageContainerManager.class, argv, LOG);
    StorageContainerManager scm = new StorageContainerManager(
        new OzoneConfiguration());
    scm.start();
    scm.join();
  }
}
