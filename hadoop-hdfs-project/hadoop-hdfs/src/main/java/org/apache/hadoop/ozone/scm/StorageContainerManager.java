/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.scm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeAddressList;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SendContainerReportProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.SCMNodeManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map;
import java.util.HashMap;

import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * StorageContainerManager is the main entry point for the service that provides
 * information about which SCM nodes host containers.
 *
 * DataNodes report to StorageContainerManager using heartbeat
 * messages. SCM allocates containers and returns a pipeline.
 *
 * A client once it gets a pipeline (a list of datanodes) will connect to the
 * datanodes and create a container, which then can be used to store data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public class StorageContainerManager
    implements StorageContainerDatanodeProtocol,
    StorageContainerLocationProtocol, SCMMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerManager.class);

  /**
   * NodeManager and container Managers for SCM.
   */
  private final NodeManager scmNodeManager;
  private final Mapping scmContainerManager;

  /** The RPC server that listens to requests from DataNodes. */
  private final RPC.Server datanodeRpcServer;
  private final InetSocketAddress datanodeRpcAddress;

  /** The RPC server that listens to requests from clients. */
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;

  /** SCM mxbean. */
  private ObjectName scmInfoBeanName;

  /**
   * Creates a new StorageContainerManager.  Configuration will be updated with
   * information on the actual listening addresses used for RPC servers.
   *
   * @param conf configuration
   */
  public StorageContainerManager(OzoneConfiguration conf)
      throws IOException {

    final int handlerCount = conf.getInt(
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT);
    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

    // TODO : Fix the ClusterID generation code.
    scmNodeManager = new SCMNodeManager(conf, UUID.randomUUID().toString());
    scmContainerManager = new ContainerMapping(conf, scmNodeManager, cacheSize);

    RPC.setProtocolEngine(conf, StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService dnProtoPbService = StorageContainerDatanodeProtocolProtos.
        StorageContainerDatanodeProtocolService.newReflectiveBlockingService(
        new StorageContainerDatanodeProtocolServerSideTranslatorPB(this));

    final InetSocketAddress datanodeRpcAddr =
        OzoneClientUtils.getScmDataNodeBindAddress(conf);
    datanodeRpcServer = startRpcServer(conf, datanodeRpcAddr,
        StorageContainerDatanodeProtocolPB.class, dnProtoPbService,
        handlerCount);
    datanodeRpcAddress = updateListenAddress(conf,
        OZONE_SCM_DATANODE_ADDRESS_KEY, datanodeRpcAddr, datanodeRpcServer);

    BlockingService storageProtoPbService =
        StorageContainerLocationProtocolProtos
            .StorageContainerLocationProtocolService
            .newReflectiveBlockingService(
                new StorageContainerLocationProtocolServerSideTranslatorPB(
                    this));

    final InetSocketAddress scmAddress =
        OzoneClientUtils.getScmClientBindAddress(conf);
    clientRpcServer = startRpcServer(conf, scmAddress,
        StorageContainerLocationProtocolPB.class, storageProtoPbService,
        handlerCount);
    clientRpcAddress = updateListenAddress(conf,
        OZONE_SCM_CLIENT_ADDRESS_KEY, scmAddress, clientRpcServer);

    registerMXBean();
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
        description, addr.toString()) :
        String.format("%s not started", description);
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   *
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      int handlerCount)
      throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    DFSUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }

  private void registerMXBean() {
    this.scmInfoBeanName = MBeans.register("StorageContainerManager",
        "StorageContainerManagerInfo", this);
  }

  private void unregisterMXBean() {
    if(this.scmInfoBeanName != null) {
      MBeans.unregister(this.scmInfoBeanName);
      this.scmInfoBeanName = null;
    }
  }

  /**
   * After starting an RPC server, updates configuration with the actual
   * listening address of that server. The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf configuration to update
   * @param rpcAddressKey configuration key for RPC server address
   * @param addr configured address
   * @param rpcServer started RPC server.
   */
  private static InetSocketAddress updateListenAddress(OzoneConfiguration conf,
      String rpcAddressKey, InetSocketAddress addr, RPC.Server rpcServer) {
    InetSocketAddress listenAddr = rpcServer.getListenerAddress();
    InetSocketAddress updatedAddr = new InetSocketAddress(
        addr.getHostString(), listenAddr.getPort());
    conf.set(rpcAddressKey,
        listenAddr.getHostString() + ":" + listenAddr.getPort());
    return updatedAddr;
  }

  /**
   * Main entry point for starting StorageContainerManager.
   *
   * @param argv arguments
   * @throws IOException if startup fails due to I/O error
   */
  public static void main(String[] argv) throws IOException {
    StringUtils.startupShutdownMessage(StorageContainerManager.class,
        argv, LOG);
    try {
      StorageContainerManager scm = new StorageContainerManager(
          new OzoneConfiguration());
      scm.start();
      scm.join();
    } catch (Throwable t) {
      LOG.error("Failed to start the StorageContainerManager.", t);
      terminate(1, t);
    }
  }

  /**
   * Returns a SCMCommandRepose from the SCM Command.
   * @param cmd - Cmd
   * @return SCMCommandResponseProto
   * @throws InvalidProtocolBufferException
   */
  @VisibleForTesting
  public static SCMCommandResponseProto getCommandResponse(SCMCommand cmd)
      throws InvalidProtocolBufferException {
    Type type = cmd.getType();
    SCMCommandResponseProto.Builder builder =
        SCMCommandResponseProto.newBuilder();
    switch (type) {
    case registeredCommand:
      return builder.setCmdType(Type.registeredCommand)
          .setRegisteredProto(
              SCMRegisteredCmdResponseProto.getDefaultInstance())
          .build();
    case versionCommand:
      return builder.setCmdType(Type.versionCommand)
          .setVersionProto(SCMVersionResponseProto.getDefaultInstance())
          .build();
    case sendContainerReport:
      return builder.setCmdType(Type.sendContainerReport)
          .setSendReport(SendContainerReportProto.getDefaultInstance())
          .build();
    default:
      throw new IllegalArgumentException("Not implemented");
    }
  }

  @VisibleForTesting
  public static SCMRegisteredCmdResponseProto getRegisteredResponse(
      SCMCommand cmd, SCMNodeAddressList addressList) {
    Preconditions.checkState(cmd.getClass() == RegisteredCommand.class);
    RegisteredCommand rCmd = (RegisteredCommand) cmd;
    StorageContainerDatanodeProtocolProtos.Type type = cmd.getType();
    if (type != Type.registeredCommand) {
      throw new IllegalArgumentException("Registered command is not well " +
          "formed. Internal Error.");
    }
    return SCMRegisteredCmdResponseProto.newBuilder()
        //TODO : Fix this later when we have multiple SCM support.
        //.setAddressList(addressList)
        .setErrorCode(rCmd.getError())
        .setClusterID(rCmd.getClusterID())
        .setDatanodeUUID(rCmd.getDatanodeUUID()).build();
  }

  // TODO : This code will move into KSM later. Write now this code is stubbed
  // implementation that lets the ozone tests pass.
  @Override
  public Set<LocatedContainer> getStorageContainerLocations(Set<String> keys)
      throws IOException {
    throw new IOException("Not Implemented.");
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container.
   *
   * @param containerName - Name of the container.
   * @return Pipeline.
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(String containerName) throws IOException {
    return scmContainerManager.allocateContainer(containerName,
        ScmClient.ReplicationFactor.ONE);
  }

  @VisibleForTesting
  Pipeline getContainer(String containerName) throws IOException {
    return scmContainerManager.getContainer(containerName);
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container.
   *
   * @param containerName - Name of the container.
   * @param replicationFactor - replication factor.
   * @return Pipeline.
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(String containerName,
      ScmClient.ReplicationFactor replicationFactor) throws IOException {
    return scmContainerManager.allocateContainer(containerName,
        replicationFactor);
  }

  /**
   * Returns listening address of StorageLocation Protocol RPC server.
   *
   * @return listen address of StorageLocation RPC server
   */
  @VisibleForTesting
  public InetSocketAddress getClientRpcAddress() {
    return clientRpcAddress;
  }

  @Override
  public String getClientRpcPort() {
    InetSocketAddress addr = getClientRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  /**
   * Returns listening address of StorageDatanode Protocol RPC server.
   *
   * @return Address where datanode are communicating.
   */
  public InetSocketAddress getDatanodeRpcAddress() {
    return datanodeRpcAddress;
  }

  @Override
  public String getDatanodeRpcPort() {
    InetSocketAddress addr = getDatanodeRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  /**
   * Start service.
   */
  public void start() {
    LOG.info(buildRpcServerStartMessage(
        "StorageContainerLocationProtocol RPC server", clientRpcAddress));
    clientRpcServer.start();
    LOG.info(buildRpcServerStartMessage("RPC server for DataNodes",
        datanodeRpcAddress));
    datanodeRpcServer.start();
  }

  /**
   * Stop service.
   */
  public void stop() {
    LOG.info("Stopping the StorageContainerLocationProtocol RPC server");
    clientRpcServer.stop();
    LOG.info("Stopping the RPC server for DataNodes");
    datanodeRpcServer.stop();
    unregisterMXBean();
    IOUtils.closeQuietly(scmContainerManager);
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      clientRpcServer.join();
      datanodeRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  /**
   * Returns SCM version.
   *
   * @return Version info.
   */
  @Override
  public SCMVersionResponseProto getVersion(
      SCMVersionRequestProto versionRequest) throws IOException {
    return getScmNodeManager().getVersion(versionRequest).getProtobufMessage();
  }

  /**
   * Used by data node to send a Heartbeat.
   *
   * @param datanodeID - Datanode ID.
   * @param nodeReport - Node Report
   * @param reportState - Container report ready info.
   * @return - SCMHeartbeatResponseProto
   * @throws IOException
   */
  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(DatanodeID datanodeID,
      SCMNodeReport nodeReport, ReportState reportState) throws IOException {
    List<SCMCommand> commands =
        getScmNodeManager().sendHeartbeat(datanodeID, nodeReport);
    List<SCMCommandResponseProto> cmdResponses = new LinkedList<>();
    for (SCMCommand cmd : commands) {
      cmdResponses.add(getCommandResponse(cmd));
    }
    return SCMHeartbeatResponseProto.newBuilder().addAllCommands(cmdResponses)
        .build();
  }

  /**
   * Register Datanode.
   *
   * @param datanodeID - DatanodID.
   * @param scmAddresses - List of SCMs this datanode is configured to
   * communicate.
   * @return SCM Command.
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto
      register(DatanodeID datanodeID, String[] scmAddresses)
      throws IOException {
    // TODO : Return the list of Nodes that forms the SCM HA.
    return getRegisteredResponse(scmNodeManager.register(datanodeID), null);
  }

  /**
   * Send a container report.
   *
   * @param reports -- Container report
   * @return HeartbeatRespose.nullcommand.
   * @throws IOException
   */
  @Override
  public SCMHeartbeatResponseProto
      sendContainerReport(ContainerReportsProto reports) throws IOException {
    // TODO : fix this in the server side code changes for handling this request
    // correctly.
    List<SCMCommandResponseProto> cmdResponses = new LinkedList<>();
    return SCMHeartbeatResponseProto.newBuilder().addAllCommands(cmdResponses)
        .build();
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate Healthy, Dead etc.
   * @return int -- count
   */
  public int getNodeCount(SCMNodeManager.NODESTATE nodestate) {
    return scmNodeManager.getNodeCount(nodestate);
  }

  @Override
  public Map<String, Integer> getNodeCount() {
    Map<String, Integer> countMap = new HashMap<String, Integer>();
    for (SCMNodeManager.NODESTATE state : SCMNodeManager.NODESTATE.values()) {
      countMap.put(state.toString(), scmNodeManager.getNodeCount(state));
    }
    return countMap;
  }

  /**
   * Returns node manager.
   * @return - Node Manager
   */
  @VisibleForTesting
  public NodeManager getScmNodeManager() {
    return scmNodeManager;
  }

}
