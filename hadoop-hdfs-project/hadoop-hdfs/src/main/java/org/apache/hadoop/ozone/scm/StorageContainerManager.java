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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.jmx.ServiceRuntimeInfoImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.StorageInfo;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState;
import org.apache.hadoop.ozone.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeAddressList;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMReregisterCmdResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SendContainerReportProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.NotifyObjectCreationStageRequestProto;
import org.apache.hadoop.ozone.protocolPB.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.scm.block.BlockManager;
import org.apache.hadoop.ozone.scm.block.BlockManagerImpl;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.ozone.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.ozone.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.SCMNodeManager;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.ozone.protocol.proto
    .ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
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
public class StorageContainerManager extends ServiceRuntimeInfoImpl
    implements StorageContainerDatanodeProtocol,
    StorageContainerLocationProtocol, ScmBlockLocationProtocol, SCMMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerManager.class);

  /**
   *  Startup options.
   */
  public enum StartupOption {
    INIT("-init"),
    CLUSTERID("-clusterid"),
    GENCLUSTERID("-genclusterid"),
    REGULAR("-regular"),
    HELP("-help");

    private final String name;
    private String clusterId = null;

    public void setClusterId(String cid) {
      if(cid != null && !cid.isEmpty()) {
        clusterId = cid;
      }
    }

    public String getClusterId() {
      return clusterId;
    }

    StartupOption(String arg) {
      this.name = arg;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * NodeManager and container Managers for SCM.
   */
  private final NodeManager scmNodeManager;
  private final Mapping scmContainerManager;
  private final BlockManager scmBlockManager;
  private final SCMStorage scmStorage;

  /** The RPC server that listens to requests from DataNodes. */
  private final RPC.Server datanodeRpcServer;
  private final InetSocketAddress datanodeRpcAddress;

  /** The RPC server that listens to requests from clients. */
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;

  /** The RPC server that listens to requests from block service clients. */
  private final RPC.Server blockRpcServer;
  private final InetSocketAddress blockRpcAddress;

  private final StorageContainerManagerHttpServer httpServer;

  /** SCM mxbean. */
  private ObjectName scmInfoBeanName;

  /** SCM super user. */
  private final String scmUsername;
  private final Collection<String> scmAdminUsernames;

  /** SCM metrics. */
  private static SCMMetrics metrics;

  private static final String USAGE =
      "Usage: \n hdfs scm [ " + StartupOption.INIT.getName() + " [ "
          + StartupOption.CLUSTERID.getName() + " <cid> ] ]\n " + "hdfs scm [ "
          + StartupOption.GENCLUSTERID.getName() + " ]\n " + "hdfs scm [ "
          + StartupOption.HELP.getName() + " ]\n";
  /**
   * Creates a new StorageContainerManager.  Configuration will be updated with
   * information on the actual listening addresses used for RPC servers.
   *
   * @param conf configuration
   */
  private StorageContainerManager(OzoneConfiguration conf)
      throws IOException {

    final int handlerCount = conf.getInt(
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT);
    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

    StorageContainerManager.initMetrics();
    scmStorage = new SCMStorage(conf);
    String clusterId = scmStorage.getClusterID();
    if (clusterId == null) {
      throw new SCMException("clusterId not found",
          ResultCodes.SCM_NOT_INITIALIZED);
    }
    scmNodeManager = new SCMNodeManager(conf, scmStorage.getClusterID());
    scmContainerManager = new ContainerMapping(conf, scmNodeManager, cacheSize);
    scmBlockManager = new BlockManagerImpl(conf, scmNodeManager,
        scmContainerManager, cacheSize);

    scmAdminUsernames = conf.getTrimmedStringCollection(
        OzoneConfigKeys.OZONE_ADMINISTRATORS);
    scmUsername = UserGroupInformation.getCurrentUser().getUserName();
    if (!scmAdminUsernames.contains(scmUsername)) {
      scmAdminUsernames.add(scmUsername);
    }

    RPC.setProtocolEngine(conf, StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService dnProtoPbService = StorageContainerDatanodeProtocolProtos.
        StorageContainerDatanodeProtocolService.newReflectiveBlockingService(
        new StorageContainerDatanodeProtocolServerSideTranslatorPB(this));

    final InetSocketAddress datanodeRpcAddr =
        OzoneClientUtils.getScmDataNodeBindAddress(conf);
    datanodeRpcServer = startRpcServer(conf, datanodeRpcAddr,
        StorageContainerDatanodeProtocolPB.class, dnProtoPbService,
        handlerCount);
    datanodeRpcAddress = OzoneClientUtils.updateRPCListenAddress(conf,
        OZONE_SCM_DATANODE_ADDRESS_KEY, datanodeRpcAddr, datanodeRpcServer);

    // SCM Container Service RPC
    BlockingService storageProtoPbService =
        StorageContainerLocationProtocolProtos
            .StorageContainerLocationProtocolService
            .newReflectiveBlockingService(
            new StorageContainerLocationProtocolServerSideTranslatorPB(this));

    final InetSocketAddress scmAddress =
        OzoneClientUtils.getScmClientBindAddress(conf);
    clientRpcServer = startRpcServer(conf, scmAddress,
        StorageContainerLocationProtocolPB.class, storageProtoPbService,
        handlerCount);
    clientRpcAddress = OzoneClientUtils.updateRPCListenAddress(conf,
        OZONE_SCM_CLIENT_ADDRESS_KEY, scmAddress, clientRpcServer);

    // SCM Block Service RPC
    BlockingService blockProtoPbService =
        ScmBlockLocationProtocolProtos
            .ScmBlockLocationProtocolService
            .newReflectiveBlockingService(
            new ScmBlockLocationProtocolServerSideTranslatorPB(this));

    final InetSocketAddress scmBlockAddress =
        OzoneClientUtils.getScmBlockClientBindAddress(conf);
    blockRpcServer = startRpcServer(conf, scmBlockAddress,
        ScmBlockLocationProtocolPB.class, blockProtoPbService,
        handlerCount);
    blockRpcAddress = OzoneClientUtils.updateRPCListenAddress(conf,
        OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, scmBlockAddress, blockRpcServer);

    httpServer = new StorageContainerManagerHttpServer(conf);

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
    Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.scmInfoBeanName =
        MBeans.register("StorageContainerManager",
            "StorageContainerManagerInfo",
            jmxProperties,
            this);
  }

  private void unregisterMXBean() {
    if(this.scmInfoBeanName != null) {
      MBeans.unregister(this.scmInfoBeanName);
      this.scmInfoBeanName = null;
    }
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
    OzoneConfiguration conf = new OzoneConfiguration();
    try {
      StorageContainerManager scm = createSCM(argv, conf);
      if (scm != null) {
        scm.start();
        scm.join();
      }
    } catch (Throwable t) {
      LOG.error("Failed to start the StorageContainerManager.", t);
      terminate(1, t);
    }
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  public static StorageContainerManager createSCM(String[] argv,
      OzoneConfiguration conf) throws IOException {
    if (!DFSUtil.isOzoneEnabled(conf)) {
      System.err.println("SCM cannot be started in secure mode or when " +
          OZONE_ENABLED + " is set to false");
      System.exit(1);
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      terminate(1);
      return null;
    }
    switch (startOpt) {
    case INIT:
      terminate(scmInit(conf) ? 0 : 1);
      return null;
    case GENCLUSTERID:
      System.out.println("Generating new cluster id:");
      System.out.println(StorageInfo.newClusterID());
      terminate(0);
      return null;
    case HELP:
      printUsage(System.err);
      terminate(0);
      return null;
    default:
      return new StorageContainerManager(conf);
    }
  }

  /**
   * Routine to set up the Version info for StorageContainerManager.
   *
   * @param conf OzoneConfiguration
   * @return true if SCM initialization is successful, false otherwise.
   * @throws IOException if init fails due to I/O error
   */
  public static boolean scmInit(OzoneConfiguration conf) throws IOException {
    SCMStorage scmStorage = new SCMStorage(conf);
    StorageState state = scmStorage.getState();
    if (state != StorageState.INITIALIZED) {
      try {
        String clusterId = StartupOption.INIT.getClusterId();
        if (clusterId != null && !clusterId.isEmpty()) {
          scmStorage.setClusterId(clusterId);
        }
        scmStorage.initialize();
        System.out.println("SCM initialization succeeded." +
            "Current cluster id for sd=" + scmStorage.getStorageDir() + ";cid="
                + scmStorage.getClusterID());
        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize SCM version file", ioe);
        return false;
      }
    } else {
      System.out.println("SCM already initialized. Reusing existing" +
          " cluster id for sd=" + scmStorage.getStorageDir() + ";cid="
              + scmStorage.getClusterID());
      return true;
    }
  }

  private static StartupOption parseArguments(String[] args) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.HELP;
    if (argsLen == 0) {
      startOpt = StartupOption.REGULAR;
    }
    for (int i = 0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.INIT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.INIT;
        if (argsLen > 3) {
          return null;
        }
        for (i = i + 1; i < argsLen; i++) {
          if (args[i].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            i++;
            if (i < argsLen && !args[i].isEmpty()) {
              startOpt.setClusterId(args[i]);
            } else {
              // if no cluster id specified or is empty string, return null
              LOG.error("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
          } else {
            return null;
          }
        }
      } else if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
        if (argsLen > 1) {
          return null;
        }
        startOpt = StartupOption.GENCLUSTERID;
      }
    }
    return startOpt;
  }

  /**
   * Returns a SCMCommandRepose from the SCM Command.
   * @param cmd - Cmd
   * @return SCMCommandResponseProto
   * @throws InvalidProtocolBufferException
   */
  @VisibleForTesting
  public SCMCommandResponseProto getCommandResponse(SCMCommand cmd)
      throws IOException {
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
    case reregisterCommand:
      return builder.setCmdType(Type.reregisterCommand)
          .setReregisterProto(SCMReregisterCmdResponseProto
              .getDefaultInstance())
          .build();
    case deleteBlocksCommand:
      // Once SCM sends out the deletion message, increment the count.
      // this is done here instead of when SCM receives the ACK, because
      // DN might not be able to response the ACK for sometime. In case
      // it times out, SCM needs to re-send the message some more times.
      List<Long> txs = ((DeleteBlocksCommand) cmd).blocksTobeDeleted()
          .stream().map(tx -> tx.getTxID()).collect(Collectors.toList());
      this.getScmBlockManager().getDeletedBlockLog().incrementCount(txs);
      return builder.setCmdType(Type.deleteBlocksCommand)
          .setDeleteBlocksProto(((DeleteBlocksCommand) cmd).getProto())
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Pipeline getContainer(String containerName) throws IOException {
    checkAdminAccess();
    return scmContainerManager.getContainer(containerName).getPipeline();
  }

  @VisibleForTesting
  ContainerInfo getContainerInfo(String containerName) throws IOException {
    return scmContainerManager.getContainer(containerName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerInfo> listContainer(String startName,
      String prefixName, int count) throws IOException {
    return scmContainerManager.listContainer(startName, prefixName, count);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteContainer(String containerName) throws IOException {
    checkAdminAccess();
    scmContainerManager.deleteContainer(containerName);
  }

  /**
   * Queries a list of Node Statuses.
   *
   * @param nodeStatuses
   * @param queryScope
   * @param poolName @return List of Datanodes.
   */
  @Override
  public OzoneProtos.NodePool queryNode(EnumSet<NodeState> nodeStatuses,
      OzoneProtos.QueryScope queryScope, String poolName) throws IOException {

    if (queryScope == OzoneProtos.QueryScope.POOL) {
      throw new IllegalArgumentException("Not Supported yet");
    }

    List<DatanodeID> datanodes = queryNode(nodeStatuses);
    OzoneProtos.NodePool.Builder poolBuilder =
        OzoneProtos.NodePool.newBuilder();

    for (DatanodeID datanode : datanodes) {
      OzoneProtos.Node node = OzoneProtos.Node.newBuilder()
          .setNodeID(datanode.getProtoBufMessage())
          .addAllNodeStates(nodeStatuses)
          .build();
      poolBuilder.addNodes(node);
    }

    return poolBuilder.build();
  }

  /**
   * Notify from client when begin/finish creating container/pipeline objects
   * on datanodes.
   * @param type
   * @param name
   * @param stage
   */
  @Override
  public void notifyObjectCreationStage(
      NotifyObjectCreationStageRequestProto.Type type, String name,
      NotifyObjectCreationStageRequestProto.Stage stage) throws IOException {

    if (type == NotifyObjectCreationStageRequestProto.Type.container) {
      ContainerInfo info = scmContainerManager.getContainer(name);
      LOG.info("Container {} current state {} new stage {}", name,
          info.getState(), stage);
      if (stage == NotifyObjectCreationStageRequestProto.Stage.begin) {
        scmContainerManager.updateContainerState(name,
            OzoneProtos.LifeCycleEvent.BEGIN_CREATE);
      } else {
        scmContainerManager.updateContainerState(name,
            OzoneProtos.LifeCycleEvent.COMPLETE_CREATE);
      }
    } else if (type == NotifyObjectCreationStageRequestProto.Type.pipeline) {
      // TODO: pipeline state update will be addressed in future patch.
    }
  }

  /**
   * Creates a replication pipeline of a specified type.
   */
  @Override
  public Pipeline createReplicationPipeline(
      OzoneProtos.ReplicationType replicationType,
      OzoneProtos.ReplicationFactor factor,
      OzoneProtos.NodePool nodePool)
      throws IOException {
     // TODO: will be addressed in future patch.
    return null;
  }

  @Override
  public void closeContainer(String containerName) throws IOException {
    checkAdminAccess();
    scmContainerManager.closeContainer(containerName);
  }

  /**
   * Queries a list of Node that match a set of statuses.
   * <p>
   * For example, if the nodeStatuses is HEALTHY and RAFT_MEMBER,
   * then this call will return all healthy nodes which members in
   * Raft pipeline.
   * <p>
   * Right now we don't support operations, so we assume it is an AND operation
   * between the operators.
   *
   * @param nodeStatuses - A set of NodeStates.
   * @return List of Datanodes.
   */

  public List<DatanodeID> queryNode(EnumSet<NodeState> nodeStatuses) {
    Preconditions.checkNotNull(nodeStatuses, "Node Query set cannot be null");
    Preconditions.checkState(nodeStatuses.size() > 0, "No valid arguments " +
        "in the query set");
    List<DatanodeID> resultList = new LinkedList<>();
    Set<DatanodeID> currentSet = new TreeSet<>();

    for (NodeState nodeState : nodeStatuses) {
      Set<DatanodeID> nextSet = queryNodeState(nodeState);
      if ((nextSet == null) || (nextSet.size() == 0)) {
        // Right now we only support AND operation. So intersect with
        // any empty set is null.
        return resultList;
      }
      // First time we have to add all the elements, next time we have to
      // do an intersection operation on the set.
      if (currentSet.size() == 0) {
        currentSet.addAll(nextSet);
      } else {
        currentSet.retainAll(nextSet);
      }
    }

    resultList.addAll(currentSet);
    return resultList;
  }

  /**
   * Query the System for Nodes.
   *
   * @param nodeState - NodeState that we are interested in matching.
   * @return Set of Datanodes that match the NodeState.
   */
  private Set<DatanodeID> queryNodeState(NodeState nodeState) {
    if (nodeState == NodeState.RAFT_MEMBER ||
        nodeState == NodeState.FREE_NODE) {
      throw new IllegalStateException("Not implemented yet");
    }
    Set<DatanodeID> returnSet = new TreeSet<>();
    List<DatanodeID> tmp = getScmNodeManager().getNodes(nodeState);
    if ((tmp != null) && (tmp.size() > 0)) {
      returnSet.addAll(tmp);
    }
    return returnSet;
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container.
   *
   * @param containerName - Name of the container.
   * @param replicationFactor - replication factor.
   * @return pipeline
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(OzoneProtos.ReplicationType replicationType,
      OzoneProtos.ReplicationFactor replicationFactor, String containerName)
      throws IOException {

    //TODO : FIX ME : Pass the owner argument to this function.
    // This causes a lot of test change and cblock change to filing
    // another JIRA to fix it.
    final OzoneProtos.Owner owner = OzoneProtos.Owner.OZONE;
    checkAdminAccess();
    return scmContainerManager.allocateContainer(replicationType,
        replicationFactor, containerName, owner).getPipeline();
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
  public void start() throws IOException {
    LOG.info(buildRpcServerStartMessage(
        "StorageContainerLocationProtocol RPC server", clientRpcAddress));
    DefaultMetricsSystem.initialize("StorageContainerManager");
    clientRpcServer.start();
    LOG.info(buildRpcServerStartMessage(
        "ScmBlockLocationProtocol RPC server", blockRpcAddress));
    blockRpcServer.start();
    LOG.info(buildRpcServerStartMessage("RPC server for DataNodes",
        datanodeRpcAddress));
    datanodeRpcServer.start();
    httpServer.start();
    scmBlockManager.start();

    setStartTime();

  }

  /**
   * Stop service.
   */
  public void stop() {
    try {
      LOG.info("Stopping block service RPC server");
      blockRpcServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager blockRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping the StorageContainerLocationProtocol RPC server");
      clientRpcServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager clientRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping the RPC server for DataNodes");
      datanodeRpcServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager datanodeRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping Storage Container Manager HTTP server.");
      httpServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager HTTP server stop failed.", ex);
    }

    try {
      LOG.info("Stopping Block Manager Service.");
      scmBlockManager.stop();
    } catch (Exception ex) {
      LOG.error("SCM block manager service stop failed.", ex);
    }

    metrics.unRegister();
    unregisterMXBean();
    IOUtils.cleanupWithLogger(LOG, scmContainerManager);
    IOUtils.cleanupWithLogger(LOG, scmBlockManager);
    IOUtils.cleanupWithLogger(LOG, scmNodeManager);
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      blockRpcServer.join();
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
        getScmNodeManager().sendHeartbeat(datanodeID, nodeReport, reportState);
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
  public ContainerReportsResponseProto sendContainerReport(
      ContainerReportsRequestProto reports) throws IOException {
    // TODO: We should update the logic once incremental container report
    // type is supported.
    if (reports.getType() ==
        ContainerReportsRequestProto.reportType.fullReport) {
      ContainerStat stat = new ContainerStat();
      for (StorageContainerDatanodeProtocolProtos.ContainerInfo info : reports
          .getReportsList()) {
        stat.add(new ContainerStat(info.getSize(), info.getUsed(),
            info.getKeyCount(), info.getReadBytes(), info.getWriteBytes(),
            info.getReadCount(), info.getWriteCount()));
      }

      // update container metrics
      metrics.setLastContainerReportSize(stat.getSize().get());
      metrics.setLastContainerReportUsed(stat.getUsed().get());
      metrics.setLastContainerReportKeyCount(stat.getKeyCount().get());
      metrics.setLastContainerReportReadBytes(stat.getReadBytes().get());
      metrics.setLastContainerReportWriteBytes(stat.getWriteBytes().get());
      metrics.setLastContainerReportReadCount(stat.getReadCount().get());
      metrics.setLastContainerReportWriteCount(stat.getWriteCount().get());
    }

    // should we process container reports async?
    scmContainerManager.processContainerReports(
        DatanodeID.getFromProtoBuf(reports.getDatanodeID()),
        reports.getType(), reports.getReportsList());
    return ContainerReportsResponseProto.newBuilder().build();
  }

  /**
   * Handles the block deletion ACKs sent by datanodes. Once ACKs recieved,
   * SCM considers the blocks are deleted and update the metadata in SCM DB.
   *
   * @param acks
   * @return
   * @throws IOException
   */
  @Override
  public ContainerBlocksDeletionACKResponseProto sendContainerBlocksDeletionACK(
      ContainerBlocksDeletionACKProto acks) throws IOException {
    if (acks.getResultsCount() > 0) {
      List<DeleteBlockTransactionResult> resultList = acks.getResultsList();
      for (DeleteBlockTransactionResult result : resultList) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got block deletion ACK from datanode, TXIDs={}, "
                  + "success={}", result.getTxID(), result.getSuccess());
        }
        if (result.getSuccess()) {
          LOG.debug("Purging TXID={} from block deletion log",
              result.getTxID());
          this.getScmBlockManager().getDeletedBlockLog()
              .commitTransactions(Collections.singletonList(result.getTxID()));
        } else {
          LOG.warn("Got failed ACK for TXID={}, prepare to resend the "
              + "TX in next interval", result.getTxID());
        }
      }
    }
    return ContainerBlocksDeletionACKResponseProto.newBuilder()
        .getDefaultInstanceForType();
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate Healthy, Dead etc.
   * @return int -- count
   */
  public int getNodeCount(NodeState nodestate) {
    return scmNodeManager.getNodeCount(nodestate);
  }

  /**
   * Returns SCM container manager.
   */
  @VisibleForTesting
  public Mapping getScmContainerManager() {
    return scmContainerManager;
  }

  /**
   * Returns node manager.
   * @return - Node Manager
   */
  @VisibleForTesting
  public NodeManager getScmNodeManager() {
    return scmNodeManager;
  }

  @VisibleForTesting
  public BlockManager getScmBlockManager() {
    return scmBlockManager;
  }

  /**
   * Get block locations.
   * @param keys batch of block keys to retrieve.
   * @return set of allocated blocks.
   * @throws IOException
   */
  @Override
  public Set<AllocatedBlock> getBlockLocations(final Set<String> keys)
      throws IOException {
    Set<AllocatedBlock> locatedBlocks = new HashSet<>();
    for (String key: keys) {
      Pipeline pipeline = scmBlockManager.getBlock(key);
      AllocatedBlock block = new AllocatedBlock.Builder()
          .setKey(key)
          .setPipeline(pipeline).build();
      locatedBlocks.add(block);
    }
    return locatedBlocks;
  }

  /**
   * Asks SCM where a block should be allocated. SCM responds with the set of
   * datanodes that should be used creating this block.
   *
   * @param size - size of the block.
   * @param type - Replication type.
   * @param factor
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  @Override
  public AllocatedBlock allocateBlock(long size, OzoneProtos.ReplicationType
      type, OzoneProtos.ReplicationFactor factor) throws IOException {
    return scmBlockManager.allocateBlock(size, type, factor);
  }


  /**
   * Delete blocks for a set of object keys.
   *
   * @param keyBlocksInfoList list of block keys with object keys to delete.
   * @return deletion results.
   */
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {
    LOG.info("SCM is informed by KSM to delete {} blocks",
        keyBlocksInfoList.size());
    List<DeleteBlockGroupResult> results = new ArrayList<>();
    for (BlockGroup keyBlocks : keyBlocksInfoList) {
      Result resultCode;
      try {
        // We delete blocks in an atomic operation to prevent getting
        // into state like only a partial of blocks are deleted,
        // which will leave key in an inconsistent state.
        scmBlockManager.deleteBlocks(keyBlocks.getBlockIDList());
        resultCode = Result.success;
      } catch (SCMException scmEx) {
        LOG.warn("Fail to delete block: {}", keyBlocks.getGroupID(), scmEx);
        switch (scmEx.getResult()) {
        case CHILL_MODE_EXCEPTION:
          resultCode = Result.chillMode;
          break;
        case FAILED_TO_FIND_BLOCK:
          resultCode = Result.errorNotFound;
          break;
        default:
          resultCode = Result.unknownFailure;
        }
      } catch (IOException ex) {
        LOG.warn("Fail to delete blocks for object key: {}",
            keyBlocks.getGroupID(), ex);
        resultCode = Result.unknownFailure;
      }
      List<DeleteBlockResult> blockResultList = new ArrayList<>();
      for (String blockKey : keyBlocks.getBlockIDList()) {
        blockResultList.add(new DeleteBlockResult(blockKey, resultCode));
      }
      results.add(new DeleteBlockGroupResult(keyBlocks.getGroupID(),
          blockResultList));
    }
    return results;
  }

  @VisibleForTesting
  public String getPpcRemoteUsername() {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    return user == null ? null : user.getUserName();
  }

  private void checkAdminAccess() throws IOException {
    String remoteUser = getPpcRemoteUsername();
    if(remoteUser != null) {
      if (!scmAdminUsernames.contains(remoteUser)) {
        throw new IOException(
            "Access denied for user " + remoteUser
                + ". Superuser privilege is required.");
      }
    }
  }

  /**
   * Initialize SCM metrics.
   */
  public static void initMetrics() {
    metrics = SCMMetrics.create();
  }

  /**
   * Return SCM metrics instance.
   */
  public static SCMMetrics getMetrics() {
    return metrics == null ? SCMMetrics.create() : metrics;
  }
}
