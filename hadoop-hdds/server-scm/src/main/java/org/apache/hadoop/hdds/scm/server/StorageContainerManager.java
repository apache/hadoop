/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.PendingDeleteHandler;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.CloseContainerWatcher;
import org.apache.hadoop.hdds.scm.container.ContainerActionsHandler;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.replication
    .ReplicationActivityStatus;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.NewNodeHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeReportHandler;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.pipelines.PipelineCloseHandler;
import org.apache.hadoop.hdds.scm.pipelines.PipelineActionEventHandler;
import org.apache.hadoop.hdds.scm.pipelines.PipelineReportHandler;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.protocol.commands.RetriableDatanodeEventWatcher;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.common.StorageInfo;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .HDDS_SCM_WATCHER_TIMEOUT_DEFAULT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * StorageContainerManager is the main entry point for the service that
 * provides information about
 * which SCM nodes host containers.
 *
 * <p>DataNodes report to StorageContainerManager using heartbeat messages.
 * SCM allocates containers
 * and returns a pipeline.
 *
 * <p>A client once it gets a pipeline (a list of datanodes) will connect to
 * the datanodes and
 * create a container, which then can be used to store data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public final class StorageContainerManager extends ServiceRuntimeInfoImpl
    implements SCMMXBean {

  private static final Logger LOG = LoggerFactory
      .getLogger(StorageContainerManager.class);
  private static final String USAGE =
      "Usage: \n ozone scm [genericOptions] "
          + "[ "
          + StartupOption.INIT.getName()
          + " [ "
          + StartupOption.CLUSTERID.getName()
          + " <cid> ] ]\n "
          + "ozone scm [genericOptions] [ "
          + StartupOption.GENCLUSTERID.getName()
          + " ]\n "
          + "ozone scm [ "
          + StartupOption.HELP.getName()
          + " ]\n";
  /**
   * SCM metrics.
   */
  private static SCMMetrics metrics;

  /*
   * RPC Endpoints exposed by SCM.
   */
  private final SCMDatanodeProtocolServer datanodeProtocolServer;
  private final SCMBlockProtocolServer blockProtocolServer;
  private final SCMClientProtocolServer clientProtocolServer;

  /*
   * State Managers of SCM.
   */
  private final NodeManager scmNodeManager;
  private final ContainerManager containerManager;
  private final BlockManager scmBlockManager;
  private final SCMStorage scmStorage;

  private final EventQueue eventQueue;
  /*
   * HTTP endpoint for JMX access.
   */
  private final StorageContainerManagerHttpServer httpServer;
  /**
   * SCM super user.
   */
  private final String scmUsername;
  private final Collection<String> scmAdminUsernames;
  /**
   * SCM mxbean.
   */
  private ObjectName scmInfoBeanName;
  /**
   * Key = DatanodeUuid, value = ContainerStat.
   */
  private Cache<String, ContainerStat> containerReportCache;

  private final ReplicationManager replicationManager;

  private final LeaseManager<Long> commandWatcherLeaseManager;

  private final ReplicationActivityStatus replicationStatus;
  private final SCMChillModeManager scmChillModeManager;

  /**
   * Creates a new StorageContainerManager. Configuration will be updated
   * with information on the
   * actual listening addresses used for RPC servers.
   *
   * @param conf configuration
   */
  private StorageContainerManager(OzoneConfiguration conf) throws IOException {

    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

    StorageContainerManager.initMetrics();
    initContainerReportCache(conf);

    scmStorage = new SCMStorage(conf);
    if (scmStorage.getState() != StorageState.INITIALIZED) {
      throw new SCMException("SCM not initialized.", ResultCodes
          .SCM_NOT_INITIALIZED);
    }

    eventQueue = new EventQueue();

    scmNodeManager = new SCMNodeManager(
        conf, scmStorage.getClusterID(), this, eventQueue);
    containerManager = new SCMContainerManager(
        conf, getScmNodeManager(), cacheSize, eventQueue);
    scmBlockManager = new BlockManagerImpl(
        conf, getScmNodeManager(), containerManager, eventQueue);

    replicationStatus = new ReplicationActivityStatus();

    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(containerManager);
    NodeReportHandler nodeReportHandler =
        new NodeReportHandler(scmNodeManager);
    PipelineReportHandler pipelineReportHandler =
            new PipelineReportHandler(
                    containerManager.getPipelineSelector());
    CommandStatusReportHandler cmdStatusReportHandler =
        new CommandStatusReportHandler();

    NewNodeHandler newNodeHandler = new NewNodeHandler(scmNodeManager);
    StaleNodeHandler staleNodeHandler =
        new StaleNodeHandler(containerManager.getPipelineSelector());
    DeadNodeHandler deadNodeHandler = new DeadNodeHandler(scmNodeManager,
        getContainerManager().getStateManager());
    ContainerActionsHandler actionsHandler = new ContainerActionsHandler();
    PendingDeleteHandler pendingDeleteHandler =
        new PendingDeleteHandler(scmBlockManager.getSCMBlockDeletingService());

    ContainerReportHandler containerReportHandler =
        new ContainerReportHandler(containerManager, scmNodeManager,
            replicationStatus);
    scmChillModeManager = new SCMChillModeManager(conf,
        getContainerManager().getStateManager().getAllContainers(),
        eventQueue);
    PipelineActionEventHandler pipelineActionEventHandler =
        new PipelineActionEventHandler();

    PipelineCloseHandler pipelineCloseHandler =
        new PipelineCloseHandler(containerManager.getPipelineSelector());

    long watcherTimeout =
        conf.getTimeDuration(ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT,
            HDDS_SCM_WATCHER_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);

    commandWatcherLeaseManager = new LeaseManager<>("CommandWatcher",
        watcherTimeout);

    RetriableDatanodeEventWatcher retriableDatanodeEventWatcher =
        new RetriableDatanodeEventWatcher<>(
            SCMEvents.RETRIABLE_DATANODE_COMMAND,
            SCMEvents.DELETE_BLOCK_STATUS,
            commandWatcherLeaseManager);
    retriableDatanodeEventWatcher.start(eventQueue);

    //TODO: support configurable containerPlacement policy
    ContainerPlacementPolicy containerPlacementPolicy =
        new SCMContainerPlacementCapacity(scmNodeManager, conf);

    replicationManager = new ReplicationManager(containerPlacementPolicy,
        containerManager.getStateManager(), eventQueue,
        commandWatcherLeaseManager);

    // setup CloseContainer watcher
    CloseContainerWatcher closeContainerWatcher =
        new CloseContainerWatcher(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
            SCMEvents.CLOSE_CONTAINER_STATUS, commandWatcherLeaseManager,
            containerManager);
    closeContainerWatcher.start(eventQueue);

    scmAdminUsernames = conf.getTrimmedStringCollection(OzoneConfigKeys
        .OZONE_ADMINISTRATORS);
    scmUsername = UserGroupInformation.getCurrentUser().getUserName();
    if (!scmAdminUsernames.contains(scmUsername)) {
      scmAdminUsernames.add(scmUsername);
    }

    datanodeProtocolServer = new SCMDatanodeProtocolServer(conf, this,
        eventQueue);
    blockProtocolServer = new SCMBlockProtocolServer(conf, this);
    clientProtocolServer = new SCMClientProtocolServer(conf, this);
    httpServer = new StorageContainerManagerHttpServer(conf);

    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.RETRIABLE_DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.NODE_REPORT, nodeReportHandler);
    eventQueue.addHandler(SCMEvents.CONTAINER_REPORT, containerReportHandler);
    eventQueue.addHandler(SCMEvents.CONTAINER_ACTIONS, actionsHandler);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    eventQueue.addHandler(SCMEvents.NEW_NODE, newNodeHandler);
    eventQueue.addHandler(SCMEvents.STALE_NODE, staleNodeHandler);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    eventQueue.addHandler(SCMEvents.CMD_STATUS_REPORT, cmdStatusReportHandler);
    eventQueue.addHandler(SCMEvents.START_REPLICATION,
        replicationStatus.getReplicationStatusListener());
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS,
        replicationStatus.getChillModeStatusListener());
    eventQueue
        .addHandler(SCMEvents.PENDING_DELETE_STATUS, pendingDeleteHandler);
    eventQueue.addHandler(SCMEvents.DELETE_BLOCK_STATUS,
        (DeletedBlockLogImpl) scmBlockManager.getDeletedBlockLog());
    eventQueue.addHandler(SCMEvents.PIPELINE_ACTIONS,
        pipelineActionEventHandler);
    eventQueue.addHandler(SCMEvents.PIPELINE_CLOSE, pipelineCloseHandler);
    eventQueue.addHandler(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        scmChillModeManager);
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS,
        (BlockManagerImpl) scmBlockManager);
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS, clientProtocolServer);
    eventQueue.addHandler(SCMEvents.PIPELINE_REPORT, pipelineReportHandler);

    registerMXBean();
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr RPC server listening address
   * @return server startup message
   */
  public static String buildRpcServerStartMessage(String description,
      InetSocketAddress addr) {
    return addr != null
        ? String.format("%s is listening at %s", description, addr.toString())
        : String.format("%s not started", description);
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  public static RPC.Server startRpcServer(
      OzoneConfiguration conf,
      InetSocketAddress addr,
      Class<?> protocol,
      BlockingService instance,
      int handlerCount)
      throws IOException {
    RPC.Server rpcServer =
        new RPC.Builder(conf)
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

  /**
   * Main entry point for starting StorageContainerManager.
   *
   * @param argv arguments
   * @throws IOException if startup fails due to I/O error
   */
  public static void main(String[] argv) throws IOException {
    if (DFSUtil.parseHelpArgument(argv, USAGE, System.out, true)) {
      System.exit(0);
    }
    try {
      OzoneConfiguration conf = new OzoneConfiguration();
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      if (!hParser.isParseSuccessful()) {
        System.err.println("USAGE: " + USAGE + "\n");
        hParser.printGenericCommandUsage(System.err);
        System.exit(1);
      }
      StorageContainerManager scm = createSCM(
          hParser.getRemainingArgs(), conf, true);
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

  /**
   * Create an SCM instance based on the supplied command-line arguments.
   *
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal
   * handlers.
   *
   * @param args command line arguments.
   * @param conf HDDS configuration
   * @return SCM instance
   * @throws IOException
   */
  @VisibleForTesting
  public static StorageContainerManager createSCM(
      String[] args, OzoneConfiguration conf) throws IOException {
    return createSCM(args, conf, false);
  }

  /**
   * Create an SCM instance based on the supplied command-line arguments.
   *
   * @param args command-line arguments.
   * @param conf HDDS configuration
   * @param printBanner if true, then log a verbose startup message.
   * @return SCM instance
   * @throws IOException
   */
  private static StorageContainerManager createSCM(
      String[] args,
      OzoneConfiguration conf,
      boolean printBanner) throws IOException {
    String[] argv = (args == null) ? new String[0] : args;
    if (!HddsUtils.isHddsEnabled(conf)) {
      System.err.println(
          "SCM cannot be started in secure mode or when " + OZONE_ENABLED + "" +
              " is set to false");
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
      if (printBanner) {
        StringUtils.startupShutdownMessage(StorageContainerManager.class, argv,
            LOG);
      }
      terminate(scmInit(conf) ? 0 : 1);
      return null;
    case GENCLUSTERID:
      if (printBanner) {
        StringUtils.startupShutdownMessage(StorageContainerManager.class, argv,
            LOG);
      }
      System.out.println("Generating new cluster id:");
      System.out.println(StorageInfo.newClusterID());
      terminate(0);
      return null;
    case HELP:
      printUsage(System.err);
      terminate(0);
      return null;
    default:
      if (printBanner) {
        StringUtils.startupShutdownMessage(StorageContainerManager.class, argv,
            LOG);
      }
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
        System.out.println(
            "SCM initialization succeeded."
                + "Current cluster id for sd="
                + scmStorage.getStorageDir()
                + ";cid="
                + scmStorage.getClusterID());
        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize SCM version file", ioe);
        return false;
      }
    } else {
      System.out.println(
          "SCM already initialized. Reusing existing"
              + " cluster id for sd="
              + scmStorage.getStorageDir()
              + ";cid="
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
              LOG.error(
                  "Must specify a valid cluster ID after the "
                      + StartupOption.CLUSTERID.getName()
                      + " flag");
              return null;
            }
          } else {
            return null;
          }
        }
      } else {
        if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
          if (argsLen > 1) {
            return null;
          }
          startOpt = StartupOption.GENCLUSTERID;
        }
      }
    }
    return startOpt;
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

  public SCMStorage getScmStorage() {
    return scmStorage;
  }

  public SCMDatanodeProtocolServer getDatanodeProtocolServer() {
    return datanodeProtocolServer;
  }

  public SCMBlockProtocolServer getBlockProtocolServer() {
    return blockProtocolServer;
  }

  public SCMClientProtocolServer getClientProtocolServer() {
    return clientProtocolServer;
  }

  /**
   * Initialize container reports cache that sent from datanodes.
   *
   * @param conf
   */
  private void initContainerReportCache(OzoneConfiguration conf) {
    containerReportCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            .maximumSize(Integer.MAX_VALUE)
            .removalListener(
                new RemovalListener<String, ContainerStat>() {
                  @Override
                  public void onRemoval(
                      RemovalNotification<String, ContainerStat>
                          removalNotification) {
                    synchronized (containerReportCache) {
                      ContainerStat stat = removalNotification.getValue();
                      // remove invalid container report
                      metrics.decrContainerStat(stat);
                      LOG.debug(
                          "Remove expired container stat entry for datanode: " +
                              "{}.",
                          removalNotification.getKey());
                    }
                  }
                })
            .build();
  }

  private void registerMXBean() {
    Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.scmInfoBeanName =
        MBeans.register(
            "StorageContainerManager", "StorageContainerManagerInfo",
            jmxProperties, this);
  }

  private void unregisterMXBean() {
    if (this.scmInfoBeanName != null) {
      MBeans.unregister(this.scmInfoBeanName);
      this.scmInfoBeanName = null;
    }
  }

  @VisibleForTesting
  public ContainerInfo getContainerInfo(long containerID) throws
      IOException {
    return containerManager.getContainer(containerID);
  }

  /**
   * Returns listening address of StorageLocation Protocol RPC server.
   *
   * @return listen address of StorageLocation RPC server
   */
  @VisibleForTesting
  public InetSocketAddress getClientRpcAddress() {
    return getClientProtocolServer().getClientRpcAddress();
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
    return getDatanodeProtocolServer().getDatanodeRpcAddress();
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
    LOG.info(
        buildRpcServerStartMessage(
            "StorageContainerLocationProtocol RPC server",
            getClientRpcAddress()));
    DefaultMetricsSystem.initialize("StorageContainerManager");

    commandWatcherLeaseManager.start();
    getClientProtocolServer().start();

    LOG.info(buildRpcServerStartMessage("ScmBlockLocationProtocol RPC " +
        "server", getBlockProtocolServer().getBlockRpcAddress()));
    getBlockProtocolServer().start();

    LOG.info(buildRpcServerStartMessage("ScmDatanodeProtocl RPC " +
        "server", getDatanodeProtocolServer().getDatanodeRpcAddress()));
    getDatanodeProtocolServer().start();

    httpServer.start();
    scmBlockManager.start();
    replicationStatus.start();
    replicationManager.start();
    setStartTime();
  }

  /**
   * Stop service.
   */
  public void stop() {

    try {
      LOG.info("Stopping Replication Activity Status tracker.");
      replicationStatus.close();
    } catch (Exception ex) {
      LOG.error("Replication Activity Status tracker stop failed.", ex);
    }


    try {
      LOG.info("Stopping Replication Manager Service.");
      replicationManager.stop();
    } catch (Exception ex) {
      LOG.error("Replication manager service stop failed.", ex);
    }

    try {
      LOG.info("Stopping Lease Manager of the command watchers");
      commandWatcherLeaseManager.shutdown();
    } catch (Exception ex) {
      LOG.error("Lease Manager of the command watchers stop failed");
    }

    try {
      LOG.info("Stopping datanode service RPC server");
      getDatanodeProtocolServer().stop();

    } catch (Exception ex) {
      LOG.error("Storage Container Manager datanode RPC stop failed.", ex);
    }

    try {
      LOG.info("Stopping block service RPC server");
      getBlockProtocolServer().stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager blockRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping the StorageContainerLocationProtocol RPC server");
      getClientProtocolServer().stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager clientRpcServer stop failed.", ex);
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

    if (containerReportCache != null) {
      containerReportCache.invalidateAll();
      containerReportCache.cleanUp();
    }

    if (metrics != null) {
      metrics.unRegister();
    }

    unregisterMXBean();
    // Event queue must be stopped before the DB store is closed at the end.
    try {
      LOG.info("Stopping SCM Event Queue.");
      eventQueue.close();
    } catch (Exception ex) {
      LOG.error("SCM Event Queue stop failed", ex);
    }
    IOUtils.cleanupWithLogger(LOG, containerManager);
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      getBlockProtocolServer().join();
      getClientProtocolServer().join();
      getDatanodeProtocolServer().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
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
  public ContainerManager getContainerManager() {
    return containerManager;
  }

  /**
   * Returns node manager.
   *
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

  public void checkAdminAccess(String remoteUser) throws IOException {
    if (remoteUser != null) {
      if (!scmAdminUsernames.contains(remoteUser)) {
        throw new IOException(
            "Access denied for user " + remoteUser + ". Superuser privilege " +
                "is required.");
      }
    }
  }

  /**
   * Invalidate container stat entry for given datanode.
   *
   * @param datanodeUuid
   */
  public void removeContainerReport(String datanodeUuid) {
    synchronized (containerReportCache) {
      containerReportCache.invalidate(datanodeUuid);
    }
  }

  /**
   * Get container stat of specified datanode.
   *
   * @param datanodeUuid
   * @return
   */
  public ContainerStat getContainerReport(String datanodeUuid) {
    ContainerStat stat = null;
    synchronized (containerReportCache) {
      stat = containerReportCache.getIfPresent(datanodeUuid);
    }

    return stat;
  }

  /**
   * Returns a view of the container stat entries. Modifications made to the
   * map will directly
   * affect the cache.
   *
   * @return
   */
  public ConcurrentMap<String, ContainerStat> getContainerReportCache() {
    return containerReportCache.asMap();
  }

  @Override
  public Map<String, String> getContainerReport() {
    Map<String, String> id2StatMap = new HashMap<>();
    synchronized (containerReportCache) {
      ConcurrentMap<String, ContainerStat> map = containerReportCache.asMap();
      for (Map.Entry<String, ContainerStat> entry : map.entrySet()) {
        id2StatMap.put(entry.getKey(), entry.getValue().toJsonString());
      }
    }

    return id2StatMap;
  }

  /**
   * Returns live chill mode container threshold.
   *
   * @return String
   */
  @Override
  public double getChillModeCurrentContainerThreshold() {
    return getCurrentContainerThreshold();
  }

  /**
   * Returns chill mode status.
   * @return boolean
   */
  @Override
  public boolean isInChillMode() {
    return scmChillModeManager.getInChillMode();
  }

  /**
   * Returns EventPublisher.
   */
  public EventPublisher getEventQueue(){
    return eventQueue;
  }

  /**
   * Force SCM out of chill mode.
   */
  public boolean exitChillMode() {
    scmChillModeManager.exitChillMode(eventQueue);
    return true;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    return scmChillModeManager.getCurrentContainerThreshold();
  }

  /**
   * Startup options.
   */
  public enum StartupOption {
    INIT("-init"),
    CLUSTERID("-clusterid"),
    GENCLUSTERID("-genclusterid"),
    REGULAR("-regular"),
    HELP("-help");

    private final String name;
    private String clusterId = null;

    StartupOption(String arg) {
      this.name = arg;
    }

    public String getClusterId() {
      return clusterId;
    }

    public void setClusterId(String cid) {
      if (cid != null && !cid.isEmpty()) {
        clusterId = cid;
      }
    }

    public String getName() {
      return name;
    }
  }
}
