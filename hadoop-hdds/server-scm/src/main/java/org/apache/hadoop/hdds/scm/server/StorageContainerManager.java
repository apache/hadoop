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
import java.util.Objects;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.PendingDeleteHandler;
import org.apache.hadoop.hdds.scm.safemode.SafeModeHandler;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerActionsHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreRDBImpl;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.NewNodeHandler;
import org.apache.hadoop.hdds.scm.node.NonHealthyToHealthyNodeHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeReportHandler;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineActionHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineReportHandler;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.common.StorageInfo;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.RetriableDatanodeEventWatcher;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.utils.HddsVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT_DEFAULT;
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
 * the datanodes and create a container, which then can be used to store data.
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
  private SCMSecurityProtocolServer securityProtocolServer;

  /*
   * State Managers of SCM.
   */
  private NodeManager scmNodeManager;
  private PipelineManager pipelineManager;
  private ContainerManager containerManager;
  private BlockManager scmBlockManager;
  private final SCMStorageConfig scmStorageConfig;

  private SCMMetadataStore scmMetadataStore;

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

  private ReplicationManager replicationManager;

  private final LeaseManager<Long> commandWatcherLeaseManager;

  private SCMSafeModeManager scmSafeModeManager;
  private CertificateServer certificateServer;

  private JvmPauseMonitor jvmPauseMonitor;
  private final OzoneConfiguration configuration;
  private final SafeModeHandler safeModeHandler;
  private SCMContainerMetrics scmContainerMetrics;

  /**
   * Creates a new StorageContainerManager. Configuration will be
   * updated with information on the actual listening addresses used
   * for RPC servers.
   *
   * @param conf configuration
   */
  public StorageContainerManager(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    // default empty configurator means default managers will be used.
    this(conf, new SCMConfigurator());
  }


  /**
   * This constructor offers finer control over how SCM comes up.
   * To use this, user needs to create a SCMConfigurator and set various
   * managers that user wants SCM to use, if a value is missing then SCM will
   * use the default value for that manager.
   *
   * @param conf - Configuration
   * @param configurator - configurator
   */
  public StorageContainerManager(OzoneConfiguration conf,
                                 SCMConfigurator configurator)
      throws IOException, AuthenticationException  {
    super(HddsVersionInfo.HDDS_VERSION_INFO);

    Objects.requireNonNull(configurator, "configurator cannot not be null");
    Objects.requireNonNull(conf, "configuration cannot not be null");

    configuration = conf;
    initMetrics();
    initContainerReportCache(conf);
    /**
     * It is assumed the scm --init command creates the SCM Storage Config.
     */
    scmStorageConfig = new SCMStorageConfig(conf);
    if (scmStorageConfig.getState() != StorageState.INITIALIZED) {
      LOG.error("Please make sure you have run \'ozone scm --init\' " +
          "command to generate all the required metadata.");
      throw new SCMException("SCM not initialized due to storage config " +
          "failure.", ResultCodes.SCM_NOT_INITIALIZED);
    }

    /**
     * Important : This initialization sequence is assumed by some of our tests.
     * The testSecureOzoneCluster assumes that security checks have to be
     * passed before any artifacts like SCM DB is created. So please don't
     * add any other initialization above the Security checks please.
     */
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      loginAsSCMUser(conf);
    }

    // Creates the SCM DBs or opens them if it exists.
    // A valid pointer to the store is required by all the other services below.
    initalizeMetadataStore(conf, configurator);

    // Authenticate SCM if security is enabled, this initialization can only
    // be done after the metadata store is initialized.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      initializeCAnSecurityProtocol(conf, configurator);
    } else {
      // if no Security, we do not create a Certificate Server at all.
      // This allows user to boot SCM without security temporarily
      // and then come back and enable it without any impact.
      certificateServer = null;
      securityProtocolServer = null;
    }


    eventQueue = new EventQueue();
    long watcherTimeout =
        conf.getTimeDuration(ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT,
            HDDS_SCM_WATCHER_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    commandWatcherLeaseManager = new LeaseManager<>("CommandWatcher",
        watcherTimeout);
    initalizeSystemManagers(conf, configurator);

    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(pipelineManager, containerManager);
    NodeReportHandler nodeReportHandler =
        new NodeReportHandler(scmNodeManager);
    PipelineReportHandler pipelineReportHandler =
        new PipelineReportHandler(scmSafeModeManager, pipelineManager, conf);
    CommandStatusReportHandler cmdStatusReportHandler =
        new CommandStatusReportHandler();

    NewNodeHandler newNodeHandler = new NewNodeHandler(pipelineManager, conf);
    StaleNodeHandler staleNodeHandler =
        new StaleNodeHandler(scmNodeManager, pipelineManager, conf);
    DeadNodeHandler deadNodeHandler = new DeadNodeHandler(scmNodeManager,
        containerManager);
    NonHealthyToHealthyNodeHandler nonHealthyToHealthyNodeHandler =
        new NonHealthyToHealthyNodeHandler(pipelineManager, conf);
    ContainerActionsHandler actionsHandler = new ContainerActionsHandler();
    PendingDeleteHandler pendingDeleteHandler =
        new PendingDeleteHandler(scmBlockManager.getSCMBlockDeletingService());

    ContainerReportHandler containerReportHandler =
        new ContainerReportHandler(scmNodeManager, containerManager);

    IncrementalContainerReportHandler incrementalContainerReportHandler =
        new IncrementalContainerReportHandler(containerManager);

    PipelineActionHandler pipelineActionHandler =
        new PipelineActionHandler(pipelineManager, conf);


    RetriableDatanodeEventWatcher retriableDatanodeEventWatcher =
        new RetriableDatanodeEventWatcher<>(
            SCMEvents.RETRIABLE_DATANODE_COMMAND,
            SCMEvents.DELETE_BLOCK_STATUS,
            commandWatcherLeaseManager);
    retriableDatanodeEventWatcher.start(eventQueue);

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

    safeModeHandler = new SafeModeHandler(configuration,
        clientProtocolServer, scmBlockManager, replicationManager,
        pipelineManager);

    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.RETRIABLE_DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.NODE_REPORT, nodeReportHandler);
    eventQueue.addHandler(SCMEvents.CONTAINER_REPORT, containerReportHandler);
    eventQueue.addHandler(SCMEvents.INCREMENTAL_CONTAINER_REPORT,
        incrementalContainerReportHandler);
    eventQueue.addHandler(SCMEvents.CONTAINER_ACTIONS, actionsHandler);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    eventQueue.addHandler(SCMEvents.NEW_NODE, newNodeHandler);
    eventQueue.addHandler(SCMEvents.STALE_NODE, staleNodeHandler);
    eventQueue.addHandler(SCMEvents.NON_HEALTHY_TO_HEALTHY_NODE,
        nonHealthyToHealthyNodeHandler);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    eventQueue.addHandler(SCMEvents.CMD_STATUS_REPORT, cmdStatusReportHandler);
    eventQueue
        .addHandler(SCMEvents.PENDING_DELETE_STATUS, pendingDeleteHandler);
    eventQueue.addHandler(SCMEvents.DELETE_BLOCK_STATUS,
        (DeletedBlockLogImpl) scmBlockManager.getDeletedBlockLog());
    eventQueue.addHandler(SCMEvents.PIPELINE_ACTIONS, pipelineActionHandler);
    eventQueue.addHandler(SCMEvents.PIPELINE_REPORT, pipelineReportHandler);
    eventQueue.addHandler(SCMEvents.SAFE_MODE_STATUS, safeModeHandler);
    registerMXBean();
    registerMetricsSource(this);
  }

  /**
   * This function initializes the following managers. If the configurator
   * specifies a value, we will use it, else we will use the default value.
   *
   *  Node Manager
   *  Pipeline Manager
   *  Container Manager
   *  Block Manager
   *  Replication Manager
   *  Safe Mode Manager
   *
   * @param conf - Ozone Configuration.
   * @param configurator - A customizer which allows different managers to be
   *                    used if needed.
   * @throws IOException - on Failure.
   */
  private void initalizeSystemManagers(OzoneConfiguration conf,
                                       SCMConfigurator configurator)
      throws IOException {
    if(configurator.getScmNodeManager() != null) {
      scmNodeManager = configurator.getScmNodeManager();
    } else {
      scmNodeManager = new SCMNodeManager(
          conf, scmStorageConfig.getClusterID(), this, eventQueue);
    }

    //TODO: support configurable containerPlacement policy
    ContainerPlacementPolicy containerPlacementPolicy =
        new SCMContainerPlacementCapacity(scmNodeManager, conf);

    if (configurator.getPipelineManager() != null) {
      pipelineManager = configurator.getPipelineManager();
    } else {
      pipelineManager =
          new SCMPipelineManager(conf, scmNodeManager, eventQueue);
    }

    if(configurator.getContainerManager() != null) {
      containerManager = configurator.getContainerManager();
    } else {
      containerManager = new SCMContainerManager(
          conf, scmNodeManager, pipelineManager, eventQueue);
    }

    if(configurator.getScmBlockManager() != null) {
      scmBlockManager = configurator.getScmBlockManager();
    } else {
      scmBlockManager = new BlockManagerImpl(conf, this);
    }
    if (configurator.getReplicationManager() != null) {
      replicationManager = configurator.getReplicationManager();
    }  else {
      replicationManager = new ReplicationManager(conf,
          containerManager, containerPlacementPolicy, eventQueue);
    }
    if(configurator.getScmSafeModeManager() != null) {
      scmSafeModeManager = configurator.getScmSafeModeManager();
    } else {
      scmSafeModeManager = new SCMSafeModeManager(conf,
          containerManager.getContainers(), pipelineManager, eventQueue);
    }
  }

  /**
   * If security is enabled we need to have the Security Protocol and a
   * default CA. This function initializes those values based on the
   * configurator.
   *
   * @param conf - Config
   * @param configurator - configurator
   * @throws IOException - on Failure
   * @throws AuthenticationException - on Failure
   */
  private void initializeCAnSecurityProtocol(OzoneConfiguration conf,
                                             SCMConfigurator configurator)
      throws IOException {
    if(configurator.getCertificateServer() != null) {
      this.certificateServer = configurator.getCertificateServer();
    } else {
      // This assumes that SCM init has run, and DB metadata stores are created.
      certificateServer = initializeCertificateServer(
          getScmStorageConfig().getClusterID(),
          getScmStorageConfig().getScmId());
    }
    // TODO: Support Intermediary CAs in future.
    certificateServer.init(new SecurityConfig(conf),
        CertificateServer.CAType.SELF_SIGNED_CA);
    securityProtocolServer = new SCMSecurityProtocolServer(conf,
        certificateServer);
  }

  /**
   * Init the metadata store based on the configurator.
   * @param conf - Config
   * @param configurator - configurator
   * @throws IOException - on Failure
   */
  private void initalizeMetadataStore(OzoneConfiguration conf,
                                      SCMConfigurator configurator)
      throws IOException {
    if(configurator.getMetadataStore() != null) {
      scmMetadataStore = configurator.getMetadataStore();
    } else {
      scmMetadataStore = new SCMMetadataStoreRDBImpl(conf);
      if (scmMetadataStore == null) {
        throw new SCMException("Unable to initialize metadata store",
            ResultCodes.SCM_NOT_INITIALIZED);
      }
    }
  }

  /**
   * Login as the configured user for SCM.
   *
   * @param conf
   */
  private void loginAsSCMUser(Configuration conf)
      throws IOException, AuthenticationException {
    LOG.debug("Ozone security is enabled. Attempting login for SCM user. "
            + "Principal: {}, keytab: {}",
        conf.get(HDDS_SCM_KERBEROS_PRINCIPAL_KEY),
        conf.get(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY));

    if (SecurityUtil.getAuthenticationMethod(conf).equals(
        AuthenticationMethod.KERBEROS)) {
      UserGroupInformation.setConfiguration(conf);
      InetSocketAddress socAddr = HddsServerUtil
          .getScmBlockClientBindAddress(conf);
      SecurityUtil.login(conf, HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
          HDDS_SCM_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
    } else {
      throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
          conf) + " authentication method not support. "
          + "SCM user login failed.");
    }
    LOG.info("SCM login successful.");
  }


  /**
   * This function creates/initializes a certificate server as needed.
   * This function is idempotent, so calling this again and again after the
   * server is initialized is not a problem.
   *
   * @param clusterID - Cluster ID
   * @param scmID     - SCM ID
   */
  private CertificateServer initializeCertificateServer(String clusterID,
      String scmID) throws IOException {
    // TODO: Support Certificate Server loading via Class Name loader.
    // So it is easy to use different Certificate Servers if needed.
    String subject = "scm@" + InetAddress.getLocalHost().getHostName();
    if(this.scmMetadataStore == null) {
      LOG.error("Cannot initialize Certificate Server without a valid meta " +
          "data layer.");
      throw new SCMException("Cannot initialize CA without a valid metadata " +
          "store", ResultCodes.SCM_NOT_INITIALIZED);
    }
    SCMCertStore certStore = new SCMCertStore(this.scmMetadataStore);
    return new DefaultCAServer(subject, clusterID, scmID, certStore);
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr        RPC server listening address
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
      TracingUtil.initTracing("StorageContainerManager");
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
   * <p>
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal
   * handlers.
   *
   * @param args command line arguments.
   * @param conf HDDS configuration
   * @return SCM instance
   * @throws IOException, AuthenticationException
   */
  @VisibleForTesting
  public static StorageContainerManager createSCM(
      String[] args, OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    return createSCM(args, conf, false);
  }

  /**
   * Create an SCM instance based on the supplied command-line arguments.
   *
   * @param args        command-line arguments.
   * @param conf        HDDS configuration
   * @param printBanner if true, then log a verbose startup message.
   * @return SCM instance
   * @throws IOException, AuthenticationException
   */
  private static StorageContainerManager createSCM(
      String[] args,
      OzoneConfiguration conf,
      boolean printBanner)
      throws IOException, AuthenticationException {
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
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    StorageState state = scmStorageConfig.getState();
    if (state != StorageState.INITIALIZED) {
      try {
        String clusterId = StartupOption.INIT.getClusterId();
        if (clusterId != null && !clusterId.isEmpty()) {
          scmStorageConfig.setClusterId(clusterId);
        }
        scmStorageConfig.initialize();
        System.out.println(
            "SCM initialization succeeded."
                + "Current cluster id for sd="
                + scmStorageConfig.getStorageDir()
                + ";cid="
                + scmStorageConfig.getClusterID());
        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize SCM version file", ioe);
        return false;
      }
    } else {
      System.out.println(
          "SCM already initialized. Reusing existing"
              + " cluster id for sd="
              + scmStorageConfig.getStorageDir()
              + ";cid="
              + scmStorageConfig.getClusterID());
      return true;
    }
  }

  private static StartupOption parseArguments(String[] args) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = null;
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

  public SCMStorageConfig getScmStorageConfig() {
    return scmStorageConfig;
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

  public SCMSecurityProtocolServer getSecurityProtocolServer() {
    return securityProtocolServer;
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
    final Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.scmInfoBeanName = HddsUtils.registerWithJmxProperties(
        "StorageContainerManager", "StorageContainerManagerInfo",
        jmxProperties, this);
  }

  private void registerMetricsSource(SCMMXBean scmMBean) {
    scmContainerMetrics = SCMContainerMetrics.create(scmMBean);
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
    return containerManager.getContainer(ContainerID.valueof(containerID));
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
    if (getSecurityProtocolServer() != null) {
      getSecurityProtocolServer().start();
    }

    httpServer.start();
    scmBlockManager.start();

    // Start jvm monitor
    jvmPauseMonitor = new JvmPauseMonitor();
    jvmPauseMonitor.init(configuration);
    jvmPauseMonitor.start();

    setStartTime();
  }

  /**
   * Stop service.
   */
  public void stop() {

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

    if (getSecurityProtocolServer() != null) {
      getSecurityProtocolServer().stop();
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
    if (scmContainerMetrics != null) {
      scmContainerMetrics.unRegister();
    }

    // Event queue must be stopped before the DB store is closed at the end.
    try {
      LOG.info("Stopping SCM Event Queue.");
      eventQueue.close();
    } catch (Exception ex) {
      LOG.error("SCM Event Queue stop failed", ex);
    }

    if (jvmPauseMonitor != null) {
      jvmPauseMonitor.stop();
    }
    IOUtils.cleanupWithLogger(LOG, containerManager);
    IOUtils.cleanupWithLogger(LOG, pipelineManager);

    try {
      scmMetadataStore.stop();
    } catch (Exception ex) {
      LOG.error("SCM Metadata store stop failed", ex);
    }
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      getBlockProtocolServer().join();
      getClientProtocolServer().join();
      getDatanodeProtocolServer().join();
      if (getSecurityProtocolServer() != null) {
        getSecurityProtocolServer().join();
      }
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

  /**
   * Returns pipeline manager.
   *
   * @return - Pipeline Manager
   */
  @VisibleForTesting
  public PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  @VisibleForTesting
  public BlockManager getScmBlockManager() {
    return scmBlockManager;
  }

  @VisibleForTesting
  public SafeModeHandler getSafeModeHandler() {
    return safeModeHandler;
  }

  @VisibleForTesting
  public SCMSafeModeManager getScmSafeModeManager() {
    return scmSafeModeManager;
  }

  @VisibleForTesting
  public ReplicationManager getReplicationManager() {
    return replicationManager;
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
   * Returns live safe mode container threshold.
   *
   * @return String
   */
  @Override
  public double getSafeModeCurrentContainerThreshold() {
    return getCurrentContainerThreshold();
  }

  /**
   * Returns safe mode status.
   * @return boolean
   */
  @Override
  public boolean isInSafeMode() {
    return scmSafeModeManager.getInSafeMode();
  }

  /**
   * Returns EventPublisher.
   */
  public EventPublisher getEventQueue() {
    return eventQueue;
  }

  /**
   * Force SCM out of safe mode.
   */
  public boolean exitSafeMode() {
    scmSafeModeManager.exitSafeMode(eventQueue);
    return true;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    return scmSafeModeManager.getCurrentContainerThreshold();
  }

  @Override
  public Map<String, Integer> getContainerStateCount() {
    Map<String, Integer> nodeStateCount = new HashMap<>();
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      nodeStateCount.put(state.toString(),
          containerManager.getContainerCountByState(state));
    }
    return nodeStateCount;
  }


  /**
   * Returns the SCM metadata Store.
   * @return SCMMetadataStore
   */
  public SCMMetadataStore getScmMetadataStore() {
    return scmMetadataStore;
  }
  /**
   * Startup options.
   */
  public enum StartupOption {
    INIT("--init"),
    CLUSTERID("--clusterid"),
    GENCLUSTERID("--genclusterid"),
    REGULAR("--regular"),
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
