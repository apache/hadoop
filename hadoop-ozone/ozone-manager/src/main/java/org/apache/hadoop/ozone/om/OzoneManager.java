/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneAclException;
import org.apache.hadoop.ozone.security.acl.OzoneAclException.ErrorCode;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForBlockClients;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.HddsUtils.isHddsEnabled;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.ozone.OmUtils.getOmAddress;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_TEMP_FILE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_METRICS_SAVE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneManagerService.newReflectiveBlockingService;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Ozone Manager is the metadata manager of ozone.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public final class OzoneManager extends ServiceRuntimeInfoImpl
    implements OzoneManagerProtocol, OMMXBean, Auditor {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.OMLOGGER);

  private static final String USAGE =
      "Usage: \n ozone om [genericOptions] " + "[ "
          + StartupOption.INIT.getName() + " ]\n " + "ozone om [ "
          + StartupOption.HELP.getName() + " ]\n";
  private final OzoneConfiguration configuration;
  private RPC.Server omRpcServer;
  private InetSocketAddress omRpcAddress;
  private OzoneManagerRatisServer omRatisServer;
  private final OMMetadataManager metadataManager;
  private final VolumeManager volumeManager;
  private final BucketManager bucketManager;
  private final KeyManager keyManager;
  private final OMMetrics metrics;
  private OzoneManagerHttpServer httpServer;
  private final OMStorage omStorage;
  private final ScmBlockLocationProtocol scmBlockClient;
  private final StorageContainerLocationProtocol scmContainerClient;
  private ObjectName omInfoBeanName;
  private final S3BucketManager s3BucketManager;
  private Timer metricsTimer;
  private ScheduleOMMetricsWriteTask scheduleOMMetricsWriteTask;
  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(OmMetricsInfo.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private final Runnable shutdownHook;
  private final File omMetaDir;
  private final boolean isAclEnabled;
  private final IAccessAuthorizer accessAuthorizer;
  private JvmPauseMonitor jvmPauseMonitor;

  private OzoneManager(OzoneConfiguration conf) throws IOException {
    Preconditions.checkNotNull(conf);
    configuration = conf;
    omStorage = new OMStorage(conf);
    scmBlockClient = getScmBlockClient(configuration);
    scmContainerClient = getScmContainerClient(configuration);
    if (omStorage.getState() != StorageState.INITIALIZED) {
      throw new OMException("OM not initialized.",
          ResultCodes.OM_NOT_INITIALIZED);
    }

    // verifies that the SCM info in the OM Version file is correct.
    ScmInfo scmInfo = scmBlockClient.getScmInfo();
    if (!(scmInfo.getClusterId().equals(omStorage.getClusterID()) && scmInfo
        .getScmId().equals(omStorage.getScmId()))) {
      throw new OMException("SCM version info mismatch.",
          ResultCodes.SCM_VERSION_MISMATCH_ERROR);
    }

    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    metadataManager = new OmMetadataManagerImpl(configuration);
    volumeManager = new VolumeManagerImpl(metadataManager, configuration);
    bucketManager = new BucketManagerImpl(metadataManager);
    metrics = OMMetrics.create();

    s3BucketManager = new S3BucketManagerImpl(configuration, metadataManager,
        volumeManager, bucketManager);
    keyManager =
        new KeyManagerImpl(scmBlockClient, metadataManager, configuration,
            omStorage.getOmId());

    shutdownHook = () -> {
      saveOmMetrics();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
    isAclEnabled = conf.getBoolean(OZONE_ACL_ENABLED,
            OZONE_ACL_ENABLED_DEFAULT);
    if (isAclEnabled) {
      accessAuthorizer = getACLAuthorizerInstance(conf);
    } else {
      accessAuthorizer = null;
    }
    omMetaDir = OmUtils.getOmDbDir(configuration);

  }

  /**
   * Returns an instance of {@link IAccessAuthorizer}.
   * Looks up the configuration to see if there is custom class specified.
   * Constructs the instance by passing the configuration directly to the
   * constructor to achieve thread safety using final fields.
   * @param conf
   * @return IAccessAuthorizer
   */
  private IAccessAuthorizer getACLAuthorizerInstance(OzoneConfiguration conf) {
    Class<? extends IAccessAuthorizer> clazz = conf.getClass(
        OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthorizer.class,
        IAccessAuthorizer.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  /**
   * Class which schedule saving metrics to a file.
   */
  private class ScheduleOMMetricsWriteTask extends TimerTask {
    public void run() {
      saveOmMetrics();
    }
  }

  private void saveOmMetrics() {
    try {
      boolean success;
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(
              getTempMetricsStorageFile()), "UTF-8"))) {
        OmMetricsInfo metricsInfo = new OmMetricsInfo();
        metricsInfo.setNumKeys(metrics.getNumKeys());
        WRITER.writeValue(writer, metricsInfo);
        success = true;
      }

      if (success) {
        Files.move(getTempMetricsStorageFile().toPath(),
            getMetricsStorageFile().toPath(), StandardCopyOption
                .ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException ex) {
      LOG.error("Unable to write the om Metrics file", ex);
    }
  }

  /**
   * Returns temporary metrics storage file.
   * @return File
   */
  private File getTempMetricsStorageFile() {
    return new File(omMetaDir, OM_METRICS_TEMP_FILE);
  }

  /**
   * Returns metrics storage file.
   * @return File
   */
  private File getMetricsStorageFile() {
    return new File(omMetaDir, OM_METRICS_FILE);
  }


  /**
   * Create a scm block client, used by putKey() and getKey().
   *
   * @return {@link ScmBlockLocationProtocol}
   * @throws IOException
   */
  private static ScmBlockLocationProtocol getScmBlockClient(
      OzoneConfiguration conf) throws IOException {
    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress scmBlockAddress =
        getScmAddressForBlockClients(conf);
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(ScmBlockLocationProtocolPB.class, scmVersion,
                scmBlockAddress, UserGroupInformation.getCurrentUser(), conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmBlockLocationClient;
  }

  /**
   * Returns a scm container client.
   *
   * @return {@link StorageContainerLocationProtocol}
   * @throws IOException
   */
  private static StorageContainerLocationProtocol getScmContainerClient(
      OzoneConfiguration conf) throws IOException {
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(StorageContainerLocationProtocolPB.class);
    InetSocketAddress scmAddr = getScmAddressForClients(
        conf);
    StorageContainerLocationProtocolClientSideTranslatorPB scmContainerClient =
        new StorageContainerLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(StorageContainerLocationProtocolPB.class, scmVersion,
                scmAddr, UserGroupInformation.getCurrentUser(), conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmContainerClient;
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
  private static RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      int handlerCount) throws IOException {
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

  /**
   * Main entry point for starting OzoneManager.
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
        System.err.println("USAGE: " + USAGE + " \n");
        hParser.printGenericCommandUsage(System.err);
        System.exit(1);
      }
      OzoneManager om = createOm(hParser.getRemainingArgs(), conf, true);
      if (om != null) {
        om.start();
        om.join();
      }
    } catch (Throwable t) {
      LOG.error("Failed to start the OzoneManager.", t);
      terminate(1, t);
    }
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  /**
   * Constructs OM instance based on command line arguments.
   *
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal
   * handlers.
   *
   * @param argv Command line arguments
   * @param conf OzoneConfiguration
   * @return OM instance
   * @throws IOException in case OM instance creation fails.
   */
  @VisibleForTesting
  public static OzoneManager createOm(
      String[] argv, OzoneConfiguration conf) throws IOException {
    return createOm(argv, conf, false);
  }


  /**
   * Constructs OM instance based on command line arguments.
   *
   * @param argv Command line arguments
   * @param conf OzoneConfiguration
   * @param printBanner if true then log a verbose startup message.
   * @return OM instance
   * @throws IOException in case OM instance creation fails.
   */
  private static OzoneManager createOm(String[] argv,
      OzoneConfiguration conf, boolean printBanner) throws IOException {
    if (!isHddsEnabled(conf)) {
      System.err.println("OM cannot be started in secure mode or when " +
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
      if (printBanner) {
        StringUtils.startupShutdownMessage(OzoneManager.class, argv, LOG);
      }
      terminate(omInit(conf) ? 0 : 1);
      return null;
    case HELP:
      printUsage(System.err);
      terminate(0);
      return null;
    default:
      if (argv == null) {
        argv = new String[]{};
      }
      if (printBanner) {
        StringUtils.startupShutdownMessage(OzoneManager.class, argv, LOG);
      }
      return new OzoneManager(conf);
    }
  }

  /**
   * Initializes the OM instance.
   *
   * @param conf OzoneConfiguration
   * @return true if OM initialization succeeds, false otherwise
   * @throws IOException in case ozone metadata directory path is not
   *                     accessible
   */
  @VisibleForTesting
  static boolean omInit(OzoneConfiguration conf) throws IOException {
    OMStorage omStorage = new OMStorage(conf);
    StorageState state = omStorage.getState();
    if (state != StorageState.INITIALIZED) {
      try {
        ScmBlockLocationProtocol scmBlockClient = getScmBlockClient(conf);
        ScmInfo scmInfo = scmBlockClient.getScmInfo();
        String clusterId = scmInfo.getClusterId();
        String scmId = scmInfo.getScmId();
        if (clusterId == null || clusterId.isEmpty()) {
          throw new IOException("Invalid Cluster ID");
        }
        if (scmId == null || scmId.isEmpty()) {
          throw new IOException("Invalid SCM ID");
        }
        omStorage.setClusterId(clusterId);
        omStorage.setScmId(scmId);
        omStorage.initialize();
        System.out.println(
            "OM initialization succeeded.Current cluster id for sd="
                + omStorage.getStorageDir() + ";cid=" + omStorage
                .getClusterID());
        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize OM version file", ioe);
        return false;
      }
    } else {
      System.out.println(
          "OM already initialized.Reusing existing cluster id for sd="
              + omStorage.getStorageDir() + ";cid=" + omStorage
              .getClusterID());
      return true;
    }
  }

  /**
   * Parses the command line options for OM initialization.
   *
   * @param args command line arguments
   * @return StartupOption if options are valid, null otherwise
   */
  private static StartupOption parseArguments(String[] args) {
    if (args == null || args.length == 0) {
      return StartupOption.REGULAR;
    } else {
      if (args.length == 1) {
        return StartupOption.parse(args[0]);
      }
    }
    return null;
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

  @VisibleForTesting
  public KeyManager getKeyManager() {
    return keyManager;
  }

  @VisibleForTesting
  public ScmInfo getScmInfo() throws IOException {
    return scmBlockClient.getScmInfo();
  }

  @VisibleForTesting
  public OMStorage getOmStorage() {
    return omStorage;
  }

  @VisibleForTesting
  public LifeCycle.State getOmRatisServerState() {
    if (omRatisServer == null) {
      return null;
    } else {
      return omRatisServer.getServerState();
    }
  }

  /**
   * Get metadata manager.
   *
   * @return metadata manager.
   */
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  public OMMetrics getMetrics() {
    return metrics;
  }

  /**
   * Start service.
   */
  public void start() throws IOException {

    InetSocketAddress omNodeRpcAddr = getOmAddress(configuration);
    int handlerCount = configuration.getInt(OZONE_OM_HANDLER_COUNT_KEY,
        OZONE_OM_HANDLER_COUNT_DEFAULT);
    BlockingService omService = newReflectiveBlockingService(
        new OzoneManagerProtocolServerSideTranslatorPB(this));
    omRpcServer = startRpcServer(configuration, omNodeRpcAddr,
        OzoneManagerProtocolPB.class, omService,
        handlerCount);
    omRpcAddress = updateRPCListenAddress(configuration,
        OZONE_OM_ADDRESS_KEY, omNodeRpcAddr, omRpcServer);
    omRpcServer.start();

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    boolean omRatisEnabled = configuration.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_DEFAULT);
    // This is a temporary check. Once fully implemented, all OM state change
    // should go through Ratis, either standalone (for non-HA) or replicated
    // (for HA).
    if (omRatisEnabled) {
      omRatisServer = OzoneManagerRatisServer.newOMRatisServer(
          omStorage.getOmId(), configuration);
      omRatisServer.start();

      LOG.info("OzoneManager Ratis server started at port {}",
          omRatisServer.getServerPort());
    } else {
      omRatisServer = null;
    }

    DefaultMetricsSystem.initialize("OzoneManager");

    metadataManager.start(configuration);


    // Set metrics and start metrics back ground thread
    metrics.setNumVolumes(metadataManager.countRowsInTable(metadataManager
        .getVolumeTable()));
    metrics.setNumBuckets(metadataManager.countRowsInTable(metadataManager
        .getBucketTable()));

    if (getMetricsStorageFile().exists()) {
      OmMetricsInfo metricsInfo = READER.readValue(getMetricsStorageFile());
      metrics.setNumKeys(metricsInfo.getNumKeys());
    }

    // Schedule save metrics
    long period = configuration.getTimeDuration(OZONE_OM_METRICS_SAVE_INTERVAL,
        OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    scheduleOMMetricsWriteTask = new ScheduleOMMetricsWriteTask();
    metricsTimer = new Timer();
    metricsTimer.schedule(scheduleOMMetricsWriteTask, 0, period);

    keyManager.start(configuration);

    httpServer = new OzoneManagerHttpServer(configuration, this);
    httpServer.start();
    registerMXBean();

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
      // Cancel the metrics timer and set to null.
      metricsTimer.cancel();
      metricsTimer = null;
      scheduleOMMetricsWriteTask = null;
      omRpcServer.stop();
      if (omRatisServer != null) {
        omRatisServer.stop();
      }
      keyManager.stop();
      httpServer.stop();
      metadataManager.stop();
      metrics.unRegister();
      unregisterMXBean();
      if (jvmPauseMonitor != null) {
        jvmPauseMonitor.stop();
      }
    } catch (Exception e) {
      LOG.error("OzoneManager stop failed.", e);
    }
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      omRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during OzoneManager join.", e);
    }
  }

  /**
   * Creates a volume.
   *
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  @Override
  public void createVolume(OmVolumeArgs args) throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.VOLUME, StoreType.OZONE,
            ACLType.CREATE, args.getVolume(), null, null);
      }
      metrics.incNumVolumeCreates();
      volumeManager.createVolume(args);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.CREATE_VOLUME,
          (args == null) ? null : args.toAuditMap()));
      metrics.incNumVolumes();
    } catch (Exception ex) {
      metrics.incNumVolumeCreateFails();
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(OMAction.CREATE_VOLUME,
              (args == null) ? null : args.toAuditMap(), ex)
      );
      throw ex;
    }
  }

  /**
   * Checks if current caller has acl permissions.
   *
   * @param resType - Type of ozone resource. Ex volume, bucket.
   * @param store   - Store type. i.e Ozone, S3.
   * @param acl     - type of access to be checked.
   * @param vol     - name of volume
   * @param bucket  - bucket name
   * @param key     - key
   * @throws OzoneAclException
   */
  private void checkAcls(ResourceType resType, StoreType store,
      ACLType acl, String vol, String bucket, String key)
      throws OzoneAclException {
    if(!isAclEnabled) {
      return;
    }

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(resType)
        .setStoreType(store)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setKeyName(key).build();
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    RequestContext context = RequestContext.newBuilder()
        .setClientUgi(user)
        .setIp(ProtobufRpcEngine.Server.getRemoteIp())
        .setAclType(ACLIdentityType.USER)
        .setAclRights(acl)
        .build();
    if (!accessAuthorizer.checkAccess(obj, context)) {
      LOG.warn("User {} doesn't have {} permission to access {}",
          user.getUserName(), acl, resType);
      throw new OzoneAclException("User " + user.getUserName() + " doesn't " +
          "have " + acl + " permission to access " + resType,
          ErrorCode.PERMISSION_DENIED);
    }
  }

  /**
   * Changes the owner of a volume.
   *
   * @param volume - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  @Override
  public void setOwner(String volume, String owner) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.WRITE_ACL, volume,
          null, null);
    }
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.OWNER, owner);
    try {
      metrics.incNumVolumeUpdates();
      volumeManager.setOwner(volume, owner);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.SET_OWNER,
          auditMap));
    } catch (Exception ex) {
      metrics.incNumVolumeUpdateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.SET_OWNER,
          auditMap, ex)
      );
      throw ex;
    }
  }

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  @Override
  public void setQuota(String volume, long quota) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.WRITE, volume,
          null, null);
    }

    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.QUOTA, String.valueOf(quota));
    try {
      metrics.incNumVolumeUpdates();
      volumeManager.setQuota(volume, quota);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.SET_QUOTA,
          auditMap));
    } catch (Exception ex) {
      metrics.incNumVolumeUpdateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.SET_QUOTA,
          auditMap, ex));
      throw ex;
    }
  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume, false
   * otherwise
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE,
          ACLType.READ, volume, null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.USER_ACL,
        (userAcl == null) ? null : userAcl.getName());
    try {
      metrics.incNumVolumeCheckAccesses();
      return volumeManager.checkVolumeAccess(volume, userAcl);
    } catch (Exception ex) {
      metrics.incNumVolumeCheckAccessFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          OMAction.CHECK_VOLUME_ACCESS, auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.CHECK_VOLUME_ACCESS, auditMap));
      }
    }
  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.READ, volume,
          null, null);
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    try {
      metrics.incNumVolumeInfos();
      return volumeManager.getVolumeInfo(volume);
    } catch (Exception ex) {
      metrics.incNumVolumeInfoFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_VOLUME,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_VOLUME,
            auditMap));
      }
    }
  }

  /**
   * Deletes an existing empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.DELETE, volume,
            null, null);
      }
      metrics.incNumVolumeDeletes();
      volumeManager.deleteVolume(volume);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.DELETE_VOLUME,
          buildAuditMap(volume)));
      metrics.decNumVolumes();
    } catch (Exception ex) {
      metrics.incNumVolumeDeleteFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.DELETE_VOLUME,
          buildAuditMap(volume), ex));
      throw ex;
    }
  }

  /**
   * Lists volume owned by a specific user.
   *
   * @param userName - user name
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<OmVolumeArgs> listVolumeByUser(String userName, String prefix,
      String prevKey, int maxKeys) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.LIST, prefix,
          null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.PREV_KEY, prevKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.USERNAME, userName);
    try {
      metrics.incNumVolumeLists();
      return volumeManager.listVolumes(userName, prefix, prevKey, maxKeys);
    } catch (Exception ex) {
      metrics.incNumVolumeListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_VOLUMES,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_VOLUMES,
            auditMap));
      }
    }
  }

  /**
   * Lists volume all volumes in the cluster.
   *
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<OmVolumeArgs> listAllVolumes(String prefix, String prevKey, int
      maxKeys) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.LIST, prefix,
          null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.PREV_KEY, prevKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.USERNAME, null);
    try {
      metrics.incNumVolumeLists();
      return volumeManager.listVolumes(null, prefix, prevKey, maxKeys);
    } catch (Exception ex) {
      metrics.incNumVolumeListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_VOLUMES,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_VOLUMES,
            auditMap));
      }
    }
  }

  /**
   * Creates a bucket.
   *
   * @param bucketInfo - BucketInfo to create bucket.
   * @throws IOException
   */
  @Override
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.CREATE,
            bucketInfo.getVolumeName(), bucketInfo.getBucketName(), null);
      }
      metrics.incNumBucketCreates();
      bucketManager.createBucket(bucketInfo);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.CREATE_BUCKET,
          (bucketInfo == null) ? null : bucketInfo.toAuditMap()));
      metrics.incNumBuckets();
    } catch (Exception ex) {
      metrics.incNumBucketCreateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.CREATE_BUCKET,
          (bucketInfo == null) ? null : bucketInfo.toAuditMap(), ex));
      throw ex;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmBucketInfo> listBuckets(String volumeName,
      String startKey, String prefix, int maxNumOfBuckets)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.LIST, volumeName,
          null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_NUM_OF_BUCKETS,
        String.valueOf(maxNumOfBuckets));
    try {
      metrics.incNumBucketLists();
      return bucketManager.listBuckets(volumeName,
          startKey, prefix, maxNumOfBuckets);
    } catch (IOException ex) {
      metrics.incNumBucketListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_BUCKETS,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_BUCKETS,
            auditMap));
      }
    }
  }

  /**
   * Gets the bucket information.
   *
   * @param volume - Volume name.
   * @param bucket - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmBucketInfo getBucketInfo(String volume, String bucket)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.READ, volume,
          bucket, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.BUCKET, bucket);
    try {
      metrics.incNumBucketInfos();
      return bucketManager.getBucketInfo(volume, bucket);
    } catch (Exception ex) {
      metrics.incNumBucketInfoFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_BUCKET,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_BUCKET,
            auditMap));
      }
    }
  }

  /**
   * Allocate a key.
   *
   * @param args - attributes of the key.
   * @return OmKeyInfo - the info about the allocated key.
   * @throws IOException
   */
  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumKeyAllocates();
      return keyManager.openKey(args);
    } catch (Exception ex) {
      metrics.incNumKeyAllocateFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.ALLOCATE_KEY,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.ALLOCATE_KEY, (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientID)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    Map<String, String> auditMap = (args == null) ? new LinkedHashMap<>() :
        args.toAuditMap();
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));
    try {
      metrics.incNumKeyCommits();
      keyManager.commitKey(args, clientID);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.COMMIT_KEY,
          auditMap));
      // As when we commit the key it is visible, so we should increment here.
      // As key also can have multiple versions, we need to increment keys
      // only if version is 0. Currently we have not complete support of
      // versioning of keys. So, this can be revisited later.
      if (args != null && args.getLocationInfoList() != null &&
          args.getLocationInfoList().size() > 0 &&
          args.getLocationInfoList().get(0) != null &&
          args.getLocationInfoList().get(0).getCreateVersion() == 0) {
        metrics.incNumKeys();
      }
    } catch (Exception ex) {
      metrics.incNumKeyCommitFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.COMMIT_KEY,
          auditMap, ex));
      throw ex;
    }
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = (args == null) ? new LinkedHashMap<>() :
        args.toAuditMap();
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));
    try {
      metrics.incNumBlockAllocateCalls();
      return keyManager.allocateBlock(args, clientID);
    } catch (Exception ex) {
      metrics.incNumBlockAllocateCallFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.ALLOCATE_BLOCK,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.ALLOCATE_BLOCK, auditMap));
      }
    }
  }

  /**
   * Lookup a key.
   *
   * @param args - attributes of the key.
   * @return OmKeyInfo - the info about the requested key.
   * @throws IOException
   */
  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumKeyLookups();
      return keyManager.lookupKey(args);
    } catch (Exception ex) {
      metrics.incNumKeyLookupFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
            (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  @Override
  public void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    Map<String, String> auditMap = (args == null) ? new LinkedHashMap<>() :
        args.toAuditMap();
    auditMap.put(OzoneConsts.TO_KEY_NAME, toKeyName);
    try {
      metrics.incNumKeyRenames();
      keyManager.renameKey(args, toKeyName);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.RENAME_KEY,
          auditMap));
    } catch (IOException e) {
      metrics.incNumKeyRenameFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.RENAME_KEY,
          auditMap, e));
      throw e;
    }
  }

  /**
   * Deletes an existing key.
   *
   * @param args - attributes of the key.
   * @throws IOException
   */
  @Override
  public void deleteKey(OmKeyArgs args) throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.DELETE,
            args.getVolumeName(), args.getBucketName(), args.getKeyName());
      }
      metrics.incNumKeyDeletes();
      keyManager.deleteKey(args);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.DELETE_KEY,
          (args == null) ? null : args.toAuditMap()));
      metrics.decNumKeys();
    } catch (Exception ex) {
      metrics.incNumKeyDeleteFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.DELETE_KEY,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    }
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.LIST, volumeName,
          bucketName, keyPrefix);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.KEY_PREFIX, keyPrefix);
    try {
      metrics.incNumKeyLists();
      return keyManager.listKeys(volumeName, bucketName,
          startKey, keyPrefix, maxKeys);
    } catch (IOException ex) {
      metrics.incNumKeyListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_KEYS,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_KEYS,
            auditMap));
      }
    }
  }

  /**
   * Sets bucket property from args.
   *
   * @param args - BucketArgs.
   * @throws IOException
   */
  @Override
  public void setBucketProperty(OmBucketArgs args)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), null);
    }
    try {
      metrics.incNumBucketUpdates();
      bucketManager.setBucketProperty(args);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.UPDATE_BUCKET,
          (args == null) ? null : args.toAuditMap()));
    } catch (Exception ex) {
      metrics.incNumBucketUpdateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.UPDATE_BUCKET,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    }
  }

  /**
   * Deletes an existing empty bucket from volume.
   *
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  @Override
  public void deleteBucket(String volume, String bucket) throws IOException {
    checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE, volume,
        bucket, null);
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.BUCKET, bucket);
    try {
      metrics.incNumBucketDeletes();
      bucketManager.deleteBucket(volume, bucket);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.DELETE_BUCKET,
          auditMap));
      metrics.decNumBuckets();
    } catch (Exception ex) {
      metrics.incNumBucketDeleteFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.DELETE_BUCKET,
          auditMap, ex));
      throw ex;
    }
  }

  private Map<String, String> buildAuditMap(String volume){
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {
    return new AuditMessage.Builder()
        .setUser((Server.getRemoteUser() == null) ? null :
            Server.getRemoteUser().getUserName())
        .atIp((Server.getRemoteIp() == null) ? null :
            Server.getRemoteIp().getHostAddress())
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS.toString())
        .withException(null)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {
    return new AuditMessage.Builder()
        .setUser((Server.getRemoteUser() == null) ? null :
            Server.getRemoteUser().getUserName())
        .atIp((Server.getRemoteIp() == null) ? null :
            Server.getRemoteIp().getHostAddress())
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE.toString())
        .withException(throwable)
        .build();
  }

  private void registerMXBean() {
    Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.omInfoBeanName = HddsUtils.registerWithJmxProperties(
        "OzoneManager", "OzoneManagerInfo", jmxProperties, this);
  }

  private void unregisterMXBean() {
    if (this.omInfoBeanName != null) {
      MBeans.unregister(this.omInfoBeanName);
      this.omInfoBeanName = null;
    }
  }

  @Override
  public String getRpcPort() {
    return "" + omRpcAddress.getPort();
  }

  @VisibleForTesting
  public OzoneManagerHttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public List<ServiceInfo> getServiceList() throws IOException {
    // When we implement multi-home this call has to be handled properly.
    List<ServiceInfo> services = new ArrayList<>();
    ServiceInfo.Builder omServiceInfoBuilder = ServiceInfo.newBuilder()
        .setNodeType(HddsProtos.NodeType.OM)
        .setHostname(omRpcAddress.getHostName())
        .addServicePort(ServicePort.newBuilder()
            .setType(ServicePort.Type.RPC)
            .setValue(omRpcAddress.getPort())
            .build());
    if (httpServer.getHttpAddress() != null) {
      omServiceInfoBuilder.addServicePort(ServicePort.newBuilder()
          .setType(ServicePort.Type.HTTP)
          .setValue(httpServer.getHttpAddress().getPort())
          .build());
    }
    if (httpServer.getHttpsAddress() != null) {
      omServiceInfoBuilder.addServicePort(ServicePort.newBuilder()
          .setType(ServicePort.Type.HTTPS)
          .setValue(httpServer.getHttpsAddress().getPort())
          .build());
    }
    services.add(omServiceInfoBuilder.build());

    // For client we have to return SCM with container protocol port,
    // not block protocol.
    InetSocketAddress scmAddr = getScmAddressForClients(
        configuration);
    ServiceInfo.Builder scmServiceInfoBuilder = ServiceInfo.newBuilder()
        .setNodeType(HddsProtos.NodeType.SCM)
        .setHostname(scmAddr.getHostName())
        .addServicePort(ServicePort.newBuilder()
            .setType(ServicePort.Type.RPC)
            .setValue(scmAddr.getPort()).build());
    services.add(scmServiceInfoBuilder.build());

    List<HddsProtos.Node> nodes = scmContainerClient.queryNode(HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "");

    for (HddsProtos.Node node : nodes) {
      HddsProtos.DatanodeDetailsProto datanode = node.getNodeID();

      ServiceInfo.Builder dnServiceInfoBuilder = ServiceInfo.newBuilder()
          .setNodeType(HddsProtos.NodeType.DATANODE)
          .setHostname(datanode.getHostName());

      if(DatanodeDetails.getFromProtoBuf(datanode)
          .getPort(DatanodeDetails.Port.Name.REST) != null) {
        dnServiceInfoBuilder.addServicePort(ServicePort.newBuilder()
            .setType(ServicePort.Type.HTTP)
            .setValue(DatanodeDetails.getFromProtoBuf(datanode)
                .getPort(DatanodeDetails.Port.Name.REST).getValue())
            .build());
      }

      services.add(dnServiceInfoBuilder.build());
    }

    metrics.incNumGetServiceLists();
    // For now there is no exception that can can happen in this call,
    // so failure metrics is not handled. In future if there is any need to
    // handle exception in this method, we need to incorporate
    // metrics.incNumGetServiceListFails()
    return services;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void createS3Bucket(String userName, String s3BucketName)
      throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.BUCKET, StoreType.S3, ACLType.CREATE,
            null, s3BucketName, null);
      }
      metrics.incNumBucketCreates();
      try {
        boolean newVolumeCreate = s3BucketManager.createOzoneVolumeIfNeeded(
            userName);
        if (newVolumeCreate) {
          metrics.incNumVolumeCreates();
          metrics.incNumVolumes();
        }
      } catch (IOException ex) {
        // We need to increment volume creates also because this is first
        // time we are trying to create a volume, it failed. As we increment
        // ops and create when we try to do that operation.
        metrics.incNumVolumeCreates();
        metrics.incNumVolumeCreateFails();
        throw ex;
      }

      s3BucketManager.createS3Bucket(userName, s3BucketName);
      metrics.incNumBuckets();
    } catch (IOException ex) {
      metrics.incNumBucketCreateFails();
      throw ex;
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void deleteS3Bucket(String s3BucketName) throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.BUCKET, StoreType.S3, ACLType.DELETE, null,
            s3BucketName, null);
      }
      metrics.incNumBucketDeletes();
      s3BucketManager.deleteS3Bucket(s3BucketName);
      metrics.decNumBuckets();
    } catch (IOException ex) {
      metrics.incNumBucketDeleteFails();
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public String getOzoneBucketMapping(String s3BucketName)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.S3, ACLType.READ,
          null, s3BucketName, null);
    }
    return s3BucketManager.getOzoneBucketMapping(s3BucketName);
  }

  @Override
  public List<OmBucketInfo> listS3Buckets(String userName, String startKey,
                                          String prefix, int maxNumOfBuckets)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.S3, ACLType.LIST,
          s3BucketManager.getOzoneVolumeNameForUser(userName), null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(userName);
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_NUM_OF_BUCKETS,
        String.valueOf(maxNumOfBuckets));
    try {
      metrics.incNumListS3Buckets();
      String volumeName = s3BucketManager.getOzoneVolumeNameForUser(userName);
      return bucketManager.listBuckets(volumeName, startKey, prefix,
          maxNumOfBuckets);
    } catch (IOException ex) {
      metrics.incNumListS3BucketsFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_S3BUCKETS,
          auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction
                .LIST_S3BUCKETS, auditMap));
      }
    }
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(OmKeyArgs keyArgs) throws
      IOException {
    OmMultipartInfo multipartInfo;
    metrics.incNumInitiateMultipartUploads();
    try {
      multipartInfo = keyManager.initiateMultipartUpload(keyArgs);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          OMAction.INITIATE_MULTIPART_UPLOAD, (keyArgs == null) ? null :
              keyArgs.toAuditMap()));
    } catch (IOException ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          OMAction.INITIATE_MULTIPART_UPLOAD,
          (keyArgs == null) ? null : keyArgs.toAuditMap(), ex));
      metrics.incNumInitiateMultipartUploadFails();
      throw ex;
    }
    return multipartInfo;
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs keyArgs, long clientID) throws IOException {
    boolean auditSuccess = false;
    OmMultipartCommitUploadPartInfo commitUploadPartInfo;
    metrics.incNumCommitMultipartUploadParts();
    try {
      commitUploadPartInfo = keyManager.commitMultipartUploadPart(keyArgs,
          clientID);
      auditSuccess = true;
    } catch (IOException ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
              .INITIATE_MULTIPART_UPLOAD, (keyArgs == null) ? null : keyArgs
          .toAuditMap(), ex));
      metrics.incNumCommitMultipartUploadPartFails();
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.COMMIT_MULTIPART_UPLOAD_PARTKEY, (keyArgs == null) ? null :
                keyArgs.toAuditMap()));
      }
    }
    return commitUploadPartInfo;
  }




  /**
   * Startup options.
   */
  public enum StartupOption {
    INIT("--init"),
    HELP("--help"),
    REGULAR("--regular");

    private final String name;

    StartupOption(String arg) {
      this.name = arg;
    }

    public static StartupOption parse(String value) {
      for (StartupOption option : StartupOption.values()) {
        if (option.name.equalsIgnoreCase(value)) {
          return option;
        }
      }
      return null;
    }

    public String getName() {
      return name;
    }
  }

  public static  Logger getLogger() {
    return LOG;
  }
}
