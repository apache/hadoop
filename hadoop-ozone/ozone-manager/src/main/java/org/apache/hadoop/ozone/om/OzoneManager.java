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

import java.net.InetAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Objects;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerServerProtocol;
import org.apache.hadoop.ozone.om.snapshot.OzoneManagerSnapshotProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.security.OzoneSecurityException;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
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
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisClient;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.KMSUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.utils.RetriableTask;
import org.apache.hadoop.utils.db.DBUpdatesWrapper;
import org.apache.hadoop.utils.db.SequenceNumberNotFoundException;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForBlockClients;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForSecurityProtocol;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_TEMP_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_INDEX;
import static org.apache.hadoop.ozone.OzoneConsts.RPC_PORT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODE_ID_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneManagerService
    .newReflectiveBlockingService;

/**
 * Ozone Manager is the metadata manager of ozone.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public final class OzoneManager extends ServiceRuntimeInfoImpl
    implements OzoneManagerServerProtocol, OMMXBean, Auditor {
  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private static final String OM_DAEMON = "om";
  private static boolean securityEnabled = false;
  private OzoneDelegationTokenSecretManager delegationTokenMgr;
  private OzoneBlockTokenSecretManager blockTokenMgr;
  private CertificateClient certClient;
  private static boolean testSecureOmFlag = false;
  private final Text omRpcAddressTxt;
  private final OzoneConfiguration configuration;
  private RPC.Server omRpcServer;
  private InetSocketAddress omRpcAddress;
  private String omId;

  private OMMetadataManager metadataManager;
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManagerImpl prefixManager;
  private S3BucketManager s3BucketManager;

  private final OMMetrics metrics;
  private OzoneManagerHttpServer httpServer;
  private final OMStorage omStorage;
  private final ScmBlockLocationProtocol scmBlockClient;
  private final StorageContainerLocationProtocol scmContainerClient;
  private ObjectName omInfoBeanName;
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
  private IAccessAuthorizer accessAuthorizer;
  private JvmPauseMonitor jvmPauseMonitor;
  private final SecurityConfig secConfig;
  private S3SecretManager s3SecretManager;
  private volatile boolean isOmRpcServerRunning = false;
  private String omComponent;
  private OzoneManagerProtocolServerSideTranslatorPB omServerProtocol;

  private boolean isRatisEnabled;
  private OzoneManagerRatisServer omRatisServer;
  private OzoneManagerRatisClient omRatisClient;
  private OzoneManagerSnapshotProvider omSnapshotProvider;
  private OMNodeDetails omNodeDetails;
  private List<OMNodeDetails> peerNodes;
  private File omRatisSnapshotDir;
  private final File ratisSnapshotFile;
  private long snapshotIndex;
  private final Collection<String> ozAdmins;

  private KeyProviderCryptoExtension kmsProvider = null;
  private static String keyProviderUriKeyName =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;

  // Adding parameters needed for VolumeRequests here, so that during request
  // execution, we can get from ozoneManager.
  private long maxUserVolumeCount;


  private final ScmClient scmClient;
  private final long scmBlockSize;
  private final int preallocateBlocksMax;
  private final boolean grpcBlockTokenEnabled;
  private final boolean useRatisForReplication;


  private OzoneManager(OzoneConfiguration conf) throws IOException,
      AuthenticationException {
    super(OzoneVersionInfo.OZONE_VERSION_INFO);
    Preconditions.checkNotNull(conf);
    configuration = conf;
    this.maxUserVolumeCount = conf.getInt(OZONE_OM_USER_MAX_VOLUME,
        OZONE_OM_USER_MAX_VOLUME_DEFAULT);
    Preconditions.checkArgument(this.maxUserVolumeCount > 0,
        OZONE_OM_USER_MAX_VOLUME + " value should be greater than zero");
    omStorage = new OMStorage(conf);
    omId = omStorage.getOmId();
    if (omStorage.getState() != StorageState.INITIALIZED) {
      throw new OMException("OM not initialized.",
          ResultCodes.OM_NOT_INITIALIZED);
    }

    // Read configuration and set values.
    ozAdmins = conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS);
    omMetaDir = OmUtils.getOmDbDir(configuration);
    this.isAclEnabled = conf.getBoolean(OZONE_ACL_ENABLED,
        OZONE_ACL_ENABLED_DEFAULT);
    this.scmBlockSize = (long) conf.getStorageSize(OZONE_SCM_BLOCK_SIZE,
        OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    this.preallocateBlocksMax = conf.getInt(
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX,
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
    this.grpcBlockTokenEnabled = conf.getBoolean(HDDS_BLOCK_TOKEN_ENABLED,
        HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
    this.useRatisForReplication = conf.getBoolean(
        DFS_CONTAINER_RATIS_ENABLED_KEY, DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    // TODO: This is a temporary check. Once fully implemented, all OM state
    //  change should go through Ratis - be it standalone (for non-HA) or
    //  replicated (for HA).
    isRatisEnabled = configuration.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_DEFAULT);

    // Load HA related configurations
    loadOMHAConfigs(configuration);
    InetSocketAddress omNodeRpcAddr = omNodeDetails.getRpcAddress();
    omRpcAddressTxt = new Text(omNodeDetails.getRpcAddressString());

    scmContainerClient = getScmContainerClient(configuration);
    // verifies that the SCM info in the OM Version file is correct.
    scmBlockClient = getScmBlockClient(configuration);
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient);

    // For testing purpose only, not hit scm from om as Hadoop UGI can't login
    // two principals in the same JVM.
    if (!testSecureOmFlag) {
      ScmInfo scmInfo = getScmInfo(configuration);
      if (!(scmInfo.getClusterId().equals(omStorage.getClusterID()) && scmInfo
          .getScmId().equals(omStorage.getScmId()))) {
        throw new OMException("SCM version info mismatch.",
            ResultCodes.SCM_VERSION_MISMATCH_ERROR);
      }
    }

    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    secConfig = new SecurityConfig(configuration);
    // Create the KMS Key Provider
    try {
      kmsProvider = createKeyProviderExt(configuration);
    } catch (IOException ioe) {
      kmsProvider = null;
      LOG.error("Fail to create Key Provider");
    }
    if (secConfig.isSecurityEnabled()) {
      omComponent = OM_DAEMON + "-" + omId;
      if(omStorage.getOmCertSerialId() == null) {
        throw new RuntimeException("OzoneManager started in secure mode but " +
            "doesn't have SCM signed certificate.");
      }
      certClient = new OMCertificateClient(new SecurityConfig(conf),
          omStorage.getOmCertSerialId());
    }
    if (secConfig.isBlockTokenEnabled()) {
      blockTokenMgr = createBlockTokenSecretManager(configuration);
    }

    instantiateServices();

    initializeRatisServer();
    initializeRatisClient();

    if (isRatisEnabled) {
      // Create Ratis storage dir
      String omRatisDirectory = OmUtils.getOMRatisDirectory(configuration);
      if (omRatisDirectory == null || omRatisDirectory.isEmpty()) {
        throw new IllegalArgumentException(HddsConfigKeys.OZONE_METADATA_DIRS +
            " must be defined.");
      }
      OmUtils.createOMDir(omRatisDirectory);
      // Create Ratis snapshot dir
      omRatisSnapshotDir = OmUtils.createOMDir(
          OmUtils.getOMRatisSnapshotDirectory(configuration));

      if (peerNodes != null && !peerNodes.isEmpty()) {
        this.omSnapshotProvider = new OzoneManagerSnapshotProvider(
            configuration, omRatisSnapshotDir, peerNodes);
      }
    }

    this.ratisSnapshotFile = new File(omStorage.getCurrentDir(),
        OM_RATIS_SNAPSHOT_INDEX);
    this.snapshotIndex = loadRatisSnapshotIndex();

    metrics = OMMetrics.create();

    // Start Om Rpc Server.
    omRpcServer = getRpcServer(conf);
    omRpcAddress = updateRPCListenAddress(configuration,
        OZONE_OM_ADDRESS_KEY, omNodeRpcAddr, omRpcServer);

    shutdownHook = () -> {
      saveOmMetrics();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Instantiate services which are dependent on the OM DB state.
   * When OM state is reloaded, these services are re-initialized with the
   * new OM state.
   */
  private void instantiateServices() throws IOException {

    metadataManager = new OmMetadataManagerImpl(configuration);
    volumeManager = new VolumeManagerImpl(metadataManager, configuration);
    bucketManager = new BucketManagerImpl(metadataManager, getKmsProvider(),
        isRatisEnabled);
    s3BucketManager = new S3BucketManagerImpl(configuration, metadataManager,
        volumeManager, bucketManager);
    if (secConfig.isSecurityEnabled()) {
      s3SecretManager = new S3SecretManagerImpl(configuration, metadataManager);
      delegationTokenMgr = createDelegationTokenSecretManager(configuration);
    }

    prefixManager = new PrefixManagerImpl(metadataManager);
    keyManager = new KeyManagerImpl(this, scmClient, configuration,
        omStorage.getOmId());

    if (isAclEnabled) {
      accessAuthorizer = getACLAuthorizerInstance(configuration);
      if (accessAuthorizer instanceof OzoneNativeAuthorizer) {
        OzoneNativeAuthorizer authorizer =
            (OzoneNativeAuthorizer) accessAuthorizer;
        authorizer.setVolumeManager(volumeManager);
        authorizer.setBucketManager(bucketManager);
        authorizer.setKeyManager(keyManager);
        authorizer.setPrefixManager(prefixManager);
      }
    } else {
      accessAuthorizer = null;
    }
  }

  /**
   * Return configuration value of
   * {@link OzoneConfigKeys#DFS_CONTAINER_RATIS_ENABLED_KEY}.
   */
  public boolean shouldUseRatis() {
    return useRatisForReplication;
  }

  /**
   * Return scmClient.
   */
  public ScmClient getScmClient() {
    return scmClient;
  }

  /**
   * Return SecretManager for OM.
   */
  public OzoneBlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenMgr;
  }

  /**
   * Return config value of {@link OzoneConfigKeys#OZONE_SCM_BLOCK_SIZE}.
   */
  public long getScmBlockSize() {
    return scmBlockSize;
  }

  /**
   * Return config value of
   * {@link OzoneConfigKeys#OZONE_KEY_PREALLOCATION_BLOCKS_MAX}.
   */
  public int getPreallocateBlocksMax() {
    return preallocateBlocksMax;
  }

  /**
   * Return config value of
   * {@link HddsConfigKeys#HDDS_BLOCK_TOKEN_ENABLED}.
   */
  public boolean isGrpcBlockTokenEnabled() {
    return grpcBlockTokenEnabled;
  }

  /**
   * Inspects and loads OM node configurations.
   *
   * If {@link OMConfigKeys#OZONE_OM_SERVICE_IDS_KEY} is configured with
   * multiple ids and/ or if {@link OMConfigKeys#OZONE_OM_NODE_ID_KEY} is not
   * specifically configured , this method determines the omServiceId
   * and omNodeId by matching the node's address with the configured
   * addresses. When a match is found, it sets the omServicId and omNodeId from
   * the corresponding configuration key. This method also finds the OM peers
   * nodes belonging to the same OM service.
   *
   * @param conf
   */
  private void loadOMHAConfigs(Configuration conf) {
    InetSocketAddress localRpcAddress = null;
    String localOMServiceId = null;
    String localOMNodeId = null;
    int localRatisPort = 0;
    Collection<String> omServiceIds = conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY);

    String knownOMNodeId = conf.get(OZONE_OM_NODE_ID_KEY);
    int found = 0;
    boolean isOMAddressSet = false;

    for (String serviceId : OmUtils.emptyAsSingletonNull(omServiceIds)) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(conf, serviceId);

      List<OMNodeDetails> peerNodesList = new ArrayList<>();
      boolean isPeer = false;
      for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {
        if (knownOMNodeId != null && !knownOMNodeId.equals(nodeId)) {
          isPeer = true;
        } else {
          isPeer = false;
        }
        String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);
        String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
        if (rpcAddrStr == null) {
          continue;
        }

        // If OM address is set for any node id, we will not fallback to the
        // default
        isOMAddressSet = true;

        String ratisPortKey = OmUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
            serviceId, nodeId);
        int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

        InetSocketAddress addr = null;
        try {
          addr = NetUtils.createSocketAddr(rpcAddrStr);
        } catch (Exception e) {
          LOG.warn("Exception in creating socket address " + addr, e);
          continue;
        }
        if (!addr.isUnresolved()) {
          if (!isPeer && OmUtils.isAddressLocal(addr)) {
            localRpcAddress = addr;
            localOMServiceId = serviceId;
            localOMNodeId = nodeId;
            localRatisPort = ratisPort;
            found++;
          } else {
            // This OMNode belongs to same OM service as the current OMNode.
            // Add it to peerNodes list.
            String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
                serviceId, nodeId, addr.getHostName());
            String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
                serviceId, nodeId, addr.getHostName());
            OMNodeDetails peerNodeInfo = new OMNodeDetails.Builder()
                .setOMServiceId(serviceId)
                .setOMNodeId(nodeId)
                .setRpcAddress(addr)
                .setRatisPort(ratisPort)
                .setHttpAddress(httpAddr)
                .setHttpsAddress(httpsAddr)
                .build();
            peerNodesList.add(peerNodeInfo);
          }
        }
      }
      if (found == 1) {
        LOG.debug("Found one matching OM address with service ID: {} and node" +
                " ID: {}", localOMServiceId, localOMNodeId);

        setOMNodeDetails(localOMServiceId, localOMNodeId, localRpcAddress,
            localRatisPort);

        this.peerNodes = peerNodesList;

        LOG.info("Found matching OM address with OMServiceId: {}, " +
            "OMNodeId: {}, RPC Address: {} and Ratis port: {}",
            localOMServiceId, localOMNodeId,
            NetUtils.getHostPortString(localRpcAddress), localRatisPort);
        return;
      } else if (found > 1) {
        String msg = "Configuration has multiple " + OZONE_OM_ADDRESS_KEY +
            " addresses that match local node's address. Please configure the" +
            " system with " + OZONE_OM_SERVICE_IDS_KEY + " and " +
            OZONE_OM_ADDRESS_KEY;
        throw new OzoneIllegalArgumentException(msg);
      }
    }

    if (!isOMAddressSet) {
      // No OM address is set. Fallback to default
      InetSocketAddress omAddress = OmUtils.getOmAddress(conf);
      int ratisPort = conf.getInt(OZONE_OM_RATIS_PORT_KEY,
          OZONE_OM_RATIS_PORT_DEFAULT);

      LOG.info("Configuration either no {} set. Falling back to the default " +
          "OM address {}", OZONE_OM_ADDRESS_KEY, omAddress);

      setOMNodeDetails(null, null, omAddress, ratisPort);

    } else {
      String msg = "Configuration has no " + OZONE_OM_ADDRESS_KEY + " " +
          "address that matches local node's address. Please configure the " +
          "system with " + OZONE_OM_ADDRESS_KEY;
      LOG.info(msg);
      throw new OzoneIllegalArgumentException(msg);
    }
  }

  /**
   * Builds and sets OMNodeDetails object.
   */
  private void setOMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddress, int ratisPort) {

    if (serviceId == null) {
      // If no serviceId is set, take the default serviceID om-service
      serviceId = OzoneConsts.OM_SERVICE_ID_DEFAULT;
      LOG.info("OM Service ID is not set. Setting it to the default ID: {}",
          serviceId);
    }
    if (nodeId == null) {
      // If no nodeId is set, take the omId from omStorage as the nodeID
      nodeId = omId;
      LOG.info("OM Node ID is not set. Setting it to the OmStorage's " +
          "OmID: {}", nodeId);
    }

    this.omNodeDetails = new OMNodeDetails.Builder()
        .setOMServiceId(serviceId)
        .setOMNodeId(nodeId)
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .build();

    // Set this nodes OZONE_OM_ADDRESS_KEY to the discovered address.
    configuration.set(OZONE_OM_ADDRESS_KEY,
        NetUtils.getHostPortString(rpcAddress));

    // Get and set Http(s) address of local node. If base config keys are
    // not set, check for keys suffixed with OM serivce ID and node ID.
    setOMNodeSpecificConfigs(serviceId, nodeId);
  }

  /**
   * Check if any of the following configuration keys have been set using OM
   * Node ID suffixed to the key. If yes, then set the base key with the
   * configured valued.
   *    1. {@link OMConfigKeys#OZONE_OM_HTTP_ADDRESS_KEY}
   *    2. {@link OMConfigKeys#OZONE_OM_HTTPS_ADDRESS_KEY}
   *    3. {@link OMConfigKeys#OZONE_OM_HTTP_BIND_HOST_KEY}
   *    4. {@link OMConfigKeys#OZONE_OM_HTTPS_BIND_HOST_KEY}
   */
  private void setOMNodeSpecificConfigs(String omServiceId, String omNodeId) {
    String[] confKeys = new String[] {
        OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY,
        OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY,
        OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY,
        OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY};

    for (String confKey : confKeys) {
      String confValue = OmUtils.getConfSuffixedWithOMNodeId(
          configuration, confKey, omServiceId, omNodeId);
      if (confValue != null) {
        LOG.info("Setting configuration key {} with value of key {}: {}",
            confKey, OmUtils.addKeySuffixes(confKey, omNodeId), confValue);
        configuration.set(confKey, confValue);
      }
    }
  }

  private KeyProviderCryptoExtension createKeyProviderExt(
      OzoneConfiguration conf) throws IOException {
    KeyProvider keyProvider = KMSUtil.createKeyProvider(conf,
        keyProviderUriKeyName);
    if (keyProvider == null) {
      return null;
    }
    KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
        .createKeyProviderCryptoExtension(keyProvider);
    return cryptoProvider;
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

  @Override
  public void close() throws IOException {
    stop();
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
      Files.createDirectories(
          getTempMetricsStorageFile().getParentFile().toPath());
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


  private OzoneDelegationTokenSecretManager createDelegationTokenSecretManager(
      OzoneConfiguration conf) throws IOException {
    long tokenRemoverScanInterval =
        conf.getTimeDuration(OMConfigKeys.DELEGATION_REMOVER_SCAN_INTERVAL_KEY,
            OMConfigKeys.DELEGATION_REMOVER_SCAN_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    long tokenMaxLifetime =
        conf.getTimeDuration(OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
            TimeUnit.MILLISECONDS);
    long tokenRenewInterval =
        conf.getTimeDuration(OMConfigKeys.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            OMConfigKeys.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    return new OzoneDelegationTokenSecretManager(conf, tokenMaxLifetime,
        tokenRenewInterval, tokenRemoverScanInterval, omRpcAddressTxt,
        s3SecretManager);
  }

  private OzoneBlockTokenSecretManager createBlockTokenSecretManager(
      OzoneConfiguration conf) {

    long expiryTime = conf.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME,
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    // TODO: Pass OM cert serial ID.
    if (testSecureOmFlag) {
      return new OzoneBlockTokenSecretManager(secConfig, expiryTime, "1");
    }
    Objects.requireNonNull(certClient);
    return new OzoneBlockTokenSecretManager(secConfig, expiryTime,
        certClient.getCertificate().getSerialNumber().toString());
  }

  private void stopSecretManager() {
    if (blockTokenMgr != null) {
      LOG.info("Stopping OM block token manager.");
      try {
        blockTokenMgr.stop();
      } catch (IOException e) {
        LOG.error("Failed to stop block token manager", e);
      }
    }

    if (delegationTokenMgr != null) {
      LOG.info("Stopping OM delegation token secret manager.");
      try {
        delegationTokenMgr.stop();
      } catch (IOException e) {
        LOG.error("Failed to stop delegation token manager", e);
      }
    }
  }

  @VisibleForTesting
  public void startSecretManager() {
    try {
      readKeyPair();
    } catch (OzoneSecurityException e) {
      LOG.error("Unable to read key pair for OM.", e);
      throw new RuntimeException(e);
    }
    if (secConfig.isBlockTokenEnabled() && blockTokenMgr != null) {
      try {
        LOG.info("Starting OM block token secret manager");
        blockTokenMgr.start(certClient);
      } catch (IOException e) {
        // Unable to start secret manager.
        LOG.error("Error starting block token secret manager.", e);
        throw new RuntimeException(e);
      }
    }

    if (delegationTokenMgr != null) {
      try {
        LOG.info("Starting OM delegation token secret manager");
        delegationTokenMgr.start(certClient);
      } catch (IOException e) {
        // Unable to start secret manager.
        LOG.error("Error starting delegation token secret manager.", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * For testing purpose only.
   * */
  public void setCertClient(CertificateClient certClient) {
    // TODO: Initialize it in constructor with implementation for certClient.
    this.certClient = certClient;
  }

  /**
   * Read private key from file.
   */
  private void readKeyPair() throws OzoneSecurityException {
    try {
      LOG.info("Reading keypair and certificate from file system.");
      PublicKey pubKey = certClient.getPublicKey();
      PrivateKey pvtKey = certClient.getPrivateKey();
      Objects.requireNonNull(pubKey);
      Objects.requireNonNull(pvtKey);
      Objects.requireNonNull(certClient.getCertificate());
    } catch (Exception e) {
      throw new OzoneSecurityException("Error reading keypair & certificate "
          + "OzoneManager.", e, OzoneSecurityException
          .ResultCodes.OM_PUBLIC_PRIVATE_KEY_FILE_NOT_EXIST);
    }
  }

  /**
   * Login OM service user if security and Kerberos are enabled.
   *
   * @param  conf
   * @throws IOException, AuthenticationException
   */
  private static void loginOMUser(OzoneConfiguration conf)
      throws IOException, AuthenticationException {

    if (SecurityUtil.getAuthenticationMethod(conf).equals(
        AuthenticationMethod.KERBEROS)) {
      LOG.debug("Ozone security is enabled. Attempting login for OM user. "
              + "Principal: {},keytab: {}", conf.get(
          OZONE_OM_KERBEROS_PRINCIPAL_KEY),
          conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY));

      UserGroupInformation.setConfiguration(conf);

      InetSocketAddress socAddr = OmUtils.getOmAddress(conf);
      SecurityUtil.login(conf, OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
          OZONE_OM_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
    } else {
      throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
          conf) + " authentication method not supported. OM user login "
          + "failed.");
    }
    LOG.info("Ozone Manager login successful.");
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
    return TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
  }

  /**
   * Create a scm security client, used to get SCM signed certificate.
   *
   * @return {@link SCMSecurityProtocol}
   * @throws IOException
   */
  private static SCMSecurityProtocolClientSideTranslatorPB
      getScmSecurityClient(OzoneConfiguration conf) throws IOException {
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress scmSecurityProtoAdd =
        getScmAddressForSecurityProtocol(conf);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        new SCMSecurityProtocolClientSideTranslatorPB(
            RPC.getProxy(SCMSecurityProtocolPB.class, scmVersion,
                scmSecurityProtoAdd, UserGroupInformation.getCurrentUser(),
                conf, NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmSecurityClient;
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
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(StorageContainerLocationProtocolPB.class, scmVersion,
                scmAddr, UserGroupInformation.getCurrentUser(), conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf))),
            StorageContainerLocationProtocol.class, conf);
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
  private RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      int handlerCount) throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(delegationTokenMgr)
        .build();

    DFSUtil.addPBProtocol(conf, protocol, instance, rpcServer);

    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      rpcServer.refreshServiceAcl(conf, OMPolicyProvider.getInstance());
    }
    return rpcServer;
  }

  private static boolean isOzoneSecurityEnabled() {
    return securityEnabled;
  }

  /**
   * Constructs OM instance based on the configuration.
   *
   * @param conf OzoneConfiguration
   * @return OM instance
   * @throws IOException, AuthenticationException in case OM instance
   *   creation fails.
   */
  public static OzoneManager createOm(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    loginOMUserIfSecurityEnabled(conf);
    return new OzoneManager(conf);
  }

  /**
   * Logs in the OM use if security is enabled in the configuration.
   *
   * @param conf OzoneConfiguration
   * @throws IOException, AuthenticationException in case login failes.
   */
  private static void loginOMUserIfSecurityEnabled(OzoneConfiguration  conf)
      throws IOException, AuthenticationException {
    securityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    if (securityEnabled) {
      loginOMUser(conf);
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
  public static boolean omInit(OzoneConfiguration conf) throws IOException,
      AuthenticationException {
    loginOMUserIfSecurityEnabled(conf);
    OMStorage omStorage = new OMStorage(conf);
    StorageState state = omStorage.getState();
    if (state != StorageState.INITIALIZED) {
      try {
        ScmInfo scmInfo = getScmInfo(conf);
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
        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          initializeSecurity(conf, omStorage);
        }
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
      if(OzoneSecurityUtil.isSecurityEnabled(conf) &&
          omStorage.getOmCertSerialId() == null) {
        LOG.info("OM storage is already initialized. Initializing security");
        initializeSecurity(conf, omStorage);
        omStorage.persistCurrentState();
      }
      System.out.println(
          "OM already initialized.Reusing existing cluster id for sd="
              + omStorage.getStorageDir() + ";cid=" + omStorage
              .getClusterID());
      return true;
    }
  }

  /**
   * Initializes secure OzoneManager.
   * */
  @VisibleForTesting
  public static void initializeSecurity(OzoneConfiguration conf,
      OMStorage omStore)
      throws IOException {
    LOG.info("Initializing secure OzoneManager.");

    CertificateClient certClient =
        new OMCertificateClient(new SecurityConfig(conf),
            omStore.getOmCertSerialId());
    CertificateClient.InitResponse response = certClient.init();
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful.");
      break;
    case GETCERT:
      getSCMSignedCert(certClient, conf, omStore);
      LOG.info("Successfully stored SCM signed certificate.");
      break;
    case FAILURE:
      LOG.error("OM security initialization failed.");
      throw new RuntimeException("OM security initialization failed.");
    case RECOVER:
      LOG.error("OM security initialization failed. OM certificate is " +
          "missing.");
      throw new RuntimeException("OM security initialization failed.");
    default:
      LOG.error("OM security initialization failed. Init response: {}",
          response);
      throw new RuntimeException("OM security initialization failed.");
    }
  }

  private static ScmInfo getScmInfo(OzoneConfiguration conf)
      throws IOException {
    try {
      RetryPolicy retryPolicy = retryUpToMaximumCountWithFixedSleep(
          10, 5, TimeUnit.SECONDS);
      RetriableTask<ScmInfo> retriable = new RetriableTask<>(
          retryPolicy, "OM#getScmInfo",
          () -> getScmBlockClient(conf).getScmInfo());
      return retriable.call();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to get SCM info", e);
    }
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
  public OzoneManagerRatisServer getOmRatisServer() {
    return omRatisServer;
  }

  @VisibleForTesting
  public OzoneManagerSnapshotProvider getOmSnapshotProvider() {
    return omSnapshotProvider;
  }

  @VisibleForTesting
  public InetSocketAddress getOmRpcServerAddr() {
    return omRpcAddress;
  }

  @VisibleForTesting
  public LifeCycle.State getOmRatisServerState() {
    if (omRatisServer == null) {
      return null;
    } else {
      return omRatisServer.getServerState();
    }
  }

  @VisibleForTesting
  public KeyProviderCryptoExtension getKmsProvider() {
    return kmsProvider;
  }

  public PrefixManager getPrefixManager() {
    return prefixManager;
  }

  /**
   * Get metadata manager.
   *
   * @return metadata manager.
   */
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  public OzoneBlockTokenSecretManager getBlockTokenMgr() {
    return blockTokenMgr;
  }

  public OzoneManagerProtocolServerSideTranslatorPB getOmServerProtocol() {
    return omServerProtocol;
  }

  public OMMetrics getMetrics() {
    return metrics;
  }

  /**
   * Start service.
   */
  public void start() throws IOException {

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    DefaultMetricsSystem.initialize("OzoneManager");

    // Start Ratis services
    if (omRatisServer != null) {
      omRatisServer.start();
    }
    if (omRatisClient != null) {
      omRatisClient.connect();
    }

    metadataManager.start(configuration);
    startSecretManagerIfNecessary();

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
    omRpcServer.start();
    isOmRpcServerRunning = true;
    try {
      httpServer = new OzoneManagerHttpServer(configuration, this);
      httpServer.start();
    } catch (Exception ex) {
      // Allow OM to start as Http Server failure is not fatal.
      LOG.error("OM HttpServer failed to start.", ex);
    }
    registerMXBean();
    setStartTime();
  }

  /**
   * Restarts the service. This method re-initializes the rpc server.
   */
  public void restart() throws IOException {
    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    HddsUtils.initializeMetrics(configuration, "OzoneManager");

    metadataManager.start(configuration);
    startSecretManagerIfNecessary();

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
    omRpcServer = getRpcServer(configuration);
    omRpcServer.start();
    isOmRpcServerRunning = true;

    initializeRatisServer();
    if (omRatisServer != null) {
      omRatisServer.start();
    }
    initializeRatisClient();
    if (omRatisClient != null) {
      omRatisClient.connect();
    }

    try {
      httpServer = new OzoneManagerHttpServer(configuration, this);
      httpServer.start();
    } catch (Exception ex) {
      // Allow OM to start as Http Server failure is not fatal.
      LOG.error("OM HttpServer failed to start.", ex);
    }
    registerMXBean();

    // Start jvm monitor
    jvmPauseMonitor = new JvmPauseMonitor();
    jvmPauseMonitor.init(configuration);
    jvmPauseMonitor.start();
    setStartTime();
  }

  /**
   * Creates a new instance of rpc server. If an earlier instance is already
   * running then returns the same.
   */
  private RPC.Server getRpcServer(OzoneConfiguration conf) throws IOException {
    if (isOmRpcServerRunning) {
      return omRpcServer;
    }

    InetSocketAddress omNodeRpcAddr = OmUtils.getOmAddress(configuration);

    final int handlerCount = conf.getInt(OZONE_OM_HANDLER_COUNT_KEY,
        OZONE_OM_HANDLER_COUNT_DEFAULT);
    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    this.omServerProtocol = new OzoneManagerProtocolServerSideTranslatorPB(
        this, omRatisServer, isRatisEnabled);

    BlockingService omService = newReflectiveBlockingService(omServerProtocol);

    return startRpcServer(configuration, omNodeRpcAddr,
        OzoneManagerProtocolPB.class, omService,
        handlerCount);
  }

  /**
   * Creates an instance of ratis server.
   */
  private void initializeRatisServer() throws IOException {
    if (isRatisEnabled) {
      if (omRatisServer == null) {
        omRatisServer = OzoneManagerRatisServer.newOMRatisServer(
            configuration, this, omNodeDetails, peerNodes);
      }
      LOG.info("OzoneManager Ratis server initialized at port {}",
          omRatisServer.getServerPort());
    } else {
      omRatisServer = null;
    }
  }

  /**
   * Creates an instance of ratis client.
   */
  private void initializeRatisClient() throws IOException {
    if (isRatisEnabled) {
      if (omRatisClient == null) {
        omRatisClient = OzoneManagerRatisClient.newOzoneManagerRatisClient(
            omNodeDetails.getOMNodeId(), omRatisServer.getRaftGroup(),
            configuration);
      }
    } else {
      omRatisClient = null;
    }
  }

  @VisibleForTesting
  public long loadRatisSnapshotIndex() {
    if (ratisSnapshotFile.exists()) {
      try {
        return PersistentLongFile.readFile(ratisSnapshotFile, 0);
      } catch (IOException e) {
        LOG.error("Unable to read the ratis snapshot index (last applied " +
            "transaction log index)", e);
      }
    }
    return 0;
  }

  @Override
  public long saveRatisSnapshot(boolean flush) throws IOException {
    snapshotIndex = omRatisServer.getStateMachineLastAppliedIndex();

    if (flush) {
      // Flush the OM state to disk
      metadataManager.getStore().flush();
    }

    PersistentLongFile.writeFile(ratisSnapshotFile, snapshotIndex);
    LOG.info("Saved Ratis Snapshot on the OM with snapshotIndex {}",
        snapshotIndex);

    return snapshotIndex;
  }

  /**
   * Stop service.
   */
  public void stop() {
    try {
      // Cancel the metrics timer and set to null.
      if (metricsTimer!= null) {
        metricsTimer.cancel();
        metricsTimer = null;
        scheduleOMMetricsWriteTask = null;
      }
      omRpcServer.stop();
      // When ratis is not enabled, we need to call stop() to stop
      // OzoneManageDoubleBuffer in OM server protocol.
      if (!isRatisEnabled) {
        omServerProtocol.stop();
      }
      if (omRatisServer != null) {
        omRatisServer.stop();
      }
      if (omRatisClient != null) {
        omRatisClient.close();
      }
      isOmRpcServerRunning = false;
      keyManager.stop();
      stopSecretManager();
      if (httpServer != null) {
        httpServer.stop();
      }
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

  private void startSecretManagerIfNecessary() {
    boolean shouldRun = isOzoneSecurityEnabled();
    if (shouldRun) {
      boolean running = delegationTokenMgr.isRunning()
          && blockTokenMgr.isRunning();
      if(!running){
        startSecretManager();
      }
    }
  }

  /**
   * Get SCM signed certificate and store it using certificate client.
   * */
  private static void getSCMSignedCert(CertificateClient client,
      OzoneConfiguration config, OMStorage omStore) throws IOException {
    CertificateSignRequest.Builder builder = client.getCSRBuilder();
    KeyPair keyPair = new KeyPair(client.getPublicKey(),
        client.getPrivateKey());
    InetSocketAddress omRpcAdd;
    omRpcAdd = OmUtils.getOmAddress(config);
    if (omRpcAdd == null || omRpcAdd.getAddress() == null) {
      LOG.error("Incorrect om rpc address. omRpcAdd:{}", omRpcAdd);
      throw new RuntimeException("Can't get SCM signed certificate. " +
          "omRpcAdd: " + omRpcAdd);
    }
    // Get host name.
    String hostname = omRpcAdd.getAddress().getHostName();
    String ip = omRpcAdd.getAddress().getHostAddress();

    String subject = UserGroupInformation.getCurrentUser()
        .getShortUserName() + "@" + hostname;

    builder.setCA(false)
        .setKey(keyPair)
        .setConfiguration(config)
        .setScmID(omStore.getScmId())
        .setClusterID(omStore.getClusterID())
        .setSubject(subject)
        .addIpAddress(ip);

    LOG.info("Creating csr for OM->dns:{},ip:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, ip,
        omStore.getScmId(), omStore.getClusterID(), subject);

    HddsProtos.OzoneManagerDetailsProto.Builder omDetailsProtoBuilder =
        HddsProtos.OzoneManagerDetailsProto.newBuilder()
            .setHostName(omRpcAdd.getHostName())
            .setIpAddress(ip)
            .setUuid(omStore.getOmId())
            .addPorts(HddsProtos.Port.newBuilder()
                .setName(RPC_PORT)
                .setValue(omRpcAdd.getPort())
                .build());

    PKCS10CertificationRequest csr = builder.build();
    HddsProtos.OzoneManagerDetailsProto omDetailsProto =
        omDetailsProtoBuilder.build();
    LOG.info("OzoneManager ports added:{}", omDetailsProto.getPortsList());
    SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
        getScmSecurityClient(config);

    SCMGetCertResponseProto response = secureScmClient.
        getOMCertChain(omDetailsProto, getEncodedString(csr));
    String pemEncodedCert = response.getX509Certificate();

    try {


      // Store SCM CA certificate.
      if(response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        client.storeCertificate(pemEncodedRootCert, true, true);
        client.storeCertificate(pemEncodedCert, true);
        // Persist om cert serial id.
        omStore.setOmCertSerialId(CertificateCodec.
            getX509Certificate(pemEncodedCert).getSerialNumber().toString());
      } else {
        throw new RuntimeException("Unable to retrieve OM certificate " +
            "chain");
      }
    } catch (IOException | CertificateException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }

  }

  /**
   *
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }

  /**
   * Returns authentication method used to establish the connection.
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = getRemoteUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synch'ed
  private static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Get delegation token from OzoneManager.
   * @param renewer Renewer information
   * @return delegationToken DelegationToken signed by OzoneManager
   * @throws IOException on error
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws OMException {
    Token<OzoneTokenIdentifier> token;
    try {
      if (!isAllowedDelegationTokenOp()) {
        throw new OMException("Delegation Token can be issued only with "
            + "kerberos or web authentication",
            INVALID_AUTH_METHOD);
      }
      if (delegationTokenMgr == null || !delegationTokenMgr.isRunning()) {
        LOG.warn("trying to get DT with no secret manager running in OM.");
        return null;
      }

      UserGroupInformation ugi = getRemoteUser();
      String user = ugi.getUserName();
      Text owner = new Text(user);
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }

      return delegationTokenMgr.createToken(owner, renewer, realUser);
    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      LOG.error("Get Delegation token failed, cause: {}", ex.getMessage());
      throw new OMException("Get Delegation token failed.", ex,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Method to renew a delegationToken issued by OzoneManager.
   * @param token token to renew
   * @return new expiryTime of the token
   * @throws InvalidToken if {@code token} is invalid
   * @throws IOException on other errors
   */
  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    long expiryTime;

    try {

      if (!isAllowedDelegationTokenOp()) {
        throw new OMException("Delegation Token can be renewed only with "
            + "kerberos or web authentication",
            INVALID_AUTH_METHOD);
      }
      String renewer = getRemoteUser().getShortUserName();
      expiryTime = delegationTokenMgr.renewToken(token, renewer);

    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      OzoneTokenIdentifier id = null;
      try {
        id = OzoneTokenIdentifier.readProtoBuf(token.getIdentifier());
      } catch (IOException exe) {
      }
      LOG.error("Delegation token renewal failed for dt id: {}, cause: {}",
          id, ex.getMessage());
      throw new OMException("Delegation token renewal failed for dt: " + token,
          ex, TOKEN_ERROR_OTHER);
    }
    return expiryTime;
  }

  /**
   * Cancels a delegation token.
   * @param token token to cancel
   * @throws IOException on error
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    OzoneTokenIdentifier id = null;
    try {
      String canceller = getRemoteUser().getUserName();
      id = delegationTokenMgr.cancelToken(token, canceller);
      LOG.trace("Delegation token cancelled for dt: {}", id);
    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      LOG.error("Delegation token cancellation failed for dt id: {}, cause: {}",
          id, ex.getMessage());
      throw new OMException("Delegation token renewal failed for dt: " + token,
          ex, TOKEN_ERROR_OTHER);
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
        if (!ozAdmins.contains(OZONE_ADMINISTRATORS_WILDCARD) && 
            !ozAdmins.contains(ProtobufRpcEngine.Server.getRemoteUser()
                .getUserName())) {
          LOG.error("Only admin users are authorized to create " +
              "Ozone volumes. User :{} is not an admin.",
              ProtobufRpcEngine.Server.getRemoteUser().getUserName());
          throw new OMException("Only admin users are authorized to create " +
              "Ozone volumes.", ResultCodes.PERMISSION_DENIED);
        }
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
   * @throws OMException
   */
  private void checkAcls(ResourceType resType, StoreType store,
      ACLType acl, String vol, String bucket, String key)
      throws OMException {
    checkAcls(resType, store, acl, vol, bucket, key,
        ProtobufRpcEngine.Server.getRemoteUser(),
        ProtobufRpcEngine.Server.getRemoteIp());
  }

  /**
   * CheckAcls for the ozone object.
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @param ugi
   * @param remoteAddress
   * @throws OMException
   */
  @SuppressWarnings("parameternumber")
  public void checkAcls(ResourceType resType, StoreType storeType,
      ACLType aclType, String vol, String bucket, String key,
      UserGroupInformation ugi, InetAddress remoteAddress)
      throws OMException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(resType)
        .setStoreType(storeType)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setKeyName(key).build();
    RequestContext context = RequestContext.newBuilder()
        .setClientUgi(ugi)
        .setIp(remoteAddress)
        .setAclType(ACLIdentityType.USER)
        .setAclRights(aclType)
        .build();
    if (!accessAuthorizer.checkAccess(obj, context)) {
      LOG.warn("User {} doesn't have {} permission to access {}",
          ugi.getUserName(), aclType, resType);
      throw new OMException("User " + ugi.getUserName() + " doesn't " +
          "have " + aclType + " permission to access " + resType,
          ResultCodes.PERMISSION_DENIED);
    }
  }

  /**
   *
   * Return true if Ozone acl's are enabled, else false.
   * @return boolean
   */
  public boolean getAclsEnabled() {
    return isAclEnabled;
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
      UserGroupInformation remoteUserUgi = ProtobufRpcEngine.Server.
          getRemoteUser();
      if (remoteUserUgi == null) {
        LOG.error("Rpc user UGI is null. Authorization failed.");
        throw new OMException("Rpc user UGI is null. Authorization " +
            "failed.", ResultCodes.PERMISSION_DENIED);
      }
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
      if (!ozAdmins.contains(ProtobufRpcEngine.Server.
          getRemoteUser().getUserName())
          && !ozAdmins.contains(OZONE_ADMINISTRATORS_WILDCARD)) {
        LOG.error("Only admin users are authorized to create " +
            "Ozone volumes.");
        throw new OMException("Only admin users are authorized to create " +
            "Ozone volumes.", ResultCodes.PERMISSION_DENIED);
      }
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
        checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.CREATE,
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
      try {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
            args.getVolumeName(), args.getBucketName(), args.getKeyName());
      } catch (OMException ex) {
        // For new keys key checkAccess call will fail as key doesn't exist.
        // Check user access for bucket.
        if (ex.getResult().equals(KEY_NOT_FOUND)) {
          checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
              args.getVolumeName(), args.getBucketName(), args.getKeyName());
        } else {
          throw ex;
        }
      }
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

  private Map<String, String> toAuditMap(KeyArgs omKeyArgs) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, omKeyArgs.getVolumeName());
    auditMap.put(OzoneConsts.BUCKET, omKeyArgs.getBucketName());
    auditMap.put(OzoneConsts.KEY, omKeyArgs.getKeyName());
    auditMap.put(OzoneConsts.DATA_SIZE,
        String.valueOf(omKeyArgs.getDataSize()));
    auditMap.put(OzoneConsts.REPLICATION_TYPE,
        omKeyArgs.hasType() ? omKeyArgs.getType().name() : null);
    auditMap.put(OzoneConsts.REPLICATION_FACTOR,
        omKeyArgs.hasFactor() ? omKeyArgs.getFactor().name() : null);
    auditMap.put(OzoneConsts.KEY_LOCATION_INFO,
        (omKeyArgs.getKeyLocationsList() != null) ?
            omKeyArgs.getKeyLocationsList().toString() : null);
    return auditMap;
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientID)
      throws IOException {
    if(isAclEnabled) {
      try {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
            args.getVolumeName(), args.getBucketName(), args.getKeyName());
      } catch (OMException ex) {
        // For new keys key checkAccess call will fail as key doesn't exist.
        // Check user access for bucket.
        if (ex.getResult().equals(KEY_NOT_FOUND)) {
          checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
              args.getVolumeName(), args.getBucketName(), args.getKeyName());
        } else {
          throw ex;
        }
      }
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
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException {
    if(isAclEnabled) {
      try {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
            args.getVolumeName(), args.getBucketName(), args.getKeyName());
      } catch (OMException ex) {
        // For new keys key checkAccess call will fail as key doesn't exist.
        // Check user access for bucket.
        if (ex.getResult().equals(KEY_NOT_FOUND)) {
          checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
              args.getVolumeName(), args.getBucketName(), args.getKeyName());
        } else {
          throw ex;
        }
      }
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = (args == null) ? new LinkedHashMap<>() :
        args.toAuditMap();
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));
    try {
      metrics.incNumBlockAllocateCalls();
      return keyManager.allocateBlock(args, clientID, excludeList);
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
      return keyManager.lookupKey(args, getClientAddress());
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
      checkAcls(ResourceType.BUCKET,
          StoreType.OZONE, ACLType.LIST, volumeName, bucketName, keyPrefix);
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
    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE, volume,
          bucket, null);
    }
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

  public AuditLogger getAuditLogger() {
    return AUDIT;
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

  private static String getClientAddress() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) { //not a RPC client
      clientMachine = "";
    }
    return clientMachine;
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

    boolean acquiredS3Lock = false;
    boolean acquiredVolumeLock = false;
    try {
      metrics.incNumBucketCreates();
      acquiredS3Lock = metadataManager.getLock().acquireLock(S3_BUCKET_LOCK,
          s3BucketName);
      try {
        acquiredVolumeLock = metadataManager.getLock().acquireLock(VOLUME_LOCK,
            s3BucketManager.formatOzoneVolumeName(userName));
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
    } finally {
      if (acquiredVolumeLock) {
        metadataManager.getLock().releaseLock(VOLUME_LOCK,
            s3BucketManager.formatOzoneVolumeName(userName));
      }
      if (acquiredS3Lock) {
        metadataManager.getLock().releaseLock(S3_BUCKET_LOCK, s3BucketName);
      }
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void deleteS3Bucket(String s3BucketName) throws IOException {
    try {
      if(isAclEnabled) {
        checkAcls(ResourceType.BUCKET, StoreType.S3, ACLType.DELETE, 
            getS3VolumeName(), s3BucketName, null);
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
  public S3SecretValue getS3Secret(String kerberosID) throws IOException{
    return s3SecretManager.getS3Secret(kerberosID);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public String getOzoneBucketMapping(String s3BucketName)
      throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.S3, ACLType.READ,
          getS3VolumeName(), s3BucketName, null);
    }
    return s3BucketManager.getOzoneBucketMapping(s3BucketName);
  }

  /**
   * Helper function to return volume name for S3 users.
   * */
  private String getS3VolumeName() {
    return s3BucketManager.formatOzoneVolumeName(DigestUtils.md5Hex(
        ProtobufRpcEngine.Server.getRemoteUser().getUserName().toLowerCase()));
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

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadList multipartUploadList)
      throws IOException {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo;
    metrics.incNumCompleteMultipartUploads();

    Map<String, String> auditMap = (omKeyArgs == null) ? new LinkedHashMap<>() :
        omKeyArgs.toAuditMap();
    auditMap.put(OzoneConsts.MULTIPART_LIST, multipartUploadList
        .getMultipartMap().toString());
    try {
      omMultipartUploadCompleteInfo = keyManager.completeMultipartUpload(
          omKeyArgs, multipartUploadList);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap));
      return omMultipartUploadCompleteInfo;
    } catch (IOException ex) {
      metrics.incNumCompleteMultipartUploadFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {

    Map<String, String> auditMap = (omKeyArgs == null) ? new LinkedHashMap<>() :
        omKeyArgs.toAuditMap();
    metrics.incNumAbortMultipartUploads();
    try {
      keyManager.abortMultipartUpload(omKeyArgs);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap));
    } catch (IOException ex) {
      metrics.incNumAbortMultipartUploadFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap, ex));
      throw ex;
    }

  }

  @Override
  public OmMultipartUploadListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException {
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    auditMap.put(OzoneConsts.KEY, keyName);
    auditMap.put(OzoneConsts.UPLOAD_ID, uploadID);
    auditMap.put(OzoneConsts.PART_NUMBER_MARKER,
        Integer.toString(partNumberMarker));
    auditMap.put(OzoneConsts.MAX_PARTS, Integer.toString(maxParts));
    metrics.incNumListMultipartUploadParts();
    try {
      OmMultipartUploadListParts omMultipartUploadListParts =
          keyManager.listParts(volumeName, bucketName, keyName, uploadID,
              partNumberMarker, maxParts);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .LIST_MULTIPART_UPLOAD_PARTS, auditMap));
      return omMultipartUploadListParts;
    } catch (IOException ex) {
      metrics.incNumAbortMultipartUploadFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
          .LIST_MULTIPART_UPLOAD_PARTS, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    if (isAclEnabled) {
      checkAcls(getResourceType(args), StoreType.OZONE, ACLType.READ,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumGetFileStatus();
      return keyManager.getFileStatus(args);
    } catch (IOException ex) {
      metrics.incNumGetFileStatusFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(OMAction.GET_FILE_STATUS,
              (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(OMAction.GET_FILE_STATUS,
                (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  private ResourceType getResourceType(OmKeyArgs args) {
    if (args.getKeyName() == null || args.getKeyName().length() == 0) {
      return ResourceType.BUCKET;
    }
    return ResourceType.KEY;
  }

  @Override
  public void createDirectory(OmKeyArgs args) throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumCreateDirectory();
      keyManager.createDirectory(args);
    } catch (IOException ex) {
      metrics.incNumCreateDirectoryFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(OMAction.CREATE_DIRECTORY,
              (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(OMAction.CREATE_DIRECTORY,
                (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  @Override
  public OpenKeySession createFile(OmKeyArgs args, boolean overWrite,
      boolean recursive) throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), null);
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumCreateFile();
      return keyManager.createFile(args, overWrite, recursive);
    } catch (Exception ex) {
      metrics.incNumCreateFileFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.CREATE_FILE,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.CREATE_FILE, (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    if(isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumLookupFile();
      return keyManager.lookupFile(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumLookupFileFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.LOOKUP_FILE,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.LOOKUP_FILE, (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries) throws IOException {
    if(isAclEnabled) {
      checkAcls(getResourceType(args), StoreType.OZONE, ACLType.READ,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
    }
    boolean auditSuccess = true;
    try {
      metrics.incNumListStatus();
      return keyManager.listStatus(args, recursive, startKey, numEntries);
    } catch (Exception ex) {
      metrics.incNumListStatusFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.LIST_STATUS,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    } finally {
      if(auditSuccess){
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.LIST_STATUS, (args == null) ? null : args.toAuditMap()));
      }
    }
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl top be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    if(isAclEnabled) {
      checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.WRITE_ACL,
          obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
    }
    // TODO: Audit ACL operation.
    switch (obj.getResourceType()) {
    case VOLUME:
      return volumeManager.addAcl(obj, acl);
    case BUCKET:
      return bucketManager.addAcl(obj, acl);
    case KEY:
      return keyManager.addAcl(obj, acl);
    case PREFIX:
      return prefixManager.addAcl(obj, acl);
    default:
      throw new OMException("Unexpected resource type: " +
          obj.getResourceType(), INVALID_REQUEST);
    }
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    if(isAclEnabled) {
      checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.WRITE_ACL,
          obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
    }
    // TODO: Audit ACL operation.
    switch (obj.getResourceType()) {
    case VOLUME:
      return volumeManager.removeAcl(obj, acl);
    case BUCKET:
      return bucketManager.removeAcl(obj, acl);
    case KEY:
      return keyManager.removeAcl(obj, acl);
    case PREFIX:
      return prefixManager.removeAcl(obj, acl);

    default:
      throw new OMException("Unexpected resource type: " +
          obj.getResourceType(), INVALID_REQUEST);
    }
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    if(isAclEnabled) {
      checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.WRITE_ACL,
          obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
    }
    // TODO: Audit ACL operation.
    switch (obj.getResourceType()) {
    case VOLUME:
      return volumeManager.setAcl(obj, acls);
    case BUCKET:
      return bucketManager.setAcl(obj, acls);
    case KEY:
      return keyManager.setAcl(obj, acls);
    case PREFIX:
      return prefixManager.setAcl(obj, acls);
    default:
      throw new OMException("Unexpected resource type: " +
          obj.getResourceType(), INVALID_REQUEST);
    }
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    if(isAclEnabled) {
      checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.READ_ACL,
          obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
    }
    // TODO: Audit ACL operation.
    switch (obj.getResourceType()) {
    case VOLUME:
      return volumeManager.getAcl(obj);
    case BUCKET:
      return bucketManager.getAcl(obj);
    case KEY:
      return keyManager.getAcl(obj);
    case PREFIX:
      return prefixManager.getAcl(obj);

    default:
      throw new OMException("Unexpected resource type: " +
          obj.getResourceType(), INVALID_REQUEST);
    }
  }

  /**
   * Download and install latest checkpoint from leader OM.
   * If the download checkpoints snapshot index is greater than this OM's
   * last applied transaction index, then re-initialize the OM state via this
   * checkpoint. Before re-initializing OM state, the OM Ratis server should
   * be stopped so that no new transactions can be applied.
   * @param leaderId peerNodeID of the leader OM
   * @return If checkpoint is installed, return the corresponding termIndex.
   * Otherwise, return null.
   */
  public TermIndex installSnapshot(String leaderId) {
    if (omSnapshotProvider == null) {
      LOG.error("OM Snapshot Provider is not configured as there are no peer " +
          "nodes.");
      return null;
    }

    DBCheckpoint omDBcheckpoint = getDBCheckpointFromLeader(leaderId);
    Path newDBlocation = omDBcheckpoint.getCheckpointLocation();

    // Check if current ratis log index is smaller than the downloaded
    // snapshot index. If yes, proceed by stopping the ratis server so that
    // the OM state can be re-initialized. If no, then do not proceed with
    // installSnapshot.
    long lastAppliedIndex = omRatisServer.getStateMachineLastAppliedIndex();
    long checkpointSnapshotIndex = omDBcheckpoint.getRatisSnapshotIndex();
    if (checkpointSnapshotIndex <= lastAppliedIndex) {
      LOG.error("Failed to install checkpoint from OM leader: {}. The last " +
          "applied index: {} is greater than or equal to the checkpoint's " +
          "snapshot index: {}. Deleting the downloaded checkpoint {}", leaderId,
          lastAppliedIndex, checkpointSnapshotIndex,
          newDBlocation);
      try {
        FileUtils.deleteFully(newDBlocation);
      } catch (IOException e) {
        LOG.error("Failed to fully delete the downloaded DB checkpoint {} " +
            "from OM leader {}.", newDBlocation,
            leaderId, e);
      }
      return null;
    }

    // Pause the State Machine so that no new transactions can be applied.
    // This action also clears the OM Double Buffer so that if there are any
    // pending transactions in the buffer, they are discarded.
    // TODO: The Ratis server should also be paused here. This is required
    //  because a leader election might happen while the snapshot
    //  installation is in progress and the new leader might start sending
    //  append log entries to the ratis server.
    omRatisServer.getOmStateMachine().pause();

    File dbBackup;
    try {
      dbBackup = replaceOMDBWithCheckpoint(lastAppliedIndex, newDBlocation);
    } catch (Exception e) {
      LOG.error("OM DB checkpoint replacement with new downloaded checkpoint " +
          "failed.", e);
      return null;
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    try {
      reloadOMState(checkpointSnapshotIndex);
      omRatisServer.getOmStateMachine().unpause(checkpointSnapshotIndex);
    } catch (IOException e) {
      LOG.error("Failed to reload OM state with new DB checkpoint.", e);
      return null;
    }

    // Delete the backup DB
    try {
      FileUtils.deleteFully(dbBackup);
    } catch (IOException e) {
      LOG.error("Failed to delete the backup of the original DB {}", dbBackup);
    }

    // TODO: We should only return the snpashotIndex to the leader.
    //  Should be fixed after RATIS-586
    TermIndex newTermIndex = TermIndex.newTermIndex(0,
        checkpointSnapshotIndex);

    return newTermIndex;
  }

  /**
   * Download the latest OM DB checkpoint from the leader OM.
   * @param leaderId OMNodeID of the leader OM node.
   * @return latest DB checkpoint from leader OM.
   */
  private DBCheckpoint getDBCheckpointFromLeader(String leaderId) {
    LOG.info("Downloading checkpoint from leader OM {} and reloading state " +
        "from the checkpoint.", leaderId);

    try {
      return omSnapshotProvider.getOzoneManagerDBSnapshot(leaderId);
    } catch (IOException e) {
      LOG.error("Failed to download checkpoint from OM leader {}", leaderId, e);
    }
    return null;
  }

  /**
   * Replace the current OM DB with the new DB checkpoint.
   * @param lastAppliedIndex the last applied index in the current OM DB.
   * @param checkpointPath path to the new DB checkpoint
   * @return location of the backup of the original DB
   * @throws Exception
   */
  File replaceOMDBWithCheckpoint(long lastAppliedIndex, Path checkpointPath)
      throws Exception {
    // Stop the DB first
    DBStore store = metadataManager.getStore();
    store.close();

    // Take a backup of the current DB
    File db = store.getDbLocation();
    String dbBackupName = OzoneConsts.OM_DB_BACKUP_PREFIX +
        lastAppliedIndex + "_" + System.currentTimeMillis();
    File dbBackup = new File(db.getParentFile(), dbBackupName);

    try {
      Files.move(db.toPath(), dbBackup.toPath());
    } catch (IOException e) {
      LOG.error("Failed to create a backup of the current DB. Aborting " +
          "snapshot installation.");
      throw e;
    }

    // Move the new DB checkpoint into the om metadata dir
    try {
      Files.move(checkpointPath, db.toPath());
    } catch (IOException e) {
      LOG.error("Failed to move downloaded DB checkpoint {} to metadata " +
          "directory {}. Resetting to original DB.", checkpointPath,
          db.toPath());
      Files.move(dbBackup.toPath(), db.toPath());
      throw e;
    }
    return dbBackup;
  }

  /**
   * Re-instantiate MetadataManager with new DB checkpoint.
   * All the classes which use/ store MetadataManager should also be updated
   * with the new MetadataManager instance.
   */
  void reloadOMState(long newSnapshotIndex) throws IOException {

    instantiateServices();

    // Restart required services
    metadataManager.start(configuration);
    keyManager.start(configuration);

    // Set metrics and start metrics back ground thread
    metrics.setNumVolumes(metadataManager.countRowsInTable(metadataManager
        .getVolumeTable()));
    metrics.setNumBuckets(metadataManager.countRowsInTable(metadataManager
        .getBucketTable()));
    metrics.setNumKeys(metadataManager.countEstimatedRowsInTable(metadataManager
        .getKeyTable()));

    // Delete the omMetrics file if it exists and save the a new metrics file
    // with new data
    Files.deleteIfExists(getMetricsStorageFile().toPath());
    saveOmMetrics();

    // Update OM snapshot index with the new snapshot index (from the new OM
    // DB state) and save the snapshot index to disk
    this.snapshotIndex = newSnapshotIndex;
    saveRatisSnapshot(false);
  }

  public static  Logger getLogger() {
    return LOG;
  }

  public OzoneConfiguration getConfiguration() {
    return configuration;
  }

  public static void setTestSecureOmFlag(boolean testSecureOmFlag) {
    OzoneManager.testSecureOmFlag = testSecureOmFlag;
  }

  public String getOMNodeId() {
    return omNodeDetails.getOMNodeId();
  }

  public String getOMServiceId() {
    return omNodeDetails.getOMServiceId();
  }

  @VisibleForTesting
  public List<OMNodeDetails> getPeerNodes() {
    return peerNodes;
  }

  @VisibleForTesting
  public CertificateClient getCertificateClient() {
    return certClient;
  }

  public String getComponent() {
    return omComponent;
  }

  @Override
  public OMFailoverProxyProvider getOMFailoverProxyProvider() {
    return null;
  }

  /**
   * Return maximum volumes count per user.
   * @return maxUserVolumeCount
   */
  public long getMaxUserVolumeCount() {
    return maxUserVolumeCount;
  }

  /**
   * Checks the Leader status of OM Ratis Server.
   * Note that this status has a small window of error. It should not be used
   * to determine the absolute leader status.
   * If it is the leader, the role status is cached till Ratis server
   * notifies of leader change. If it is not leader, the role information is
   * retrieved through by submitting a GroupInfoRequest to Ratis server.
   *
   * If ratis is not enabled, then it always returns true.
   *
   * @return Return true if this node is the leader, false otherwsie.
   */
  public boolean isLeader() {
    return isRatisEnabled ? omRatisServer.isLeader() : true;
  }

  /**
   * Return if Ratis is enabled or not.
   * @return
   */
  public boolean isRatisEnabled() {
    return isRatisEnabled;
  }

  /**
   * Get DB updates since a specific sequence number.
   * @param dbUpdatesRequest request that encapsulates a sequence number.
   * @return Wrapper containing the updates.
   * @throws SequenceNumberNotFoundException if db is unable to read the data.
   */
  public DBUpdatesWrapper getDBUpdates(
      DBUpdatesRequest dbUpdatesRequest)
      throws SequenceNumberNotFoundException {
    return metadataManager.getStore()
        .getUpdatesSince(dbUpdatesRequest.getSequenceNumber());

  }
}
