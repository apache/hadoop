/*
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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;

import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Globber;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.impl.FlagSet;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.api.PerformanceFlagEnum;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.auth.SignerManager;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationOperations;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.fs.s3a.commit.magic.InMemoryMagicCommitTracker;
import org.apache.hadoop.fs.s3a.impl.AWSCannedACL;
import org.apache.hadoop.fs.s3a.impl.BaseS3AFileSystemOperations;
import org.apache.hadoop.fs.s3a.impl.BulkDeleteOperation;
import org.apache.hadoop.fs.s3a.impl.BulkDeleteOperationCallbacksImpl;
import org.apache.hadoop.fs.s3a.impl.CSES3AFileSystemOperations;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ClientManager;
import org.apache.hadoop.fs.s3a.impl.ClientManagerImpl;
import org.apache.hadoop.fs.s3a.impl.ConfigurationHelper;
import org.apache.hadoop.fs.s3a.impl.ContextAccessors;
import org.apache.hadoop.fs.s3a.impl.CopyFromLocalOperation;
import org.apache.hadoop.fs.s3a.impl.CreateFileBuilder;
import org.apache.hadoop.fs.s3a.impl.InputStreamCallbacksImpl;
import org.apache.hadoop.fs.s3a.impl.S3AFileSystemOperations;
import org.apache.hadoop.fs.s3a.impl.CSEV1CompatibleS3AFileSystemOperations;
import org.apache.hadoop.fs.s3a.impl.CSEMaterials;
import org.apache.hadoop.fs.s3a.impl.DeleteOperation;
import org.apache.hadoop.fs.s3a.impl.GetContentSummaryOperation;
import org.apache.hadoop.fs.s3a.impl.HeaderProcessing;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.ListingOperationCallbacks;
import org.apache.hadoop.fs.s3a.impl.MkdirOperation;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;
import org.apache.hadoop.fs.s3a.impl.OpenFileSupport;
import org.apache.hadoop.fs.s3a.impl.OperationCallbacks;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.RenameOperation;
import org.apache.hadoop.fs.s3a.impl.RequestFactoryImpl;
import org.apache.hadoop.fs.s3a.impl.S3AMultipartUploaderBuilder;
import org.apache.hadoop.fs.s3a.impl.S3AStoreBuilder;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.StoreContextBuilder;
import org.apache.hadoop.fs.s3a.impl.StoreContextFactory;
import org.apache.hadoop.fs.s3a.impl.UploadContentProviders;
import org.apache.hadoop.fs.s3a.impl.CSEUtils;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamCallbacks;
import org.apache.hadoop.fs.s3a.impl.streams.StreamThreadOptions;
import org.apache.hadoop.fs.s3a.tools.MarkerToolOperations;
import org.apache.hadoop.fs.s3a.tools.MarkerToolOperationsImpl;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.FileSystemStatisticNames;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.fs.store.audit.AuditEntryPoint;
import org.apache.hadoop.fs.store.audit.ActiveThreadSpanSource;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AuditManagerS3A;
import org.apache.hadoop.fs.s3a.audit.AuditIntegration;
import org.apache.hadoop.fs.s3a.audit.OperationAuditor;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3a.auth.delegation.AWSPolicyProvider;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens;
import org.apache.hadoop.fs.s3a.auth.delegation.AbstractS3ATokenIdentifier;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.PutTracker;
import org.apache.hadoop.fs.s3a.commit.MagicCommitIntegration;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.s3a.statistics.impl.BondedS3AStatisticsContext;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.fs.store.EtagChecksum;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.RateLimitingFactory;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CommonPathCapabilities.DIRECTORY_LISTING_INCONSISTENT;
import static org.apache.hadoop.fs.impl.FlagSet.buildFlagSet;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.*;
import static org.apache.hadoop.fs.s3a.Listing.toLocatedFileStatusIterator;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.INITIALIZE_SPAN;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.createAWSCredentialProviderList;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.STATEMENT_ALLOW_KMS_RW;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.allowS3Operations;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.TokenIssuingPolicy.NoTokensAvailable;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.hasDelegationTokenBinding;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_COMMITTER_PENDING_OBJECT_ETAG_NAME;
import static org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTrackerUtils.isTrackMagicCommitsInMemoryEnabled;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CreateFileBuilder.OPTIONS_CREATE_FILE_NO_OVERWRITE;
import static org.apache.hadoop.fs.s3a.impl.CreateFileBuilder.OPTIONS_CREATE_FILE_OVERWRITE;
import static org.apache.hadoop.fs.s3a.impl.CreateFileBuilder.OPTIONS_CREATE_FILE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isUnknownBucket;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.CONTENT_TYPE_OCTET_STREAM;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.AP_REQUIRED_EXCEPTION;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.ARN_BUCKET_OPTION;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DEFAULT_UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_403_FORBIDDEN;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404_NOT_FOUND;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.fixBucketRegion;
import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.logDnsLookup;
import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.STORE_CAPABILITY_S3_EXPRESS_STORAGE;
import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.isS3ExpressStore;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.checkNoS3Guard;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtLevel;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.pairedTrackerFactory;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfSupplier;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.RateLimitingFactory.unlimitedRate;
import static org.apache.hadoop.util.functional.RemoteIterators.foreach;
import static org.apache.hadoop.util.functional.RemoteIterators.typeCastingRemoteIterator;

/**
 * The core S3A Filesystem implementation.
 *
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(Configuration)} and variants to
 * create one.
 *
 * If cast to {@code S3AFileSystem}, extra methods and features may be accessed.
 * Consider those private and unstable.
 *
 * Because it prints some of the state of the instrumentation,
 * the output of {@link #toString()} must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AFileSystem extends FileSystem implements StreamCapabilities,
    AWSPolicyProvider, DelegationTokenProvider, IOStatisticsSource,
    AuditSpanSource<AuditSpanS3A>, ActiveThreadSpanSource<AuditSpanS3A>,
        StoreContextFactory {

  /**
   * Default blocksize as used in blocksize and FS status queries.
   */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;

  private URI uri;

  private Path workingDir;

  private String username;

  private S3AStore store;

  /**
   * The core S3 client is created and managed by the ClientManager.
   * It is copied here within {@link #initialize(URI, Configuration)}.
   * Some mocking tests modify this so take care with changes.
   */
  private S3Client s3Client;

  // initial callback policy is fail-once; it's there just to assist
  // some mock tests and other codepaths trying to call the low level
  // APIs on an uninitialized filesystem.
  private Invoker invoker = new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL,
      Invoker.LOG_EVENT);

  private final Retried onRetry = this::operationRetried;

  /**
   * Represents bucket name for all S3 operations. If per bucket override for
   * {@link InternalConstants#ARN_BUCKET_OPTION} property  is set, then the bucket is updated to
   * point to the configured Arn.
   */
  private String bucket;
  private int maxKeys;
  private Listing listing;
  private long partSize;
  private boolean enableMultiObjectsDelete;
  private ExecutorService boundedThreadPool;
  private ThreadPoolExecutor unboundedThreadPool;

  /**
   * Future pool built on the bounded thread pool.
   */
  private ExecutorServiceFuturePool futurePool;

  private int executorCapacity;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem.class);

  /** Log to warn of storage class configuration problems. */
  private static final LogExactlyOnce STORAGE_CLASS_WARNING = new LogExactlyOnce(LOG);

  private String cannedACL;

  /**
   * This must never be null; until initialized it just declares that there
   * is no encryption.
   */
  private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();
  /** The core instrumentation. */
  private S3AInstrumentation instrumentation;
  /** Accessors to statistics for this FS. */
  private S3AStatisticsContext statisticsContext;
  /** Storage Statistics Bonded to the instrumentation. */
  private S3AStorageStatistics storageStatistics;

  /**
   * Performance flags.
   */
  private FlagSet<PerformanceFlagEnum> performanceFlags;

  /**
   * Default input policy; may be overridden in
   * {@code openFile()}.
   */
  private S3AInputPolicy inputPolicy;
  /** Vectored IO context. */
  private VectoredIOContext vectoredIOContext;

  /**
   * Maximum number of active range read operation a single
   * input stream can have.
   */
  private int vectoredActiveRangeReads;

  private long readAhead;
  private ChangeDetectionPolicy changeDetectionPolicy;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile boolean isClosed = false;

  /** Delegation token integration; non-empty when DT support is enabled. */
  private Optional<S3ADelegationTokens> delegationTokens = Optional.empty();

  /** Principal who created the FS; recorded during initialization. */
  private UserGroupInformation owner;

  private String blockOutputBuffer;
  private S3ADataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;
  private boolean useListV1;
  private MagicCommitIntegration committerIntegration;

  private AWSCredentialProviderList credentials;
  private SignerManager signerManager;
  private S3AInternals s3aInternals;

  /**
   * Do directory operations purge pending uploads?
   */
  private boolean dirOperationsPurgeUploads;

  /**
   * Page size for deletions.
   */
  private int pageSize;

  private final ListingOperationCallbacks listingOperationCallbacks =
          new ListingOperationCallbacksImpl();

  /**
   * Helper for the openFile() method.
   */
  private OpenFileSupport openFileHelper;

  /**
   * Context accessors for re-use.
   */
  private final ContextAccessors contextAccessors = new ContextAccessorsImpl();

  /**
   * Factory for AWS requests.
   */
  private RequestFactory requestFactory;

  /**
   * Audit manager (service lifecycle).
   * Creates the audit service and manages the binding of different audit spans
   * to different threads.
   * Initially this is a no-op manager; once the service is initialized it will
   * be replaced with a configured one.
   */
  private AuditManagerS3A auditManager =
      AuditIntegration.stubAuditManager();

  /**
   * Is this S3A FS instance using S3 client side encryption?
   */
  private boolean isCSEEnabled;

  /**
   * Bucket AccessPoint.
   */
  private ArnResource accessPoint;

  /**
   * Handler for certain filesystem operations.
   */
  private S3AFileSystemOperations fsHandler;


  /**
   * Does this S3A FS instance have multipart upload enabled?
   */
  private boolean isMultipartUploadEnabled = DEFAULT_MULTIPART_UPLOAD_ENABLED;

  /**
   * Should file copy operations use the S3 transfer manager?
   * True unless multipart upload is disabled.
   */
  private boolean isMultipartCopyEnabled;

  /**
   * Is FIPS enabled?
   */
  private boolean fipsEnabled;

  /**
   * A cache of files that should be deleted when the FileSystem is closed
   * or the JVM is exited.
   */
  private final Set<Path> deleteOnExit = new TreeSet<>();

  /**
   * Scheme for the current filesystem.
   */
  private String scheme = FS_S3A;

  /**
   * Flag to indicate that the higher performance copyFromLocalFile implementation
   * should be used.
   */
  private boolean optimizedCopyFromLocal;

  /**
   * Is this an S3 Express store?
   */
  private boolean s3ExpressStore;

  /**
   * Store endpoint from configuration info or access point ARN.
   */
  private String endpoint;

  /**
   * Region from configuration info or access point ARN.
   */
  private String configuredRegion;

  /**
   * Are S3 Access Grants Enabled?
   */
  private boolean s3AccessGrantsEnabled;

  /** Add any deprecated keys. */
  @SuppressWarnings("deprecation")
  private static void addDeprecatedKeys() {
    Configuration.DeprecationDelta[] deltas = {
        new Configuration.DeprecationDelta(
            FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS,
            FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS),
        new Configuration.DeprecationDelta(
            SERVER_SIDE_ENCRYPTION_ALGORITHM,
            S3_ENCRYPTION_ALGORITHM),
        new Configuration.DeprecationDelta(
            SERVER_SIDE_ENCRYPTION_KEY,
            S3_ENCRYPTION_KEY)
    };

    if (deltas.length > 0) {
      Configuration.addDeprecations(deltas);
      Configuration.reloadExistingConfigurations();
    }
  }

  static {
    addDeprecatedKeys();
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param originalConf the configuration to use for the FS. The
   * bucket-specific options are patched over the base ones before any use is
   * made of the config.
   */
  public void initialize(URI name, Configuration originalConf)
      throws IOException {
    // get the host; this is guaranteed to be non-null, non-empty
    bucket = name.getHost();
    AuditSpan span = null;
    // track initialization duration; will only be set after
    // statistics are set up.
    Optional<DurationTracker> trackInitialization = Optional.empty();
    try {
      LOG.debug("Initializing S3AFileSystem for {}", bucket);
      if (LOG.isTraceEnabled()) {
        // log a full trace for deep diagnostics of where an object is created,
        // for tracking down memory leak issues.
        LOG.trace("Filesystem for {} created; fs.s3a.impl.disable.cache = {}",
            name, originalConf.getBoolean("fs.s3a.impl.disable.cache", false),
            new RuntimeException(super.toString()));
      }
      // clone the configuration into one with propagated bucket options
      Configuration conf = propagateBucketOptions(originalConf, bucket);
      // HADOOP-17894. remove references to s3a stores in JCEKS credentials.
      conf = ProviderUtils.excludeIncompatibleCredentialProviders(
          conf, S3AFileSystem.class);
      String arn = String.format(ARN_BUCKET_OPTION, bucket);
      String configuredArn = conf.getTrimmed(arn, "");
      if (!configuredArn.isEmpty()) {
        accessPoint = ArnResource.accessPointFromArn(configuredArn);
        LOG.info("Using AccessPoint ARN \"{}\" for bucket {}", configuredArn, bucket);
        bucket = accessPoint.getFullArn();
      } else if (conf.getBoolean(AWS_S3_ACCESSPOINT_REQUIRED, false)) {
        LOG.warn("Access Point usage is required because \"{}\" is enabled," +
            " but not configured for the bucket: {}", AWS_S3_ACCESSPOINT_REQUIRED, bucket);
        throw new PathIOException(bucket, AP_REQUIRED_EXCEPTION);
      }

      // fix up the classloader of the configuration to be whatever
      // classloader loaded this filesystem.
      // See: HADOOP-17372 and follow-up on HADOOP-18993
      S3AUtils.maybeIsolateClassloader(conf, this.getClass().getClassLoader());

      // patch the Hadoop security providers
      patchSecurityCredentialProviders(conf);
      // look for delegation token support early.
      boolean delegationTokensEnabled = hasDelegationTokenBinding(conf);
      if (delegationTokensEnabled) {
        LOG.debug("Using delegation tokens");
      }
      // set the URI, this will do any fixup of the URI to remove secrets,
      // canonicalize.
      setUri(name, delegationTokensEnabled);
      super.initialize(uri, conf);
      setConf(conf);

      // initialize statistics, after which statistics
      // can be collected.
      instrumentation = new S3AInstrumentation(uri);
      initializeStatisticsBinding();

      // track initialization duration.
      // this should really be done in a onceTrackingDuration() call,
      // but then all methods below would need to be in the lambda and
      // it would create a merge/backport headache for all.
      trackInitialization = Optional.of(
          instrumentation.trackDuration(FileSystemStatisticNames.FILESYSTEM_INITIALIZATION));

      s3aInternals = createS3AInternals();

      // look for encryption data
      // DT Bindings may override this
      setEncryptionSecrets(
          buildEncryptionSecrets(bucket, conf));

      invoker = new Invoker(new S3ARetryPolicy(getConf()), onRetry);

      // If encryption method is set to CSE-KMS or CSE-CUSTOM then CSE is enabled.
      isCSEEnabled = CSEUtils.isCSEEnabled(getS3EncryptionAlgorithm().getMethod());

      // Create the appropriate fsHandler instance using a factory method
      fsHandler = createFileSystemHandler();
      fsHandler.setCSEGauge((IOStatisticsStore) getIOStatistics());
      // Username is the current user at the time the FS was instantiated.
      owner = UserGroupInformation.getCurrentUser();
      username = owner.getShortUserName();
      workingDir = new Path("/user", username)
          .makeQualified(this.uri, this.getWorkingDirectory());

      maxKeys = intOption(conf, MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS, 1);
      partSize = getMultipartSizeProperty(conf,
          MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
      multiPartThreshold = getMultipartSizeProperty(conf,
          MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

      //check but do not store the block size
      longBytesOption(conf, FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE, 1);
      enableMultiObjectsDelete = conf.getBoolean(ENABLE_MULTI_DELETE, true);

      // determine and cache the endpoints
      endpoint = accessPoint == null
          ? conf.getTrimmed(ENDPOINT, DEFAULT_ENDPOINT)
          : accessPoint.getEndpoint();

      configuredRegion = accessPoint == null
          ? conf.getTrimmed(AWS_REGION)
          : accessPoint.getRegion();

      fipsEnabled = conf.getBoolean(FIPS_ENDPOINT, ENDPOINT_FIPS_DEFAULT);

      // is this an S3Express store?
      s3ExpressStore = isS3ExpressStore(bucket, endpoint);

      // should the delete also purge uploads?
      // happens if explicitly enabled, or if the store is S3Express storage.
      dirOperationsPurgeUploads = conf.getBoolean(DIRECTORY_OPERATIONS_PURGE_UPLOADS,
          s3ExpressStore);

      this.isMultipartUploadEnabled = conf.getBoolean(MULTIPART_UPLOADS_ENABLED,
          DEFAULT_MULTIPART_UPLOAD_ENABLED);
      // multipart copy and upload are the same; this just makes it explicit
      this.isMultipartCopyEnabled = isMultipartUploadEnabled;

      int listVersion = conf.getInt(LIST_VERSION, DEFAULT_LIST_VERSION);
      if (listVersion < 1 || listVersion > 2) {
        LOG.warn("Configured fs.s3a.list.version {} is invalid, forcing " +
            "version 2", listVersion);
      }
      useListV1 = (listVersion == 1);
      if (accessPoint != null && useListV1) {
        LOG.warn("V1 list configured in fs.s3a.list.version. This is not supported in by" +
            " access points. Upgrading to V2");
        useListV1 = false;
      }

      signerManager = new SignerManager(bucket, this, conf, owner);
      signerManager.initCustomSigners();

      // start auditing
      initializeAuditService();

      // create the requestFactory.
      // requires the audit manager to be initialized.
      requestFactory = createRequestFactory();

      // create an initial span for all other operations.
      span = createSpan(INITIALIZE_SPAN, bucket, null);

      // creates the AWS client, including overriding auth chain if
      // the FS came with a DT
      // this may do some patching of the configuration (e.g. setting
      // the encryption algorithms)
      ClientManager clientManager = createClientManager(name, delegationTokensEnabled);

      inputPolicy = S3AInputPolicy.getPolicy(
          conf.getTrimmed(INPUT_FADVISE,
              Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_DEFAULT),
          S3AInputPolicy.Normal);
      LOG.debug("Input fadvise policy = {}", inputPolicy);
      changeDetectionPolicy = ChangeDetectionPolicy.getPolicy(conf);
      LOG.debug("Change detection policy = {}", changeDetectionPolicy);
      boolean magicCommitterEnabled = conf.getBoolean(
          CommitConstants.MAGIC_COMMITTER_ENABLED,
          CommitConstants.DEFAULT_MAGIC_COMMITTER_ENABLED);
      LOG.debug("Filesystem support for magic committers {} enabled",
          magicCommitterEnabled ? "is" : "is not");
      committerIntegration = new MagicCommitIntegration(
          this, magicCommitterEnabled);

      boolean blockUploadEnabled = conf.getBoolean(FAST_UPLOAD, true);

      if (!blockUploadEnabled) {
        LOG.warn("The \"slow\" output stream is no longer supported");
      }
      blockOutputBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER,
          DEFAULT_FAST_UPLOAD_BUFFER);
      blockFactory = S3ADataBlocks.createFactory(createStoreContext(), blockOutputBuffer);
      blockOutputActiveBlocks = intOption(conf,
          FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
      // If CSE is enabled, do multipart uploads serially.
      if (isCSEEnabled) {
        blockOutputActiveBlocks = 1;
      }
      LOG.debug("Using S3ABlockOutputStream with buffer = {}; block={};" +
              " queue limit={}; multipart={}",
          blockOutputBuffer, partSize, blockOutputActiveBlocks, isMultipartUploadEnabled);
      // verify there's no S3Guard in the store config.
      checkNoS3Guard(this.getUri(), getConf());

      // read in performance options and parse them to a list of flags.
      performanceFlags = buildFlagSet(
          PerformanceFlagEnum.class,
          conf,
          FS_S3A_PERFORMANCE_FLAGS,
          true);
      // performance creation flag for code which wants performance
      // at the risk of overwrites.
      // this uses the performance flags as the default and then
      // updates the performance flags to match.
      // a bit convoluted.
      boolean performanceCreation = conf.getBoolean(FS_S3A_CREATE_PERFORMANCE,
          performanceFlags.enabled(PerformanceFlagEnum.Create));
      performanceFlags.set(PerformanceFlagEnum.Create, performanceCreation);
      // freeze.
      performanceFlags.makeImmutable();

      LOG.debug("{} = {}", FS_S3A_CREATE_PERFORMANCE, performanceCreation);

      pageSize = intOption(getConf(), BULK_DELETE_PAGE_SIZE,
          BULK_DELETE_PAGE_SIZE_DEFAULT, 0);
      checkArgument(pageSize <= InternalConstants.MAX_ENTRIES_TO_DELETE,
              "page size out of range: %s", pageSize);
      listing = new Listing(listingOperationCallbacks, createStoreContext());
      // now the open file logic
      openFileHelper = new OpenFileSupport(
          changeDetectionPolicy,
          longBytesOption(conf, READAHEAD_RANGE,
              DEFAULT_READAHEAD_RANGE, 0),
          username,
          intOption(conf, IO_FILE_BUFFER_SIZE_KEY,
              IO_FILE_BUFFER_SIZE_DEFAULT, 0),
          longBytesOption(conf, ASYNC_DRAIN_THRESHOLD,
                        DEFAULT_ASYNC_DRAIN_THRESHOLD, 0),
          inputPolicy);
      vectoredActiveRangeReads = intOption(conf,
              AWS_S3_VECTOR_ACTIVE_RANGE_READS, DEFAULT_AWS_S3_VECTOR_ACTIVE_RANGE_READS, 1);
      vectoredIOContext = populateVectoredIOContext(conf);
      scheme = (this.uri != null && this.uri.getScheme() != null) ? this.uri.getScheme() : FS_S3A;
      optimizedCopyFromLocal = conf.getBoolean(OPTIMIZED_COPY_FROM_LOCAL,
          OPTIMIZED_COPY_FROM_LOCAL_DEFAULT);
      LOG.debug("Using optimized copyFromLocal implementation: {}", optimizedCopyFromLocal);
      s3AccessGrantsEnabled = conf.getBoolean(AWS_S3_ACCESS_GRANTS_ENABLED, false);

      int rateLimitCapacity = intOption(conf, S3A_IO_RATE_LIMIT, DEFAULT_S3A_IO_RATE_LIMIT, 0);
      // now create and initialize the store
      store = createS3AStore(clientManager, rateLimitCapacity);
      // the s3 client is created through the store, rather than
      // directly through the client manager.
      // this is to aid mocking.
      s3Client = getStore().getOrCreateS3Client();

      // thread pool init requires store to be created
      initThreadPools();

      // The filesystem is now ready to perform operations against
      // S3
      // This initiates a probe against S3 for the bucket existing.
      doBucketProbing();
      initMultipartUploads(conf);
      trackInitialization.ifPresent(DurationTracker::close);
    } catch (SdkException e) {
      // amazon client exception: stop all services then throw the translation
      cleanupWithLogger(LOG, span);
      stopAllServices();
      trackInitialization.ifPresent(DurationTracker::failed);
      throw translateException("initializing ", new Path(name), e);
    } catch (IOException | RuntimeException e) {
      // other exceptions: stop the services.
      cleanupWithLogger(LOG, span);
      stopAllServices();
      trackInitialization.ifPresent(DurationTracker::failed);
      throw e;
    }
  }

  /**
   * Creates and returns an instance of the appropriate S3AFileSystemOperations.
   * Creation is baaed on the client-side encryption (CSE) settings.
   *
   * @return An instance of the appropriate S3AFileSystemOperations implementation.
   */
  private S3AFileSystemOperations createFileSystemHandler() {
    if (isCSEEnabled) {
      if (getConf().getBoolean(S3_ENCRYPTION_CSE_V1_COMPATIBILITY_ENABLED,
          S3_ENCRYPTION_CSE_V1_COMPATIBILITY_ENABLED_DEFAULT)) {
        return new CSEV1CompatibleS3AFileSystemOperations();
      } else {
        return new CSES3AFileSystemOperations();
      }
    } else {
      return new BaseS3AFileSystemOperations();
    }
  }


  /**
   * Create and start the S3AStore instance.
   * This is protected so that tests can override it.
   * @param clientManager client manager
   * @param rateLimitCapacity rate limit
   * @return a new store instance
   */
  @VisibleForTesting
  protected S3AStore createS3AStore(final ClientManager clientManager,
      final int rateLimitCapacity) {
    final S3AStore st = new S3AStoreBuilder()
        .withAuditSpanSource(getAuditManager())
        .withClientManager(clientManager)
        .withDurationTrackerFactory(getDurationTrackerFactory())
        .withFsStatistics(getFsStatistics())
        .withInstrumentation(getInstrumentation())
        .withStatisticsContext(statisticsContext)
        .withStoreContextFactory(this)
        .withStorageStatistics(getStorageStatistics())
        .withReadRateLimiter(unlimitedRate())
        .withWriteRateLimiter(RateLimitingFactory.create(rateLimitCapacity))
        .build();
    st.init(getConf());
    st.start();
    return st;
  }

  /**
   * Populates the configurations related to vectored IO operation
   * in the context which has to passed down to input streams.
   * @param conf configuration object.
   * @return VectoredIOContext.
   */
  private VectoredIOContext populateVectoredIOContext(Configuration conf) {
    final int minSeekVectored = (int) longBytesOption(conf, AWS_S3_VECTOR_READS_MIN_SEEK_SIZE,
            DEFAULT_AWS_S3_VECTOR_READS_MIN_SEEK_SIZE, 0);
    final int maxReadSizeVectored = (int) longBytesOption(conf, AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
            DEFAULT_AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE, 0);
    return new VectoredIOContext()
            .setMinSeekForVectoredReads(minSeekVectored)
            .setMaxReadSizeForVectoredReads(maxReadSizeVectored)
            .build();
  }

  /**
   * Test bucket existence in S3.
   * When the value of {@link Constants#S3A_BUCKET_PROBE} is set to 0,
   * bucket existence check is not done to improve performance of
   * S3AFileSystem initialization. When set to 1 or 2, bucket existence check
   * will be performed which is potentially slow.
   * If 3 or higher: warn and skip check.
   * Also logging DNS address of the s3 endpoint if the bucket probe value is
   * greater than 0 else skipping it for increased performance.
   * @throws UnknownStoreException the bucket is absent
   * @throws IOException any other problem talking to S3
   */
  @Retries.RetryTranslated
  private void doBucketProbing() throws IOException {
    int bucketProbe = getConf()
            .getInt(S3A_BUCKET_PROBE, S3A_BUCKET_PROBE_DEFAULT);
    Preconditions.checkArgument(bucketProbe >= 0,
            "Value of " + S3A_BUCKET_PROBE + " should be >= 0");
    switch (bucketProbe) {
    case 0:
      LOG.debug("skipping check for bucket existence");
      break;
    case 1:
    case 2:
      logDnsLookup(getConf());
      verifyBucketExists();
      break;
    default:
      // we have no idea what this is, assume it is from a later release.
      LOG.warn("Unknown bucket probe option {}: {}; skipping check for bucket existence",
          S3A_BUCKET_PROBE, bucketProbe);
      break;
    }
  }

  /**
   * Initialize the statistics binding.
   * This is done by creating an {@code IntegratedS3AStatisticsContext}
   * with callbacks to get the FS's instrumentation and FileSystem.statistics
   * field; the latter may change after {@link #initialize(URI, Configuration)},
   * so needs to be dynamically adapted.
   * Protected so that (mock) subclasses can replace it with a
   * different statistics binding, if desired.
   */
  protected void initializeStatisticsBinding() {
    storageStatistics = createStorageStatistics(
        requireNonNull(getIOStatistics()));
    statisticsContext = new BondedS3AStatisticsContext(
        new BondedS3AStatisticsContext.S3AFSStatisticsSource() {

          @Override
          public S3AInstrumentation getInstrumentation() {
            return S3AFileSystem.this.getInstrumentation();
          }

          @Override
          public Statistics getInstanceStatistics() {
            return S3AFileSystem.this.statistics;
          }
        });
  }

  /**
   * Initialize the thread pools.
   * <p>
   * This must be re-invoked after replacing the S3Client during test
   * runs.
   * <p>
   * It requires the S3Store to have been instantiated.
   * @param conf configuration.
   */
  private void initThreadPools() {

    Configuration conf = getConf();

    final String name = "s3a-transfer-" + getBucket();
    int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
    if (maxThreads < 2) {
      LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }
    int totalTasks = intOption(conf,
        MAX_TOTAL_TASKS, DEFAULT_MAX_TOTAL_TASKS, 1);
    // keepalive time takes a time suffix; default unit is seconds
    long keepAliveTime = ConfigurationHelper.getDuration(conf,
            KEEPALIVE_TIME,
            Duration.ofSeconds(DEFAULT_KEEPALIVE_TIME),
            TimeUnit.SECONDS,
            Duration.ZERO).getSeconds();

    final StreamThreadOptions threadRequirements =
        getStore().threadRequirements();
    int numPrefetchThreads = threadRequirements.sharedThreads();

    int activeTasksForBoundedThreadPool = maxThreads;
    int waitingTasksForBoundedThreadPool = maxThreads + totalTasks + numPrefetchThreads;
    boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
        activeTasksForBoundedThreadPool,
        waitingTasksForBoundedThreadPool,
        keepAliveTime, TimeUnit.SECONDS,
        name + "-bounded");
    unboundedThreadPool = new ThreadPoolExecutor(
        maxThreads, Integer.MAX_VALUE,
        keepAliveTime, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        BlockingThreadPoolExecutorService.newDaemonThreadFactory(
            name + "-unbounded"));
    unboundedThreadPool.allowCoreThreadTimeOut(true);
    executorCapacity = intOption(conf,
        EXECUTOR_CAPACITY, DEFAULT_EXECUTOR_CAPACITY, 1);
    if (threadRequirements.createFuturePool()) {
      // create a future pool.
      final S3AInputStreamStatistics s3AInputStreamStatistics =
          statisticsContext.newInputStreamStatistics();
      futurePool = new ExecutorServiceFuturePool(
          new SemaphoredDelegatingExecutor(
              boundedThreadPool,
              activeTasksForBoundedThreadPool + waitingTasksForBoundedThreadPool,
              true,
              s3AInputStreamStatistics));
    }
  }

  /**
   * Create the storage statistics or bind to an existing one.
   * @param ioStatistics IOStatistics to build the storage statistics from.
   * @return a storage statistics instance; expected to be that of the FS.
   */
  protected static S3AStorageStatistics createStorageStatistics(
      final IOStatistics ioStatistics) {
    return (S3AStorageStatistics)
        GlobalStorageStatistics.INSTANCE
            .put(S3AStorageStatistics.NAME,
                () -> new S3AStorageStatistics(ioStatistics));
  }

  /**
   * Verify that the bucket exists.
   * Retry policy: retrying, translated.
   * @throws UnknownStoreException the bucket is absent
   * @throws IOException any other problem talking to S3
   */
  @Retries.RetryTranslated
  protected void verifyBucketExists() throws UnknownStoreException, IOException {

    if(!trackDurationAndSpan(
        STORE_EXISTS_PROBE, bucket, null, () ->
            invoker.retry("doesBucketExist", bucket, true, () -> {
              try {
                getS3Client().headBucket(HeadBucketRequest.builder().bucket(bucket).build());
                return true;
              } catch (AwsServiceException ex) {
                int statusCode = ex.statusCode();
                if (statusCode == SC_404_NOT_FOUND ||
                    (statusCode == SC_403_FORBIDDEN && accessPoint != null)) {
                  return false;
                }
              }

              return true;
            }))) {

      throw new UnknownStoreException("s3a://" + bucket + "/",
          " Bucket does " + "not exist. " + "Accessing with " + ENDPOINT + " set to "
              + getConf().getTrimmed(ENDPOINT, null));
    }
  }

  /**
   * Get S3A Instrumentation. For test purposes.
   * @return this instance's instrumentation.
   */
  @VisibleForTesting
  public S3AInstrumentation getInstrumentation() {
    return instrumentation;
  }

  /**
   * Get FS Statistic for this S3AFS instance.
   *
   * @return FS statistic instance.
   */
  @VisibleForTesting
  public FileSystem.Statistics getFsStatistics() {
    return statistics;
  }

  /**
   * Get current listing instance.
   * @return this instance's listing.
   */
  public Listing getListing() {
    return listing;
  }

  /**
   * Set up the client bindings.
   * If delegation tokens are enabled, the FS first looks for a DT
   * ahead of any other bindings.
   * If there is a DT it uses that to do the auth
   * and switches to the DT authenticator automatically (and exclusively).
   * <p>
   * Delegation tokens are configured and started, but the actual
   * S3 clients are not: instead a {@link ClientManager} is created
   * and returned, from which they can be created on demand.
   * This is to reduce delays in FS initialization, especially
   * for features (transfer manager, async client) which are not
   * always used.
   * @param fsURI URI of the FS
   * @param dtEnabled are delegation tokens enabled?
   * @return the client manager which can generate the clients.
   * @throws IOException failure.
   */
  private ClientManager createClientManager(URI fsURI, boolean dtEnabled) throws IOException {
    Configuration conf = getConf();
    credentials = null;
    String uaSuffix = "";

    if (dtEnabled) {
      // Delegation support.
      // Create and start the DT integration.
      // Then look for an existing DT for this bucket, switch to authenticating
      // with it if so.

      LOG.debug("Using delegation tokens");
      S3ADelegationTokens tokens = new S3ADelegationTokens();
      this.delegationTokens = Optional.of(tokens);
      tokens.bindToFileSystem(getCanonicalUri(),
          createStoreContext(),
          createDelegationOperations());
      tokens.init(conf);
      tokens.start();
      // switch to the DT provider and bypass all other configured
      // providers.
      if (tokens.isBoundToDT()) {
        // A DT was retrieved.
        LOG.debug("Using existing delegation token");
        // and use the encryption settings from that client, whatever they were
      } else {
        LOG.debug("No delegation token for this instance");
      }
      // Get new credential chain
      credentials = tokens.getCredentialProviders();
      // and any encryption secrets which came from a DT
      tokens.getEncryptionSecrets()
          .ifPresent(this::setEncryptionSecrets);
      // and update the UA field with any diagnostics provided by
      // the DT binding.
      uaSuffix = tokens.getUserAgentField();
    } else {
      // DT support is disabled, so create the normal credential chain
      credentials = createAWSCredentialProviderList(fsURI, conf);
    }
    LOG.debug("Using credential provider {}", credentials);

    S3ClientFactory clientFactory = fsHandler.getS3ClientFactory(conf);
    S3ClientFactory unencryptedClientFactory = fsHandler.getUnencryptedS3ClientFactory(conf);
    CSEMaterials cseMaterials = fsHandler.getClientSideEncryptionMaterials(conf, bucket,
        getS3EncryptionAlgorithm());

    S3ClientFactory.S3ClientCreationParameters parameters =
        new S3ClientFactory.S3ClientCreationParameters()
        .withCredentialSet(credentials)
        .withPathUri(fsURI)
        .withEndpoint(endpoint)
        .withMetrics(statisticsContext.newStatisticsFromAwsSdk())
        .withPathStyleAccess(conf.getBoolean(PATH_STYLE_ACCESS, false))
        .withUserAgentSuffix(uaSuffix)
        .withRequesterPays(conf.getBoolean(ALLOW_REQUESTER_PAYS, DEFAULT_ALLOW_REQUESTER_PAYS))
        .withExecutionInterceptors(auditManager.createExecutionInterceptors())
        .withMinimumPartSize(partSize)
        .withMultipartCopyEnabled(isMultipartCopyEnabled)
        .withMultipartThreshold(multiPartThreshold)
        .withTransferManagerExecutor(unboundedThreadPool)
        .withRegion(configuredRegion)
        .withFipsEnabled(fipsEnabled)
        .withExpressCreateSession(
            conf.getBoolean(S3EXPRESS_CREATE_SESSION, S3EXPRESS_CREATE_SESSION_DEFAULT))
        .withChecksumValidationEnabled(
            conf.getBoolean(CHECKSUM_VALIDATION, CHECKSUM_VALIDATION_DEFAULT))
        .withClientSideEncryptionEnabled(isCSEEnabled)
        .withClientSideEncryptionMaterials(cseMaterials)
        .withKMSRegion(conf.get(S3_ENCRYPTION_CSE_KMS_REGION));

    // this is where clients and the transfer manager are created on demand.
    return createClientManager(clientFactory, unencryptedClientFactory, parameters,
        getDurationTrackerFactory());
  }

  /**
   * Create the Client Manager; protected to allow for mocking.
   * Requires {@link #unboundedThreadPool} to be initialized.
   * @param clientFactory (reflection-bonded) client factory.
   * @param unencryptedClientFactory (reflection-bonded) client factory.
   * @param clientCreationParameters parameters for client creation.
   * @param durationTrackerFactory factory for duration tracking.
   * @return a client manager instance.
   */
  @VisibleForTesting
  protected ClientManager createClientManager(
      final S3ClientFactory clientFactory,
      final S3ClientFactory unencryptedClientFactory,
      final S3ClientFactory.S3ClientCreationParameters clientCreationParameters,
      final DurationTrackerFactory durationTrackerFactory) {
    return new ClientManagerImpl(clientFactory,
        unencryptedClientFactory,
        clientCreationParameters,
        durationTrackerFactory
    );
  }

  /**
   * Initialize and launch the audit manager and service.
   * As this takes the FS IOStatistics store, it must be invoked
   * after instrumentation is initialized.
   * @throws IOException failure to instantiate/initialize.
   */
  protected void initializeAuditService() throws IOException {
    auditManager = AuditIntegration.createAndStartAuditManager(
        getConf(),
        instrumentation.createMetricsUpdatingStore());
  }

  /**
   * The audit manager.
   * @return the audit manager
   */
  @InterfaceAudience.Private
  public AuditManagerS3A getAuditManager() {
    return auditManager;
  }

  /**
   * Get the auditor; valid once initialized.
   * @return the auditor.
   */
  @InterfaceAudience.Private
  public OperationAuditor getAuditor() {
    return getAuditManager().getAuditor();
  }

  /**
   * Get the active audit span.
   * @return the span.
   */
  @InterfaceAudience.Private
  @Override
  public AuditSpanS3A getActiveAuditSpan() {
    return getAuditManager().getActiveAuditSpan();
  }

  /**
   * Get the audit span source; allows for components like the committers
   * to have a source of spans without being hard coded to the FS only.
   * @return the source of spans -base implementation is this instance.
   */
  @InterfaceAudience.Private
  public AuditSpanSource getAuditSpanSource() {
    return this;
  }

  /**
   * Start an operation; this informs the audit service of the event
   * and then sets it as the active span.
   * @param operation operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   * @throws IOException failure
   */
  public AuditSpanS3A createSpan(String operation,
      @Nullable String path1,
      @Nullable String path2)
      throws IOException {

    return getAuditManager().createSpan(operation, path1, path2);
  }

  /**
   * Build the request factory.
   * MUST be called after reading encryption secrets from settings/
   * delegation token.
   * Protected, in case test/mock classes want to implement their
   * own variants.
   * @return request factory.
   */
  protected RequestFactory createRequestFactory() {
    long partCountLimit = longOption(getConf(),
        UPLOAD_PART_COUNT_LIMIT,
        DEFAULT_UPLOAD_PART_COUNT_LIMIT,
        1);
    if (partCountLimit != DEFAULT_UPLOAD_PART_COUNT_LIMIT) {
      LOG.warn("Configuration property {} shouldn't be overridden by client",
          UPLOAD_PART_COUNT_LIMIT);
    }

    // ACLs; this is passed to the
    // request factory.
    initCannedAcls(getConf());

    // Any encoding type
    String contentEncoding = getConf().getTrimmed(CONTENT_ENCODING, null);
    if (contentEncoding != null) {
      LOG.debug("Using content encoding set in {} = {}", CONTENT_ENCODING,  contentEncoding);
    }

    String storageClassConf = getConf()
        .getTrimmed(STORAGE_CLASS, "")
        .toUpperCase(Locale.US);
    StorageClass storageClass = null;
    if (!storageClassConf.isEmpty()) {
      storageClass = StorageClass.fromValue(storageClassConf);
      LOG.debug("Using storage class {}", storageClass);
      if (storageClass.equals(StorageClass.UNKNOWN_TO_SDK_VERSION)) {
        STORAGE_CLASS_WARNING.warn("Unknown storage class \"{}\" from option: {};"
                + " falling back to default storage class",
            storageClassConf, STORAGE_CLASS);
        storageClass = null;
      }

    } else {
      LOG.debug("Unset storage class property {}; falling back to default storage class",
          STORAGE_CLASS);
    }

    // optional custom timeout for bulk uploads
    Duration partUploadTimeout = ConfigurationHelper.getDuration(getConf(),
        PART_UPLOAD_TIMEOUT,
        DEFAULT_PART_UPLOAD_TIMEOUT,
        TimeUnit.MILLISECONDS,
        Duration.ZERO);

    return RequestFactoryImpl.builder()
        .withBucket(requireNonNull(bucket))
        .withCannedACL(getCannedACL())
        .withEncryptionSecrets(requireNonNull(encryptionSecrets))
        .withMultipartPartCountLimit(partCountLimit)
        .withRequestPreparer(getAuditManager()::requestCreated)
        .withContentEncoding(contentEncoding)
        .withStorageClass(storageClass)
        .withMultipartUploadEnabled(isMultipartUploadEnabled)
        .withPartUploadTimeout(partUploadTimeout)
        .build();
  }

  /**
   * Get the request factory which uses this store's audit span.
   * @return the request factory.
   */
  @VisibleForTesting
  public RequestFactory getRequestFactory() {
    return requestFactory;
  }

  /**
   * Get the performance flags.
   * @return performance flags.
   */
  public FlagSet<PerformanceFlagEnum> getPerformanceFlags() {
    return performanceFlags;
  }


  /**
   * Get the store for low-level operations.
   * @return the store the S3A FS is working through.
   */
  private S3AStore getStore() {
    return store;
  }

  /**
   * Implementation of all operations used by delegation tokens.
   */
  private class DelegationOperationsImpl implements DelegationOperations {

    @Override
    public List<RoleModel.Statement> listAWSPolicyRules(final Set<AccessLevel> access) {
      return S3AFileSystem.this.listAWSPolicyRules(access);
    }
  }

  /**
   * Create an instance of the delegation operations.
   * @return callbacks for DT support.
   */
  @VisibleForTesting
  public DelegationOperations createDelegationOperations() {
    return new DelegationOperationsImpl();
  }

  /**
   * Set the encryption secrets for requests.
   * @param secrets secrets
   */
  protected void setEncryptionSecrets(final EncryptionSecrets secrets) {
    this.encryptionSecrets = secrets;
    if (requestFactory != null) {
      requestFactory.setEncryptionSecrets(secrets);
    }
  }

  /**
   * Get the encryption secrets.
   * This potentially sensitive information and must be treated with care.
   * @return the current encryption secrets.
   */
  public EncryptionSecrets getEncryptionSecrets() {
    return encryptionSecrets;
  }

  private void initCannedAcls(Configuration conf) {
    String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
    if (!cannedACLName.isEmpty()) {
      cannedACL = AWSCannedACL.valueOf(cannedACLName).toString();
    } else {
      cannedACL = null;
    }
  }

  @Retries.RetryTranslated
  private void initMultipartUploads(Configuration conf) throws IOException {
    boolean purgeExistingMultipart = conf.getBoolean(PURGE_EXISTING_MULTIPART,
        DEFAULT_PURGE_EXISTING_MULTIPART);

    if (purgeExistingMultipart) {
      try {
        Duration purgeDuration = ConfigurationHelper.getDuration(conf,
            PURGE_EXISTING_MULTIPART_AGE,
            Duration.ofSeconds(DEFAULT_PURGE_EXISTING_MULTIPART_AGE),
            TimeUnit.SECONDS,
            Duration.ZERO);
        abortOutstandingMultipartUploads(purgeDuration.getSeconds());
      } catch (AccessDeniedException e) {
        instrumentation.errorIgnored();
        LOG.debug("Failed to purge multipart uploads against {}," +
            " FS may be read only", bucket);
      }
    }
  }

  /**
   * Abort all outstanding MPUs older than a given age.
   * @param seconds time in seconds
   * @throws IOException on any failure, other than 403 "permission denied"
   */
  @Retries.RetryTranslated
  public void abortOutstandingMultipartUploads(long seconds)
      throws IOException {
    Preconditions.checkArgument(seconds >= 0);
    Instant purgeBefore =
        Instant.now().minusSeconds(seconds);
    LOG.debug("Purging outstanding multipart uploads older than {}",
        purgeBefore);
    invoker.retry("Purging multipart uploads", bucket, true,
        () -> {
          RemoteIterator<MultipartUpload> uploadIterator =
              MultipartUtils.listMultipartUploads(createStoreContext(),
                  getS3Client(), null, maxKeys);

          while (uploadIterator.hasNext()) {
            MultipartUpload upload = uploadIterator.next();
            if (upload.initiated().compareTo(purgeBefore) < 0) {
              abortMultipartUpload(upload);
            }
          }
        });
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return "s3a"
   */
  @Override
  public String getScheme() {
    return this.scheme;
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Set the URI field through {@link S3xLoginHelper} and
   * optionally {@link #canonicalizeUri(URI)}
   * Exported for testing.
   * @param fsUri filesystem URI.
   * @param canonicalize true if the URI should be canonicalized.
   */
  @VisibleForTesting
  protected void setUri(URI fsUri, boolean canonicalize) {
    URI u = S3xLoginHelper.buildFSURI(fsUri);
    this.uri = canonicalize ? u : canonicalizeUri(u);
  }

  /**
   * Get the canonical URI.
   * @return the canonical URI of this FS.
   */
  public URI getCanonicalUri() {
    return uri;
  }

  @VisibleForTesting
  @Override
  public int getDefaultPort() {
    return 0;
  }

  /**
   * Set the client -used in mocking tests to force in a different client.
   * @param client client.
   */
  @VisibleForTesting
  protected void setAmazonS3Client(S3Client client) {
    Preconditions.checkNotNull(client, "clientV2");
    LOG.debug("Setting S3V2 client to {}", client);
    s3Client = client;
  }

  /**
   * Get the S3 client created in {@link #initialize(URI, Configuration)}.
   * @return the s3Client
   * @throws UncheckedIOException if the client could not be created.
   */
  @VisibleForTesting
  protected S3Client getS3Client() {
    return s3Client;
  }

  /**
   * S3AInternals method.
   * {@inheritDoc}.
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  public String getBucketLocation() throws IOException {
    return s3aInternals.getBucketLocation(bucket);
  }

  /**
   * Create the S3AInternals; left as something mocking
   * subclasses may want to override.
   * @return the internal implementation
   */
  protected S3AInternals createS3AInternals() {
    return new S3AInternalsImpl();
  }

  /**
   * Get the S3AInternals.
   * @return the internal implementation
   */
  public S3AInternals getS3AInternals() {
    return s3aInternals;
  }

  /**
   * Implementation of the S3A Internals operations; pulled out of S3AFileSystem to
   * force code accessing it to call {@link #getS3AInternals()}.
   */
  private final class S3AInternalsImpl implements S3AInternals {

    @Override
    public S3Client getAmazonS3Client(String reason) {
      LOG.debug("Access to S3 client requested, reason {}", reason);
      return getS3Client();
    }

    @Override
    public S3AStore getStore() {
      return S3AFileSystem.this.getStore();
    }

    /**
     * S3AInternals method.
     * {@inheritDoc}.
     */
    @Override
    @AuditEntryPoint
    @Retries.RetryTranslated
    public String getBucketLocation() throws IOException {
      return s3aInternals.getBucketLocation(bucket);
    }

    /**
     * S3AInternals method.
     * {@inheritDoc}.
     */
    @Override
    @AuditEntryPoint
    @Retries.RetryTranslated
    public String getBucketLocation(String bucketName) throws IOException {
      final String region = trackDurationAndSpan(
          STORE_EXISTS_PROBE, bucketName, null, () ->
              invoker.retry("getBucketLocation()", bucketName, true, () ->
                  // If accessPoint then region is known from Arn
                  accessPoint != null
                      ? accessPoint.getRegion()
                      : getS3Client().getBucketLocation(GetBucketLocationRequest.builder()
                          .bucket(bucketName)
                          .build())
                      .locationConstraintAsString()));
      return fixBucketRegion(region);
    }

    /**
     * S3AInternals method.
     * {@inheritDoc}.
     */
    @Override
    @AuditEntryPoint
    @Retries.RetryTranslated
    public HeadObjectResponse getObjectMetadata(Path path) throws IOException {
      return trackDurationAndSpan(INVOCATION_GET_FILE_STATUS, path, () ->
          S3AFileSystem.this.getObjectMetadata(makeQualified(path), null, invoker,
              "getObjectMetadata"));
    }

    /**
     * S3AInternals method.
     * {@inheritDoc}.
     */
    @Override
    @AuditEntryPoint
    @Retries.RetryTranslated
    public HeadBucketResponse getBucketMetadata() throws IOException {
      return S3AFileSystem.this.getBucketMetadata();
    }

    /**
     * Get a shared copy of the AWS credentials, with its reference
     * counter updated.
     * Caller is required to call {@code close()} on this after
     * they have finished using it.
     * @param purpose what is this for? This is initially for logging
     * @return a reference to shared credentials.
     */
    public AWSCredentialProviderList shareCredentials(final String purpose) {
      LOG.debug("Sharing credentials for: {}", purpose);
      return credentials.share();
    }

    @Override
    public boolean isMultipartCopyEnabled() {
      return S3AFileSystem.this.isMultipartUploadEnabled;
    }

    @Override
    public long abortMultipartUploads(final Path path) throws IOException {
      final String prefix = pathToKey(path);
      try (AuditSpan span = createSpan("object_multipart_bulk_abort", prefix, null)) {
        return S3AFileSystem.this.abortMultipartUploadsUnderPrefix(
            createStoreContext(),
            span,
            prefix);
      }
    }

  } // end S3AInternals

  /**
   * Get the input policy for this FS instance.
   * @return the input policy
   */
  @InterfaceStability.Unstable
  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  /**
   * Get the change detection policy for this FS instance.
   * Only public to allow access in tests in other packages.
   * @return the change detection policy
   */
  @VisibleForTesting
  public ChangeDetectionPolicy getChangeDetectionPolicy() {
    return changeDetectionPolicy;
  }

  /**
   * Get the encryption algorithm of this connector.
   * @return the encryption algorithm.
   */
  public S3AEncryptionMethods getS3EncryptionAlgorithm() {
    return encryptionSecrets.getEncryptionMethod();
  }

  /**
   * Demand create the directory allocator, then create a temporary file.
   * This does not mark the file for deletion when a process exits.
   * {@link LocalDirAllocator#createTmpFileForWrite(String, long, Configuration)}.
   * @param pathStr prefix for the temporary file
   * @param size the size of the file that is going to be written
   * @param conf the Configuration object
   * @return a unique temporary file
   * @throws IOException IO problems
   */
  File createTmpFileForWrite(String pathStr, long size,
      Configuration conf) throws IOException {

    return getS3AInternals().getStore().createTemporaryFileForWriting(pathStr, size, conf);
  }

  /**
   * Get the bucket of this filesystem.
   * @return the bucket
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public String getBucket() {
    return bucket;
  }

  /**
   * Set the bucket.
   * @param bucket the bucket
   */
  @VisibleForTesting
  protected void setBucket(String bucket) {
    this.bucket = bucket;
  }

  /**
   * Get the canned ACL of this FS.
   * @return an ACL, if any
   */
  String getCannedACL() {
    return cannedACL;
  }

  /**
   * Change the input policy for this FS.
   * This is now a no-op, retained in case some application
   * or external test invokes it.
   *
   * @deprecated use openFile() options
   * @param inputPolicy new policy
   */
  @InterfaceStability.Unstable
  @Deprecated
  public void setInputPolicy(S3AInputPolicy inputPolicy) {
    LOG.warn("setInputPolicy is no longer supported");
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key.
   *
   * @param path input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  @VisibleForTesting
  public String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key, adding a trailing
   * "/" if the path is not the root <i>and</i> does not already have a "/"
   * at the end.
   *
   * @param key s3 key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  @InterfaceAudience.Private
  public String maybeAddTrailingSlash(String key) {
    return S3AUtils.maybeAddTrailingSlash(key);
  }

  /**
   * Convert a path back to a key.
   * @param key input key
   * @return the path from this key
   */
  Path keyToPath(String key) {
    return new Path("/" + key);
  }

  /**
   * Convert a key to a fully qualified path.
   * This includes fixing up the URI so that if it ends with a trailing slash,
   * that is corrected, similar to {@code Path.normalizePath()}.
   * @param key input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  public Path keyToQualifiedPath(String key) {
    return qualify(keyToPath(key));
  }

  @Override
  public Path makeQualified(final Path path) {
    Path q = super.makeQualified(path);
    if (!q.isRoot()) {
      String urlString = q.toUri().toString();
      if (urlString.endsWith(Path.SEPARATOR)) {
        // this is a path which needs root stripping off to avoid
        // confusion, See HADOOP-15430
        LOG.debug("Stripping trailing '/' from {}", q);
        // deal with an empty "/" at the end by mapping to the parent and
        // creating a new path from it
        q = new Path(urlString.substring(0, urlString.length() - 1));
      }
    }
    if (!q.isRoot() && q.getName().isEmpty()) {
      q = q.getParent();
    }
    return q;
  }

  /**
   * Qualify a path.
   * This includes fixing up the URI so that if it ends with a trailing slash,
   * that is corrected, similar to {@code Path.normalizePath()}.
   * @param path path to qualify
   * @return a qualified path.
   */
  public Path qualify(Path path) {
    return makeQualified(path);
  }

  /**
   * Check that a Path belongs to this FileSystem.
   * Unlike the superclass, this version does not look at authority,
   * only hostnames.
   * @param path to check
   * @throws IllegalArgumentException if there is an FS mismatch
   */
  @Override
  public void checkPath(Path path) {
    S3xLoginHelper.checkPath(getConf(), getUri(), path, getDefaultPort());
  }

  /**
   * Override the base canonicalization logic and relay to
   * {@link S3xLoginHelper#canonicalizeUri(URI, int)}.
   * This allows for the option of changing this logic for better DT handling.
   * @param rawUri raw URI.
   * @return the canonical URI to use in delegation tokens and file context.
   */
  @Override
  protected URI canonicalizeUri(URI rawUri) {
    return S3xLoginHelper.canonicalizeUri(rawUri, getDefaultPort());
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Retries.RetryTranslated
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    return executeOpen(qualify(f),
        openFileHelper.openSimpleFile(bufferSize));
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * The {@code fileInformation} parameter controls how the file
   * is opened, can a HEAD be skipped, etc.
   * @param path the file to open
   * @param fileInformation information about the file to open
   * @throws IOException IO failure.
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  private FSDataInputStream executeOpen(
      final Path path,
      final OpenFileSupport.OpenFileInformation fileInformation)
      throws IOException {
    // create the input stream statistics before opening
    // the file so that the time to prepare to open the file is included.
    S3AInputStreamStatistics inputStreamStats =
        statisticsContext.newInputStreamStatistics();
    // this span is passed into the stream.
    final AuditSpan auditSpan = entryPoint(INVOCATION_OPEN, path);
    final S3AFileStatus fileStatus =
        trackDuration(inputStreamStats,
            ACTION_FILE_OPENED.getSymbol(), () ->
            extractOrFetchSimpleFileStatus(path, fileInformation));
    S3AReadOpContext readContext = createReadContext(
        fileStatus,
        auditSpan);
    fileInformation.applyOptions(readContext);
    LOG.debug("Opening '{}'", readContext);

    // what does the stream need
    final StreamThreadOptions requirements =
        getStore().threadRequirements();

    // calculate the permit count.
    final int permitCount = requirements.streamThreads() +
        (requirements.vectorSupported()
            ? vectoredActiveRangeReads
            : 0);
    // create an executor which is a subset of the
    // bounded thread pool.
    final SemaphoredDelegatingExecutor pool = new SemaphoredDelegatingExecutor(
        boundedThreadPool,
        permitCount,
        true,
        inputStreamStats);

    // do not validate() the parameters as the store
    // completes this.
    ObjectReadParameters parameters = new ObjectReadParameters()
        .withBoundedThreadPool(pool)
        .withCallbacks(createInputStreamCallbacks(auditSpan))
        .withContext(readContext.build())
        .withObjectAttributes(createObjectAttributes(path, fileStatus))
        .withStreamStatistics(inputStreamStats);
    return new FSDataInputStream(getStore().readObject(parameters));
  }

  /**
   * Override point: create the callbacks for ObjectInputStream.
   * @return an implementation of callbacks,
   */
  private ObjectInputStreamCallbacks createInputStreamCallbacks(
      final AuditSpan auditSpan) {
    return new InputStreamCallbacksImpl(auditSpan, getStore(), fsHandler, unboundedThreadPool);
  }


  /**
   * Callbacks for WriteOperationHelper.
   */
  private final class WriteOperationHelperCallbacksImpl
      implements WriteOperationHelper.WriteOperationHelperCallbacks {

    @Override
    @Retries.OnceRaw
    public CompleteMultipartUploadResponse completeMultipartUpload(
        CompleteMultipartUploadRequest request) {
      return getStore().completeMultipartUpload(request);
    }

    @Override
    @Retries.OnceRaw
    public UploadPartResponse uploadPart(
        final UploadPartRequest request,
        final RequestBody body,
        final DurationTrackerFactory durationTrackerFactory)
        throws AwsServiceException, UncheckedIOException {
      return getStore().uploadPart(request, body, durationTrackerFactory);
    }

  }

  /**
   * Create the read context for reading from the referenced file,
   * using FS state as well as the status.
   * @param fileStatus file status.
   * @param auditSpan audit span.
   * @return a context for read operations.
   */
  @VisibleForTesting
  protected S3AReadOpContext createReadContext(
      final FileStatus fileStatus,
      final AuditSpan auditSpan) {
    final S3AReadOpContext roc = new S3AReadOpContext(
        fileStatus.getPath(),
        invoker,
        statistics,
        statisticsContext,
        fileStatus,
        vectoredIOContext,
        IOStatisticsContext.getCurrentIOStatisticsContext().getAggregator(),
        futurePool
    )
        .withAuditSpan(auditSpan);
    openFileHelper.applyDefaultOptions(roc);
    return roc.build();
  }

  /**
   * Create the attributes of an object for subsequent use.
   * @param f path path of the request.
   * @param eTag the eTag of the S3 object
   * @param versionId S3 object version ID
   * @param len length of the file
   * @return attributes to use when building the query.
   */
  private S3ObjectAttributes createObjectAttributes(
      final Path f,
      final String eTag,
      final String versionId,
      final long len) {
    return new S3ObjectAttributes(bucket,
        f,
        pathToKey(f),
        getS3EncryptionAlgorithm(),
        encryptionSecrets.getEncryptionKey(),
        eTag,
        versionId,
        len);
  }

  /**
   * Create the attributes of an object for subsequent use.
   * @param path path -this is used over the file status path.
   * @param fileStatus file status to build from.
   * @return attributes to use when building the query.
   */
  private S3ObjectAttributes createObjectAttributes(
      final Path path,
      final S3AFileStatus fileStatus) {
    return createObjectAttributes(
        path,
        fileStatus.getEtag(),
        fileStatus.getVersionId(),
        fileStatus.getLen());
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Retry policy: retrying, translated on the getFileStatus() probe.
   * No data is uploaded to S3 in this call, so retry issues related to that.
   * @param f the file name to open
   * @param permission the permission to set.
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the requested block size.
   * @param progress the progress reporter.
   * @throws IOException in the event of IO related errors.
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  @AuditEntryPoint
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    final Path path = qualify(f);

    // work out the options to pass down
    CreateFileBuilder.CreateFileOptions options;
    if (getPerformanceFlags().enabled(PerformanceFlagEnum.Create)) {
      options = OPTIONS_CREATE_FILE_PERFORMANCE;
    } else {
      options = overwrite
          ? OPTIONS_CREATE_FILE_OVERWRITE
          : OPTIONS_CREATE_FILE_NO_OVERWRITE;
    }

    // the span will be picked up inside the output stream
    return trackDurationAndSpan(INVOCATION_CREATE, path, () ->
        innerCreateFile(path,
            progress,
            getActiveAuditSpan(),
            options));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting; in the active span.
   * Retry policy: retrying, translated on the getFileStatus() probe.
   * No data is uploaded to S3 in this call, so no retry issues related to that.
   * The "performance" flag disables safety checks for the path being a file,
   * or parent directory existing.
   * If true, this method call does no IO at all.
   * @param path the file name to open
   * @param progress the progress reporter.
   * @param auditSpan audit span
   * @param options options for the file
   * @throws IOException in the event of IO related errors.
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  @Retries.RetryTranslated
  private FSDataOutputStream innerCreateFile(
      final Path path,
      final Progressable progress,
      final AuditSpan auditSpan,
      final CreateFileBuilder.CreateFileOptions options) throws IOException {
    auditSpan.activate();
    String key = pathToKey(path);
    if (key.isEmpty()) {
      // no matter the creation options, root cannot be written to.
      throw new PathIOException("/", "Can't create root path");
    }
    EnumSet<CreateFlag> flags = options.getFlags();

    boolean skipProbes = options.isPerformance() || isUnderMagicCommitPath(path);
    if (skipProbes) {
      LOG.debug("Skipping existence/overwrite checks");
    } else {
      try {
        boolean overwrite = flags.contains(CreateFlag.OVERWRITE);

        // get the status or throw an FNFE.
        // when overwriting, there is no need to look for any existing file,
        // just a directory (for safety)
        FileStatus status = innerGetFileStatus(path, false,
            overwrite
                ? StatusProbeEnum.DIRECTORIES
                : StatusProbeEnum.ALL);

        // if the thread reaches here, there is something at the path
        if (status.isDirectory()) {
          // path references a directory: automatic error
          throw new FileAlreadyExistsException(path + " is a directory");
        }
        if (!overwrite) {
          // path references a file and overwrite is disabled
          throw new FileAlreadyExistsException(path + " already exists");
        }
        LOG.debug("Overwriting file {}", path);
      } catch (FileNotFoundException e) {
        // this means there is nothing at the path; all good.
      }
    }
    instrumentation.fileCreated();
    final BlockOutputStreamStatistics outputStreamStatistics
        = statisticsContext.newOutputStreamStatistics();
    PutTracker putTracker =
        committerIntegration.createTracker(path, key, outputStreamStatistics);
    String destKey = putTracker.getDestKey();

    // put options are derived from the option builder.
    final PutObjectOptions putOptions =
        new PutObjectOptions(null, options.getHeaders());

    validateOutputStreamConfiguration(path, getConf());

    final S3ABlockOutputStream.BlockOutputStreamBuilder builder =
        S3ABlockOutputStream.builder()
        .withKey(destKey)
        .withBlockFactory(blockFactory)
        .withBlockSize(partSize)
        .withStatistics(outputStreamStatistics)
        .withProgress(progress)
        .withPutTracker(putTracker)
        .withWriteOperations(
            createWriteOperationHelper(auditSpan))
        .withExecutorService(
            new SemaphoredDelegatingExecutor(
                boundedThreadPool,
                blockOutputActiveBlocks,
                true,
                outputStreamStatistics))
        .withDowngradeSyncableExceptions(
            getConf().getBoolean(
                DOWNGRADE_SYNCABLE_EXCEPTIONS,
                DOWNGRADE_SYNCABLE_EXCEPTIONS_DEFAULT))
        .withCSEEnabled(isCSEEnabled)
        .withPutOptions(putOptions)
        .withIOStatisticsAggregator(
            IOStatisticsContext.getCurrentIOStatisticsContext().getAggregator())
        .withMultipartEnabled(isMultipartUploadEnabled);
    return new FSDataOutputStream(
        new S3ABlockOutputStream(builder),
        null);
  }
  /**
   * Create a Write Operation Helper with the current active span.
   * All operations made through this helper will activate the
   * span before execution.
   *
   * This class permits other low-level operations against the store.
   * It is unstable and
   * only intended for code with intimate knowledge of the object store.
   * If using this, be prepared for changes even on minor point releases.
   * @return a new helper.
   */
  @InterfaceAudience.Private
  public WriteOperationHelper getWriteOperationHelper() {
    return createWriteOperationHelper(getActiveAuditSpan());
  }

  /**
   * Create a Write Operation Helper with the given span.
   * All operations made through this helper will activate the
   * span before execution.
   * @param auditSpan audit span
   * @return a new helper.
   */
  @InterfaceAudience.Private
  public WriteOperationHelper createWriteOperationHelper(AuditSpan auditSpan) {
    return new WriteOperationHelper(this,
        getConf(),
        statisticsContext,
        getAuditSpanSource(),
        auditSpan,
        new WriteOperationHelperCallbacksImpl());
  }

  /**
   * Create instance of an FSDataOutputStreamBuilder for
   * creating a file at the given path.
   * @param path path to create
   * @return a builder.
   * @throws UncheckedIOException for problems creating the audit span
   */
  @Override
  @AuditEntryPoint
  public FSDataOutputStreamBuilder createFile(final Path path) {
    try {
      final Path qualified = qualify(path);
      final AuditSpan span = entryPoint(INVOCATION_CREATE_FILE,
          pathToKey(qualified),
          null);
      final CreateFileBuilder builder = new CreateFileBuilder(this,
          qualified,
          new CreateFileBuilderCallbacksImpl(INVOCATION_CREATE_FILE, span));
      builder
          .create()
          .overwrite(true)
          .must(FS_S3A_CREATE_PERFORMANCE,
              getPerformanceFlags().enabled(PerformanceFlagEnum.Create));
      return builder;
    } catch (IOException e) {
      // catch any IOEs raised in span creation and convert to
      // an UncheckedIOException
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Callback for create file operations.
   */
  private final class CreateFileBuilderCallbacksImpl implements
      CreateFileBuilder.CreateFileBuilderCallbacks {

    private final Statistic statistic;
    /** span for operations. */
    private final AuditSpan span;

    private CreateFileBuilderCallbacksImpl(
        final Statistic statistic,
        final AuditSpan span) {
      this.statistic = statistic;
      this.span = span;
    }

    @Override
    public FSDataOutputStream createFileFromBuilder(
        final Path path,
        final Progressable progress,
        final CreateFileBuilder.CreateFileOptions options) throws IOException {
      // the span will be picked up inside the output stream
      return trackDuration(getDurationTrackerFactory(), statistic.getSymbol(), () ->
          innerCreateFile(path, progress, span, options));
    }
  }

  /**
   * {@inheritDoc}
   * The S3A implementations downgrades to the recursive creation, to avoid
   * any race conditions with parent entries "disappearing".
   */
  @Override
  @AuditEntryPoint
  public FSDataOutputStream createNonRecursive(Path p,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    final Path path = makeQualified(p);

    // span is created and passed in to the callbacks.
    final AuditSpan span = entryPoint(INVOCATION_CREATE_NON_RECURSIVE,
        pathToKey(path),
        null);
    // uses the CreateFileBuilder, filling it in with the relevant arguments.
    final CreateFileBuilder builder = new CreateFileBuilder(this,
        path,
        new CreateFileBuilderCallbacksImpl(INVOCATION_CREATE_NON_RECURSIVE, span))
        .create()
        .withFlags(flags)
        .blockSize(blockSize)
        .bufferSize(bufferSize)
        .must(FS_S3A_CREATE_PERFORMANCE,
            getPerformanceFlags().enabled(PerformanceFlagEnum.Create));
    if (progress != null) {
      builder.progress(progress);
    }
    return builder.build();
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException indicating that append is not supported.
   */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Append is not supported "
        + "by S3AFileSystem");
  }


  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * Warning: S3 does not support renames. This method does a copy which can
   * take S3 some time to execute with large files and directories. Since
   * there is no Progressable passed in, this can time out jobs.
   *
   * Note: This implementation differs with other S3 drivers. Specifically:
   * <pre>
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   * </pre>
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on IO failure
   * @return true if rename is successful
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  public boolean rename(Path src, Path dst) throws IOException {
    try {
      long bytesCopied = trackDurationAndSpan(
          INVOCATION_RENAME, src.toString(), dst.toString(), () ->
          innerRename(src, dst));
      LOG.debug("Copied {} bytes", bytesCopied);
      return true;
    } catch (SdkException e) {
      throw translateException("rename(" + src +", " + dst + ")", src, e);
    } catch (RenameFailedException e) {
      LOG.info("{}", e.getMessage());
      LOG.debug("rename failure", e);
      return e.getExitCode();
    }
  }

  /**
   * Validate the rename parameters and status of the filesystem;
   * returns the source and any destination File Status.
   * @param src qualified path to be renamed
   * @param dst qualified path after rename
   * @return the source and (possibly null) destination status entries.
   * @throws RenameFailedException if some criteria for a state changing
   * rename was not met. This means work didn't happen; it's not something
   * which is reported upstream to the FileSystem APIs, for which the semantics
   * of "false" are pretty vague.
   * @throws FileNotFoundException there's no source file.
   * @throws IOException on IO failure.
   */
  @Retries.RetryTranslated
  private Pair<S3AFileStatus, S3AFileStatus> initiateRename(
      final Path src,
      final Path dst) throws IOException {
    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "source is root directory");
    }
    if (dstKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "dest is root directory");
    }

    // get the source file status; this raises a FNFE if there is no source
    // file.
    S3AFileStatus srcStatus = innerGetFileStatus(src, true,
        StatusProbeEnum.ALL);

    if (srcKey.equals(dstKey)) {
      LOG.debug("rename: src and dest refer to the same file or directory: {}",
          dst);
      throw new RenameFailedException(src, dst,
          "source and dest refer to the same file or directory")
          .withExitCode(srcStatus.isFile());
    }

    S3AFileStatus dstStatus = null;
    try {
      dstStatus = innerGetFileStatus(dst, true, StatusProbeEnum.ALL);
      // if there is no destination entry, an exception is raised.
      // hence this code sequence can assume that there is something
      // at the end of the path; the only detail being what it is and
      // whether or not it can be the destination of the rename.
      if (srcStatus.isDirectory()) {
        if (dstStatus.isFile()) {
          throw new FileAlreadyExistsException(
              "Failed to rename " + src + " to " + dst
               +"; source is a directory and dest is a file");
        } else if (dstStatus.isEmptyDirectory() != Tristate.TRUE) {
          throw new RenameFailedException(src, dst,
              "Destination is a non-empty directory")
              .withExitCode(false);
        }
        // at this point the destination is an empty directory
      } else {
        // source is a file. The destination must be a directory,
        // empty or not
        if (dstStatus.isFile()) {
          throw new FileAlreadyExistsException(
              "Failed to rename " + src + " to " + dst
                  + "; destination file exists");
        }
      }

    } catch (FileNotFoundException e) {
      LOG.debug("rename: destination path {} not found", dst);
      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()
          && !parent.equals(src.getParent())) {
        try {
          // make sure parent isn't a file.
          // don't look for parent being a dir as there is a risk
          // of a race between dest dir cleanup and rename in different
          // threads.
          S3AFileStatus dstParentStatus = innerGetFileStatus(parent,
              false, StatusProbeEnum.FILE);
          // if this doesn't raise an exception then
          // the parent is a file or a dir.
          if (!dstParentStatus.isDirectory()) {
            throw new RenameFailedException(src, dst,
                "destination parent is not a directory");
          }
        } catch (FileNotFoundException expected) {
          // nothing was found. Don't worry about it;
          // expect rename to implicitly create the parent dir
        }
      }
    }
    return Pair.of(srcStatus, dstStatus);
  }

  /**
   * The inner rename operation. See {@link #rename(Path, Path)} for
   * the description of the operation.
   * This operation throws an exception on any failure which needs to be
   * reported and downgraded to a failure.
   * Retries: retry translated, assuming all operations it is called do
   * so. For safely, consider catch and handle SdkException
   * because this is such a complex method there's a risk it could surface.
   * @param source path to be renamed
   * @param dest new path after rename
   * @throws RenameFailedException if some criteria for a state changing
   * rename was not met. This means work didn't happen; it's not something
   * which is reported upstream to the FileSystem APIs, for which the semantics
   * of "false" are pretty vague.
   * @return the number of bytes copied.
   * @throws FileNotFoundException there's no source file.
   * @throws IOException on IO failure.
   * @throws SdkException on failures inside the AWS SDK
   */
  @Retries.RetryMixed
  private long innerRename(Path source, Path dest)
      throws RenameFailedException, FileNotFoundException, IOException,
      SdkException {
    Path src = qualify(source);
    Path dst = qualify(dest);

    LOG.debug("Rename path {} to {}", src, dst);

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    Pair<S3AFileStatus, S3AFileStatus> p = initiateRename(src, dst);

    // Initiate the rename.
    // this will call back into this class via the rename callbacks
    final StoreContext storeContext = createStoreContext();
    RenameOperation renameOperation = new RenameOperation(
        storeContext,
        src, srcKey, p.getLeft(),
        dst, dstKey, p.getRight(),
        new OperationCallbacksImpl(storeContext),
        pageSize,
        dirOperationsPurgeUploads);
    return renameOperation.execute();
  }

  @Override public Token<? extends TokenIdentifier> getFsDelegationToken()
      throws IOException {
    return getDelegationToken(null);
  }

  /**
   * The callbacks made by the rename and delete operations.
   * This separation allows the operation to be factored out and
   * still avoid knowledge of the S3AFilesystem implementation.
   * The Audit span active at the time of creation is cached and activated
   * before every call.
   */
  private final class OperationCallbacksImpl implements OperationCallbacks {

    /** Audit Span at time of creation. */
    private final AuditSpan auditSpan;

    private final StoreContext storeContext;

    private OperationCallbacksImpl(final StoreContext storeContext) {
      this.storeContext = requireNonNull(storeContext);
      this.auditSpan = storeContext.getActiveAuditSpan();
    }

    /**
     * Get the audit span.
     * @return the span
     */
    private AuditSpan getAuditSpan() {
      return auditSpan;
    }

    @Override
    public S3ObjectAttributes createObjectAttributes(final Path path,
        final String eTag,
        final String versionId,
        final long len) {
      return S3AFileSystem.this.createObjectAttributes(path, eTag, versionId,
          len);
    }

    @Override
    public S3ObjectAttributes createObjectAttributes(
        final S3AFileStatus fileStatus) {
      return S3AFileSystem.this.createObjectAttributes(
          fileStatus.getPath(),
          fileStatus);
    }

    @Override
    public S3AReadOpContext createReadContext(final FileStatus fileStatus) {
      return S3AFileSystem.this.createReadContext(fileStatus,
          auditSpan);
    }

    @Override
    @Retries.RetryTranslated
    public void deleteObjectAtPath(final Path path,
        final String key,
        final boolean isFile)
        throws IOException {
      auditSpan.activate();
      once("delete", path.toString(), () ->
          S3AFileSystem.this.deleteObjectAtPath(path, key, isFile));
    }

    @Override
    @Retries.RetryTranslated
    public RemoteIterator<S3ALocatedFileStatus> listFilesAndDirectoryMarkers(
        final Path path,
        final S3AFileStatus status,
        final boolean includeSelf) throws IOException {
      auditSpan.activate();
      return innerListFiles(
          path,
          true,
          includeSelf
              ? Listing.ACCEPT_ALL_OBJECTS
              : new Listing.AcceptAllButSelf(path),
          status
      );
    }

    @Override
    public CopyObjectResponse copyFile(final String srcKey,
        final String destKey,
        final S3ObjectAttributes srcAttributes,
        final S3AReadOpContext readContext) throws IOException {
      auditSpan.activate();
      return S3AFileSystem.this.copyFile(srcKey, destKey,
          srcAttributes.getLen(), srcAttributes, readContext);
    }

    @Override
    public void removeKeys(
            final List<ObjectIdentifier> keysToDelete,
            final boolean deleteFakeDir)
        throws MultiObjectDeleteException, SdkException, IOException {
      auditSpan.activate();
      S3AFileSystem.this.removeKeys(keysToDelete, deleteFakeDir);
    }

    @Override
    public void finishRename(final Path sourceRenamed, final Path destCreated)
        throws IOException {
      auditSpan.activate();
      Path destParent = destCreated.getParent();
      if (!sourceRenamed.getParent().equals(destParent)) {
        LOG.debug("source & dest parents are different; fix up dir markers");
        maybeCreateFakeParentDirectory(sourceRenamed);
      }
    }

    @Override
    @Retries.RetryTranslated
    public RemoteIterator<S3AFileStatus> listObjects(
        final Path path,
        final String key)
        throws IOException {
      return once("listObjects", key, () ->
          listing.createFileStatusListingIterator(path,
              createListObjectsRequest(key, null),
              ACCEPT_ALL,
              Listing.ACCEPT_ALL_OBJECTS,
              auditSpan));
    }

    /**
     * Abort multipart uploads under a path.
     * @param prefix prefix for uploads to abort
     * @return a count of aborts
     * @throws IOException trouble; FileNotFoundExceptions are swallowed.
     */
    @Override
    @Retries.RetryTranslated
    public long abortMultipartUploadsUnderPrefix(String prefix)
        throws IOException {
      return S3AFileSystem.this.abortMultipartUploadsUnderPrefix(storeContext, auditSpan, prefix);
    }

  }  // end OperationCallbacksImpl

  /**
   * Abort multipart uploads under a prefix.
   * @param storeContext store context
   * @param span span for the operations
   * @param prefix prefix for uploads to abort
   * @return a count of aborts
   * @throws IOException trouble; FileNotFoundExceptions are swallowed.
   */
  private long abortMultipartUploadsUnderPrefix(StoreContext storeContext,
      AuditSpan span,
      String prefix) throws IOException {

    span.activate();
    // this deactivates the audit span somehow
    final RemoteIterator<MultipartUpload> uploads =
        listUploadsUnderPrefix(storeContext, prefix);
    // so reactivate it.
    span.activate();
    return foreach(uploads, upload ->
            invoker.retry("Aborting multipart commit", upload.key(), true, () ->
                abortMultipartUpload(upload)));
  }

  /**
   * Callbacks from {@link Listing}.
   * Auditing: the listing object is long-lived; the audit span
   * for a single listing is passed in from the listing
   * method calls and then down to the callbacks.
   */
  protected class ListingOperationCallbacksImpl implements
          ListingOperationCallbacks {

    @Override
    public CompletableFuture<S3ListResult> listObjectsAsync(
        S3ListRequest request,
        DurationTrackerFactory trackerFactory,
        AuditSpan span) {
      return submit(unboundedThreadPool, span, () ->
          listObjects(request,
              pairedTrackerFactory(trackerFactory,
                  getDurationTrackerFactory())));
    }

    @Override
    @Retries.RetryRaw
    public CompletableFuture<S3ListResult> continueListObjectsAsync(
        S3ListRequest request,
        S3ListResult prevResult,
        DurationTrackerFactory trackerFactory,
        AuditSpan span) {
      return submit(unboundedThreadPool, span,
          () -> continueListObjects(request, prevResult,
              pairedTrackerFactory(trackerFactory,
                  getDurationTrackerFactory())));
    }

    @Override
    public S3ALocatedFileStatus toLocatedFileStatus(
            S3AFileStatus status)
            throws IOException {
      return S3AFileSystem.this.toLocatedFileStatus(status);
    }

    @Override
    public S3ListRequest createListObjectsRequest(
        String key,
        String delimiter,
        AuditSpan span) {
      span.activate();
      return S3AFileSystem.this.createListObjectsRequest(key, delimiter);
    }

    @Override
    public long getDefaultBlockSize(Path path) {
      return S3AFileSystem.this.getDefaultBlockSize(path);
    }

    /**
     * Get the S3 object size.
     * If the object is encrypted, the unpadded size will be returned.
     * @param s3Object S3object
     * @return plaintext S3 object size
     * @throws IOException IO problems
     */
    @Override
    public long getObjectSize(S3Object s3Object) throws IOException {
      return fsHandler.getS3ObjectSize(s3Object.key(), s3Object.size(), getStore(), null);
    }

    @Override
    public int getMaxKeys() {
      return S3AFileSystem.this.getMaxKeys();
    }

  }

  /**
   * Low-level call to get at the object metadata.
   * This method is used in some external applications and so
   * must be viewed as a public entry point.
   * @deprecated use S3AInternals API.
   * @param path path to the object. This will be qualified.
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @AuditEntryPoint
  @InterfaceAudience.LimitedPrivate("utilities")
  @Retries.RetryTranslated
  @Deprecated
  public HeadObjectResponse getObjectMetadata(Path path) throws IOException {
    return getS3AInternals().getObjectMetadata(path);
  }

  /**
   * Low-level call to get at the object metadata.
   * @param path path to the object
   * @param changeTracker the change tracker to detect version inconsistencies
   * @param changeInvoker the invoker providing the retry policy
   * @param operation the operation being performed (e.g. "read" or "copy")
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @Retries.RetryTranslated
  private HeadObjectResponse getObjectMetadata(Path path,
      ChangeTracker changeTracker, Invoker changeInvoker, String operation)
      throws IOException {
    String key = pathToKey(path);
    return once(operation, path.toString(), () ->
            // HEAD against the object
            getObjectMetadata(
                key, changeTracker, changeInvoker, operation));
  }

  /**
   * Entry point to an operation.
   * Increments the statistic; verifies the FS is active.
   * @param operation The operation being invoked
   * @param path first path of operation
   * @return a span for the audit
   * @throws IOException failure of audit service
   */
  protected AuditSpan entryPoint(Statistic operation,
      Path path) throws IOException {
    return entryPoint(operation,
        (path != null ? pathToKey(path): null),
        null);
  }

  /**
   * Entry point to an operation.
   * Increments the statistic; verifies the FS is active.
   * @param operation The operation being invoked
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   * @throws IOException failure of audit service
   */
  protected AuditSpan entryPoint(Statistic operation,
      @Nullable String path1,
      @Nullable String path2) throws IOException {
    checkNotClosed();
    incrementStatistic(operation);
    return createSpan(operation.getSymbol(),
        path1, path2);
  }

  /**
   * Given an IOException raising callable/lambda expression,
   * execute it and update the relevant statistic within a span
   * of the same statistic.
   * @param statistic statistic key
   * @param path first path for span (nullable)
   * @param path2 second path for span
   * @param input input callable.
   * @param <B> return type.
   * @return the result of the operation.
   * @throws IOException if raised in the callable
   */
  private <B> B trackDurationAndSpan(
      Statistic statistic, String path, String path2,
      CallableRaisingIOE<B> input) throws IOException {
    checkNotClosed();
    try (AuditSpan span = createSpan(statistic.getSymbol(),
        path, path2)) {
      return trackDuration(getDurationTrackerFactory(),
          statistic.getSymbol(), input);
    }
  }

  /**
   * Overloaded version of {@code trackDurationAndSpan()}.
   * Takes a single nullable path as the path param,
   * @param statistic statistic key
   * @param path path for span (nullable)
   * @param input input callable.
   * @param <B> return type.
   * @return the result of the operation.
   * @throws IOException if raised in the callable
   */
  private <B> B trackDurationAndSpan(
      Statistic statistic,
      @Nullable Path path,
      CallableRaisingIOE<B> input) throws IOException {
    return trackDurationAndSpan(statistic,
        path != null ? pathToKey(path): null,
        null, input);
  }

  /**
   * Increment a statistic by 1.
   * This increments both the instrumentation and storage statistics.
   * @param statistic The operation to increment
   */
  protected void incrementStatistic(Statistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value.
   * This increments both the instrumentation and storage statistics.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementStatistic(Statistic statistic, long count) {
    statisticsContext.incrementCounter(statistic, count);
  }

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  protected void decrementGauge(Statistic statistic, long count) {
    statisticsContext.decrementGauge(statistic, count);
  }

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementGauge(Statistic statistic, long count) {
    statisticsContext.incrementGauge(statistic, count);
  }

  /**
   * Callback when an operation was retried.
   * Increments the statistics of ignored errors or throttled requests,
   * depending up on the exception class.
   * @param ex exception.
   */
  public void operationRetried(Exception ex) {
    if (isThrottleException(ex)) {
      LOG.debug("Request throttled");
      incrementStatistic(STORE_IO_THROTTLED);
      statisticsContext.addValueToQuantiles(STORE_IO_THROTTLE_RATE, 1);
    } else {
      incrementStatistic(STORE_IO_RETRY);
      incrementStatistic(IGNORED_ERRORS);
    }
  }

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  public void operationRetried(
      String text,
      Exception ex,
      int retries,
      boolean idempotent) {
    operationRetried(ex);
  }

  /**
   * Get the storage statistics of this filesystem.
   * @return the storage statistics
   */
  @Override
  public S3AStorageStatistics getStorageStatistics() {
    return storageStatistics;
  }

  /**
   * Get the instrumentation's IOStatistics.
   * @return statistics or null if instrumentation has not yet been instantiated.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return instrumentation != null
        ? instrumentation.getIOStatistics()
        : null;
  }

  /**
   * Get the factory for duration tracking.
   * @return a factory from the instrumentation.
   */
  protected DurationTrackerFactory getDurationTrackerFactory() {
    return instrumentation != null ?
        instrumentation.getDurationTrackerFactory()
        : null;
  }

  /**
   * Given a possibly null duration tracker factory, return a non-null
   * one for use in tracking durations -either that or the FS tracker
   * itself.
   *
   * @param factory factory.
   * @return a non-null factory.
   */
  protected DurationTrackerFactory nonNullDurationTrackerFactory(
      DurationTrackerFactory factory) {
    return getStore().nonNullDurationTrackerFactory(factory);
  }

  /**
   * Request object metadata; increments counters in the process.
   * Retry policy: retry untranslated.
   * This method is used in some external applications and so
   * must be viewed as a public entry point.
   * Auditing: this call does NOT initiate a new AuditSpan; the expectation
   * is that there is already an active span.
   * @param key key
   * @return the metadata
   * @throws IOException if the retry invocation raises one (it shouldn't).
   */
  @Retries.RetryRaw
  @VisibleForTesting
  @InterfaceAudience.LimitedPrivate("external utilities")
  HeadObjectResponse getObjectMetadata(String key) throws IOException {
    return getObjectMetadata(key, null, invoker, "getObjectMetadata");
  }

  /**
   * Request object metadata; increments counters in the process.
   * Retry policy: retry untranslated.
   * Uses changeTracker to detect an unexpected file version (eTag or versionId)
   * @param key key
   * @param changeTracker the change tracker to detect unexpected object version
   * @param changeInvoker the invoker providing the retry policy
   * @param operation the operation (e.g. "read" or "copy") triggering this call
   * @return the metadata
   * @throws IOException if the retry invocation raises one (it shouldn't).
   * @throws RemoteFileChangedException if an unexpected version is detected
   */
  @Retries.RetryRaw
  protected HeadObjectResponse getObjectMetadata(String key,
      ChangeTracker changeTracker,
      Invoker changeInvoker,
      String operation) throws IOException {
    return getStore().headObject(key, changeTracker, changeInvoker, fsHandler, operation);
  }

  /**
   * Request bucket metadata.
   * @return the metadata
   * @throws UnknownStoreException the bucket is absent
   * @throws IOException  any other problem talking to S3
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  protected HeadBucketResponse getBucketMetadata() throws IOException {
    final HeadBucketResponse response = trackDurationAndSpan(STORE_EXISTS_PROBE, bucket, null,
        () -> invoker.retry("getBucketMetadata()", bucket, true, () -> {
          try {
            return getS3Client().headBucket(
                getRequestFactory().newHeadBucketRequestBuilder(bucket).build());
          } catch (NoSuchBucketException e) {
            throw new UnknownStoreException("s3a://" + bucket + "/", " Bucket does " + "not exist");
          }
        }));
    return response;
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics
   * in the process.
   *
   * Retry policy: retry untranslated.
   * @param request request to initiate
   * @param trackerFactory duration tracking
   * @return the results
   * @throws IOException if the retry invocation raises one (it shouldn't).
   */
  @Retries.RetryRaw
  protected S3ListResult listObjects(S3ListRequest request,
      @Nullable final DurationTrackerFactory trackerFactory)
      throws IOException {
    incrementReadOperations();
    LOG.debug("LIST {}", request);
    validateListArguments(request);
    try(DurationInfo ignored =
            new DurationInfo(LOG, false, "LIST")) {
      return invoker.retryUntranslated(
          request.toString(),
          true,
          trackDurationOfOperation(trackerFactory,
              OBJECT_LIST_REQUEST,
              () -> {
                if (useListV1) {
                  return S3ListResult.v1(getS3Client().listObjects(request.getV1()));
                } else {
                  return S3ListResult.v2(getS3Client().listObjectsV2(request.getV2()));
                }
              }));
    }
  }

  /**
   * Validate the list arguments with this bucket's settings.
   * @param request the request to validate
   */
  private void validateListArguments(S3ListRequest request) {
    if (useListV1) {
      Preconditions.checkArgument(request.isV1());
    } else {
      Preconditions.checkArgument(!request.isV1());
    }
  }

  /**
   * List the next set of objects.
   * Retry policy: retry untranslated.
   * @param request last list objects request to continue
   * @param prevResult last paged result to continue from
   * @param trackerFactory duration tracking
   * @return the next result object
   * @throws IOException none, just there for retryUntranslated.
   */
  @Retries.RetryRaw
  protected S3ListResult continueListObjects(S3ListRequest request,
      S3ListResult prevResult,
      final DurationTrackerFactory trackerFactory) throws IOException {
    incrementReadOperations();
    validateListArguments(request);
    try(DurationInfo ignored =
            new DurationInfo(LOG, false, "LIST (continued)")) {
      return invoker.retryUntranslated(
          request.toString(),
          true,
          trackDurationOfOperation(
              trackerFactory,
              OBJECT_CONTINUE_LIST_REQUEST,
              () -> {
                if (useListV1) {
                  List<S3Object> prevListResult = prevResult.getV1().contents();

                  // Next markers are only present when a delimiter is specified.
                  String nextMarker;
                  if (prevResult.getV1().nextMarker() != null) {
                    nextMarker = prevResult.getV1().nextMarker();
                  } else {
                    nextMarker = prevListResult.get(prevListResult.size() - 1).key();
                  }

                  return S3ListResult.v1(getS3Client().listObjects(
                      request.getV1().toBuilder().marker(nextMarker).build()));
                } else {
                  return S3ListResult.v2(getS3Client().listObjectsV2(request.getV2().toBuilder()
                      .continuationToken(prevResult.getV2().nextContinuationToken()).build()));
                }
              }));
    }
  }

  /**
   * Increment read operations.
   */
  public void incrementReadOperations() {
    statistics.incrementReadOps(1);
  }

  /**
   * Increment the write operation counter.
   * This is somewhat inaccurate, as it appears to be invoked more
   * often than needed in progress callbacks.
   */
  public void incrementWriteOperations() {
    statistics.incrementWriteOps(1);
  }

  /**
   * Delete an object.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * This call does <i>not</i> create any mock parent entries.
   *
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param key key to blob to delete.
   * @throws SdkException problems working with S3
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  @VisibleForTesting
  @Retries.RetryRaw
  protected void deleteObject(String key)
      throws SdkException, IOException {
    incrementWriteOperations();
    getStore().deleteObject(getRequestFactory()
        .newDeleteObjectRequestBuilder(key)
        .build());
  }

  /**
   * Delete an object.
   * This call does <i>not</i> create any mock parent entries.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param f path path to delete
   * @param key key of entry
   * @param isFile is the path a file (used for instrumentation only)
   * @throws SdkException problems working with S3
   * @throws IOException from invoker signature only -should not be raised.
   */
  @Retries.RetryRaw
  void deleteObjectAtPath(Path f,
      String key,
      boolean isFile)
      throws SdkException, IOException {
    if (isFile) {
      instrumentation.fileDeleted(1);
    } else {
      instrumentation.directoryDeleted();
    }
    deleteObject(key);
  }

  /**
   * Perform a bulk object delete operation against S3.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics
   * <p></p>
   * {@code OBJECT_DELETE_OBJECTS} is updated with the actual number
   * of objects deleted in the request.
   * <p></p>
   * Retry policy: retry untranslated; delete considered idempotent.
   * If the request is throttled, this is logged in the throttle statistics,
   * with the counter set to the number of keys, rather than the number
   * of invocations of the delete operation.
   * This is because S3 considers each key as one mutating operation on
   * the store when updating its load counters on a specific partition
   * of an S3 bucket.
   * If only the request was measured, this operation would under-report.
   * @param deleteRequest keys to delete on the s3-backend
   * @return the AWS response
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted.
   * @throws SdkException amazon-layer failure.
   */
  @Retries.RetryRaw
  private DeleteObjectsResponse deleteObjects(DeleteObjectsRequest deleteRequest)
      throws MultiObjectDeleteException, SdkException, IOException {
    incrementWriteOperations();
    DeleteObjectsResponse response = getStore().deleteObjects(deleteRequest).getValue();
    if (!response.errors().isEmpty()) {
      throw new MultiObjectDeleteException(response.errors());
    }
    return response;
  }

  /**
   * Create a putObject request builder.
   * Adds the ACL and metadata
   * @param key key of object
   * @param length length of object to be uploaded
   * @param isDirectoryMarker true if object to be uploaded is a directory marker
   * @return the request
   */
  public PutObjectRequest.Builder newPutObjectRequestBuilder(String key,
      long length,
      boolean isDirectoryMarker) {
    return requestFactory.newPutObjectRequestBuilder(key, null, length, isDirectoryMarker);
  }

  /**
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * Retry policy: N/A: the transfer manager is performing the upload.
   * Auditing: must be inside an audit span.
   * @param putObjectRequest the request
   * @param file the file to be uploaded
   * @param listener the progress listener for the request
   * @return the upload initiated
   * @throws IOException if transfer manager creation failed.
   */
  @Retries.OnceRaw
  public UploadInfo putObject(PutObjectRequest putObjectRequest, File file,
      ProgressableProgressListener listener) throws IOException {
    return getStore().putObject(putObjectRequest, file, listener);
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   *
   * Retry Policy: none.
   * Auditing: must be inside an audit span.
   * <i>Important: this call will close any input stream in the request.</i>
   * @param putObjectRequest the request
   * @param putOptions put object options
   * @param uploadData data to be uploaded
   * @param durationTrackerFactory factory for duration tracking
   * @return the upload initiated
   * @throws SdkException on problems
   */
  @VisibleForTesting
  @Retries.OnceRaw("For PUT; post-PUT actions are RetryExceptionsSwallowed")
  PutObjectResponse putObjectDirect(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      S3ADataBlocks.BlockUploadData uploadData,
      DurationTrackerFactory durationTrackerFactory)
      throws SdkException {

    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {}", len, putObjectRequest.key());
    incrementPutStartStatistics(len);
    final UploadContentProviders.BaseContentProvider provider =
        uploadData.getContentProvider();
    try {
      PutObjectResponse response =
          trackDurationOfSupplier(nonNullDurationTrackerFactory(durationTrackerFactory),
              OBJECT_PUT_REQUESTS.getSymbol(),
              () -> getS3Client().putObject(putObjectRequest,
                  RequestBody.fromContentProvider(
                      provider,
                      provider.getSize(),
                      CONTENT_TYPE_OCTET_STREAM)));
      incrementPutCompletedStatistics(true, len);
      return response;
    } catch (SdkException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Get the length of the PUT, verifying that the length is known.
   * @param putObjectRequest a request bound to a file or a stream.
   * @return the request length
   * @throws IllegalArgumentException if the length is negative
   */
  private long getPutRequestLength(PutObjectRequest putObjectRequest) {
    long len = putObjectRequest.contentLength();

    Preconditions.checkState(len >= 0, "Cannot PUT object of unknown length");
    return len;
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the body.</i>
   *
   * Retry Policy: none.
   * @param durationTrackerFactory duration tracker factory for operation
   * @param request the upload part request.
   * @param body the request body.
   * @return the result of the operation.
   * @throws AwsServiceException on problems
   */
  @Retries.OnceRaw
  UploadPartResponse uploadPart(UploadPartRequest request, RequestBody body,
      final DurationTrackerFactory durationTrackerFactory)
      throws AwsServiceException {
    long len = request.contentLength();
    incrementPutStartStatistics(len);
    try {
      UploadPartResponse uploadPartResponse = trackDurationOfSupplier(
          nonNullDurationTrackerFactory(durationTrackerFactory),
          MULTIPART_UPLOAD_PART_PUT.getSymbol(), () ->
              getS3Client().uploadPart(request, body));
      incrementPutCompletedStatistics(true, len);
      return uploadPartResponse;
    } catch (AwsServiceException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * At the start of a put/multipart upload operation, update the
   * relevant counters.
   *
   * @param bytes bytes in the request.
   */
  protected void incrementPutStartStatistics(long bytes) {
    getStore().incrementPutStartStatistics(bytes);
  }

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  protected void incrementPutCompletedStatistics(boolean success, long bytes) {
    getStore().incrementPutCompletedStatistics(success, bytes);
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  protected void incrementPutProgressStatistics(String key, long bytes) {
    getStore().incrementPutProgressStatistics(key, bytes);
  }

  /**
   * Delete a list of keys on a s3-backend.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * The number of rejected objects will be added to the metric
   * {@link Statistic#FILES_DELETE_REJECTED}.
   * @throws AwsServiceException other amazon-layer failure.
   */
  @Retries.RetryRaw
  private void removeKeysS3(
          List<ObjectIdentifier> keysToDelete,
          boolean deleteFakeDir)
      throws MultiObjectDeleteException, AwsServiceException, IOException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      return;
    }
    if (keysToDelete.size() == 1) {
      // single object is a single delete call.
      // this is more informative in server logs and may be more efficient..
      deleteObject(keysToDelete.get(0).key());
      noteDeleted(1, deleteFakeDir);
      return;
    }
    try {
      if (enableMultiObjectsDelete) {
        if (keysToDelete.size() <= pageSize) {
          deleteObjects(getRequestFactory()
              .newBulkDeleteRequestBuilder(keysToDelete)
              .build());
        } else {
          // Multi object deletion of more than 1000 keys is not supported
          // by s3. So we are paging the keys by page size.
          LOG.debug("Partitioning the keys to delete as it is more than " +
                  "page size. Number of keys: {}, Page size: {}",
                  keysToDelete.size(), pageSize);
          for (List<ObjectIdentifier> batchOfKeysToDelete :
                  Lists.partition(keysToDelete, pageSize)) {
            deleteObjects(getRequestFactory()
                .newBulkDeleteRequestBuilder(batchOfKeysToDelete)
                .build());
          }
        }
      } else {
        for (ObjectIdentifier objectIdentifier : keysToDelete) {
          deleteObject(objectIdentifier.key());
        }
      }
    } catch (MultiObjectDeleteException ex) {
      // partial delete.
      // Update the stats with the count of the actual number of successful
      // deletions.
      int rejected = ex.errors().size();
      noteDeleted(keysToDelete.size() - rejected, deleteFakeDir);
      incrementStatistic(FILES_DELETE_REJECTED, rejected);
      throw ex;
    }
    noteDeleted(keysToDelete.size(), deleteFakeDir);
  }

  /**
   * Note the deletion of files or fake directories deleted.
   * @param count count of keys deleted.
   * @param deleteFakeDir are the deletions fake directories?
   */
  private void noteDeleted(final int count, final boolean deleteFakeDir) {
    if (!deleteFakeDir) {
      instrumentation.fileDeleted(count);
    } else {
      instrumentation.fakeDirsDeleted(count);
    }
  }

  /**
   * Invoke {@link #removeKeysS3(List, boolean)}.
   * If a {@code MultiObjectDeleteException} is raised, the
   * relevant statistics are updated.
   *
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AwsServiceException amazon-layer failure.
   * @throws IOException other IO Exception.
   */
  @VisibleForTesting
  @Retries.RetryRaw
  public void removeKeys(
      final List<ObjectIdentifier> keysToDelete,
      final boolean deleteFakeDir)
      throws MultiObjectDeleteException, AwsServiceException,
      IOException {
    try (DurationInfo ignored = new DurationInfo(LOG, false,
            "Deleting %d keys", keysToDelete.size())) {
      removeKeysS3(keysToDelete, deleteFakeDir);
    }
  }

  /**
   * Delete a Path. This operation is at least {@code O(files)}, with
   * added overheads to enumerate the path. It is also not atomic.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return true if the path existed and then was deleted; false if there
   * was no path in the first place, or the corner cases of root path deletion
   * have surfaced.
   * @throws IOException due to inability to delete a directory or file.
   */
  @Override
  @Retries.RetryTranslated
  @AuditEntryPoint
  public boolean delete(Path f, boolean recursive) throws IOException {
    checkNotClosed();
    return deleteWithoutCloseCheck(f, recursive);
  }

  /**
   * Same as delete(), except that it does not check if fs is closed.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return true if the path existed and then was deleted; false if there
   * was no path in the first place, or the corner cases of root path deletion
   * have surfaced.
   * @throws IOException due to inability to delete a directory or file.
   */

  @VisibleForTesting
  protected boolean deleteWithoutCloseCheck(Path f, boolean recursive) throws IOException {
    final Path path = qualify(f);
    // span covers delete, getFileStatus, fake directory operations.
    try (AuditSpan span = createSpan(INVOCATION_DELETE.getSymbol(),
        path.toString(), null)) {
      // SC will include active span
      final StoreContext storeContext = createStoreContext();
      boolean outcome = trackDuration(getDurationTrackerFactory(),
          INVOCATION_DELETE.getSymbol(),
          new DeleteOperation(
              storeContext,
              innerGetFileStatus(path, true, StatusProbeEnum.ALL),
              recursive,
              new OperationCallbacksImpl(storeContext),
              pageSize,
              dirOperationsPurgeUploads));
      if (outcome) {
        try {
          maybeCreateFakeParentDirectory(path);
        } catch (AccessDeniedException e) {
          LOG.warn("Cannot create directory marker at {}: {}",
              f.getParent(), e.toString());
          LOG.debug("Failed to create fake dir above {}", path, e);
        }
      }
      return outcome;
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist: {}", path, e.toString());
      instrumentation.errorIgnored();
      return false;
    } catch (SdkException e) {
      throw translateException("delete", path, e);
    }
  }

  /**
   * Create a fake directory if required.
   * That is: it is not the root path and the path does not exist.
   * Retry policy: retrying; untranslated.
   * @param f path to create
   * @throws IOException IO problem
   */
  @Retries.RetryTranslated
  private void createFakeDirectoryIfNecessary(Path f)
      throws IOException, SdkException {
    String key = pathToKey(f);
    // we only make the LIST call; the codepaths to get here should not
    // be reached if there is an empty dir marker -and if they do, it
    // is mostly harmless to create a new one.
    if (!key.isEmpty() && !s3Exists(f, StatusProbeEnum.DIRECTORIES)) {
      LOG.debug("Creating new fake directory at {}", f);
      createFakeDirectory(key, PutObjectOptions.defaultOptions());
    }
  }

  /**
   * Create a fake parent directory if required.
   * That is: it parent is not the root path and does not yet exist.
   * @param path whose parent is created if needed.
   * @throws IOException IO problem
   */
  @Retries.RetryTranslated
  @VisibleForTesting
  protected void maybeCreateFakeParentDirectory(Path path)
      throws IOException, SdkException {
    Path parent = path.getParent();
    if (parent != null && !parent.isRoot() && !isUnderMagicCommitPath(parent)) {
      createFakeDirectoryIfNecessary(parent);
    }
  }

  /**
   * Override subclass such that we benefit for async listing done
   * in {@code S3AFileSystem}. See {@code Listing#ObjectListingIterator}.
   * {@inheritDoc}
   *
   */
  @Override
  @AuditEntryPoint
  public RemoteIterator<FileStatus> listStatusIterator(Path p)
          throws FileNotFoundException, IOException {
    Path path = qualify(p);
    return typeCastingRemoteIterator(trackDurationAndSpan(
        INVOCATION_LIST_STATUS, path, () ->
            once("listStatus", path.toString(), () ->
                innerListStatus(p))));
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  @Override
  @AuditEntryPoint
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    Path path = qualify(f);
    return trackDurationAndSpan(INVOCATION_LIST_STATUS, path, () ->
        once("listStatus", path.toString(),
            () -> iteratorToStatuses(innerListStatus(path))));
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory. The returned iterator is within the current active span.
   *
   * Auditing: This method MUST be called within a span.
   * The span is attached to the iterator. All further S3 calls
   * made by the iterator will be within the span.
   * @param f qualified path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException due to an IO problem.
   * @throws SdkException on failures inside the AWS SDK
   */
  private RemoteIterator<S3AFileStatus> innerListStatus(Path f)
          throws FileNotFoundException,
          IOException, SdkException {
    Path path = qualify(f);
    LOG.debug("List status for path: {}", path);

    final RemoteIterator<S3AFileStatus> statusIt = listing
        .getFileStatusesAssumingNonEmptyDir(path, getActiveAuditSpan());
    if (!statusIt.hasNext()) {
      // We may have an empty dir, or may have file or may have nothing.
      // So we call innerGetFileStatus to get the status, this may throw
      // FileNotFoundException if we have nothing.
      // So We are guaranteed to have either a dir marker or a file.
      final S3AFileStatus fileStatus = innerGetFileStatus(path, false,
              StatusProbeEnum.ALL);
      // If it is a file return directly.
      if (fileStatus.isFile()) {
        LOG.debug("Adding: rd (not a dir): {}", path);
        S3AFileStatus[] stats = new S3AFileStatus[1];
        stats[0] = fileStatus;
        return listing.createProvidedFileStatusIterator(
                stats,
                ACCEPT_ALL,
                Listing.ACCEPT_ALL_OBJECTS);
      }
    }
    // Here we have a directory which may or may not be empty.
    return statusIt;
  }

  /**
   * Create a {@code ListObjectsRequest} request against this bucket,
   * with the maximum keys returned in a query set by {@link #maxKeys}.
   * @param key key for request
   * @param delimiter any delimiter
   * @return the request
   */
  @VisibleForTesting
  public S3ListRequest createListObjectsRequest(String key,
      String delimiter) {
    return createListObjectsRequest(key, delimiter, maxKeys);
  }

  /**
   * Create the List objects request appropriate for the
   * active list request option.
   * @param key key for request
   * @param delimiter any delimiter
   * @param limit limit of keys
   * @return the request
   */
  private S3ListRequest createListObjectsRequest(String key,
      String delimiter, int limit) {
    if (!useListV1) {
      ListObjectsV2Request.Builder requestBuilder =
          getRequestFactory().newListObjectsV2RequestBuilder(
              key, delimiter, limit);
      return S3ListRequest.v2(requestBuilder.build());
    } else {
      ListObjectsRequest.Builder requestBuilder =
          getRequestFactory().newListObjectsV1RequestBuilder(
              key, delimiter, limit);
      return S3ListRequest.v1(requestBuilder.build());
    }
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param newDir the current working directory.
   */
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeQualified(newDir);
  }

  /**
   * Get the current working directory for the given file system.
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Get the username of the FS.
   * @return the short name of the user who instantiated the FS
   */
  public String getUsername() {
    return username;
  }

  /**
   * Get the owner of this FS: who created it?
   * @return the owner of the FS.
   */
  public UserGroupInformation getOwner() {
    return owner;
  }

  /**
   *
   * Make the given path and all non-existent parents into
   * directories. Has the semantics of Unix {@code 'mkdir -p'}.
   * Existence of the directory hierarchy is not an error.
   * Parent elements are scanned to see if any are a file,
   * <i>except under "MAGIC PATH"</i> paths.
   * There the FS assumes that the destination directory creation
   * did that scan and that paths in job/task attempts are all
   * "well formed"
   * @param p path to create
   * @param permission to apply to path
   * @return true if a directory was created or already existed
   * @throws FileAlreadyExistsException there is a file at the path specified
   * or is discovered on one of its ancestors.
   * @throws IOException other IO problems
   */
  @Override
  @AuditEntryPoint
  public boolean mkdirs(Path p, FsPermission permission) throws IOException,
      FileAlreadyExistsException {
    Path path = qualify(p);
    return trackDurationAndSpan(
        INVOCATION_MKDIRS, path,
        new MkdirOperation(
            createStoreContext(),
            path,
            createMkdirOperationCallbacks(),
            isMagicCommitPath(path),
            performanceFlags.enabled(PerformanceFlagEnum.Mkdir)));
  }

  /**
   * Override point: create the callbacks for Mkdir.
   * This does not create a new span; caller must be in one.
   * @return an implementation of the MkdirCallbacks,
   */
  @VisibleForTesting
  public MkdirOperation.MkdirCallbacks createMkdirOperationCallbacks() {
    return new MkdirOperationCallbacksImpl();
  }

  /**
   * Callbacks from the {@link MkdirOperation}.
   */
  protected class MkdirOperationCallbacksImpl implements
      MkdirOperation.MkdirCallbacks {

    @Override
    public S3AFileStatus probePathStatus(final Path path,
        final Set<StatusProbeEnum> probes) throws IOException {
      return S3AFileSystem.this.innerGetFileStatus(path, false, probes);
    }

    @Override
    public void createFakeDirectory(final Path dir)
        throws IOException {
      S3AFileSystem.this.createFakeDirectory(
          pathToKey(dir),
          PutObjectOptions.defaultOptions());
    }
  }

  /**
   * This is a very slow operation against object storage.
   * Execute it as a single span with whatever optimizations
   * have been implemented.
   * {@inheritDoc}
   */
  @Override
  @Retries.RetryTranslated
  @AuditEntryPoint
  public ContentSummary getContentSummary(final Path f) throws IOException {
    final Path path = qualify(f);
    return trackDurationAndSpan(
        INVOCATION_GET_CONTENT_SUMMARY, path,
        new GetContentSummaryOperation(
            createStoreContext(),
            path,
            createGetContentSummaryCallbacks()));
  }

  /**
   * Override point: create the callbacks for getContentSummary.
   * This does not create a new span; caller must be in one.
   * @return an implementation of the GetContentSummaryCallbacksImpl
   */
  protected GetContentSummaryOperation.GetContentSummaryCallbacks
      createGetContentSummaryCallbacks() {
    return new GetContentSummaryCallbacksImpl();
  }

  /**
   * Callbacks from the {@link GetContentSummaryOperation}.
   */
  protected class GetContentSummaryCallbacksImpl implements
      GetContentSummaryOperation.GetContentSummaryCallbacks {

    @Override
    public S3AFileStatus probePathStatus(final Path path,
        final Set<StatusProbeEnum> probes) throws IOException {
      return S3AFileSystem.this.innerGetFileStatus(path, false, probes);
    }

    @Override
    public RemoteIterator<S3ALocatedFileStatus> listFilesIterator(final Path path,
        final boolean recursive) throws IOException {
      return S3AFileSystem.this.innerListFiles(path, recursive, Listing.ACCEPT_ALL_OBJECTS, null);
    }
  }

  /**
   * Soft check of access by forwarding to the audit manager
   * and so on to the auditor.
   * {@inheritDoc}
   */
  @Override
  @AuditEntryPoint
  public void access(final Path f, final FsAction mode)
      throws AccessControlException, FileNotFoundException, IOException {
    Path path = qualify(f);
    LOG.debug("check access mode {} for {}", path, mode);
    trackDurationAndSpan(
        INVOCATION_ACCESS, path, () -> {
          final S3AFileStatus stat = innerGetFileStatus(path, false,
              StatusProbeEnum.ALL);
          if (!getAuditManager().checkAccess(path, stat, mode)) {
            incrementStatistic(AUDIT_ACCESS_CHECK_FAILURE);
            throw new AccessControlException(String.format(
                "Permission denied: user=%s, path=\"%s\":%s:%s:%s%s",
                getOwner().getUserName(),
                stat.getPath(),
                stat.getOwner(), stat.getGroup(),
                stat.isDirectory() ? "d" : "-", mode));
          }
          // simply for the API binding.
          return true;
        });
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException on other problems.
   */
  @Override
  @AuditEntryPoint
  @Retries.RetryTranslated
  public FileStatus getFileStatus(final Path f) throws IOException {
    Path path = qualify(f);
    if (isTrackMagicCommitsInMemoryEnabled(getConf()) && isMagicCommitPath(path)) {
      // Some downstream apps might call getFileStatus for a magic path to get the file size.
      // when commit data is stored in memory construct the dummy S3AFileStatus with correct
      // file size fetched from the memory.
      if (InMemoryMagicCommitTracker.getPathToBytesWritten().containsKey(path)) {
        long len = InMemoryMagicCommitTracker.getPathToBytesWritten().get(path);
        return new S3AFileStatus(len,
            0L,
            path,
            getDefaultBlockSize(path),
            username,
            MAGIC_COMMITTER_PENDING_OBJECT_ETAG_NAME,
            null);
      }
    }
    return trackDurationAndSpan(
        INVOCATION_GET_FILE_STATUS, path, () ->
            innerGetFileStatus(path, false, StatusProbeEnum.ALL));
  }

  /**
   * Get the status of a file or directory.
   * Internal version of {@link #getFileStatus(Path)}.
   * @param f The path we want information from
   * @param needEmptyDirectoryFlag if true, implementation will calculate
   *        a TRUE or FALSE value for {@link S3AFileStatus#isEmptyDirectory()}
   * @param probes probes to make.
   * @return a S3AFileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException on other problems.
   */
  @VisibleForTesting
  @Retries.RetryTranslated
  S3AFileStatus innerGetFileStatus(final Path f,
      final boolean needEmptyDirectoryFlag,
      final Set<StatusProbeEnum> probes) throws IOException {
    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({}); needEmptyDirectory={}",
        path, key, needEmptyDirectoryFlag);
    return s3GetFileStatus(path,
        key,
        probes,
        needEmptyDirectoryFlag);

  }

  /**
   * Probe store for file status with control of which probes are issued..
   * Used to implement {@link #innerGetFileStatus(Path, boolean, Set)},
   * and for direct management of empty directory blobs.
   *
   * Checks made, in order:
   * <ol>
   *   <li>
   *     Head: look for an object at the given key, provided that
   *     the key doesn't end in "/"
   *   </li>
   *   <li>
   *     DirMarker/List: issue a LIST on the key (with / if needed), require one
   *     entry to be found for the path to be considered a non-empty directory.
   *   </li>
   * </ol>
   *
   * Notes:
   * <ul>
   *   <li>
   *     Objects ending in / are treated as directory markers,
   *     irrespective of length.
   *   </li>
   *   <li>
   *     The HEAD requests require the permissions to read an object,
   *     including (we believe) the ability to decrypt the file.
   *     At the very least, for SSE-C markers, you need the same key on
   *     the client for the HEAD to work.
   *   </li>
   *   <li>
   *     The List probe needs list permission.
   *   </li>
   * </ul>
   *
   * Retry policy: retry translated.
   * @param path Qualified path
   * @param key  Key string for the path
   * @param probes probes to make
   * @param needEmptyDirectoryFlag if true, implementation will calculate
   *        a TRUE or FALSE value for {@link S3AFileStatus#isEmptyDirectory()}
   * @return Status
   * @throws FileNotFoundException the supplied probes failed.
   * @throws IOException on other problems.
   */
  @VisibleForTesting
  @Retries.RetryTranslated
  S3AFileStatus s3GetFileStatus(final Path path,
      final String key,
      final Set<StatusProbeEnum> probes,
      final boolean needEmptyDirectoryFlag) throws IOException {
    LOG.debug("S3GetFileStatus {}", path);
    // either you aren't looking for the directory flag, or you are,
    // and if you are, the probe list must contain list.
    Preconditions.checkArgument(!needEmptyDirectoryFlag
        || probes.contains(StatusProbeEnum.List),
        "s3GetFileStatus(%s) wants to know if a directory is empty but"
            + " does not request a list probe", path);
    if (key.isEmpty() && !needEmptyDirectoryFlag) {
      return new S3AFileStatus(Tristate.UNKNOWN, path, username);
    }

    if (!key.isEmpty() && !key.endsWith("/")
        && probes.contains(StatusProbeEnum.Head)) {
      try {
        // look for the simple file
        HeadObjectResponse meta = getObjectMetadata(key);
        LOG.debug("Found exact file: normal file {}", key);
        return new S3AFileStatus(meta.contentLength(),
            meta.lastModified().toEpochMilli(),
            path,
            getDefaultBlockSize(path),
            username,
            meta.eTag(),
            meta.versionId());
      } catch (AwsServiceException e) {
        // if the response is a 404 error, it just means that there is
        // no file at that path...the remaining checks will be needed.
        // But: an empty bucket is also a 404, so check for that
        // and fail.
        if (e.statusCode() != SC_404_NOT_FOUND || isUnknownBucket(e)) {
          throw translateException("getFileStatus", path, e);
        }
      } catch (SdkException e) {
        throw translateException("getFileStatus", path, e);
      }
    }

    // execute the list
    if (probes.contains(StatusProbeEnum.List)) {
      try {
        // this will find a marker dir / as well as an entry.
        // When making a simple "is this a dir check" all is good.
        // but when looking for an empty dir, we need to verify there are no
        // children, so ask for two entries, so as to find
        // a child
        String dirKey = maybeAddTrailingSlash(key);
        // list size is dir marker + at least one entry

        final int listSize = 2;
        S3ListRequest request = createListObjectsRequest(dirKey, "/",
            listSize);
        // execute the request
        S3ListResult listResult = listObjects(request,
            getDurationTrackerFactory());

        if (listResult.hasPrefixesOrObjects()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found path as directory (with /)");
            listResult.logAtDebug(LOG);
          }
          // At least one entry has been found.
          // If looking for an empty directory, the marker must exist but no
          // children.
          // So the listing must contain the marker entry only.
          if (needEmptyDirectoryFlag
              && listResult.representsEmptyDirectory(dirKey)) {
            return new S3AFileStatus(Tristate.TRUE, path, username);
          }
          // either an empty directory is not needed, or the
          // listing does not meet the requirements.
          return new S3AFileStatus(Tristate.FALSE, path, username);
        } else if (key.isEmpty()) {
          LOG.debug("Found root directory");
          return new S3AFileStatus(Tristate.TRUE, path, username);
        }
      } catch (AwsServiceException e) {
        if (e.statusCode() != SC_404_NOT_FOUND || isUnknownBucket(e)) {
          throw translateException("getFileStatus", path, e);
        }
      } catch (SdkException e) {
        throw translateException("getFileStatus", path, e);
      }
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  /**
   * Probe S3 for a file or dir existing, with the given probe set.
   * Retry policy: retrying; translated.
   * @param path qualified path to look for
   * @param probes probes to make
   * @return true if path exists in S3
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private boolean s3Exists(final Path path, final Set<StatusProbeEnum> probes)
      throws IOException {
    String key = pathToKey(path);
    try {
      s3GetFileStatus(path, key, probes, false);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * If {@link Constants#OPTIMIZED_COPY_FROM_LOCAL} is set to false,
   * the superclass implementation is used.
   *
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false
   */
  @Override
  @AuditEntryPoint
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
                                Path dst) throws IOException {
    checkNotClosed();
    LOG.debug("Copying local file from {} to {} (delSrc={} overwrite={}",
        src, dst, delSrc, overwrite);
    if (optimizedCopyFromLocal) {
      trackDurationAndSpan(INVOCATION_COPY_FROM_LOCAL_FILE, dst, () ->
          new CopyFromLocalOperation(
              createStoreContext(),
              src,
              dst,
              delSrc,
              overwrite,
              createCopyFromLocalCallbacks(getActiveAuditSpan()))
              .execute());
    } else {
      // call the superclass, but still count statistics.
      // there is no overall span here, as each FS API call will
      // be in its own span.
      LOG.debug("Using base copyFromLocalFile implementation");
      trackDurationAndSpan(INVOCATION_COPY_FROM_LOCAL_FILE, dst, () -> {
        super.copyFromLocalFile(delSrc, overwrite, src, dst);
        return null;
      });
    }
  }

  /**
   * Create the CopyFromLocalCallbacks;
   * protected to assist in mocking.
   * @param span audit span.
   * @return the callbacks
   * @throws IOException failure to get the local fs.
   */
  protected CopyFromLocalOperation.CopyFromLocalOperationCallbacks
      createCopyFromLocalCallbacks(final AuditSpanS3A span) throws IOException {
    LocalFileSystem local = getLocal(getConf());
    return new CopyFromLocalCallbacksImpl(span, local);
  }

  protected final class CopyFromLocalCallbacksImpl implements
      CopyFromLocalOperation.CopyFromLocalOperationCallbacks {

    /** Span to use for all operations. */
    private final AuditSpanS3A span;
    private final LocalFileSystem local;

    private CopyFromLocalCallbacksImpl(final AuditSpanS3A span,
        LocalFileSystem local) {
      this.span = span;
      this.local = local;
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocalStatusIterator(
        final Path path) throws IOException {
      return local.listLocatedStatus(path);
    }

    @Override
    public File pathToLocalFile(Path path) {
      return local.pathToFile(path);
    }

    @Override
    public boolean deleteLocal(Path path, boolean recursive) throws IOException {
      return local.delete(path, recursive);
    }

    @Override
    @Retries.RetryTranslated
    public void copyLocalFileFromTo(File file, Path from, Path to) throws IOException {
      // the duration of the put is measured, but the active span is the
      // constructor-supplied one -this ensures all audit log events are grouped correctly
      span.activate();
      trackDuration(getDurationTrackerFactory(), OBJECT_PUT_REQUESTS.getSymbol(), () -> {
        final String key = pathToKey(to);
        PutObjectRequest.Builder putObjectRequestBuilder =
            newPutObjectRequestBuilder(key, file.length(), false);
        final String dest = to.toString();
        S3AFileSystem.this.invoker.retry("putObject(" + dest + ")", dest, true, () ->
            executePut(putObjectRequestBuilder.build(), null,
                PutObjectOptions.defaultOptions(), file));
        return null;
      });
    }

    @Override
    @Retries.RetryTranslated
    public FileStatus getFileStatus(Path f) throws IOException {
      return S3AFileSystem.this.getFileStatus(f);
    }

    @Override
    @Retries.RetryTranslated
    public boolean createEmptyDir(Path path, StoreContext storeContext)
        throws IOException {
      return trackDuration(getDurationTrackerFactory(),
          INVOCATION_MKDIRS.getSymbol(),
          new MkdirOperation(
              storeContext,
              path,
              createMkdirOperationCallbacks(),
              false,
              performanceFlags.enabled(PerformanceFlagEnum.Mkdir)));
    }
  }

  /**
   * Execute a PUT via the transfer manager, blocking for completion.
   * @param putObjectRequest request
   * @param progress optional progress callback
   * @param putOptions put object options
   * @return the upload result
   * @throws IOException IO failure
   * @throws CancellationException if the wait() was cancelled
   */
  @Retries.OnceTranslated("For PUT; post-PUT actions are RetrySwallowed")
  PutObjectResponse executePut(
      final PutObjectRequest putObjectRequest,
      final Progressable progress,
      final PutObjectOptions putOptions,
      final File file)
      throws IOException {
    String key = putObjectRequest.key();
    ProgressableProgressListener listener =
        new ProgressableProgressListener(store, key, progress);
    UploadInfo info = putObject(putObjectRequest, file, listener);
    PutObjectResponse result = getStore().waitForUploadCompletion(key, info).response();
    listener.uploadCompleted(info.getFileUpload());
    return result;
  }

  /**
   * This override bypasses checking for existence.
   *
   * @param f the path to delete; this may be unqualified.
   * @return true, always.
   * @throws IOException IO failure
   */
  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    Path qualifedPath = makeQualified(f);
    synchronized (deleteOnExit) {
      deleteOnExit.add(qualifedPath);
    }
    return true;
  }

  /**
   * Cancel the scheduled deletion of the path when the FileSystem is closed.
   * @param f the path to cancel deletion
   * @return true if the path was found in the delete-on-exit list.
   */
  @Override
  public boolean cancelDeleteOnExit(Path f) {
    Path qualifedPath = makeQualified(f);
    synchronized (deleteOnExit) {
      return deleteOnExit.remove(qualifedPath);
    }
  }

  /**
   * Delete all paths that were marked as delete-on-exit. This recursively
   * deletes all files and directories in the specified paths. It does not
   * check if file exists and filesystem is closed.
   *
   * The time to process this operation is {@code O(paths)}, with the actual
   * time dependent on the time for existence and deletion operations to
   * complete, successfully or not.
   */
  @Override
  protected void processDeleteOnExit() {
    synchronized (deleteOnExit) {
      for (Iterator<Path> iter = deleteOnExit.iterator(); iter.hasNext();) {
        Path path = iter.next();
        try {
          deleteWithoutCloseCheck(path, true);
        } catch (IOException e) {
          LOG.info("Ignoring failure to deleteOnExit for path {}", path);
          LOG.debug("The exception for deleteOnExit is {}", e);
        }
        iter.remove();
      }
    }
  }

  /**
   * Close the filesystem. This shuts down all transfers.
   * @throws IOException IO problem
   */
  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      // already closed
      return;
    }
    isClosed = true;
    LOG.debug("Filesystem {} is closed", uri);
    try {
      super.close();
    } finally {
      stopAllServices();
      // log IO statistics, including of any file deletion during
      // superclass close
      if (getConf() != null) {
        String iostatisticsLoggingLevel =
            getConf().getTrimmed(IOSTATISTICS_LOGGING_LEVEL,
                IOSTATISTICS_LOGGING_LEVEL_DEFAULT);
        logIOStatisticsAtLevel(LOG, iostatisticsLoggingLevel, getIOStatistics());
      }
    }
  }

  /**
   * Stop all services.
   * This is invoked in close() and during failures of initialize()
   * -make sure that all operations here are robust to failures in
   * both the expected state of this FS and of failures while being stopped.
   */
  protected synchronized void stopAllServices() {
    try {
      trackDuration(getDurationTrackerFactory(), FILESYSTEM_CLOSE.getSymbol(), () -> {
        closeAutocloseables(LOG, getStore());
        store = null;
        s3Client = null;

        // At this point the S3A client is shut down,
        // now the executor pools are closed

        // shut future pool first as it wraps the bounded thread pool
        if (futurePool != null) {
          futurePool.shutdown(LOG, THREAD_POOL_SHUTDOWN_DELAY_SECONDS, TimeUnit.SECONDS);
          futurePool = null;
        }
        HadoopExecutors.shutdown(boundedThreadPool, LOG,
            THREAD_POOL_SHUTDOWN_DELAY_SECONDS, TimeUnit.SECONDS);
        boundedThreadPool = null;
        HadoopExecutors.shutdown(unboundedThreadPool, LOG,
            THREAD_POOL_SHUTDOWN_DELAY_SECONDS, TimeUnit.SECONDS);
        unboundedThreadPool = null;

        // other services are shutdown.
        cleanupWithLogger(LOG,
            delegationTokens.orElse(null),
            signerManager,
            auditManager);
        closeAutocloseables(LOG, credentials);
        delegationTokens = Optional.empty();
        signerManager = null;
        credentials = null;
        return null;
      });
    } catch (IOException e) {
      // failure during shutdown.
      // this should only be from the signature of trackDurationAndSpan().
      LOG.warn("Failure during service shutdown", e);
    }
    // and once this duration has been tracked, close the statistics
    // other services are shutdown.
    cleanupWithLogger(LOG, instrumentation);
  }

  /**
   * Verify that the filesystem has not been closed. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws PathIOException if the FS is closed.
   */
  private void checkNotClosed() throws PathIOException {
    if (isClosed) {
      throw new PathIOException(uri.toString(), E_FS_CLOSED);
    }
  }

  /**
   * Get the delegation token support for this filesystem;
   * not null iff delegation support is enabled.
   * @return the token support, or an empty option.
   */
  @VisibleForTesting
  public Optional<S3ADelegationTokens> getDelegationTokens() {
    return delegationTokens;
  }

  /**
   * Return a service name iff delegation tokens are enabled and the
   * token binding is issuing delegation tokens.
   * @return the canonical service name or null
   */
  @Override
  public String getCanonicalServiceName() {
    // this could all be done in map statements, but it'd be harder to
    // understand and maintain.
    // Essentially: no DTs, no canonical service name.
    if (!delegationTokens.isPresent()) {
      return null;
    }
    // DTs present: ask the binding if it is willing to
    // serve tokens (or fail noisily).
    S3ADelegationTokens dt = delegationTokens.get();
    return dt.getTokenIssuingPolicy() != NoTokensAvailable
        ? dt.getCanonicalServiceName()
        : null;
  }

  /**
   * Get a delegation token if the FS is set up for them.
   * If the user already has a token, it is returned,
   * <i>even if it has expired</i>.
   * @param renewer the account name that is allowed to renew the token.
   * @return the delegation token or null
   * @throws IOException IO failure
   */
  @Override
  @AuditEntryPoint
  public Token<AbstractS3ATokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    checkNotClosed();
    LOG.debug("Delegation token requested");
    if (delegationTokens.isPresent()) {
      return trackDurationAndSpan(
          INVOCATION_GET_DELEGATION_TOKEN, null, () ->
              delegationTokens.get().getBoundOrNewDT(
                  encryptionSecrets,
                  (renewer != null ? new Text(renewer) : new Text())));
    } else {
      // Delegation token support is not set up
      LOG.debug("Token support is not enabled");
      return null;
    }
  }

  /**
   * Ask any DT plugin for any extra token issuers.
   * These do not get told of the encryption secrets and can
   * return any type of token.
   * This allows DT plugins to issue extra tokens for
   * ancillary services.
   */
  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    checkNotClosed();
    if (delegationTokens.isPresent()) {
      return delegationTokens.get().getAdditionalTokenIssuers();
    } else {
      // Delegation token support is not set up
      LOG.debug("Token support is not enabled");
      return null;
    }
  }

  /**
   * Build the AWS policy for restricted access to the resources needed
   * by this bucket.
   * if needed, and KMS operations.
   * @param access access level desired.
   * @return a policy for use in roles
   */
  @Override
  @InterfaceAudience.Private
  public List<RoleModel.Statement> listAWSPolicyRules(
      final Set<AccessLevel> access) {
    if (access.isEmpty()) {
      return Collections.emptyList();
    }
    List<RoleModel.Statement> statements = new ArrayList<>(
        allowS3Operations(bucket,
            access.contains(AccessLevel.WRITE)
                || access.contains(AccessLevel.ADMIN)));

    // no attempt is made to qualify KMS access; there's no
    // way to predict read keys, and not worried about granting
    // too much encryption access.
    statements.add(STATEMENT_ALLOW_KMS_RW);
    if (s3ExpressStore) {
      LOG.warn("S3Express store polices not yet implemented");
    }

    return statements;
  }

  /**
   * Copy a single object in the bucket via a COPY operation.
   * There's no update of metadata, etc.
   * Callers must implement.
   * @param srcKey source object path
   * @param dstKey destination object path
   * @param size object size
   * @param srcAttributes S3 attributes of the source object
   * @param readContext the read context
   * @return the result of the copy
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException Other IO problems
   */
  @Retries.RetryTranslated
  private CopyObjectResponse copyFile(String srcKey, String dstKey, long size,
      S3ObjectAttributes srcAttributes, S3AReadOpContext readContext)
      throws IOException {
    LOG.debug("copyFile {} -> {} ", srcKey, dstKey);

    ChangeTracker changeTracker = new ChangeTracker(
        keyToQualifiedPath(srcKey).toString(),
        changeDetectionPolicy,
        readContext.getS3AStatisticsContext()
            .newInputStreamStatistics()
            .getChangeTrackerStatistics(),
        srcAttributes);

    String action = "copyFile(" + srcKey + ", " + dstKey + ")";
    Invoker readInvoker = readContext.getReadInvoker();

    HeadObjectResponse srcom;
    try {
      srcom = once(action, srcKey,
          () ->
              getObjectMetadata(srcKey, changeTracker, readInvoker, "copy"));
    } catch (FileNotFoundException e) {
      // if rename fails at this point it means that the expected file was not
      // found.
      // This means the File was deleted since LIST enumerated it.
      LOG.debug("getObjectMetadata({}) failed to find an expected file",
          srcKey, e);
      throw new RemoteFileChangedException(
          keyToQualifiedPath(srcKey).toString(),
          action,
          RemoteFileChangedException.FILE_NOT_FOUND_SINGLE_ATTEMPT,
          e);
    }

    CopyObjectRequest.Builder copyObjectRequestBuilder =
        getRequestFactory().newCopyObjectRequestBuilder(srcKey, dstKey, srcom);
    changeTracker.maybeApplyConstraint(copyObjectRequestBuilder);
    final CopyObjectRequest copyRequest = copyObjectRequestBuilder.build();
    LOG.debug("Copy Request: {}", copyRequest);
    CopyObjectResponse response;

    // transfer manager is skipped if disabled or the file is too small to worry about
    final boolean useTransferManager = isMultipartCopyEnabled && size >= multiPartThreshold;
    if (useTransferManager) {
      // use transfer manager
      response = readInvoker.retry(
          action, srcKey,
          true,
          () -> {
            incrementStatistic(OBJECT_COPY_REQUESTS);

            Copy copy = getStore().getOrCreateTransferManager().copy(
                CopyRequest.builder()
                    .copyObjectRequest(copyRequest)
                    .build());

            try {
              CompletedCopy completedCopy = copy.completionFuture().join();
              return completedCopy.response();
            } catch (CompletionException e) {
              Throwable cause = e.getCause();
              if (cause instanceof SdkException) {
                // if this is a 412 precondition failure, it may
                // be converted to a RemoteFileChangedException
                SdkException awsException = (SdkException)cause;
                changeTracker.processException(awsException, "copy");
                throw awsException;
              }
              throw extractException(action, srcKey, e);
            }
          });
    } else {
      // single part copy bypasses transfer manager
      // note, this helps with some mock testing, e.g. HBoss. as there is less to mock.
      response = readInvoker.retry(
          action, srcKey,
          true,
          () -> {
            LOG.debug("copyFile: single part copy {} -> {} of size {}", srcKey, dstKey, size);
            incrementStatistic(OBJECT_COPY_REQUESTS);
            try {
              return getS3Client().copyObject(copyRequest);
            } catch (SdkException awsException) {
              // if this is a 412 precondition failure, it may
              // be converted to a RemoteFileChangedException
              changeTracker.processException(awsException, "copy");
              // otherwise, rethrow
              throw awsException;
            }
          });
    }

    changeTracker.processResponse(response);
    incrementWriteOperations();
    instrumentation.filesCopied(1, size);
    return response;
  }

  /**
   * Initiate a multipart upload from the preconfigured request.
   * Retry policy: none + untranslated.
   * @param request request to initiate
   * @return the result of the call
   * @throws SdkException on failures inside the AWS SDK
   * @throws IOException Other IO problems
   */
  @Retries.OnceRaw
  CreateMultipartUploadResponse initiateMultipartUpload(
      CreateMultipartUploadRequest request) throws IOException {
    LOG.debug("Initiate multipart upload to {}", request.key());
    return trackDurationOfSupplier(getDurationTrackerFactory(),
        OBJECT_MULTIPART_UPLOAD_INITIATED.getSymbol(),
        () -> getS3Client().createMultipartUpload(request));
  }

  /**
   * Perform post-write actions.
   * <p>
   * This operation MUST be called after any PUT/multipart PUT completes
   * successfully.
   * @param key key written to
   * @param length total length of file written
   * @param putOptions put object options
   */
  @InterfaceAudience.Private
  @Retries.RetryExceptionsSwallowed
  void finishedWrite(
      String key,
      long length,
      PutObjectOptions putOptions) {
    LOG.debug("Finished write to {}, len {}.",
        key, length);
    Preconditions.checkArgument(length >= 0, "content length is negative");
  }

  /**
   * Create a fake directory, always ending in "/".
   * Retry policy: retrying; translated.
   * @param objectName name of directory object.
   * @param putOptions put object options
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private void createFakeDirectory(final String objectName,
      final PutObjectOptions putOptions)
      throws IOException {
    createEmptyObject(objectName, putOptions);
  }

  /**
   * Used to create an empty file that represents an empty directory.
   * Retry policy: retrying; translated.
   * @param objectName object to create
   * @param putOptions put object options
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private void createEmptyObject(final String objectName, PutObjectOptions putOptions)
      throws IOException {

    S3ADataBlocks.BlockUploadData uploadData = new S3ADataBlocks.BlockUploadData(
        new byte[0], 0, 0, null);

    invoker.retry("PUT 0-byte object ", objectName, true,
        () -> putObjectDirect(
            getRequestFactory().newDirectoryMarkerRequest(objectName).build(),
            putOptions,
            uploadData,
            getDurationTrackerFactory()));
    incrementPutProgressStatistics(objectName, 0);
    instrumentation.directoryCreated();
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.
   */
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", workingDir=").append(workingDir);
    sb.append(", partSize=").append(partSize);
    sb.append(", enableMultiObjectsDelete=").append(enableMultiObjectsDelete);
    sb.append(", maxKeys=").append(maxKeys);
    sb.append(", performanceFlags=").append(performanceFlags);
    if (cannedACL != null) {
      sb.append(", cannedACL=").append(cannedACL);
    }
    if (openFileHelper != null) {
      sb.append(", ").append(openFileHelper);
    }
    if (getConf() != null) {
      sb.append(", blockSize=").append(getDefaultBlockSize());
    }
    sb.append(", multiPartThreshold=").append(multiPartThreshold);
    if (getS3EncryptionAlgorithm() != null) {
      sb.append(", s3EncryptionAlgorithm='")
          .append(getS3EncryptionAlgorithm())
          .append('\'');
    }
    if (blockFactory != null) {
      sb.append(", blockFactory=").append(blockFactory);
    }
    sb.append(", auditManager=").append(auditManager);
    sb.append(", useListV1=").append(useListV1);
    if (committerIntegration != null) {
      sb.append(", magicCommitter=").append(isMagicCommitEnabled());
    }
    sb.append(", boundedExecutor=").append(boundedThreadPool);
    sb.append(", unboundedExecutor=").append(unboundedThreadPool);
    sb.append(", credentials=").append(credentials);
    sb.append(", delegation tokens=")
        .append(delegationTokens.map(Objects::toString).orElse("disabled"));
    // if logging at debug, toString returns the entire IOStatistics set.
    if (getInstrumentation() != null) {
      sb.append(", instrumentation {")
          .append(getInstrumentation().toString())
          .append("}");
    }
    sb.append(", ClientSideEncryption=").append(isCSEEnabled);

    if (accessPoint != null) {
      sb.append(", arnForBucket=").append(accessPoint.getFullArn());
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the partition size for multipart operations.
   * @return the value as set during initialization
   */
  public long getPartitionSize() {
    return partSize;
  }

  /**
   * Get the threshold for multipart files.
   * @return the value as set during initialization
   */
  public long getMultiPartThreshold() {
    return multiPartThreshold;
  }

  /**
   * Get the maximum key count.
   * @return a value, valid after initialization
   */
  int getMaxKeys() {
    return maxKeys;
  }

  /**
   * Is magic commit enabled?
   * @return true if magic commit support is turned on.
   */
  public boolean isMagicCommitEnabled() {
    return committerIntegration.isMagicCommitEnabled();
  }

  /**
   * Predicate: is a path a magic commit path?
   * True if magic commit is enabled and the path qualifies as special,
   * and is not a a .pending or .pendingset file,
   * @param path path to examine
   * @return true if writing a file to the path triggers a "magic" write.
   */
  public boolean isMagicCommitPath(Path path) {
    return committerIntegration.isMagicCommitPath(path);
  }

  /**
   * Predicate: is a path under a magic commit path?
   * True if magic commit is enabled and the path is under "MAGIC PATH",
   * irrespective of file type.
   * @param path path to examine
   * @return true if the path is in a magic dir and the FS has magic writes enabled.
   */
  private boolean isUnderMagicCommitPath(Path path) {
    return committerIntegration.isUnderMagicPath(path);
  }

  /**
   * Increments the statistic {@link Statistic#INVOCATION_GLOB_STATUS}.
   * Override superclass so as to disable symlink resolution as symlinks
   * are not supported by S3A.
   * {@inheritDoc}
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return globStatus(pathPattern, ACCEPT_ALL);
  }

  /**
   * Increments the statistic {@link Statistic#INVOCATION_GLOB_STATUS}.
   * Override superclass so as to disable symlink resolution as symlinks
   * are not supported by S3A.
   *
   * Although an AuditEntryPoint, the globber itself will talk do
   * the filesystem through the filesystem API, so its operations will
   * all appear part of separate operations.
   * {@inheritDoc}
   */
  @Override
  @AuditEntryPoint
  public FileStatus[] globStatus(
      final Path pathPattern,
      final PathFilter filter)
      throws IOException {
    return trackDurationAndSpan(
        INVOCATION_GLOB_STATUS, pathPattern, () ->
            Globber.createGlobber(this)
                .withPathPattern(pathPattern)
                .withPathFiltern(filter)
                .withResolveSymlinks(false)
                .build()
                .glob());
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  @AuditEntryPoint
  public boolean exists(Path f) throws IOException {
    final Path path = qualify(f);
    try {
      trackDurationAndSpan(
          INVOCATION_EXISTS, path, () ->
              innerGetFileStatus(path, false, StatusProbeEnum.ALL));
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Optimized probe for a path referencing a dir.
   * Even though it is optimized to a single HEAD, applications
   * should not over-use this method...it is all too common.
   * {@inheritDoc}
   */
  @Override
  @AuditEntryPoint
  @SuppressWarnings("deprecation")
  public boolean isDirectory(Path f) throws IOException {
    final Path path = qualify(f);
    try {
      return trackDurationAndSpan(
          INVOCATION_IS_DIRECTORY, path, () ->
              innerGetFileStatus(path, false, StatusProbeEnum.DIRECTORIES)
                  .isDirectory());
    } catch (FileNotFoundException e) {
      // not found or it is a file.
      return false;
    }
  }

  /**
   * Optimized probe for a path referencing a file.
   * Even though it is optimized to a single HEAD, applications
   * should not over-use this method...it is all too common.
   * {@inheritDoc}
   */
  @Override
  @AuditEntryPoint
  @SuppressWarnings("deprecation")
  public boolean isFile(Path f) throws IOException {
    final Path path = qualify(f);
    try {
      return trackDurationAndSpan(INVOCATION_IS_FILE, path, () ->
          innerGetFileStatus(path, false, StatusProbeEnum.HEAD_ONLY)
              .isFile());
    } catch (FileNotFoundException e) {
      // not found or it is a dir.
      return false;
    }
  }

  /**
   * When enabled, get the etag of a object at the path via HEAD request and
   * return it as a checksum object.
   * <ol>
   *   <li>If a tag has not changed, consider the object unchanged.</li>
   *   <li>Two tags being different does not imply the data is different.</li>
   * </ol>
   * Different S3 implementations may offer different guarantees.
   *
   * This check is (currently) only made if
   * {@link Constants#ETAG_CHECKSUM_ENABLED} is set; turning it on
   * has caused problems with Distcp (HADOOP-15273).
   *
   * @param f The file path
   * @param length The length of the file range for checksum calculation
   * @return The EtagChecksum or null if checksums are not enabled or supported.
   * @throws IOException IO failure
   * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html">Common Response Headers</a>
   */
  @Override
  @Retries.RetryTranslated
  @AuditEntryPoint
  public EtagChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    Preconditions.checkArgument(length >= 0);
    final Path path = qualify(f);
    if (getConf().getBoolean(ETAG_CHECKSUM_ENABLED,
        ETAG_CHECKSUM_ENABLED_DEFAULT)) {
      return trackDurationAndSpan(INVOCATION_GET_FILE_CHECKSUM, path, () -> {
        LOG.debug("getFileChecksum({})", path);
        HeadObjectResponse headers = getObjectMetadata(path, null,
            invoker,
            "getFileChecksum are");
        String eTag = headers.eTag();
        return eTag != null ? new EtagChecksum(eTag) : null;
      });
    } else {
      // disabled
      return null;
    }
  }

  /**
   * Get header processing support.
   * @return a new header processing instance.
   */
  private HeaderProcessing getHeaderProcessing() {
    return new HeaderProcessing(createStoreContext(),
        createHeaderProcessingCallbacks());
  }

  @Override
  @AuditEntryPoint
  public byte[] getXAttr(final Path path, final String name)
      throws IOException {
    checkNotClosed();
    try (AuditSpan span = createSpan(
        INVOCATION_XATTR_GET_NAMED.getSymbol(),
        path.toString(), null)) {
      return getHeaderProcessing().getXAttr(path, name);
    }
  }

  @Override
  @AuditEntryPoint
  public Map<String, byte[]> getXAttrs(final Path path) throws IOException {
    checkNotClosed();
    try (AuditSpan span = createSpan(
        INVOCATION_XATTR_GET_MAP.getSymbol(),
        path.toString(), null)) {
      return getHeaderProcessing().getXAttrs(path);
    }
  }

  @Override
  @AuditEntryPoint
  public Map<String, byte[]> getXAttrs(final Path path,
      final List<String> names)
      throws IOException {
    checkNotClosed();
    try (AuditSpan span = createSpan(
        INVOCATION_XATTR_GET_NAMED_MAP.getSymbol(),
        path.toString(), null)) {
      return getHeaderProcessing().getXAttrs(path, names);
    }
  }

  @Override
  @AuditEntryPoint
  public List<String> listXAttrs(final Path path) throws IOException {
    checkNotClosed();
    try (AuditSpan span = createSpan(
        INVOCATION_OP_XATTR_LIST.getSymbol(),
        path.toString(), null)) {
      return getHeaderProcessing().listXAttrs(path);
    }
  }

  /**
   * Create the callbacks.
   * @return An implementation of the header processing
   * callbacks.
   */
  protected HeaderProcessing.HeaderProcessingCallbacks
      createHeaderProcessingCallbacks() {
    return new HeaderProcessingCallbacksImpl();
  }

  /**
   * Operations needed for Header Processing.
   */
  protected final class HeaderProcessingCallbacksImpl implements
      HeaderProcessing.HeaderProcessingCallbacks {

    @Override
    public HeadObjectResponse getObjectMetadata(final String key)
        throws IOException {
      return once("getObjectMetadata", key, () ->
          S3AFileSystem.this.getObjectMetadata(key));
    }

    @Override
    public HeadBucketResponse getBucketMetadata()
        throws IOException {
      return once("getBucketMetadata", bucket, () ->
          S3AFileSystem.this.getBucketMetadata());
    }
  }
  /**
   * {@inheritDoc}.
   *
   * This implementation is optimized for S3, which can do a bulk listing
   * off all entries under a path in one single operation. Thus there is
   * no need to recursively walk the directory tree.
   *
   * Instead a {@link ListObjectsRequest} is created requesting a (windowed)
   * listing of all entries under the given path. This is used to construct
   * an {@code ObjectListingIterator} instance, iteratively returning the
   * sequence of lists of elements under the path. This is then iterated
   * over in a {@code FileStatusListingIterator}, which generates
   * {@link S3AFileStatus} instances, one per listing entry.
   * These are then translated into {@link LocatedFileStatus} instances.
   *
   * This is essentially a nested and wrapped set of iterators, with some
   * generator classes.
   * @param f a path
   * @param recursive if the subdirectories need to be traversed recursively
   *
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  @Retries.RetryTranslated
  @AuditEntryPoint
  public RemoteIterator<LocatedFileStatus> listFiles(Path f,
      boolean recursive) throws FileNotFoundException, IOException {
    final Path path = qualify(f);
    return toLocatedFileStatusIterator(
        trackDurationAndSpan(INVOCATION_LIST_FILES, path, () ->
            innerListFiles(path, recursive,
                new Listing.AcceptFilesOnly(path), null)));
  }

  /**
   * Recursive List of files and empty directories.
   * @param f path to list from
   * @param recursive recursive?
   * @return an iterator.
   * @throws IOException failure
   */
  @InterfaceAudience.Private
  @Retries.RetryTranslated
  @AuditEntryPoint
  public RemoteIterator<S3ALocatedFileStatus> listFilesAndEmptyDirectories(
      Path f, boolean recursive) throws IOException {
    final Path path = qualify(f);
    return trackDurationAndSpan(INVOCATION_LIST_FILES, path, () ->
        innerListFiles(path, recursive,
            Listing.ACCEPT_ALL_OBJECTS,
            null));
  }

  /**
   * List files under the path.
   * <ol>
   *   <li>
   *     The optional {@code status} parameter will be used to skip the
   *     initial getFileStatus call.
   *   </li>
   * </ol>
   *
   * @param f path
   * @param recursive recursive listing?
   * @param acceptor file status filter
   * @param status optional status of path to list.
   * @return an iterator over the listing.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private RemoteIterator<S3ALocatedFileStatus> innerListFiles(
      final Path f,
      final boolean recursive,
      final Listing.FileStatusAcceptor acceptor,
      final S3AFileStatus status) throws IOException {
    Path path = qualify(f);
    LOG.debug("listFiles({}, {})", path, recursive);
    try {
      // if a status was given and it is a file.
      if (status != null && status.isFile()) {
        // simple case: File
        LOG.debug("Path is a file: {}", path);
        return listing.createSingleStatusIterator(
            toLocatedFileStatus(status));
      }
      // Assuming the path to be a directory
      // do a bulk operation.
      RemoteIterator<S3ALocatedFileStatus> listFilesAssumingDir =
              listing.getListFilesAssumingDir(path,
                      recursive,
                      acceptor,
                  getActiveAuditSpan());
      // If there are no list entries present, we
      // fallback to file existence check as the path
      // can be a file or empty directory.
      if (!listFilesAssumingDir.hasNext()) {
        // If file status was already passed, reuse it.
        final S3AFileStatus fileStatus = status != null
                ? status
                : innerGetFileStatus(path, false, StatusProbeEnum.ALL);
        if (fileStatus.isFile()) {
          return listing.createSingleStatusIterator(
                  toLocatedFileStatus(fileStatus));
        }
      }
      // If we have reached here, it means either there are files
      // in this directory or it is empty.
      return listFilesAssumingDir;
    } catch (SdkException e) {
      throw translateException("listFiles", path, e);
    }
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws FileNotFoundException, IOException {
    return listLocatedStatus(f, ACCEPT_ALL);
  }

  /**
   * {@inheritDoc}.
   *
   * S3 Optimized directory listing. The initial operation performs the
   * first bulk listing; extra listings will take place
   * when all the current set of results are used up.
   * @param f a path
   * @param filter a path filter
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  @Retries.OnceTranslated("s3guard not retrying")
  @AuditEntryPoint
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
      throws FileNotFoundException, IOException {
    Path path = qualify(f);
    AuditSpan span = entryPoint(INVOCATION_LIST_LOCATED_STATUS, path);
    LOG.debug("listLocatedStatus({}, {}", path, filter);
    RemoteIterator<? extends LocatedFileStatus> iterator =
        once("listLocatedStatus", path.toString(),
          () -> {
            // Assuming the path to be a directory,
            // trigger a list call directly.
            final RemoteIterator<S3ALocatedFileStatus>
                    locatedFileStatusIteratorForDir =
                    listing.getLocatedFileStatusIteratorForDir(path, filter,
                        span);

            // If no listing is present then path might be a file.
            if (!locatedFileStatusIteratorForDir.hasNext()) {
              final S3AFileStatus fileStatus =
                  innerGetFileStatus(path, false, StatusProbeEnum.ALL);
              if (fileStatus.isFile()) {
                // simple case: File
                LOG.debug("Path is a file");
                return listing.createSingleStatusIterator(
                        filter.accept(path)
                                ? toLocatedFileStatus(fileStatus)
                                : null);
              }
            }
            // Either empty or non-empty directory.
            return locatedFileStatusIteratorForDir;
          });
    return toLocatedFileStatusIterator(iterator);
  }

  /**
   * Build a {@link S3ALocatedFileStatus} from a {@link FileStatus} instance.
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  S3ALocatedFileStatus toLocatedFileStatus(S3AFileStatus status)
      throws IOException {
    return new S3ALocatedFileStatus(status,
        status.isFile() ?
          getFileBlockLocations(status, 0, status.getLen())
          : null);
  }

  /**
   * List any pending multipart uploads whose keys begin with prefix, using
   * an iterator that can handle an unlimited number of entries.
   * See {@link #listMultipartUploads(String)} for a non-iterator version of
   * this.
   *
   * @param prefix optional key prefix to search
   * @return Iterator over multipart uploads.
   * @throws IOException on failure
   */
  @InterfaceAudience.Private
  @Retries.RetryTranslated
  @AuditEntryPoint
  public RemoteIterator<MultipartUpload> listUploads(@Nullable String prefix)
      throws IOException {
    // span is picked up retained in the listing.
    checkNotClosed();
    try (AuditSpan span = createSpan(MULTIPART_UPLOAD_LIST.getSymbol(),
        prefix, null)) {
      return listUploadsUnderPrefix(createStoreContext(), prefix);
    }
  }

  /**
   * List any pending multipart uploads whose keys begin with prefix, using
   * an iterator that can handle an unlimited number of entries.
   * See {@link #listMultipartUploads(String)} for a non-iterator version of
   * this.
   * @param storeContext store conext.
   * @param prefix optional key prefix to search
   * @return Iterator over multipart uploads.
   * @throws IOException on failure
   */
  @InterfaceAudience.Private
  @Retries.RetryTranslated
  public RemoteIterator<MultipartUpload> listUploadsUnderPrefix(
      final StoreContext storeContext,
      final @Nullable String prefix)
      throws IOException {
    // span is picked up retained in the listing.
    String p = prefix;
    if (prefix != null && !prefix.isEmpty() && !prefix.endsWith("/")) {
      p = prefix + "/";
    }
    // duration tracking is done in iterator.
    return MultipartUtils.listMultipartUploads(storeContext, getS3Client(), p, maxKeys);
  }

  /**
   * Listing all multipart uploads; limited to the first few hundred.
   * See {@link #listUploads(String)} for an iterator-based version that does
   * not limit the number of entries returned.
   * Retry policy: retry, translated.
   * @return a listing of multipart uploads.
   * @param prefix prefix to scan for, "" for none
   * @throws IOException IO failure, including any uprated SdkException
   */
  @InterfaceAudience.Private
  @Retries.RetryTranslated
  public List<MultipartUpload> listMultipartUploads(String prefix)
      throws IOException {
    // add a trailing / if needed.
    if (prefix != null && !prefix.isEmpty() && !prefix.endsWith("/")) {
      prefix = prefix + "/";
    }
    String p = prefix;
    return invoker.retry("listMultipartUploads", p, true, () -> {
      final ListMultipartUploadsRequest request = getRequestFactory()
          .newListMultipartUploadsRequestBuilder(p).build();
      return trackDuration(getInstrumentation(), MULTIPART_UPLOAD_LIST.getSymbol(), () ->
          getS3Client().listMultipartUploads(request).uploads());
    });
  }

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param destKey destination key
   * @param uploadId Upload ID
   * @throws IOException IO failure, including any uprated SdkException
   */
  @Retries.OnceTranslated
  public void abortMultipartUpload(String destKey, String uploadId) throws IOException {
    LOG.debug("Aborting multipart upload {} to {}", uploadId, destKey);
    trackDuration(getInstrumentation(), OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol(), () ->
        getS3Client().abortMultipartUpload(
            getRequestFactory().newAbortMultipartUploadRequestBuilder(
                destKey,
                uploadId).build()));
  }

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param upload the listed upload to abort.
   * @throws IOException IO failure, including any uprated SdkException
   */
  @Retries.OnceTranslated
  public void abortMultipartUpload(MultipartUpload upload) throws IOException {
    String destKey = upload.key();
    String uploadId = upload.uploadId();
    if (LOG.isDebugEnabled()) {
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      LOG.debug("Aborting multipart upload {} to {} initiated by {} on {}",
          uploadId, destKey, upload.initiator(),
          df.format(Date.from(upload.initiated())));
    }
    abortMultipartUpload(destKey, uploadId);
  }

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  public CommitterStatistics newCommitterStatistics() {
    return statisticsContext.newCommitterStatistics();
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    final Path p = makeQualified(path);
    String cap = validatePathCapabilityArgs(p, capability);
    switch (cap) {

    case CommitConstants.STORE_CAPABILITY_MAGIC_COMMITTER:
    case CommitConstants.STORE_CAPABILITY_MAGIC_COMMITTER_OLD:
      // capability depends on FS configuration
      return isMagicCommitEnabled();

    case CommonPathCapabilities.FS_CHECKSUMS:
      // capability depends on FS configuration
      return getConf().getBoolean(ETAG_CHECKSUM_ENABLED,
          ETAG_CHECKSUM_ENABLED_DEFAULT);

    case CommonPathCapabilities.ABORTABLE_STREAM:
      return true;
    case CommonPathCapabilities.FS_MULTIPART_UPLOADER:
      // client side encryption doesn't support multipart uploader.
      return !isCSEEnabled;

    // this client is safe to use with buckets
    // containing directory markers anywhere in
    // the hierarchy
    case STORE_CAPABILITY_DIRECTORY_MARKER_AWARE:
      return true;

    case ENABLE_MULTI_DELETE:
      return enableMultiObjectsDelete;

      // Do directory operations purge uploads.
    case DIRECTORY_OPERATIONS_PURGE_UPLOADS:
      return dirOperationsPurgeUploads;

      // this is a v2 sdk release.
    case STORE_CAPABILITY_AWS_V2:
      return true;

      // is this store S3 Express?
      // if so, note that directory listings may be inconsistent
    case STORE_CAPABILITY_S3_EXPRESS_STORAGE:
    case DIRECTORY_LISTING_INCONSISTENT:
      return s3ExpressStore;

    // etags are available in listings, but they
    // are not consistent across renames.
    // therefore, only availability is declared
    case CommonPathCapabilities.ETAGS_AVAILABLE:
      // block locations are generated locally
    case CommonPathCapabilities.VIRTUAL_BLOCK_LOCATIONS:
      return true;

    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_KEEP:
    case STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP:
      return true;
    // never true
    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_AUTHORITATIVE:
    case STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE:
      return false;

    case STORE_CAPABILITY_DIRECTORY_MARKER_MULTIPART_UPLOAD_ENABLED:
      return isMultipartUploadEnabled();

    // create file options
    case FS_S3A_CREATE_PERFORMANCE:
    case FS_S3A_CREATE_HEADER:
      return true;

    // is the FS configured for create file performance
    case FS_S3A_CREATE_PERFORMANCE_ENABLED:
      return performanceFlags.enabled(PerformanceFlagEnum.Create);

      // is the optimized copy from local enabled.
    case OPTIMIZED_COPY_FROM_LOCAL:
      return optimizedCopyFromLocal;

    // probe for a fips endpoint
    case FIPS_ENDPOINT:
      return fipsEnabled;

    // is S3 Access Grants enabled
    case AWS_S3_ACCESS_GRANTS_ENABLED:
      return s3AccessGrantsEnabled;

    default:
      // is it a performance flag?
      if (performanceFlags.hasCapability(capability)) {
        return true;
      }

      // ask the store for what input stream capabilities it offers
      if (getStore() != null && getStore().hasCapability(capability)) {
        return true;
      }

      // fall through
    }

    // hand off to superclass
    return super.hasPathCapability(p, cap);
  }

  /**
   * Return the capabilities of this filesystem instance.
   *
   * This has been supplanted by {@link #hasPathCapability(Path, String)}.
   * @param capability string to query the stream support for.
   * @return whether the FS instance has the capability.
   */
  @Deprecated
  @Override
  public boolean hasCapability(String capability) {
    try {
      return hasPathCapability(new Path("/"), capability);
    } catch (IOException ex) {
      // should never happen, so log and downgrade.
      LOG.debug("Ignoring exception on hasCapability({}})", capability, ex);
      return false;
    }
  }

  /**
   * Get a shared copy of the AWS credentials, with its reference
   * counter updated.
   * Caller is required to call {@code close()} on this after
   * they have finished using it.
   * @param purpose what is this for? This is initially for logging
   * @return a reference to shared credentials.
   */
  public AWSCredentialProviderList shareCredentials(final String purpose) {
    LOG.debug("Sharing credentials for: {}", purpose);
    return credentials.share();
  }

  /**
   * Get the file status of the source file.
   * If in the fileInformation parameter return that
   * if not found, issue a HEAD request, looking for a
   * file only.
   * @param path path of the file to open
   * @param fileInformation information on the file to open
   * @return a file status
   * @throws FileNotFoundException if a HEAD request found no file
   * @throws IOException IO failure
   */
  private S3AFileStatus extractOrFetchSimpleFileStatus(
      final Path path,
      final OpenFileSupport.OpenFileInformation fileInformation)
      throws IOException {
    S3AFileStatus fileStatus = fileInformation.getStatus();
    if (fileStatus == null) {
      // we check here for the passed in status
      // being a directory
      fileStatus = innerGetFileStatus(path, false,
          StatusProbeEnum.HEAD_ONLY);
    }
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException(path.toString() + " is a directory");
    }

    return fileStatus;
  }

  /**
   * Initiate the open() operation.
   * This is invoked from both the FileSystem and FileContext APIs.
   * It's declared as an audit entry point but the span creation is pushed
   * down into the open operation s it ultimately calls.
   * @param rawPath path to the file
   * @param parameters open file parameters from the builder.
   * @return a future which will evaluate to the opened file.
   * @throws IOException failure to resolve the link.
   * @throws IllegalArgumentException unknown mandatory key
   */
  @Override
  @Retries.RetryTranslated
  @AuditEntryPoint
  public CompletableFuture<FSDataInputStream> openFileWithOptions(
      final Path rawPath,
      final OpenFileParameters parameters) throws IOException {
    final Path path = qualify(rawPath);
    OpenFileSupport.OpenFileInformation fileInformation =
        openFileHelper.prepareToOpenFile(
            path,
            parameters,
            getDefaultBlockSize());
    CompletableFuture<FSDataInputStream> result = new CompletableFuture<>();
    unboundedThreadPool.submit(() ->
        LambdaUtils.eval(result,
            () -> executeOpen(path, fileInformation)));
    return result;
  }

  @Override
  @AuditEntryPoint
  public S3AMultipartUploaderBuilder createMultipartUploader(
      final Path basePath)
      throws IOException {
    if(isCSEEnabled) {
      throw new UnsupportedOperationException("Multi-part uploader not "
          + "supported for Client side encryption.");
    }
    final Path path = makeQualified(basePath);
    try (AuditSpan span = entryPoint(MULTIPART_UPLOAD_INSTANTIATED, path)) {
      StoreContext ctx = createStoreContext();
      return new S3AMultipartUploaderBuilder(this,
          createWriteOperationHelper(span),
          ctx,
          path,
          statisticsContext.createMultipartUploaderStatistics());
    }
  }

  /**
   * Build an immutable store context.
   * If called while the FS is being initialized,
   * some of the context will be incomplete.
   * new store context instances should be created as appropriate.
   * @return the store context of this FS.
   */
  @Override
  @InterfaceAudience.Private
  public StoreContext createStoreContext() {

    // please keep after setFsURI() in alphabetical order
    return new StoreContextBuilder()
        .setFsURI(getUri())
        .setAuditor(getAuditor())
        .setBucket(getBucket())
        .setChangeDetectionPolicy(changeDetectionPolicy)
        .setConfiguration(getConf())
        .setContextAccessors(new ContextAccessorsImpl())
        .setEnableCSE(isCSEEnabled)
        .setExecutor(boundedThreadPool)
        .setExecutorCapacity(executorCapacity)
        .setInputPolicy(getInputPolicy())
        .setInstrumentation(statisticsContext)
        .setInvoker(invoker)
        .setMultiObjectDeleteEnabled(enableMultiObjectsDelete)
        .setOwner(owner)
        .setPerformanceFlags(performanceFlags)
        .setStorageStatistics(getStorageStatistics())
        .setUseListV1(useListV1)
        .setUsername(getUsername())
        .build();
  }

  /**
   * Create a marker tools operations binding for this store.
   * Auditing:
   * @param target target path
   * @return callbacks for operations.
   * @throws IOException if raised during span creation
   */
  @AuditEntryPoint
  @InterfaceAudience.Private
  public MarkerToolOperations createMarkerToolOperations(final String target)
      throws IOException {
    createSpan("marker-tool-scan", target,
        null);
    return new MarkerToolOperationsImpl(new OperationCallbacksImpl(createStoreContext()));
  }

  /**
   * This is purely for testing, as it force initializes all static
   * initializers. See HADOOP-17385 for details.
   */
  @InterfaceAudience.Private
  public static void initializeClass() {
    LOG.debug("Initialize S3A class");
  }

  /**
   * The implementation of context accessors.
   */
  private class ContextAccessorsImpl implements ContextAccessors {

    @Override
    public Path keyToPath(final String key) {
      return keyToQualifiedPath(key);
    }

    @Override
    public String pathToKey(final Path path) {
      return S3AFileSystem.this.pathToKey(path);
    }

    @Override
    public File createTempFile(final String prefix, final long size)
        throws IOException {
      return createTmpFileForWrite(prefix, size, getConf());
    }

    @Override
    public String getBucketLocation() throws IOException {
      return S3AFileSystem.this.getBucketLocation();
    }

    @Override
    public Path makeQualified(final Path path) {
      return S3AFileSystem.this.makeQualified(path);
    }

    @Override
    public AuditSpan getActiveAuditSpan() {
      return S3AFileSystem.this.getActiveAuditSpan();
    }

    @Override
    public RequestFactory getRequestFactory() {
      return S3AFileSystem.this.getRequestFactory();
    }
  }

  /**
   * a method to know if Client side encryption is enabled or not.
   * @return a boolean stating if CSE is enabled.
   */
  public boolean isCSEEnabled() {
    return isCSEEnabled;
  }

  public boolean isMultipartUploadEnabled() {
    return isMultipartUploadEnabled;
  }

  /**
   * S3A implementation to create a bulk delete operation using
   * which actual bulk delete calls can be made.
   * @return an implementation of the bulk delete.
   */
  @Override
  public BulkDelete createBulkDelete(final Path path)
      throws IllegalArgumentException, IOException {

    final Path p = makeQualified(path);
    final AuditSpanS3A span = createSpan("bulkdelete", p.toString(), null);
    final int size = enableMultiObjectsDelete ? pageSize : 1;
    return new BulkDeleteOperation(
        createStoreContext(),
        createBulkDeleteCallbacks(p, size, span),
        p,
        size,
        span);
  }

  /**
   * Create the callbacks for the bulk delete operation.
   * @param path path to delete.
   * @param pageSize page size.
   * @param span span for operations.
   * @return an instance of the Bulk Delete callbacks.
   */
  protected BulkDeleteOperation.BulkDeleteOperationCallbacks createBulkDeleteCallbacks(
      Path path, int pageSize, AuditSpanS3A span) {
    return new BulkDeleteOperationCallbacksImpl(getStore(), pathToKey(path), pageSize, span);
  }

}
