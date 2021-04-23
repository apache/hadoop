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
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.event.ProgressListener;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Globber;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.auth.SignerManager;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationOperations;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.fs.s3a.impl.BulkDeleteRetryHandler;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ContextAccessors;
import org.apache.hadoop.fs.s3a.impl.CopyOutcome;
import org.apache.hadoop.fs.s3a.impl.DeleteOperation;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicy;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicyImpl;
import org.apache.hadoop.fs.s3a.impl.HeaderProcessing;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.ListingOperationCallbacks;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport;
import org.apache.hadoop.fs.s3a.impl.OperationCallbacks;
import org.apache.hadoop.fs.s3a.impl.RenameOperation;
import org.apache.hadoop.fs.s3a.impl.S3AMultipartUploaderBuilder;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.StoreContextBuilder;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.select.InternalSelectConstants;
import org.apache.hadoop.fs.s3a.tools.MarkerToolOperations;
import org.apache.hadoop.fs.s3a.tools.MarkerToolOperationsImpl;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.LambdaUtils;
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
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3a.auth.delegation.AWSPolicyProvider;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecretOperations;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens;
import org.apache.hadoop.fs.s3a.auth.delegation.AbstractS3ATokenIdentifier;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.PutTracker;
import org.apache.hadoop.fs.s3a.commit.MagicCommitIntegration;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.select.SelectBinding;
import org.apache.hadoop.fs.s3a.select.SelectConstants;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.s3a.statistics.impl.BondedS3AStatisticsContext;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.fs.store.EtagChecksum;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.rejectUnknownMandatoryKeys;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.*;
import static org.apache.hadoop.fs.s3a.Listing.toLocatedFileStatusIterator;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.STATEMENT_ALLOW_SSE_KMS_RW;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.allowS3Operations;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.TokenIssuingPolicy.NoTokensAvailable;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.hasDelegationTokenBinding;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletionIgnoringExceptions;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isObjectNotFound;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isUnknownBucket;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404;
import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.fixBucketRegion;
import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.logDnsLookup;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.dirMetaToStatuses;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.pairedTrackerFactory;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
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
    AWSPolicyProvider, DelegationTokenProvider, IOStatisticsSource {
  /**
   * Default blocksize as used in blocksize and FS status queries.
   */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;

  /**
   * This declared delete as idempotent.
   * This is an "interesting" topic in past Hadoop FS work.
   * Essentially: with a single caller, DELETE is idempotent
   * but in a shared filesystem, it is is very much not so.
   * Here, on the basis that isn't a filesystem with consistency guarantees,
   * retryable results in files being deleted.
  */
  public static final boolean DELETE_CONSIDERED_IDEMPOTENT = true;

  private URI uri;
  private Path workingDir;
  private String username;
  private AmazonS3 s3;
  // initial callback policy is fail-once; it's there just to assist
  // some mock tests and other codepaths trying to call the low level
  // APIs on an uninitialized filesystem.
  private Invoker invoker = new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL,
      Invoker.LOG_EVENT);
  // Only used for very specific code paths which behave differently for
  // S3Guard. Retries FileNotFound, so be careful if you use this.
  private Invoker s3guardInvoker = new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL,
      Invoker.LOG_EVENT);
  private final Retried onRetry = this::operationRetried;
  private String bucket;
  private int maxKeys;
  private Listing listing;
  private long partSize;
  private boolean enableMultiObjectsDelete;
  private TransferManager transfers;
  private ExecutorService boundedThreadPool;
  private ThreadPoolExecutor unboundedThreadPool;
  private int executorCapacity;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem.class);
  private static final Logger PROGRESS =
      LoggerFactory.getLogger("org.apache.hadoop.fs.s3a.S3AFileSystem.Progress");
  private LocalDirAllocator directoryAllocator;
  private CannedAccessControlList cannedACL;
  private boolean failOnMetadataWriteError;

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

  private long readAhead;
  private S3AInputPolicy inputPolicy;
  private ChangeDetectionPolicy changeDetectionPolicy;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile boolean isClosed = false;
  private MetadataStore metadataStore;
  private boolean allowAuthoritativeMetadataStore;
  private Collection<String> allowAuthoritativePaths;

  /** Delegation token integration; non-empty when DT support is enabled. */
  private Optional<S3ADelegationTokens> delegationTokens = Optional.empty();

  /** Principal who created the FS; recorded during initialization. */
  private UserGroupInformation owner;

  private String blockOutputBuffer;
  private S3ADataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;
  private WriteOperationHelper writeHelper;
  private SelectBinding selectBinding;
  private boolean useListV1;
  private MagicCommitIntegration committerIntegration;

  private AWSCredentialProviderList credentials;
  private SignerManager signerManager;

  private ITtlTimeProvider ttlTimeProvider;

  /**
   * Page size for deletions.
   */
  private int pageSize;

  /**
   * Specific operations used by rename and delete operations.
   */
  private final S3AFileSystem.OperationCallbacksImpl
      operationCallbacks = new OperationCallbacksImpl();

  private final ListingOperationCallbacks listingOperationCallbacks =
          new ListingOperationCallbacksImpl();
  /**
   * Directory policy.
   */
  private DirectoryPolicy directoryPolicy;

  /**
   * Header processing for XAttr.
   */
  private HeaderProcessing headerProcessing;

  /**
   * Context accessors for re-use.
   */
  private final ContextAccessors contextAccessors = new ContextAccessorsImpl();

  /** Add any deprecated keys. */
  @SuppressWarnings("deprecation")
  private static void addDeprecatedKeys() {
    Configuration.DeprecationDelta[] deltas = {
        new Configuration.DeprecationDelta(
            FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS,
            FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS)
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
    try {
      LOG.debug("Initializing S3AFileSystem for {}", bucket);
      // clone the configuration into one with propagated bucket options
      Configuration conf = propagateBucketOptions(originalConf, bucket);
      // fix up the classloader of the configuration to be whatever
      // classloader loaded this filesystem.
      // See: HADOOP-17372
      conf.setClassLoader(this.getClass().getClassLoader());

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

      // look for encryption data
      // DT Bindings may override this
      setEncryptionSecrets(new EncryptionSecrets(
          getEncryptionAlgorithm(bucket, conf),
          getServerSideEncryptionKey(bucket, getConf())));

      invoker = new Invoker(new S3ARetryPolicy(getConf()), onRetry);
      instrumentation = new S3AInstrumentation(uri);
      initializeStatisticsBinding();

      // Username is the current user at the time the FS was instantiated.
      owner = UserGroupInformation.getCurrentUser();
      username = owner.getShortUserName();
      workingDir = new Path("/user", username)
          .makeQualified(this.uri, this.getWorkingDirectory());

      s3guardInvoker = new Invoker(new S3GuardExistsRetryPolicy(getConf()),
          onRetry);
      writeHelper = new WriteOperationHelper(this, getConf(),
          statisticsContext);

      failOnMetadataWriteError = conf.getBoolean(FAIL_ON_METADATA_WRITE_ERROR,
          FAIL_ON_METADATA_WRITE_ERROR_DEFAULT);

      maxKeys = intOption(conf, MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS, 1);
      partSize = getMultipartSizeProperty(conf,
          MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
      multiPartThreshold = getMultipartSizeProperty(conf,
          MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

      //check but do not store the block size
      longBytesOption(conf, FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE, 1);
      enableMultiObjectsDelete = conf.getBoolean(ENABLE_MULTI_DELETE, true);

      readAhead = longBytesOption(conf, READAHEAD_RANGE,
          DEFAULT_READAHEAD_RANGE, 0);

      initThreadPools(conf);

      int listVersion = conf.getInt(LIST_VERSION, DEFAULT_LIST_VERSION);
      if (listVersion < 1 || listVersion > 2) {
        LOG.warn("Configured fs.s3a.list.version {} is invalid, forcing " +
            "version 2", listVersion);
      }
      useListV1 = (listVersion == 1);

      signerManager = new SignerManager(bucket, this, conf, owner);
      signerManager.initCustomSigners();

      // creates the AWS client, including overriding auth chain if
      // the FS came with a DT
      // this may do some patching of the configuration (e.g. setting
      // the encryption algorithms)
      bindAWSClient(name, delegationTokensEnabled);

      initTransferManager();

      initCannedAcls(conf);

      // This initiates a probe against S3 for the bucket existing.
      doBucketProbing();

      inputPolicy = S3AInputPolicy.getPolicy(
          conf.getTrimmed(INPUT_FADVISE, INPUT_FADV_NORMAL));
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
      // header processing for rename and magic committer
      headerProcessing = new HeaderProcessing(createStoreContext());

      // instantiate S3 Select support
      selectBinding = new SelectBinding(writeHelper);

      boolean blockUploadEnabled = conf.getBoolean(FAST_UPLOAD, true);

      if (!blockUploadEnabled) {
        LOG.warn("The \"slow\" output stream is no longer supported");
      }
      blockOutputBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER,
          DEFAULT_FAST_UPLOAD_BUFFER);
      partSize = ensureOutputParameterInRange(MULTIPART_SIZE, partSize);
      blockFactory = S3ADataBlocks.createFactory(this, blockOutputBuffer);
      blockOutputActiveBlocks = intOption(conf,
          FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
      LOG.debug("Using S3ABlockOutputStream with buffer = {}; block={};" +
              " queue limit={}",
          blockOutputBuffer, partSize, blockOutputActiveBlocks);
      long authDirTtl = conf.getTimeDuration(METADATASTORE_METADATA_TTL,
          DEFAULT_METADATASTORE_METADATA_TTL, TimeUnit.MILLISECONDS);
      ttlTimeProvider = new S3Guard.TtlTimeProvider(authDirTtl);

      setMetadataStore(S3Guard.getMetadataStore(this, ttlTimeProvider));
      allowAuthoritativeMetadataStore = conf.getBoolean(METADATASTORE_AUTHORITATIVE,
          DEFAULT_METADATASTORE_AUTHORITATIVE);
      allowAuthoritativePaths = S3Guard.getAuthoritativePaths(this);

      if (hasMetadataStore()) {
        LOG.debug("Using metadata store {}, authoritative store={}, authoritative path={}",
            getMetadataStore(), allowAuthoritativeMetadataStore, allowAuthoritativePaths);
      }

      // LOG if S3Guard is disabled on the warn level set in config
      if (!hasMetadataStore()) {
        String warnLevel = conf.getTrimmed(S3GUARD_DISABLED_WARN_LEVEL,
            DEFAULT_S3GUARD_DISABLED_WARN_LEVEL);
        S3Guard.logS3GuardDisabled(LOG, warnLevel, bucket);
      }
      // directory policy, which may look at authoritative paths
      directoryPolicy = DirectoryPolicyImpl.getDirectoryPolicy(conf,
          this::allowAuthoritative);
      LOG.debug("Directory marker retention policy is {}", directoryPolicy);

      initMultipartUploads(conf);

      pageSize = intOption(getConf(), BULK_DELETE_PAGE_SIZE,
          BULK_DELETE_PAGE_SIZE_DEFAULT, 0);
      listing = new Listing(listingOperationCallbacks, createStoreContext());
    } catch (AmazonClientException e) {
      // amazon client exception: stop all services then throw the translation
      stopAllServices();
      throw translateException("initializing ", new Path(name), e);
    } catch (IOException | RuntimeException e) {
      // other exceptions: stop the services.
      stopAllServices();
      throw e;
    }

  }

  /**
   * Test bucket existence in S3.
   * When the value of {@link Constants#S3A_BUCKET_PROBE} is set to 0,
   * bucket existence check is not done to improve performance of
   * S3AFileSystem initialization. When set to 1 or 2, bucket existence check
   * will be performed which is potentially slow.
   * If 3 or higher: warn and use the v2 check.
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
      logDnsLookup(getConf());
      verifyBucketExists();
      break;
    case 2:
      logDnsLookup(getConf());
      verifyBucketExistsV2();
      break;
    default:
      // we have no idea what this is, assume it is from a later release.
      LOG.warn("Unknown bucket probe option {}: {}; falling back to check #2",
          S3A_BUCKET_PROBE, bucketProbe);
      verifyBucketExistsV2();
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
   * Initialize the thread pool.
   * This must be re-invoked after replacing the S3Client during test
   * runs.
   * @param conf configuration.
   */
  private void initThreadPools(Configuration conf) {
    int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
    if (maxThreads < 2) {
      LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }
    int totalTasks = intOption(conf,
        MAX_TOTAL_TASKS, DEFAULT_MAX_TOTAL_TASKS, 1);
    long keepAliveTime = longOption(conf, KEEPALIVE_TIME,
        DEFAULT_KEEPALIVE_TIME, 0);
    boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
        maxThreads,
        maxThreads + totalTasks,
        keepAliveTime, TimeUnit.SECONDS,
        "s3a-transfer-shared");
    unboundedThreadPool = new ThreadPoolExecutor(
        maxThreads, Integer.MAX_VALUE,
        keepAliveTime, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        BlockingThreadPoolExecutorService.newDaemonThreadFactory(
            "s3a-transfer-unbounded"));
    unboundedThreadPool.allowCoreThreadTimeOut(true);
    executorCapacity = intOption(conf,
        EXECUTOR_CAPACITY, DEFAULT_EXECUTOR_CAPACITY, 1);
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
   * Verify that the bucket exists. This does not check permissions,
   * not even read access.
   * Retry policy: retrying, translated.
   * @throws UnknownStoreException the bucket is absent
   * @throws IOException any other problem talking to S3
   */
  @Retries.RetryTranslated
  protected void verifyBucketExists()
      throws UnknownStoreException, IOException {
    if (!invoker.retry("doesBucketExist", bucket, true,
        () -> s3.doesBucketExist(bucket))) {
      throw new UnknownStoreException("Bucket " + bucket + " does not exist");
    }
  }

  /**
   * Verify that the bucket exists. This will correctly throw an exception
   * when credentials are invalid.
   * Retry policy: retrying, translated.
   * @throws UnknownStoreException the bucket is absent
   * @throws IOException any other problem talking to S3
   */
  @Retries.RetryTranslated
  protected void verifyBucketExistsV2()
          throws UnknownStoreException, IOException {
    if (!invoker.retry("doesBucketExistV2", bucket, true,
        () -> s3.doesBucketExistV2(bucket))) {
      throw new UnknownStoreException("Bucket " + bucket + " does not exist");
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
   * Get current listing instance.
   * @return this instance's listing.
   */
  public Listing getListing() {
    return listing;
  }

  /**
   * Set up the client bindings.
   * If delegation tokens are enabled, the FS first looks for a DT
   * ahead of any other bindings;.
   * If there is a DT it uses that to do the auth
   * and switches to the DT authenticator automatically (and exclusively)
   * @param name URI of the FS
   * @param dtEnabled are delegation tokens enabled?
   * @throws IOException failure.
   */
  private void bindAWSClient(URI name, boolean dtEnabled) throws IOException {
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
      credentials = createAWSCredentialProviderSet(name, conf);
    }
    LOG.debug("Using credential provider {}", credentials);
    Class<? extends S3ClientFactory> s3ClientFactoryClass = conf.getClass(
        S3_CLIENT_FACTORY_IMPL, DEFAULT_S3_CLIENT_FACTORY_IMPL,
        S3ClientFactory.class);

    S3ClientFactory.S3ClientCreationParameters parameters = null;
    parameters = new S3ClientFactory.S3ClientCreationParameters()
        .withCredentialSet(credentials)
        .withEndpoint(conf.getTrimmed(ENDPOINT, DEFAULT_ENDPOINT))
        .withMetrics(statisticsContext.newStatisticsFromAwsSdk())
        .withPathStyleAccess(conf.getBoolean(PATH_STYLE_ACCESS, false))
        .withUserAgentSuffix(uaSuffix);

    s3 = ReflectionUtils.newInstance(s3ClientFactoryClass, conf)
        .createS3Client(getUri(),
            parameters);
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
  }

  /**
   * Get the encryption secrets.
   * This potentially sensitive information and must be treated with care.
   * @return the current encryption secrets.
   */
  public EncryptionSecrets getEncryptionSecrets() {
    return encryptionSecrets;
  }

  private void initTransferManager() {
    TransferManagerConfiguration transferConfiguration =
        new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    transfers = new TransferManager(s3, unboundedThreadPool);
    transfers.setConfiguration(transferConfiguration);
  }

  private void initCannedAcls(Configuration conf) {
    String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
    if (!cannedACLName.isEmpty()) {
      cannedACL = CannedAccessControlList.valueOf(cannedACLName);
    } else {
      cannedACL = null;
    }
  }

  @Retries.RetryTranslated
  private void initMultipartUploads(Configuration conf) throws IOException {
    boolean purgeExistingMultipart = conf.getBoolean(PURGE_EXISTING_MULTIPART,
        DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge = longOption(conf,
        PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

    if (purgeExistingMultipart) {
      try {
        abortOutstandingMultipartUploads(purgeExistingMultipartAge);
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
    Date purgeBefore =
        new Date(new Date().getTime() - seconds * 1000);
    LOG.debug("Purging outstanding multipart uploads older than {}",
        purgeBefore);
    invoker.retry("Purging multipart uploads", bucket, true,
        () -> transfers.abortMultipartUploads(bucket, purgeBefore));
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return "s3a"
   */
  @Override
  public String getScheme() {
    return "s3a";
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
   * Returns the S3 client used by this filesystem.
   * This is for internal use within the S3A code itself.
   * @return AmazonS3Client
   */
  AmazonS3 getAmazonS3Client() {
    return s3;
  }

  /**
   * Returns the S3 client used by this filesystem.
   * <i>Warning: this must only be used for testing, as it bypasses core
   * S3A operations. </i>
   * @param reason a justification for requesting access.
   * @return AmazonS3Client
   */
  @VisibleForTesting
  public AmazonS3 getAmazonS3ClientForTesting(String reason) {
    LOG.warn("Access to S3A client requested, reason {}", reason);
    return s3;
  }

  /**
   * Set the client -used in mocking tests to force in a different client.
   * @param client client.
   */
  protected void setAmazonS3Client(AmazonS3 client) {
    Preconditions.checkNotNull(client, "client");
    LOG.debug("Setting S3 client to {}", client);
    s3 = client;

    // Need to use a new TransferManager that uses the new client.
    // Also, using a new TransferManager requires a new threadpool as the old
    // TransferManager will shut the thread pool down when it is garbage
    // collected.
    initThreadPools(getConf());
    initTransferManager();
  }

  /**
   * Get the region of a bucket.
   * @return the region in which a bucket is located
   * @throws AccessDeniedException if the caller lacks permission.
   * @throws IOException on any failure.
   */
  @Retries.RetryTranslated
  public String getBucketLocation() throws IOException {
    return getBucketLocation(bucket);
  }

  /**
   * Get the region of a bucket; fixing up the region so it can be used
   * in the builders of other AWS clients.
   * Requires the caller to have the AWS role permission
   * {@code s3:GetBucketLocation}.
   * Retry policy: retrying, translated.
   * @param bucketName the name of the bucket
   * @return the region in which a bucket is located
   * @throws AccessDeniedException if the caller lacks permission.
   * @throws IOException on any failure.
   */
  @VisibleForTesting
  @Retries.RetryTranslated
  public String getBucketLocation(String bucketName) throws IOException {
    final String region = invoker.retry("getBucketLocation()", bucketName, true,
        () -> s3.getBucketLocation(bucketName));
    return fixBucketRegion(region);
  }

  /**
   * Returns the read ahead range value used by this filesystem.
   * @return the readahead range
   */
  @VisibleForTesting
  long getReadAheadRange() {
    return readAhead;
  }

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
   * Get the encryption algorithm of this endpoint.
   * @return the encryption algorithm.
   */
  public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
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
    if (directoryAllocator == null) {
      synchronized (this) {
        String bufferDir = conf.get(BUFFER_DIR) != null
            ? BUFFER_DIR : HADOOP_TMP_DIR;
        directoryAllocator = new LocalDirAllocator(bufferDir);
      }
    }
    Path path = directoryAllocator.getLocalPathForWrite(pathStr,
        size, conf);
    File dir = new File(path.getParent().toUri().getPath());
    String prefix = path.getName();
    // create a temp file on this directory
    return File.createTempFile(prefix, null, dir);
  }

  /**
   * Get the bucket of this filesystem.
   * @return the bucket
   */
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
  CannedAccessControlList getCannedACL() {
    return cannedACL;
  }

  /**
   * Change the input policy for this FS.
   * @param inputPolicy new policy
   */
  @InterfaceStability.Unstable
  public void setInputPolicy(S3AInputPolicy inputPolicy) {
    Objects.requireNonNull(inputPolicy, "Null inputStrategy");
    LOG.debug("Setting input strategy: {}", inputPolicy);
    this.inputPolicy = inputPolicy;
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
    return open(f, Optional.empty(), Optional.empty());
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * if status contains an S3AFileStatus reference, it is used
   * and so a HEAD request to the store is avoided.
   *
   * @param file the file to open
   * @param options configuration options if opened with the builder API.
   * @param providedStatus optional file status.
   * @throws IOException IO failure.
   */
  @Retries.RetryTranslated
  private FSDataInputStream open(
      final Path file,
      final Optional<Configuration> options,
      final Optional<S3AFileStatus> providedStatus)
      throws IOException {

    entryPoint(INVOCATION_OPEN);
    final Path path = qualify(file);
    S3AFileStatus fileStatus = extractOrFetchSimpleFileStatus(path,
        providedStatus);

    S3AReadOpContext readContext;
    if (options.isPresent()) {
      Configuration o = options.get();
      // normal path. Open the file with the chosen seek policy, if different
      // from the normal one.
      // and readahead.
      S3AInputPolicy policy = S3AInputPolicy.getPolicy(
          o.get(INPUT_FADVISE, inputPolicy.toString()));
      long readAheadRange2 = o.getLong(READAHEAD_RANGE, readAhead);
      // TODO support change detection policy from options?
      readContext = createReadContext(
          fileStatus,
          policy,
          changeDetectionPolicy,
          readAheadRange2);
    } else {
      readContext = createReadContext(
          fileStatus,
          inputPolicy,
          changeDetectionPolicy,
          readAhead);
    }
    LOG.debug("Opening '{}'", readContext);

    return new FSDataInputStream(
        new S3AInputStream(
            readContext,
            createObjectAttributes(fileStatus),
            s3));
  }

  /**
   * Create the read context for reading from the referenced file,
   * using FS state as well as the status.
   * @param fileStatus file status.
   * @param seekPolicy input policy for this operation
   * @param readAheadRange readahead value.
   * @return a context for read and select operations.
   */
  private S3AReadOpContext createReadContext(
      final FileStatus fileStatus,
      final S3AInputPolicy seekPolicy,
      final ChangeDetectionPolicy changePolicy,
      final long readAheadRange) {
    return new S3AReadOpContext(fileStatus.getPath(),
        hasMetadataStore(),
        invoker,
        s3guardInvoker,
        statistics,
        statisticsContext,
        fileStatus,
        seekPolicy,
        changePolicy,
        readAheadRange);
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
        getServerSideEncryptionAlgorithm(),
        encryptionSecrets.getEncryptionKey(),
        eTag,
        versionId,
        len);
  }

  /**
   * Create the attributes of an object for subsequent use.
   * @param fileStatus file status to build from.
   * @return attributes to use when building the query.
   */
  private S3ObjectAttributes createObjectAttributes(
      final S3AFileStatus fileStatus) {
    return createObjectAttributes(
        fileStatus.getPath(),
        fileStatus.getETag(),
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
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    entryPoint(INVOCATION_CREATE);
    final Path path = qualify(f);
    String key = pathToKey(path);
    FileStatus status = null;
    try {
      // get the status or throw an FNFE.
      // when overwriting, there is no need to look for any existing file,
      // and attempting to do so can poison the load balancers with 404
      // entries.
      status = innerGetFileStatus(path, false,
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
      // this means the file is not found

    }
    instrumentation.fileCreated();
    PutTracker putTracker =
        committerIntegration.createTracker(path, key);
    String destKey = putTracker.getDestKey();
    final BlockOutputStreamStatistics outputStreamStatistics
        = statisticsContext.newOutputStreamStatistics();
    final S3ABlockOutputStream.BlockOutputStreamBuilder builder =
        S3ABlockOutputStream.builder()
        .withKey(destKey)
        .withBlockFactory(blockFactory)
        .withBlockSize(partSize)
        .withStatistics(outputStreamStatistics)
        .withProgress(progress)
        .withPutTracker(putTracker)
        .withWriteOperations(getWriteOperationHelper())
        .withExecutorService(
            new SemaphoredDelegatingExecutor(
                boundedThreadPool,
                blockOutputActiveBlocks,
                true))
        .withDowngradeSyncableExceptions(
            getConf().getBoolean(
                DOWNGRADE_SYNCABLE_EXCEPTIONS,
                DOWNGRADE_SYNCABLE_EXCEPTIONS_DEFAULT));
    return new FSDataOutputStream(
        new S3ABlockOutputStream(builder),
        null);
  }

  /**
   * Get a {@code WriteOperationHelper} instance.
   *
   * This class permits other low-level operations against the store.
   * It is unstable and
   * only intended for code with intimate knowledge of the object store.
   * If using this, be prepared for changes even on minor point releases.
   * @return a new helper.
   */
  @InterfaceAudience.Private
  public WriteOperationHelper getWriteOperationHelper() {
    return writeHelper;
  }

  /**
   * {@inheritDoc}
   * @throws FileNotFoundException if the parent directory is not present -or
   * is not a directory.
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path p,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    entryPoint(INVOCATION_CREATE_NON_RECURSIVE);
    final Path path = makeQualified(p);
    Path parent = path.getParent();
    // expect this to raise an exception if there is no parent dir
    if (parent != null && !parent.isRoot()) {
      S3AFileStatus status;
      try {
        // optimize for the directory existing: Call list first
        status = innerGetFileStatus(parent, false,
            StatusProbeEnum.DIRECTORIES);
      } catch (FileNotFoundException e) {
        // no dir, fall back to looking for a file
        // (failure condition if true)
        status = innerGetFileStatus(parent, false,
            StatusProbeEnum.HEAD_ONLY);
      }
      if (!status.isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return create(path, permission,
        flags.contains(CreateFlag.OVERWRITE), bufferSize,
        replication, blockSize, progress);
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
  @Retries.RetryTranslated
  public boolean rename(Path src, Path dst) throws IOException {
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "rename(%s, %s", src, dst)) {
      long bytesCopied = innerRename(src, dst);
      LOG.debug("Copied {} bytes", bytesCopied);
      return true;
    } catch (AmazonClientException e) {
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
          // if this doesn't raise an exception then it's one of
          // raw S3: parent is a file: error
          // guarded S3: parent is a file or a dir.
          if (!dstParentStatus.isDirectory()) {
            throw new RenameFailedException(src, dst,
                "destination parent is not a directory");
          }
        } catch (FileNotFoundException expected) {
          // nothing was found. Don't worry about it;
          // expect rename to implicitly create the parent dir (raw S3)
          // or the s3guard parents (guarded)

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
   * so. For safely, consider catch and handle AmazonClientException
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
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  @Retries.RetryMixed
  private long innerRename(Path source, Path dest)
      throws RenameFailedException, FileNotFoundException, IOException,
        AmazonClientException {
    Path src = qualify(source);
    Path dst = qualify(dest);

    LOG.debug("Rename path {} to {}", src, dst);
    entryPoint(INVOCATION_RENAME);

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    Pair<S3AFileStatus, S3AFileStatus> p = initiateRename(src, dst);

    // Initiate the rename.
    // this will call back into this class via the rename callbacks
    // and interact directly with any metastore.
    RenameOperation renameOperation = new RenameOperation(
        createStoreContext(),
        src, srcKey, p.getLeft(),
        dst, dstKey, p.getRight(),
        operationCallbacks,
        pageSize);
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
   */
  private class OperationCallbacksImpl implements OperationCallbacks {

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
      return S3AFileSystem.this.createObjectAttributes(fileStatus);
    }

    @Override
    public S3AReadOpContext createReadContext(final FileStatus fileStatus) {
      return S3AFileSystem.this.createReadContext(fileStatus,
          inputPolicy,
          changeDetectionPolicy, readAhead);
    }

    @Override
    @Retries.RetryTranslated
    public void deleteObjectAtPath(final Path path,
        final String key,
        final boolean isFile,
        final BulkOperationState operationState)
        throws IOException {
      once("delete", path.toString(), () ->
          S3AFileSystem.this.deleteObjectAtPath(path, key, isFile,
              operationState));
    }

    @Override
    @Retries.RetryTranslated
    public RemoteIterator<S3ALocatedFileStatus> listFilesAndDirectoryMarkers(
        final Path path,
        final S3AFileStatus status,
        final boolean collectTombstones,
        final boolean includeSelf) throws IOException {
      return innerListFiles(
          path,
          true,
          includeSelf
              ? Listing.ACCEPT_ALL_BUT_S3N
              : new Listing.AcceptAllButSelfAndS3nDirs(path),
          status,
          collectTombstones,
          true);
    }

    @Override
    public CopyResult copyFile(final String srcKey,
        final String destKey,
        final S3ObjectAttributes srcAttributes,
        final S3AReadOpContext readContext) throws IOException {
      return S3AFileSystem.this.copyFile(srcKey, destKey,
          srcAttributes.getLen(), srcAttributes, readContext);
    }

    @Override
    public DeleteObjectsResult removeKeys(
        final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
        final boolean deleteFakeDir,
        final List<Path> undeletedObjectsOnFailure,
        final BulkOperationState operationState,
        final boolean quiet)
        throws MultiObjectDeleteException, AmazonClientException, IOException {
      return S3AFileSystem.this.removeKeys(keysToDelete, deleteFakeDir,
          undeletedObjectsOnFailure, operationState, quiet);
    }

    @Override
    public void finishRename(final Path sourceRenamed, final Path destCreated)
        throws IOException {
      Path destParent = destCreated.getParent();
      if (!sourceRenamed.getParent().equals(destParent)) {
        LOG.debug("source & dest parents are different; fix up dir markers");
        if (!keepDirectoryMarkers(destParent)) {
          deleteUnnecessaryFakeDirectories(destParent, null);
        }
        maybeCreateFakeParentDirectory(sourceRenamed);
      }
    }

    @Override
    public boolean allowAuthoritative(final Path p) {
      return S3AFileSystem.this.allowAuthoritative(p);
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
              Listing.ACCEPT_ALL_BUT_S3N,
              null));
    }
  }

  protected class ListingOperationCallbacksImpl implements
          ListingOperationCallbacks {

    @Override
    @Retries.RetryRaw
    public CompletableFuture<S3ListResult> listObjectsAsync(
        S3ListRequest request,
        DurationTrackerFactory trackerFactory)
            throws IOException {
      return submit(unboundedThreadPool, () ->
          listObjects(request,
              pairedTrackerFactory(trackerFactory,
                  getDurationTrackerFactory())));
    }

    @Override
    @Retries.RetryRaw
    public CompletableFuture<S3ListResult> continueListObjectsAsync(
        S3ListRequest request,
        S3ListResult prevResult,
        DurationTrackerFactory trackerFactory)
            throws IOException {
      return submit(unboundedThreadPool,
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
            String delimiter) {
      return S3AFileSystem.this.createListObjectsRequest(key, delimiter);
    }

    @Override
    public long getDefaultBlockSize(Path path) {
      return S3AFileSystem.this.getDefaultBlockSize(path);
    }

    @Override
    public int getMaxKeys() {
      return S3AFileSystem.this.getMaxKeys();
    }

    @Override
    public ITtlTimeProvider getUpdatedTtlTimeProvider() {
      return S3AFileSystem.this.ttlTimeProvider;
    }

    @Override
    public boolean allowAuthoritative(final Path p) {
      return S3AFileSystem.this.allowAuthoritative(p);
    }
  }

  /**
   * Low-level call to get at the object metadata.
   * @param path path to the object. This will be qualified.
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @VisibleForTesting
  @Retries.RetryTranslated
  public ObjectMetadata getObjectMetadata(Path path) throws IOException {
    return getObjectMetadata(makeQualified(path), null, invoker,
        "getObjectMetadata");
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
  private ObjectMetadata getObjectMetadata(Path path,
      ChangeTracker changeTracker, Invoker changeInvoker, String operation)
      throws IOException {
    checkNotClosed();
    String key = pathToKey(path);
    return once(operation, path.toString(),
        () ->
            // this always does a full HEAD to the object
            getObjectMetadata(
                key, changeTracker, changeInvoker, operation));
  }

  /**
   * Does this Filesystem have a metadata store?
   * @return true iff the FS has been instantiated with a metadata store
   */
  public boolean hasMetadataStore() {
    return !S3Guard.isNullMetadataStore(metadataStore);
  }

  /**
   * Does the filesystem have an authoritative metadata store?
   * @return true if there is a metadata store and the authoritative flag
   * is set for this filesystem.
   */
  @VisibleForTesting
  public boolean hasAuthoritativeMetadataStore() {
    return hasMetadataStore() && allowAuthoritativeMetadataStore;
  }

  /**
   * Get the metadata store.
   * This will always be non-null, but may be bound to the
   * {@code NullMetadataStore}.
   * @return the metadata store of this FS instance
   */
  @VisibleForTesting
  public MetadataStore getMetadataStore() {
    return metadataStore;
  }

  /** For testing only.  See ITestS3GuardEmptyDirs. */
  @VisibleForTesting
  void setMetadataStore(MetadataStore ms) {
    Preconditions.checkNotNull(ms);
    metadataStore = ms;
  }

  /**
   * Entry point to an operation.
   * Increments the statistic; verifies the FS is active.
   * @param operation The operation to increment
   * @throws IOException if the
   */
  protected void entryPoint(Statistic operation) throws IOException {
    checkNotClosed();
    incrementStatistic(operation);
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
      operationThrottled(false);
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
   * Callback from {@link Invoker} when an operation against a metastore
   * is retried.
   * Always increments the {@link Statistic#S3GUARD_METADATASTORE_RETRY}
   * statistic/counter;
   * if it is a throttling exception will update the associated
   * throttled metrics/statistics.
   *
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  public void metastoreOperationRetried(Exception ex,
      int retries,
      boolean idempotent) {
    incrementStatistic(S3GUARD_METADATASTORE_RETRY);
    if (isThrottleException(ex)) {
      operationThrottled(true);
    } else {
      incrementStatistic(IGNORED_ERRORS);
    }
  }

  /**
   * Note that an operation was throttled -this will update
   * specific counters/metrics.
   * @param metastore was the throttling observed in the S3Guard metastore?
   */
  private void operationThrottled(boolean metastore) {
    LOG.debug("Request throttled on {}", metastore ? "S3": "DynamoDB");
    if (metastore) {
      incrementStatistic(S3GUARD_METADATASTORE_THROTTLED);
      statisticsContext.addValueToQuantiles(S3GUARD_METADATASTORE_THROTTLE_RATE,
          1);
    } else {
      incrementStatistic(STORE_IO_THROTTLED);
      statisticsContext.addValueToQuantiles(STORE_IO_THROTTLE_RATE, 1);
    }
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
   * @return statistics
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
   * Request object metadata; increments counters in the process.
   * Retry policy: retry untranslated.
   * @param key key
   * @return the metadata
   * @throws IOException if the retry invocation raises one (it shouldn't).
   */
  @Retries.RetryRaw
  @VisibleForTesting
  ObjectMetadata getObjectMetadata(String key) throws IOException {
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
  protected ObjectMetadata getObjectMetadata(String key,
      ChangeTracker changeTracker,
      Invoker changeInvoker,
      String operation) throws IOException {
    GetObjectMetadataRequest request =
        new GetObjectMetadataRequest(bucket, key);
    //SSE-C requires to be filled in if enabled for object metadata
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
    ObjectMetadata meta = changeInvoker.retryUntranslated("GET " + key, true,
        () -> {
          incrementStatistic(OBJECT_METADATA_REQUESTS);
          DurationTracker duration = getDurationTrackerFactory()
              .trackDuration(ACTION_HTTP_HEAD_REQUEST.getSymbol());
          try {
            LOG.debug("HEAD {} with change tracker {}", key, changeTracker);
            if (changeTracker != null) {
              changeTracker.maybeApplyConstraint(request);
            }
            ObjectMetadata objectMetadata = s3.getObjectMetadata(request);
            if (changeTracker != null) {
              changeTracker.processMetadata(objectMetadata, operation);
            }
            return objectMetadata;
          } catch(AmazonServiceException ase) {
            if (!isObjectNotFound(ase)) {
              // file not found is not considered a failure of the call,
              // so only switch the duration tracker to update failure
              // metrics on other exception outcomes.
              duration.failed();
            }
            throw ase;
          } finally {
            // update the tracker.
            duration.close();
          }
        });
    incrementReadOperations();
    return meta;
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
                  return S3ListResult.v1(s3.listObjects(request.getV1()));
                } else {
                  return S3ListResult.v2(s3.listObjectsV2(request.getV2()));
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
                  return S3ListResult.v1(
                      s3.listNextBatchOfObjects(prevResult.getV1()));
                } else {
                  request.getV2().setContinuationToken(prevResult.getV2()
                      .getNextContinuationToken());
                  return S3ListResult.v2(s3.listObjectsV2(request.getV2()));
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
   * Delete an object. This is the low-level internal call which
   * <i>does not</i> update the metastore.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * This call does <i>not</i> create any mock parent entries.
   *
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param key key to blob to delete.
   * @throws AmazonClientException problems working with S3
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  @VisibleForTesting
  @Retries.RetryRaw
  protected void deleteObject(String key)
      throws AmazonClientException, IOException {
    blockRootDelete(key);
    incrementWriteOperations();
    try (DurationInfo ignored =
             new DurationInfo(LOG, false,
                 "deleting %s", key)) {
      invoker.retryUntranslated(String.format("Delete %s:/%s", bucket, key),
          DELETE_CONSIDERED_IDEMPOTENT,
          ()-> {
            incrementStatistic(OBJECT_DELETE_OBJECTS);
            trackDurationOfInvocation(getDurationTrackerFactory(),
                OBJECT_DELETE_REQUEST.getSymbol(),
                () -> s3.deleteObject(bucket, key));
            return null;
          });
    }
  }

  /**
   * Delete an object, also updating the metastore.
   * This call does <i>not</i> create any mock parent entries.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param f path path to delete
   * @param key key of entry
   * @param isFile is the path a file (used for instrumentation only)
   * @param operationState (nullable) operational state for a bulk update
   * @throws AmazonClientException problems working with S3
   * @throws IOException IO failure in the metastore
   */
  @Retries.RetryMixed
  void deleteObjectAtPath(Path f,
      String key,
      boolean isFile,
      @Nullable final BulkOperationState operationState)
      throws AmazonClientException, IOException {
    if (isFile) {
      instrumentation.fileDeleted(1);
    } else {
      instrumentation.directoryDeleted();
    }
    deleteObject(key);
    metadataStore.delete(f, operationState);
  }

  /**
   * Reject any request to delete an object where the key is root.
   * @param key key to validate
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void blockRootDelete(String key) throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException("Bucket "+ bucket
          +" cannot be deleted");
    }
  }

  /**
   * Perform a bulk object delete operation against S3; leaves S3Guard
   * alone.
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
   * @throws AmazonClientException amazon-layer failure.
   */
  @Retries.RetryRaw
  private DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteRequest)
      throws MultiObjectDeleteException, AmazonClientException, IOException {
    incrementWriteOperations();
    BulkDeleteRetryHandler retryHandler =
        new BulkDeleteRetryHandler(createStoreContext());
    int keyCount = deleteRequest.getKeys().size();
    try(DurationInfo ignored =
            new DurationInfo(LOG, false, "DELETE %d keys",
                keyCount)) {
      return invoker.retryUntranslated("delete",
          DELETE_CONSIDERED_IDEMPOTENT,
          (text, e, r, i) -> {
            // handle the failure
            retryHandler.bulkDeleteRetried(deleteRequest, e);
          },
          // duration is tracked in the bulk delete counters
          trackDurationOfOperation(getDurationTrackerFactory(),
              OBJECT_BULK_DELETE_REQUEST.getSymbol(), () -> {
                incrementStatistic(OBJECT_DELETE_OBJECTS, keyCount);
                return s3.deleteObjects(deleteRequest);
            }));
    } catch (MultiObjectDeleteException e) {
      // one or more of the keys could not be deleted.
      // log and rethrow
      List<MultiObjectDeleteException.DeleteError> errors = e.getErrors();
      LOG.debug("Partial failure of delete, {} errors", errors.size(), e);
      for (MultiObjectDeleteException.DeleteError error : errors) {
        LOG.debug("{}: \"{}\" - {}",
            error.getKey(), error.getCode(), error.getMessage());
      }
      throw e;
    }
  }

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  public PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, File srcfile) {
    Preconditions.checkNotNull(srcfile);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        srcfile);
    setOptionalPutRequestParameters(putObjectRequest);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(metadata);
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data.
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata,
      InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    Preconditions.checkArgument(isNotEmpty(key), "Null/empty key");
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        inputStream, metadata);
    setOptionalPutRequestParameters(putObjectRequest);
    putObjectRequest.setCannedAcl(cannedACL);
    return putObjectRequest;
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata() {
    final ObjectMetadata om = new ObjectMetadata();
    setOptionalObjectMetadata(om);
    return om;
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   *
   * @param length length of data to set in header.
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(long length) {
    final ObjectMetadata om = newObjectMetadata();
    if (length >= 0) {
      om.setContentLength(length);
    }
    return om;
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
   * @param putObjectRequest the request
   * @return the upload initiated
   */
  @Retries.OnceRaw
  public UploadInfo putObject(PutObjectRequest putObjectRequest) {
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {} via transfer manager ",
        len, putObjectRequest.getKey());
    incrementPutStartStatistics(len);
    Upload upload = transfers.upload(putObjectRequest);
    return new UploadInfo(upload, len);
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   *
   * Retry Policy: none.
   * <i>Important: this call will close any input stream in the request.</i>
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws AmazonClientException on problems
   * @throws MetadataPersistenceException if metadata about the write could
   * not be saved to the metadata store and
   * fs.s3a.metadatastore.fail.on.write.error=true
   */
  @VisibleForTesting
  @Retries.OnceRaw("For PUT; post-PUT actions are RetryTranslated")
  PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest)
      throws AmazonClientException, MetadataPersistenceException {
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {}", len, putObjectRequest.getKey());
    incrementPutStartStatistics(len);
    try {
      PutObjectResult result = s3.putObject(putObjectRequest);
      incrementPutCompletedStatistics(true, len);
      // update metadata
      finishedWrite(putObjectRequest.getKey(), len,
          result.getETag(), result.getVersionId(), null);
      return result;
    } catch (AmazonClientException e) {
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
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    Preconditions.checkState(len >= 0, "Cannot PUT object of unknown length");
    return len;
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the request.</i>
   *
   * Retry Policy: none.
   * @param request request
   * @return the result of the operation.
   * @throws AmazonClientException on problems
   */
  @Retries.OnceRaw
  UploadPartResult uploadPart(UploadPartRequest request)
      throws AmazonClientException {
    long len = request.getPartSize();
    incrementPutStartStatistics(len);
    try {
      setOptionalUploadPartRequestParameters(request);
      UploadPartResult uploadPartResult = s3.uploadPart(request);
      incrementPutCompletedStatistics(true, len);
      return uploadPartResult;
    } catch (AmazonClientException e) {
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
  public void incrementPutStartStatistics(long bytes) {
    LOG.debug("PUT start {} bytes", bytes);
    incrementWriteOperations();
    incrementStatistic(OBJECT_PUT_REQUESTS);
    incrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
    if (bytes > 0) {
      incrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
  }

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  public void incrementPutCompletedStatistics(boolean success, long bytes) {
    LOG.debug("PUT completed success={}; {} bytes", success, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      incrementStatistic(OBJECT_PUT_BYTES, bytes);
      decrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
    incrementStatistic(OBJECT_PUT_REQUESTS_COMPLETED);
    decrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  public void incrementPutProgressStatistics(String key, long bytes) {
    PROGRESS.debug("PUT {}: {} bytes", key, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      statistics.incrementBytesWritten(bytes);
    }
  }

  /**
   * Delete a list of keys on a s3-backend.
   * This does <i>not</i> update the metastore.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @param quiet should a bulk query be quiet, or should its result list
   * all deleted keys?
   * @return the deletion result if a multi object delete was invoked
   * and it returned without a failure.
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * The number of rejected objects will be added to the metric
   * {@link Statistic#FILES_DELETE_REJECTED}.
   * @throws AmazonClientException other amazon-layer failure.
   */
  @Retries.RetryRaw
  private DeleteObjectsResult removeKeysS3(
      List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      boolean deleteFakeDir,
      boolean quiet)
      throws MultiObjectDeleteException, AmazonClientException,
      IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating delete operation for {} objects",
          keysToDelete.size());
      for (DeleteObjectsRequest.KeyVersion key : keysToDelete) {
        LOG.debug(" {} {}", key.getKey(),
            key.getVersion() != null ? key.getVersion() : "");
      }
    }
    DeleteObjectsResult result = null;
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      return result;
    }
    for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
      blockRootDelete(keyVersion.getKey());
    }
    try {
      if (enableMultiObjectsDelete) {
        result = deleteObjects(
            new DeleteObjectsRequest(bucket)
                .withKeys(keysToDelete)
                .withQuiet(quiet));
      } else {
        for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
          deleteObject(keyVersion.getKey());
        }
      }
    } catch (MultiObjectDeleteException ex) {
      // partial delete.
      // Update the stats with the count of the actual number of successful
      // deletions.
      int rejected = ex.getErrors().size();
      noteDeleted(keysToDelete.size() - rejected, deleteFakeDir);
      incrementStatistic(FILES_DELETE_REJECTED, rejected);
      throw ex;
    }
    noteDeleted(keysToDelete.size(), deleteFakeDir);
    return result;
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
   * Invoke {@link #removeKeysS3(List, boolean, boolean)} with handling of
   * {@code MultiObjectDeleteException}.
   *
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @param operationState (nullable) operational state for a bulk update
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AmazonClientException amazon-layer failure.
   * @throws IOException other IO Exception.
   */
  @VisibleForTesting
  @Retries.RetryMixed
  public void removeKeys(
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final boolean deleteFakeDir,
      final BulkOperationState operationState)
      throws MultiObjectDeleteException, AmazonClientException,
      IOException {
    removeKeys(keysToDelete, deleteFakeDir, new ArrayList<>(), operationState,
        true);
  }

  /**
   * Invoke {@link #removeKeysS3(List, boolean, boolean)} with handling of
   * {@code MultiObjectDeleteException} before the exception is rethrown.
   * Specifically:
   * <ol>
   *   <li>Failure and !deleteFakeDir: S3Guard is updated with all
   *    deleted entries</li>
   *   <li>Failure where deleteFakeDir == true: do nothing with S3Guard</li>
   *   <li>Success: do nothing with S3Guard</li>
   * </ol>
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs.
   * @param undeletedObjectsOnFailure List which will be built up of all
   * files that were not deleted. This happens even as an exception
   * is raised.
   * @param operationState (nullable) operational state for a bulk update
   * @param quiet should a bulk query be quiet, or should its result list
   * all deleted keys
   * @return the deletion result if a multi object delete was invoked
   * and it returned without a failure, else null.
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AmazonClientException amazon-layer failure.
   * @throws IOException other IO Exception.
   */
  @Retries.RetryMixed
  DeleteObjectsResult removeKeys(
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final boolean deleteFakeDir,
      final List<Path> undeletedObjectsOnFailure,
      final BulkOperationState operationState,
      final boolean quiet)
      throws MultiObjectDeleteException, AmazonClientException, IOException {
    undeletedObjectsOnFailure.clear();
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Deleting %d keys", keysToDelete.size())) {
      return removeKeysS3(keysToDelete, deleteFakeDir, quiet);
    } catch (MultiObjectDeleteException ex) {
      LOG.debug("Partial delete failure");
      // what to do if an IOE was raised? Given an exception was being
      // raised anyway, and the failures are logged, do nothing.
      if (!deleteFakeDir) {
        // when deleting fake directories we don't want to delete metastore
        // entries so we only process these failures on "real" deletes.
        Triple<List<Path>, List<Path>, List<Pair<Path, IOException>>> results =
            new MultiObjectDeleteSupport(createStoreContext(), operationState)
                .processDeleteFailure(ex, keysToDelete, new ArrayList<Path>());
        undeletedObjectsOnFailure.addAll(results.getLeft());
      }
      throw ex;
    } catch (AmazonClientException | IOException ex) {
      List<Path> paths = new MultiObjectDeleteSupport(
          createStoreContext(),
          operationState)
          .processDeleteFailureGenericException(ex, keysToDelete);
      // other failures. Assume nothing was deleted
      undeletedObjectsOnFailure.addAll(paths);
      throw ex;
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
  @Retries.RetryTranslated
  public boolean delete(Path f, boolean recursive) throws IOException {
    try {
      entryPoint(INVOCATION_DELETE);
      DeleteOperation deleteOperation = new DeleteOperation(
          createStoreContext(),
          innerGetFileStatus(f, true, StatusProbeEnum.ALL),
          recursive,
          operationCallbacks,
          pageSize);
      boolean outcome = deleteOperation.execute();
      if (outcome) {
        try {
          maybeCreateFakeParentDirectory(f);
        } catch (AccessDeniedException e) {
          LOG.warn("Cannot create directory marker at {}: {}",
              f.getParent(), e.toString());
          LOG.debug("Failed to create fake dir above {}", f, e);
        }
      }
      return outcome;
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist: {}", f, e.toString());
      instrumentation.errorIgnored();
      return false;
    } catch (AmazonClientException e) {
      throw translateException("delete", f, e);
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
      throws IOException, AmazonClientException {
    String key = pathToKey(f);
    // we only make the LIST call; the codepaths to get here should not
    // be reached if there is an empty dir marker -and if they do, it
    // is mostly harmless to create a new one.
    if (!key.isEmpty() && !s3Exists(f, StatusProbeEnum.DIRECTORIES)) {
      LOG.debug("Creating new fake directory at {}", f);
      createFakeDirectory(key);
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
      throws IOException, AmazonClientException {
    Path parent = path.getParent();
    if (parent != null && !parent.isRoot()) {
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
  public RemoteIterator<FileStatus> listStatusIterator(Path p)
          throws FileNotFoundException, IOException {
    RemoteIterator<S3AFileStatus> listStatusItr = once("listStatus",
            p.toString(), () -> innerListStatus(p));
    return typeCastingRemoteIterator(listStatusItr);
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
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    return once("listStatus",
        f.toString(),
        () -> iteratorToStatuses(innerListStatus(f), new HashSet<>()));
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException due to an IO problem.
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  private RemoteIterator<S3AFileStatus> innerListStatus(Path f)
          throws FileNotFoundException,
          IOException, AmazonClientException {
    Path path = qualify(f);
    LOG.debug("List status for path: {}", path);
    entryPoint(INVOCATION_LIST_STATUS);

    Triple<RemoteIterator<S3AFileStatus>, DirListingMetadata, Boolean>
            statusesAssumingNonEmptyDir = listing
            .getFileStatusesAssumingNonEmptyDir(path);

    if (!statusesAssumingNonEmptyDir.getLeft().hasNext() &&
            statusesAssumingNonEmptyDir.getRight()) {
      // We are sure that this is an empty directory in auth mode.
      return statusesAssumingNonEmptyDir.getLeft();
    } else if (!statusesAssumingNonEmptyDir.getLeft().hasNext()) {
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
                Listing.ACCEPT_ALL_BUT_S3N);
      }
    }
    // Here we have a directory which may or may not be empty.
    // So we update the metastore and return.
    return S3Guard.dirListingUnion(
            metadataStore,
            path,
            statusesAssumingNonEmptyDir.getLeft(),
            statusesAssumingNonEmptyDir.getMiddle(),
            allowAuthoritative(path),
            ttlTimeProvider, p ->
                    listing.createProvidedFileStatusIterator(
                    dirMetaToStatuses(statusesAssumingNonEmptyDir.getMiddle()),
                    ACCEPT_ALL,
                    Listing.ACCEPT_ALL_BUT_S3N));
  }

  /**
   * Is a path to be considered as authoritative?
   * True iff there is an authoritative metastore or if there
   * is a non-auth store with the supplied path under
   * one of the paths declared as authoritative.
   * @param path path
   * @return true if the path is auth
   */
  public boolean allowAuthoritative(final Path path) {
    return S3Guard.allowAuthoritative(path, this,
        allowAuthoritativeMetadataStore, allowAuthoritativePaths);
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
    return createListObjectsRequest(key, delimiter, null);
  }

  private S3ListRequest createListObjectsRequest(String key,
      String delimiter, Integer overrideMaxKeys) {
    if (!useListV1) {
      ListObjectsV2Request request =
          new ListObjectsV2Request().withBucketName(bucket)
              .withMaxKeys(maxKeys)
              .withPrefix(key);
      if (delimiter != null) {
        request.setDelimiter(delimiter);
      }
      if (overrideMaxKeys != null) {
        request.setMaxKeys(overrideMaxKeys);
      }
      return S3ListRequest.v2(request);
    } else {
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setMaxKeys(maxKeys);
      request.setPrefix(key);
      if (delimiter != null) {
        request.setDelimiter(delimiter);
      }
      if (overrideMaxKeys != null) {
        request.setMaxKeys(overrideMaxKeys);
      }
      return S3ListRequest.v1(request);
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
   * @param path path to create
   * @param permission to apply to f
   * @return true if a directory was created or already existed
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  public boolean mkdirs(Path path, FsPermission permission) throws IOException,
      FileAlreadyExistsException {
    try {
      entryPoint(INVOCATION_MKDIRS);
      return innerMkdirs(path, permission);
    } catch (AmazonClientException e) {
      throw translateException("mkdirs", path, e);
    }
  }

  /**
   *
   * Make the given path and all non-existent parents into
   * directories.
   * See {@link #mkdirs(Path, FsPermission)}
   * @param p path to create
   * @param permission to apply to f
   * @return true if a directory was created or already existed
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  private boolean innerMkdirs(Path p, FsPermission permission)
      throws IOException, FileAlreadyExistsException, AmazonClientException {
    Path f = qualify(p);
    LOG.debug("Making directory: {}", f);
    if (p.isRoot()) {
      // fast exit for root.
      return true;
    }
    FileStatus fileStatus;

    try {
      fileStatus = innerGetFileStatus(f, false,
          StatusProbeEnum.ALL);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      // Walk path to root, ensuring closest ancestor is a directory, not file
      Path fPart = f.getParent();
      while (fPart != null && !fPart.isRoot()) {
        try {
          fileStatus = getFileStatus(fPart);
          if (fileStatus.isDirectory()) {
            break;
          }
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(String.format(
                "Can't make directory for path '%s' since it is a file.",
                fPart));
          }
        } catch (FileNotFoundException fnfe) {
          instrumentation.errorIgnored();
        }
        fPart = fPart.getParent();
      }
      String key = pathToKey(f);
      // this will create the marker file, delete the parent entries
      // and update S3Guard
      createFakeDirectory(key);
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException on other problems.
   */
  @Retries.RetryTranslated
  public FileStatus getFileStatus(final Path f) throws IOException {
    entryPoint(INVOCATION_GET_FILE_STATUS);
    return innerGetFileStatus(f, false, StatusProbeEnum.ALL);
  }

  /**
   * Get the status of a file or directory, first through S3Guard and then
   * through S3.
   * The S3 probes can leave 404 responses in the S3 load balancers; if
   * a check is only needed for a directory, declaring this saves time and
   * avoids creating one for the object.
   * When only probing for directories, if an entry for a file is found in
   * S3Guard it is returned, but checks for updated values are skipped.
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

    boolean allowAuthoritative = allowAuthoritative(path);
    // Check MetadataStore, if any.
    PathMetadata pm = null;
    if (hasMetadataStore()) {
      pm = S3Guard.getWithTtl(metadataStore, path, ttlTimeProvider,
          needEmptyDirectoryFlag, allowAuthoritative);
    }
    Set<Path> tombstones = Collections.emptySet();
    if (pm != null) {
      S3AFileStatus msStatus = pm.getFileStatus();
      if (pm.isDeleted()) {
        OffsetDateTime deletedAt = OffsetDateTime.ofInstant(
            Instant.ofEpochMilli(msStatus.getModificationTime()),
            ZoneOffset.UTC);
        throw new FileNotFoundException("Path " + path + " is recorded as " +
            "deleted by S3Guard at " + deletedAt);
      }

      // if ms is not authoritative, check S3 if there's any recent
      // modification - compare the modTime to check if metadata is up to date
      // Skip going to s3 if the file checked is a directory. Because if the
      // dest is also a directory, there's no difference.

      if (!msStatus.isDirectory() &&
          !allowAuthoritative &&
          probes.contains(StatusProbeEnum.Head)) {
        // a file has been found in a non-auth path and the caller has not said
        // they only care about directories
        LOG.debug("Metadata for {} found in the non-auth metastore.", path);
        final long msModTime = pm.getFileStatus().getModificationTime();

        S3AFileStatus s3AFileStatus;
        try {
          s3AFileStatus = s3GetFileStatus(path,
              key,
              probes,
              tombstones,
              needEmptyDirectoryFlag);
        } catch (FileNotFoundException fne) {
          LOG.trace("File Not Found from probes for {}", key, fne);
          s3AFileStatus = null;
        }
        if (s3AFileStatus == null) {
          LOG.warn("Failed to find file {}. Either it is not yet visible, or "
              + "it has been deleted.", path);
        } else {
          final long s3ModTime = s3AFileStatus.getModificationTime();

          if(s3ModTime > msModTime) {
            LOG.debug("S3Guard metadata for {} is outdated;"
                + " s3modtime={}; msModTime={} updating metastore",
                path, s3ModTime, msModTime);
            return S3Guard.putAndReturn(metadataStore, s3AFileStatus,
                ttlTimeProvider);
          }
        }
      }

      if (needEmptyDirectoryFlag && msStatus.isDirectory()) {
        // the caller needs to know if a directory is empty,
        // and that this is a directory.
        if (pm.isEmptyDirectory() != Tristate.UNKNOWN) {
          // We have a definitive true / false from MetadataStore, we are done.
          return msStatus;
        } else {
          // execute a S3Guard listChildren command to list tombstones under the
          // path.
          // This list will be used in the forthcoming s3GetFileStatus call.
          DirListingMetadata children =
              S3Guard.listChildrenWithTtl(metadataStore, path, ttlTimeProvider,
                  allowAuthoritative);
          if (children != null) {
            tombstones = children.listTombstones();
          }
          LOG.debug("MetadataStore doesn't know if {} is empty, using S3.",
              path);
        }
      } else {
        // Either this is not a directory, or we don't care if it is empty
        return msStatus;
      }

      // now issue the S3 getFileStatus call.
      try {
        S3AFileStatus s3FileStatus = s3GetFileStatus(path,
            key,
            probes,
            tombstones,
            true);
        // entry was found, so save in S3Guard and return the final value.
        return S3Guard.putAndReturn(metadataStore, s3FileStatus,
            ttlTimeProvider);
      } catch (FileNotFoundException e) {
        // If the metadata store has no children for it and it's not listed in
        // S3 yet, we'll conclude that it is an empty directory
        return S3AFileStatus.fromFileStatus(msStatus, Tristate.TRUE,
            null, null);
      }
    } else {
      // there was no entry in S3Guard
      // retrieve the data and update the metadata store in the process.
      return S3Guard.putAndReturn(metadataStore,
          s3GetFileStatus(path,
              key,
              probes,
              tombstones,
              needEmptyDirectoryFlag),
          ttlTimeProvider);
    }
  }

  /**
   * Raw {@code getFileStatus} that talks direct to S3.
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
   *     DirMarker: look for the directory marker -the key with a trailing /
   *     if not passed in.
   *     If an object was found with size 0 bytes, a directory status entry
   *     is returned which declares that the directory is empty.
   *   </li>
   *    <li>
   *     List: issue a LIST on the key (with / if needed), require one
   *     entry to be found for the path to be considered a non-empty directory.
   *   </li>
   * </ol>
   *
   * Notes:
   * <ul>
   *   <li>
   *     Objects ending in / which are not 0-bytes long are not treated as
   *     directory markers, but instead as files.
   *   </li>
   *   <li>
   *     There's ongoing discussions about whether a dir marker
   *     should be interpreted as an empty dir.
   *   </li>
   *   <li>
   *     The HEAD requests require the permissions to read an object,
   *     including (we believe) the ability to decrypt the file.
   *     At the very least, for SSE-C markers, you need the same key on
   *     the client for the HEAD to work.
   *   </li>
   *   <li>
   *     The List probe needs list permission; it is also more prone to
   *     inconsistency, even on newly created files.
   *   </li>
   * </ul>
   *
   * Retry policy: retry translated.
   * @param path Qualified path
   * @param key  Key string for the path
   * @param probes probes to make
   * @param tombstones tombstones to filter
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
      @Nullable final Set<Path> tombstones,
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
        ObjectMetadata meta = getObjectMetadata(key);
        LOG.debug("Found exact file: normal file {}", key);
        return new S3AFileStatus(meta.getContentLength(),
            dateToLong(meta.getLastModified()),
            path,
            getDefaultBlockSize(path),
            username,
            meta.getETag(),
            meta.getVersionId());
      } catch (AmazonServiceException e) {
        // if the response is a 404 error, it just means that there is
        // no file at that path...the remaining checks will be needed.
        if (e.getStatusCode() != SC_404 || isUnknownBucket(e)) {
          throw translateException("getFileStatus", path, e);
        }
      } catch (AmazonClientException e) {
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
        // list size is dir marker + at least one non-tombstone entry
        // there's a corner case: more tombstones than you have in a
        // single page list. We assume that if you have been deleting
        // that many files, then the AWS listing will have purged some
        // by the time of listing so that the response includes some
        // which have not.

        int listSize;
        if (tombstones == null) {
          // no tombstones so look for a marker and at least one child.
          listSize = 2;
        } else {
          // build a listing > tombstones. If the caller has many thousands
          // of tombstones this won't work properly, which is why pruning
          // of expired tombstones matters.
          listSize = Math.min(2 + tombstones.size(), Math.max(2, maxKeys));
        }
        S3ListRequest request = createListObjectsRequest(dirKey, "/",
            listSize);
        // execute the request
        S3ListResult listResult = listObjects(request,
            getDurationTrackerFactory());

        if (listResult.hasPrefixesOrObjects(contextAccessors, tombstones)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found path as directory (with /)");
            listResult.logAtDebug(LOG);
          }
          // At least one entry has been found.
          // If looking for an empty directory, the marker must exist but no
          // children.
          // So the listing must contain the marker entry only.
          if (needEmptyDirectoryFlag
              && listResult.representsEmptyDirectory(
                  contextAccessors, dirKey, tombstones)) {
            return new S3AFileStatus(Tristate.TRUE, path, username);
          }
          // either an empty directory is not needed, or the
          // listing does not meet the requirements.
          return new S3AFileStatus(Tristate.FALSE, path, username);
        } else if (key.isEmpty()) {
          LOG.debug("Found root directory");
          return new S3AFileStatus(Tristate.TRUE, path, username);
        }
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() != SC_404 || isUnknownBucket(e)) {
          throw translateException("getFileStatus", path, e);
        }
      } catch (AmazonClientException e) {
        throw translateException("getFileStatus", path, e);
      }
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  /**
   * Raw version of {@link FileSystem#exists(Path)} which uses S3 only:
   * S3Guard MetadataStore, if any, will be skipped.
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
      s3GetFileStatus(path, key, probes, null, false);
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
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false
   * @throws AmazonClientException failure in the AWS SDK
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
      Path dst) throws IOException {
    entryPoint(INVOCATION_COPY_FROM_LOCAL_FILE);
    LOG.debug("Copying local file from {} to {}", src, dst);
//    innerCopyFromLocalFile(delSrc, overwrite, src, dst);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * <i>HADOOP-15932:</i> this method has been unwired from
   * {@link #copyFromLocalFile(boolean, boolean, Path, Path)} until
   * it is extended to list and copy whole directories.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src Source path: must be on local filesystem
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false, or if the destination is a directory.
   * @throws FileNotFoundException if the source file does not exit
   * @throws AmazonClientException failure in the AWS SDK
   * @throws IllegalArgumentException if the source path is not on the local FS
   */
  @Retries.RetryTranslated
  private void innerCopyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException, FileAlreadyExistsException, AmazonClientException {
    entryPoint(INVOCATION_COPY_FROM_LOCAL_FILE);
    LOG.debug("Copying local file from {} to {}", src, dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);
    if (!srcfile.exists()) {
      throw new FileNotFoundException("No file: " + src);
    }
    if (!srcfile.isFile()) {
      throw new FileNotFoundException("Not a file: " + src);
    }

    try {
      FileStatus status = getFileStatus(dst);
      if (!status.isFile()) {
        throw new FileAlreadyExistsException(dst + " exists and is not a file");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException(dst + " already exists");
      }
    } catch (FileNotFoundException e) {
      // no destination, all is well
    }
    final String key = pathToKey(dst);
    final ObjectMetadata om = newObjectMetadata(srcfile.length());
    Progressable progress = null;
    PutObjectRequest putObjectRequest = newPutObjectRequest(key, om, srcfile);
    invoker.retry("copyFromLocalFile(" + src + ")", dst.toString(), true,
        () -> executePut(putObjectRequest, progress));
    if (delSrc) {
      local.delete(src, false);
    }
  }

  /**
   * Execute a PUT via the transfer manager, blocking for completion,
   * updating the metastore afterwards.
   * If the waiting for completion is interrupted, the upload will be
   * aborted before an {@code InterruptedIOException} is thrown.
   * @param putObjectRequest request
   * @param progress optional progress callback
   * @return the upload result
   * @throws InterruptedIOException if the blocking was interrupted.
   * @throws MetadataPersistenceException if metadata about the write could
   * not be saved to the metadata store and
   * fs.s3a.metadatastore.fail.on.write.error=true
   */
  @Retries.OnceRaw("For PUT; post-PUT actions are RetryTranslated")
  UploadResult executePut(PutObjectRequest putObjectRequest,
      Progressable progress)
      throws InterruptedIOException, MetadataPersistenceException {
    String key = putObjectRequest.getKey();
    UploadInfo info = putObject(putObjectRequest);
    Upload upload = info.getUpload();
    ProgressableProgressListener listener = new ProgressableProgressListener(
        this, key, upload, progress);
    upload.addProgressListener(listener);
    UploadResult result = waitForUploadCompletion(key, info);
    listener.uploadCompleted();
    // post-write actions
    finishedWrite(key, info.getLength(),
        result.getETag(), result.getVersionId(), null);
    return result;
  }

  /**
   * Wait for an upload to complete.
   * If the waiting for completion is interrupted, the upload will be
   * aborted before an {@code InterruptedIOException} is thrown.
   * If the upload (or its result collection) failed, this is where
   * the failure is raised as an AWS exception
   * @param key destination key
   * @param uploadInfo upload to wait for
   * @return the upload result
   * @throws InterruptedIOException if the blocking was interrupted.
   */
  @Retries.OnceRaw
  UploadResult waitForUploadCompletion(String key, UploadInfo uploadInfo)
      throws InterruptedIOException {
    Upload upload = uploadInfo.getUpload();
    try {
      UploadResult result = upload.waitForUploadResult();
      incrementPutCompletedStatistics(true, uploadInfo.getLength());
      return result;
    } catch (InterruptedException e) {
      LOG.info("Interrupted: aborting upload");
      incrementPutCompletedStatistics(false, uploadInfo.getLength());
      upload.abort();
      throw (InterruptedIOException)
          new InterruptedIOException("Interrupted in PUT to "
              + keyToQualifiedPath(key))
          .initCause(e);
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
    }
  }

  /**
   * Stop all services.
   * This is invoked in close() and during failures of initialize()
   * -make sure that all operations here are robust to failures in
   * both the expected state of this FS and of failures while being stopped.
   */
  protected synchronized void stopAllServices() {
    if (transfers != null) {
      try {
        transfers.shutdownNow(true);
      } catch (RuntimeException e) {
        // catch and swallow for resilience.
        LOG.debug("When shutting down", e);
      }
      transfers = null;
    }
    HadoopExecutors.shutdown(boundedThreadPool, LOG,
        THREAD_POOL_SHUTDOWN_DELAY_SECONDS, TimeUnit.SECONDS);
    boundedThreadPool = null;
    HadoopExecutors.shutdown(unboundedThreadPool, LOG,
        THREAD_POOL_SHUTDOWN_DELAY_SECONDS, TimeUnit.SECONDS);
    unboundedThreadPool = null;
    cleanupWithLogger(LOG,
        metadataStore,
        instrumentation,
        delegationTokens.orElse(null),
        signerManager);
    closeAutocloseables(LOG, credentials);
    delegationTokens = Optional.empty();
    signerManager = null;
    credentials = null;
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (isClosed) {
      throw new IOException(uri + ": " + E_FS_CLOSED);
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
  public Token<AbstractS3ATokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    entryPoint(Statistic.INVOCATION_GET_DELEGATION_TOKEN);
    LOG.debug("Delegation token requested");
    if (delegationTokens.isPresent()) {
      return delegationTokens.get().getBoundOrNewDT(encryptionSecrets,
          (renewer != null ? new Text(renewer) : new Text()));
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
   * The policy generated includes S3 access, S3Guard access
   * if needed, and KMS operations.
   * @param access access level desired.
   * @return a policy for use in roles
   */
  @Override
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
    statements.add(STATEMENT_ALLOW_SSE_KMS_RW);

    // add any metastore policies
    if (metadataStore instanceof AWSPolicyProvider) {
      statements.addAll(
          ((AWSPolicyProvider) metadataStore).listAWSPolicyRules(access));
    }
    return statements;
  }

  /**
   * Copy a single object in the bucket via a COPY operation.
   * There's no update of metadata, directory markers, etc.
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
  private CopyResult copyFile(String srcKey, String dstKey, long size,
      S3ObjectAttributes srcAttributes, S3AReadOpContext readContext)
      throws IOException, InterruptedIOException  {
    LOG.debug("copyFile {} -> {} ", srcKey, dstKey);

    ProgressListener progressListener = progressEvent -> {
      switch (progressEvent.getEventType()) {
      case TRANSFER_PART_COMPLETED_EVENT:
        incrementWriteOperations();
        break;
      default:
        break;
      }
    };

    ChangeTracker changeTracker = new ChangeTracker(
        keyToQualifiedPath(srcKey).toString(),
        changeDetectionPolicy,
        readContext.getS3AStatisticsContext()
            .newInputStreamStatistics()
            .getChangeTrackerStatistics(),
        srcAttributes);

    String action = "copyFile(" + srcKey + ", " + dstKey + ")";
    Invoker readInvoker = readContext.getReadInvoker();

    ObjectMetadata srcom;
    try {
      srcom = once(action, srcKey,
          () ->
              getObjectMetadata(srcKey, changeTracker, readInvoker, "copy"));
    } catch (FileNotFoundException e) {
      // if rename fails at this point it means that the expected file was not
      // found.
      // The cause is believed to always be one of
      //  - File was deleted since LIST/S3Guard metastore.list.() knew of it.
      //  - S3Guard is asking for a specific version and it's been removed by
      //    lifecycle rules.
      //  - there's a 404 cached in the S3 load balancers.
      LOG.debug("getObjectMetadata({}) failed to find an expected file",
          srcKey, e);
      // We create an exception, but the text depends on the S3Guard state
      String message = hasMetadataStore()
          ? RemoteFileChangedException.FILE_NEVER_FOUND
          : RemoteFileChangedException.FILE_NOT_FOUND_SINGLE_ATTEMPT;
      throw new RemoteFileChangedException(
          keyToQualifiedPath(srcKey).toString(),
          action,
          message,
          e);
    }
    ObjectMetadata dstom = cloneObjectMetadata(srcom);
    setOptionalObjectMetadata(dstom);

    return readInvoker.retry(
        action, srcKey,
        true,
        () -> {
          CopyObjectRequest copyObjectRequest =
              new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
          changeTracker.maybeApplyConstraint(copyObjectRequest);

          setOptionalCopyObjectRequestParameters(srcom, copyObjectRequest);
          copyObjectRequest.setCannedAccessControlList(cannedACL);
          copyObjectRequest.setNewObjectMetadata(dstom);
          Optional.ofNullable(srcom.getStorageClass())
              .ifPresent(copyObjectRequest::setStorageClass);
          incrementStatistic(OBJECT_COPY_REQUESTS);
          Copy copy = transfers.copy(copyObjectRequest);
          copy.addProgressListener(progressListener);
          CopyOutcome copyOutcome = CopyOutcome.waitForCopy(copy);
          InterruptedException interruptedException =
              copyOutcome.getInterruptedException();
          if (interruptedException != null) {
            // copy interrupted: convert to an IOException.
            throw (IOException)new InterruptedIOException(
                "Interrupted copying " + srcKey
                    + " to " + dstKey + ", cancelling")
                .initCause(interruptedException);
          }
          SdkBaseException awsException = copyOutcome.getAwsException();
          if (awsException != null) {
            changeTracker.processException(awsException, "copy");
            throw awsException;
          }
          CopyResult result = copyOutcome.getCopyResult();
          changeTracker.processResponse(result);
          incrementWriteOperations();
          instrumentation.filesCopied(1, size);
          return result;
        });
  }

  /**
   * Propagate encryption parameters from source file if set else use the
   * current filesystem encryption settings.
   * @param srcom source object meta.
   * @param copyObjectRequest copy object request body.
   */
  private void setOptionalCopyObjectRequestParameters(
          ObjectMetadata srcom,
          CopyObjectRequest copyObjectRequest) {
    String sourceKMSId = srcom.getSSEAwsKmsKeyId();
    if (isNotEmpty(sourceKMSId)) {
      // source KMS ID is propagated
      LOG.debug("Propagating SSE-KMS settings from source {}",
          sourceKMSId);
      copyObjectRequest.setSSEAwsKeyManagementParams(
              new SSEAwsKeyManagementParams(sourceKMSId));
    }
    switch(getServerSideEncryptionAlgorithm()) {
    /**
     * Overriding with client encryption settings.
     */
    case SSE_C:
      generateSSECustomerKey().ifPresent(customerKey -> {
        copyObjectRequest.setSourceSSECustomerKey(customerKey);
        copyObjectRequest.setDestinationSSECustomerKey(customerKey);
      });
      break;
    case SSE_KMS:
      generateSSEAwsKeyParams().ifPresent(
              copyObjectRequest::setSSEAwsKeyManagementParams);
      break;
    default:
    }
  }

  /**
   * Set the optional parameters when initiating the request (encryption,
   * headers, storage, etc).
   * @param request request to patch.
   */
  protected void setOptionalMultipartUploadRequestParameters(
      InitiateMultipartUploadRequest request) {
    generateSSEAwsKeyParams().ifPresent(request::setSSEAwsKeyManagementParams);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  /**
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param request upload part request
   */
  protected void setOptionalUploadPartRequestParameters(
      UploadPartRequest request) {
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  /**
   * Initiate a multipart upload from the preconfigured request.
   * Retry policy: none + untranslated.
   * @param request request to initiate
   * @return the result of the call
   * @throws AmazonClientException on failures inside the AWS SDK
   * @throws IOException Other IO problems
   */
  @Retries.OnceRaw
  InitiateMultipartUploadResult initiateMultipartUpload(
      InitiateMultipartUploadRequest request) throws IOException {
    LOG.debug("Initiate multipart upload to {}", request.getKey());
    incrementStatistic(OBJECT_MULTIPART_UPLOAD_INITIATED);
    return getAmazonS3Client().initiateMultipartUpload(request);
  }

  private void setOptionalPutRequestParameters(PutObjectRequest request) {
    generateSSEAwsKeyParams().ifPresent(request::setSSEAwsKeyManagementParams);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  private void setOptionalObjectMetadata(ObjectMetadata metadata) {
    final S3AEncryptionMethods algorithm
        = getServerSideEncryptionAlgorithm();
    if (S3AEncryptionMethods.SSE_S3.equals(algorithm)) {
      metadata.setSSEAlgorithm(algorithm.getMethod());
    }
  }

  /**
   * Create the AWS SDK structure used to configure SSE,
   * if the encryption secrets contain the information/settings for this.
   * @return an optional set of KMS Key settings
   */
  private Optional<SSEAwsKeyManagementParams> generateSSEAwsKeyParams() {
    return EncryptionSecretOperations.createSSEAwsKeyManagementParams(
        encryptionSecrets);
  }

  /**
   * Create the SSE-C structure for the AWS SDK, if the encryption secrets
   * contain the information/settings for this.
   * This will contain a secret extracted from the bucket/configuration.
   * @return an optional customer key.
   */
  private Optional<SSECustomerKey> generateSSECustomerKey() {
    return EncryptionSecretOperations.createSSECustomerKey(
        encryptionSecrets);
  }

  /**
   * Perform post-write actions.
   * <p></p>
   * This operation MUST be called after any PUT/multipart PUT completes
   * successfully.
   * <p></p>
   * The actions include:
   * <ol>
   *   <li>
   *     Calling
   *     {@link #deleteUnnecessaryFakeDirectories(Path, BulkOperationState)}
   *     if directory markers are not being retained.
   *   </li>
   *   <li>
   *     Updating any metadata store with details on the newly created
   *     object.
   *     </li>
   * </ol>
   * @param key key written to
   * @param length  total length of file written
   * @param eTag eTag of the written object
   * @param versionId S3 object versionId of the written object
   * @param operationState state of any ongoing bulk operation.
   * @throws MetadataPersistenceException if metadata about the write could
   * not be saved to the metadata store and
   * fs.s3a.metadatastore.fail.on.write.error=true
   */
  @InterfaceAudience.Private
  @Retries.RetryTranslated("Except if failOnMetadataWriteError=false, in which"
      + " case RetryExceptionsSwallowed")
  void finishedWrite(String key, long length, String eTag, String versionId,
      @Nullable final BulkOperationState operationState)
      throws MetadataPersistenceException {
    LOG.debug("Finished write to {}, len {}. etag {}, version {}",
        key, length, eTag, versionId);
    Path p = keyToQualifiedPath(key);
    Preconditions.checkArgument(length >= 0, "content length is negative");
    final boolean isDir = objectRepresentsDirectory(key, length);
    // kick off an async delete
    CompletableFuture<?> deletion;
    if (!keepDirectoryMarkers(p)) {
      deletion = submit(
          unboundedThreadPool,
          () -> {
            deleteUnnecessaryFakeDirectories(
                p.getParent(),
                operationState);
            return null;
          });
    } else {
      deletion = null;
    }
    // this is only set if there is a metastore to update and the
    // operationState parameter passed in was null.
    BulkOperationState stateToClose = null;

    // See note about failure semantics in S3Guard documentation
    try {
      if (hasMetadataStore()) {
        BulkOperationState activeState = operationState;
        if (activeState == null) {
          // create an operation state if there was none, so that the
          // information gleaned from addAncestors is preserved into the
          // subsequent put.
          stateToClose = S3Guard.initiateBulkWrite(metadataStore,
              isDir
                  ? BulkOperationState.OperationType.Mkdir
                  : BulkOperationState.OperationType.Put,
              keyToPath(key));
          activeState = stateToClose;
        }
        S3Guard.addAncestors(metadataStore, p, ttlTimeProvider, activeState);
        S3AFileStatus status = createUploadFileStatus(p,
            isDir, length,
            getDefaultBlockSize(p), username, eTag, versionId);
        boolean authoritative = false;
        if (isDir) {
          // this is a directory marker so put it as such.
          status.setIsEmptyDirectory(Tristate.TRUE);
          // and maybe mark as auth
          authoritative = allowAuthoritative(p);
        }
        if (!authoritative) {
          // for files and non-auth directories
          S3Guard.putAndReturn(metadataStore, status,
              ttlTimeProvider,
              activeState);
        } else {
          // authoritative directory
          S3Guard.putAuthDirectoryMarker(metadataStore, status,
              ttlTimeProvider,
              activeState);
        }
      }
      // and catch up with any delete operation.
      waitForCompletionIgnoringExceptions(deletion);
    } catch (IOException e) {
      if (failOnMetadataWriteError) {
        throw new MetadataPersistenceException(p.toString(), e);
      } else {
        LOG.error("S3Guard: Error updating MetadataStore for write to {}",
            p, e);
      }
      instrumentation.errorIgnored();
    } finally {
      // if a new operation state was created, close it.
      IOUtils.cleanupWithLogger(LOG, stateToClose);
    }
  }

  /**
   * Should we keep directory markers under the path being created
   * by mkdir/file creation/rename?
   * @param path path to probe
   * @return true if the markers MAY be retained,
   * false if they MUST be deleted
   */
  private boolean keepDirectoryMarkers(Path path) {
    return directoryPolicy.keepDirectoryMarkers(path);
  }

  /**
   * Delete mock parent directories which are no longer needed.
   * Retry policy: retrying; exceptions swallowed.
   * @param path path
   * @param operationState (nullable) operational state for a bulk update
   */
  @Retries.RetryExceptionsSwallowed
  private void deleteUnnecessaryFakeDirectories(Path path,
      final BulkOperationState operationState) {
    List<DeleteObjectsRequest.KeyVersion> keysToRemove = new ArrayList<>();
    while (!path.isRoot()) {
      String key = pathToKey(path);
      key = (key.endsWith("/")) ? key : (key + "/");
      LOG.trace("To delete unnecessary fake directory {} for {}", key, path);
      keysToRemove.add(new DeleteObjectsRequest.KeyVersion(key));
      path = path.getParent();
    }
    try {
      removeKeys(keysToRemove, true, operationState);
    } catch(AmazonClientException | IOException e) {
      instrumentation.errorIgnored();
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        for(DeleteObjectsRequest.KeyVersion kv : keysToRemove) {
          sb.append(kv.getKey()).append(",");
        }
        LOG.debug("While deleting keys {} ", sb.toString(), e);
      }
    }
  }

  /**
   * Create a fake directory, always ending in "/".
   * Retry policy: retrying; translated.
   * @param objectName name of directory object.
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private void createFakeDirectory(final String objectName)
      throws IOException {
    if (!objectName.endsWith("/")) {
      createEmptyObject(objectName + "/");
    } else {
      createEmptyObject(objectName);
    }
  }

  /**
   * Used to create an empty file that represents an empty directory.
   * Retry policy: retrying; translated.
   * @param objectName object to create
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private void createEmptyObject(final String objectName)
      throws IOException {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    PutObjectRequest putObjectRequest = newPutObjectRequest(objectName,
        newObjectMetadata(0L),
        im);
    invoker.retry("PUT 0-byte object ", objectName,
         true,
        () -> putObjectDirect(putObjectRequest));
    incrementPutProgressStatistics(objectName, 0);
    instrumentation.directoryCreated();
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}.
   * Does so without using the {@link ObjectMetadata#clone()} method,
   * to avoid copying unnecessary headers.
   * @param source the {@link ObjectMetadata} to copy
   * @return a copy of {@link ObjectMetadata} with only relevant attributes
   */
  private ObjectMetadata cloneObjectMetadata(ObjectMetadata source) {
    ObjectMetadata ret = newObjectMetadata(source.getContentLength());
    getHeaderProcessing().cloneObjectMetadata(source, ret);
    return ret;
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
  }

  /**
   * Get the directory marker policy of this filesystem.
   * @return the marker policy.
   */
  public DirectoryPolicy getDirectoryMarkerPolicy() {
    return directoryPolicy;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", workingDir=").append(workingDir);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", partSize=").append(partSize);
    sb.append(", enableMultiObjectsDelete=").append(enableMultiObjectsDelete);
    sb.append(", maxKeys=").append(maxKeys);
    if (cannedACL != null) {
      sb.append(", cannedACL=").append(cannedACL.toString());
    }
    sb.append(", readAhead=").append(readAhead);
    if (getConf() != null) {
      sb.append(", blockSize=").append(getDefaultBlockSize());
    }
    sb.append(", multiPartThreshold=").append(multiPartThreshold);
    if (getServerSideEncryptionAlgorithm() != null) {
      sb.append(", serverSideEncryptionAlgorithm='")
          .append(getServerSideEncryptionAlgorithm())
          .append('\'');
    }
    if (blockFactory != null) {
      sb.append(", blockFactory=").append(blockFactory);
    }
    sb.append(", metastore=").append(metadataStore);
    sb.append(", authoritativeStore=").append(allowAuthoritativeMetadataStore);
    sb.append(", authoritativePath=").append(allowAuthoritativePaths);
    sb.append(", useListV1=").append(useListV1);
    if (committerIntegration != null) {
      sb.append(", magicCommitter=").append(isMagicCommitEnabled());
    }
    sb.append(", boundedExecutor=").append(boundedThreadPool);
    sb.append(", unboundedExecutor=").append(unboundedThreadPool);
    sb.append(", credentials=").append(credentials);
    sb.append(", delegation tokens=")
        .append(delegationTokens.map(Objects::toString).orElse("disabled"));
    sb.append(", ").append(directoryPolicy);
    // if logging at debug, toString returns the entire IOStatistics set.
    if (getInstrumentation() != null) {
      sb.append(", instrumentation {")
          .append(getInstrumentation().toString())
          .append("}");
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
   * True if magic commit is enabled and the path qualifies as special.
   * @param path path to examine
   * @return true if the path is or is under a magic directory
   */
  public boolean isMagicCommitPath(Path path) {
    return committerIntegration.isMagicCommitPath(path);
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
   * {@inheritDoc}
   */
  @Override
  public FileStatus[] globStatus(
      final Path pathPattern,
      final PathFilter filter)
      throws IOException {
    entryPoint(INVOCATION_GLOB_STATUS);
    return Globber.createGlobber(this)
        .withPathPattern(pathPattern)
        .withPathFiltern(filter)
        .withResolveSymlinks(false)
        .build()
        .glob();
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean exists(Path f) throws IOException {
    entryPoint(INVOCATION_EXISTS);
    return super.exists(f);
  }

  /**
   * Optimized probe for a path referencing a dir.
   * Even though it is optimized to a single HEAD, applications
   * should not over-use this method...it is all too common.
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("deprecation")
  public boolean isDirectory(Path f) throws IOException {
    entryPoint(INVOCATION_IS_DIRECTORY);
    try {
      return innerGetFileStatus(f, false, StatusProbeEnum.DIRECTORIES)
          .isDirectory();
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
  @SuppressWarnings("deprecation")
  public boolean isFile(Path f) throws IOException {
    entryPoint(INVOCATION_IS_FILE);
    try {
      return innerGetFileStatus(f, false, StatusProbeEnum.HEAD_ONLY)
          .isFile();
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
  public EtagChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    Preconditions.checkArgument(length >= 0);
    entryPoint(INVOCATION_GET_FILE_CHECKSUM);

    if (getConf().getBoolean(ETAG_CHECKSUM_ENABLED,
        ETAG_CHECKSUM_ENABLED_DEFAULT)) {
      Path path = qualify(f);
      LOG.debug("getFileChecksum({})", path);
      ObjectMetadata headers = getObjectMetadata(path);
      String eTag = headers.getETag();
      return eTag != null ? new EtagChecksum(eTag) : null;
    } else {
      // disabled
      return null;
    }
  }

  /**
   * Get header processing support.
   * @return the header processing of this instance.
   */
  private HeaderProcessing getHeaderProcessing() {
    return headerProcessing;
  }

  @Override
  public byte[] getXAttr(final Path path, final String name)
      throws IOException {
    return getHeaderProcessing().getXAttr(path, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(final Path path) throws IOException {
    return getHeaderProcessing().getXAttrs(path);
  }

  @Override
  public Map<String, byte[]> getXAttrs(final Path path,
      final List<String> names)
      throws IOException {
    return getHeaderProcessing().getXAttrs(path, names);
  }

  @Override
  public List<String> listXAttrs(final Path path) throws IOException {
    return getHeaderProcessing().listXAttrs(path);
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
  public RemoteIterator<LocatedFileStatus> listFiles(Path f,
      boolean recursive) throws FileNotFoundException, IOException {
    return toLocatedFileStatusIterator(innerListFiles(f, recursive,
        new Listing.AcceptFilesOnly(qualify(f)), null, true, false));
  }

  /**
   * Recursive List of files and empty directories.
   * @param f path to list from
   * @return an iterator.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  public RemoteIterator<S3ALocatedFileStatus> listFilesAndEmptyDirectories(
      Path f, boolean recursive) throws IOException {
    return innerListFiles(f, recursive, Listing.ACCEPT_ALL_BUT_S3N,
        null, true, false);
  }

  /**
   * Recursive List of files and empty directories, force metadatastore
   * to act like it is non-authoritative.
   * @param f path to list from
   * @param recursive
   * @return an iterator.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  public RemoteIterator<S3ALocatedFileStatus> listFilesAndEmptyDirectoriesForceNonAuth(
      Path f, boolean recursive) throws IOException {
    return innerListFiles(f, recursive, Listing.ACCEPT_ALL_BUT_S3N,
        null, true, true);
  }

  /**
   * List files under the path.
   * <ol>
   *   <li>
   *     If the path is authoritative on the client,
   *     only S3Guard will be queried.
   *   </li>
   *   <li>
   *     Otherwise, the S3Guard values are returned first, then the S3
   *     entries will be retrieved and returned if not already listed.</li>
   *   <li>
   *     when collectTombstones} is true, S3Guard tombstones will
   *     be used to filter out deleted files.
   *     They MUST be used for normal listings; it is only for
   *     deletion and low-level operations that they MAY be bypassed.
   *   </li>
   *   <li>
   *     The optional {@code status} parameter will be used to skip the
   *     initial getFileStatus call.
   *   </li>
   * </ol>
   *
   * In case of recursive listing, if any of the directories reachable from
   * the path are not authoritative on the client, this method will query S3
   * for all the directories in the listing in addition to returning S3Guard
   * entries.
   *
   * @param f path
   * @param recursive recursive listing?
   * @param acceptor file status filter
   * @param status optional status of path to list.
   * @param collectTombstones should tombstones be collected from S3Guard?
   * @param forceNonAuthoritativeMS forces metadata store to act like non
   *                                authoritative. This is useful when
   *                                listFiles output is used by import tool.
   * @return an iterator over the listing.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private RemoteIterator<S3ALocatedFileStatus> innerListFiles(
      final Path f,
      final boolean recursive,
      final Listing.FileStatusAcceptor acceptor,
      final S3AFileStatus status,
      final boolean collectTombstones,
      final boolean forceNonAuthoritativeMS) throws IOException {
    entryPoint(INVOCATION_LIST_FILES);
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
                      collectTombstones,
                      forceNonAuthoritativeMS);
      // If there are no list entries present, we
      // fallback to file existence check as the path
      // can be a file or empty directory.
      if (!listFilesAssumingDir.hasNext()) {
        // If file status was already passed, reuse it.
        final S3AFileStatus fileStatus = status != null
                ? status
                : (S3AFileStatus) getFileStatus(path);
        if (fileStatus.isFile()) {
          return listing.createSingleStatusIterator(
                  toLocatedFileStatus(fileStatus));
        }
      }
      // If we have reached here, it means either there are files
      // in this directory or it is empty.
      return listFilesAssumingDir;
    } catch (AmazonClientException e) {
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
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
      throws FileNotFoundException, IOException {
    entryPoint(INVOCATION_LIST_LOCATED_STATUS);
    Path path = qualify(f);
    LOG.debug("listLocatedStatus({}, {}", path, filter);
    RemoteIterator<? extends LocatedFileStatus> iterator =
        once("listLocatedStatus", path.toString(),
          () -> {
            // Assuming the path to be a directory,
            // trigger a list call directly.
            final RemoteIterator<S3ALocatedFileStatus>
                    locatedFileStatusIteratorForDir =
                    listing.getLocatedFileStatusIteratorForDir(path, filter);

            // If no listing is present then path might be a file.
            if (!locatedFileStatusIteratorForDir.hasNext()) {
              final S3AFileStatus fileStatus =
                      (S3AFileStatus) getFileStatus(path);
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
   * Generate list located status for a directory.
   * Also performing tombstone reconciliation for guarded directories.
   * @param dir directory to check.
   * @param filter a path filter.
   * @return an iterator that traverses statuses of the given dir.
   * @throws IOException in case of failure.
   */
  private RemoteIterator<S3ALocatedFileStatus> getLocatedFileStatusIteratorForDir(
          Path dir, PathFilter filter) throws IOException {
    final String key = maybeAddTrailingSlash(pathToKey(dir));
    final Listing.FileStatusAcceptor acceptor =
        new Listing.AcceptAllButSelfAndS3nDirs(dir);
    boolean allowAuthoritative = allowAuthoritative(dir);
    DirListingMetadata meta =
        S3Guard.listChildrenWithTtl(metadataStore, dir,
            ttlTimeProvider, allowAuthoritative);
    Set<Path> tombstones = meta != null
            ? meta.listTombstones()
            : null;
    final RemoteIterator<S3AFileStatus> cachedFileStatusIterator =
        listing.createProvidedFileStatusIterator(
            dirMetaToStatuses(meta), filter, acceptor);
    return (allowAuthoritative && meta != null
        && meta.isAuthoritative())
        ? listing.createLocatedFileStatusIterator(
        cachedFileStatusIterator)
        : listing.createTombstoneReconcilingIterator(
            listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(dir,
                createListObjectsRequest(key, "/"),
                filter,
                acceptor,
                cachedFileStatusIterator)),
            tombstones);
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
  public MultipartUtils.UploadIterator listUploads(@Nullable String prefix)
      throws IOException {
    return MultipartUtils.listMultipartUploads(s3, invoker, bucket, maxKeys,
        prefix);
  }

  /**
   * Listing all multipart uploads; limited to the first few hundred.
   * See {@link #listUploads(String)} for an iterator-based version that does
   * not limit the number of entries returned.
   * Retry policy: retry, translated.
   * @return a listing of multipart uploads.
   * @param prefix prefix to scan for, "" for none
   * @throws IOException IO failure, including any uprated AmazonClientException
   */
  @InterfaceAudience.Private
  @Retries.RetryTranslated
  public List<MultipartUpload> listMultipartUploads(String prefix)
      throws IOException {
    ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(
        bucket);
    if (!prefix.isEmpty()) {
      if (!prefix.endsWith("/")) {
        prefix = prefix + "/";
      }
      request.setPrefix(prefix);
    }

    return invoker.retry("listMultipartUploads", prefix, true,
        () -> s3.listMultipartUploads(request).getMultipartUploads());
  }

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param destKey destination key
   * @param uploadId Upload ID
   */
  @Retries.OnceRaw
  void abortMultipartUpload(String destKey, String uploadId) {
    LOG.info("Aborting multipart upload {} to {}", uploadId, destKey);
    getAmazonS3Client().abortMultipartUpload(
        new AbortMultipartUploadRequest(getBucket(),
            destKey,
            uploadId));
  }

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param upload the listed upload to abort.
   */
  @Retries.OnceRaw
  void abortMultipartUpload(MultipartUpload upload) {
    String destKey;
    String uploadId;
    destKey = upload.getKey();
    uploadId = upload.getUploadId();
    if (LOG.isInfoEnabled()) {
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      LOG.debug("Aborting multipart upload {} to {} initiated by {} on {}",
          uploadId, destKey, upload.getInitiator(),
          df.format(upload.getInitiated()));
    }
    getAmazonS3Client().abortMultipartUpload(
        new AbortMultipartUploadRequest(getBucket(),
            destKey,
            uploadId));
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

    case SelectConstants.S3_SELECT_CAPABILITY:
      // select is only supported if enabled
      return selectBinding.isEnabled();

    case CommonPathCapabilities.FS_CHECKSUMS:
      // capability depends on FS configuration
      return getConf().getBoolean(ETAG_CHECKSUM_ENABLED,
          ETAG_CHECKSUM_ENABLED_DEFAULT);

    case CommonPathCapabilities.ABORTABLE_STREAM:
    case CommonPathCapabilities.FS_MULTIPART_UPLOADER:
      return true;

    // this client is safe to use with buckets
    // containing directory markers anywhere in
    // the hierarchy
    case STORE_CAPABILITY_DIRECTORY_MARKER_AWARE:
      return true;

    /*
     * Marker policy capabilities are handed off.
     */
    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_KEEP:
    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_DELETE:
    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_AUTHORITATIVE:
    case STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP:
    case STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE:
      return getDirectoryMarkerPolicy().hasPathCapability(path, cap);

    default:
      return super.hasPathCapability(p, cap);
    }
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

  @VisibleForTesting
  public ITtlTimeProvider getTtlTimeProvider() {
    return ttlTimeProvider;
  }

  @VisibleForTesting
  protected void setTtlTimeProvider(ITtlTimeProvider ttlTimeProvider) {
    this.ttlTimeProvider = ttlTimeProvider;
    metadataStore.setTtlTimeProvider(ttlTimeProvider);
  }

  /**
   * This is a proof of concept of a select API.
   * Once a proper factory mechanism for opening files is added to the
   * FileSystem APIs, this will be deleted <i>without any warning</i>.
   * @param source path to source data
   * @param expression select expression
   * @param options request configuration from the builder.
   * @param providedStatus any passed in status
   * @return the stream of the results
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private FSDataInputStream select(final Path source,
      final String expression,
      final Configuration options,
      final Optional<S3AFileStatus> providedStatus)
      throws IOException {
    entryPoint(OBJECT_SELECT_REQUESTS);
    requireSelectSupport(source);
    final Path path = makeQualified(source);
    final S3AFileStatus fileStatus = extractOrFetchSimpleFileStatus(path,
        providedStatus);

    // readahead range can be dynamically set
    long ra = options.getLong(READAHEAD_RANGE, readAhead);
    S3ObjectAttributes objectAttributes = createObjectAttributes(fileStatus);
    S3AReadOpContext readContext = createReadContext(fileStatus, inputPolicy,
        changeDetectionPolicy, ra);

    if (changeDetectionPolicy.getSource() != ChangeDetectionPolicy.Source.None
        && fileStatus.getETag() != null) {
      // if there is change detection, and the status includes at least an
      // etag,
      // check that the object metadata lines up with what is expected
      // based on the object attributes (which may contain an eTag or
      // versionId).
      // This is because the select API doesn't offer this.
      // (note: this is trouble for version checking as cannot force the old
      // version in the final read; nor can we check the etag match)
      ChangeTracker changeTracker =
          new ChangeTracker(uri.toString(),
              changeDetectionPolicy,
              readContext.getS3AStatisticsContext()
                  .newInputStreamStatistics()
                  .getChangeTrackerStatistics(),
              objectAttributes);

      // will retry internally if wrong version detected
      Invoker readInvoker = readContext.getReadInvoker();
      getObjectMetadata(path, changeTracker, readInvoker, "select");
    }

    // build and execute the request
    return selectBinding.select(
        readContext,
        expression,
        options,
        generateSSECustomerKey(),
        objectAttributes);
  }

  /**
   * Verify the FS supports S3 Select.
   * @param source source file.
   * @throws UnsupportedOperationException if not.
   */
  private void requireSelectSupport(final Path source) throws
      UnsupportedOperationException {
    if (!selectBinding.isEnabled()) {
      throw new UnsupportedOperationException(
          SelectConstants.SELECT_UNSUPPORTED);
    }
  }

  /**
   * Extract the status from the optional parameter, querying
   * S3Guard/s3 if it is absent.
   * @param path path of the status
   * @param optStatus optional status
   * @return a file status
   * @throws FileNotFoundException if there is no normal file at that path
   * @throws IOException IO failure
   */
  private S3AFileStatus extractOrFetchSimpleFileStatus(
      final Path path, final Optional<S3AFileStatus> optStatus)
      throws IOException {
    S3AFileStatus fileStatus;
    if (optStatus.isPresent()) {
      fileStatus = optStatus.get();
    } else {
      // this looks at S3guard and gets any type of status back,
      // if it falls back to S3 it does a HEAD only.
      // therefore: if there is no S3Guard and there is a dir, this
      // will raise a FileNotFoundException
      fileStatus = innerGetFileStatus(path, false,
          StatusProbeEnum.HEAD_ONLY);
    }
    // we check here for the passed in status or the S3Guard value
    // for being a directory
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException(path.toString() + " is a directory");
    }
    return fileStatus;
  }

  /**
   * Initiate the open or select operation.
   * This is invoked from both the FileSystem and FileContext APIs
   * @param rawPath path to the file
   * @param parameters open file parameters from the builder.
   * @return a future which will evaluate to the opened/selected file.
   * @throws IOException failure to resolve the link.
   * @throws PathIOException operation is a select request but S3 select is
   * disabled
   * @throws IllegalArgumentException unknown mandatory key
   */
  @Override
  @Retries.RetryTranslated
  public CompletableFuture<FSDataInputStream> openFileWithOptions(
      final Path rawPath,
      final OpenFileParameters parameters) throws IOException {
    final Path path = qualify(rawPath);
    Configuration options = parameters.getOptions();
    Set<String> mandatoryKeys = parameters.getMandatoryKeys();
    String sql = options.get(SelectConstants.SELECT_SQL, null);
    boolean isSelect = sql != null;
    // choice of keys depends on open type
    if (isSelect) {
      rejectUnknownMandatoryKeys(
          mandatoryKeys,
          InternalSelectConstants.SELECT_OPTIONS,
          "for " + path + " in S3 Select operation");
    } else {
      rejectUnknownMandatoryKeys(
          mandatoryKeys,
          InternalConstants.STANDARD_OPENFILE_KEYS,
          "for " + path + " in non-select file I/O");
    }
    FileStatus providedStatus = parameters.getStatus();
    S3AFileStatus fileStatus;
    if (providedStatus != null) {
      Preconditions.checkArgument(path.equals(providedStatus.getPath()),
          "FileStatus parameter is not for the path %s: %s",
          path, providedStatus);
      if (providedStatus instanceof S3AFileStatus) {
        // can use this status to skip our own probes,
        // including etag and version.
        LOG.debug("File was opened with a supplied S3AFileStatus;"
            + " skipping getFileStatus call in open() operation: {}",
            providedStatus);
        fileStatus = (S3AFileStatus) providedStatus;
      } else if (providedStatus instanceof S3ALocatedFileStatus) {
        LOG.debug("File was opened with a supplied S3ALocatedFileStatus;"
            + " skipping getFileStatus call in open() operation: {}",
            providedStatus);
        fileStatus = ((S3ALocatedFileStatus) providedStatus).toS3AFileStatus();
      } else {
        LOG.debug("Ignoring file status {}", providedStatus);
        fileStatus = null;
      }
    } else {
      fileStatus = null;
    }
    Optional<S3AFileStatus> ost = Optional.ofNullable(fileStatus);
    CompletableFuture<FSDataInputStream> result = new CompletableFuture<>();
    if (!isSelect) {
      // normal path.
      unboundedThreadPool.submit(() ->
          LambdaUtils.eval(result,
              () -> open(path, Optional.of(options), ost)));
    } else {
      // it is a select statement.
      // fail fast if the operation is not available
      requireSelectSupport(path);
      // submit the query
      unboundedThreadPool.submit(() ->
          LambdaUtils.eval(result,
              () -> select(path, sql, options, ost)));
    }
    return result;
  }

  @Override
  public S3AMultipartUploaderBuilder createMultipartUploader(
      final Path basePath)
      throws IOException {
    StoreContext ctx = createStoreContext();
    return new S3AMultipartUploaderBuilder(this,
        getWriteOperationHelper(),
        ctx,
        basePath,
        statisticsContext.createMultipartUploaderStatistics());
  }

  /**
   * Build an immutable store context.
   * If called while the FS is being initialized,
   * some of the context will be incomplete.
   * new store context instances should be created as appropriate.
   * @return the store context of this FS.
   */
  @InterfaceAudience.Private
  public StoreContext createStoreContext() {
    return new StoreContextBuilder().setFsURI(getUri())
        .setBucket(getBucket())
        .setConfiguration(getConf())
        .setUsername(getUsername())
        .setOwner(owner)
        .setExecutor(boundedThreadPool)
        .setExecutorCapacity(executorCapacity)
        .setInvoker(invoker)
        .setInstrumentation(statisticsContext)
        .setStorageStatistics(getStorageStatistics())
        .setInputPolicy(getInputPolicy())
        .setChangeDetectionPolicy(changeDetectionPolicy)
        .setMultiObjectDeleteEnabled(enableMultiObjectsDelete)
        .setMetadataStore(metadataStore)
        .setUseListV1(useListV1)
        .setContextAccessors(new ContextAccessorsImpl())
        .setTimeProvider(getTtlTimeProvider())
        .build();
  }

  /**
   * Create a marker tools operations binding for this store.
   * @return callbacks for operations.
   */
  @InterfaceAudience.Private
  public MarkerToolOperations createMarkerToolOperations() {
    return new MarkerToolOperationsImpl(operationCallbacks);
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
    public ObjectMetadata getObjectMetadata(final String key)
        throws IOException {
      return once("getObjectMetadata", key, () ->
          S3AFileSystem.this.getObjectMetadata(key));
    }
  }
}
