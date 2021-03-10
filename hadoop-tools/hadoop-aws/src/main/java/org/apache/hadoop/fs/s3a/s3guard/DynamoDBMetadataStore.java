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

package org.apache.hadoop.fs.s3a.s3guard;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.AWSServiceThrottledException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3a.auth.RolePolicies;
import org.apache.hadoop.fs.s3a.auth.delegation.AWSPolicyProvider;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.allowAllDynamoDBOperations;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.allowS3GuardClientOperations;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.*;
import static org.apache.hadoop.fs.s3a.s3guard.PathOrderComparators.TOPMOST_PM_LAST;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.*;

/**
 * DynamoDBMetadataStore is a {@link MetadataStore} that persists
 * file system metadata to DynamoDB.
 *
 * The current implementation uses a schema consisting of a single table.  The
 * name of the table can be configured by config key
 * {@link org.apache.hadoop.fs.s3a.Constants#S3GUARD_DDB_TABLE_NAME_KEY}.
 * By default, it matches the name of the S3 bucket.  Each item in the table
 * represents a single directory or file.  Its path is split into separate table
 * attributes:
 * <ul>
 * <li> parent (absolute path of the parent, with bucket name inserted as
 * first path component). </li>
 * <li> child (path of that specific child, relative to parent). </li>
 * <li> optional boolean attribute tracking whether the path is a directory.
 *      Absence or a false value indicates the path is a file. </li>
 * <li> optional long attribute revealing modification time of file.
 *      This attribute is meaningful only to file items.</li>
 * <li> optional long attribute revealing file length.
 *      This attribute is meaningful only to file items.</li>
 * <li> optional long attribute revealing block size of the file.
 *      This attribute is meaningful only to file items.</li>
 * <li> optional string attribute tracking the s3 eTag of the file.
 *      May be absent if the metadata was entered with a version of S3Guard
 *      before this was tracked.
 *      This attribute is meaningful only to file items.</li>
  * <li> optional string attribute tracking the s3 versionId of the file.
 *      May be absent if the metadata was entered with a version of S3Guard
 *      before this was tracked.
 *      This attribute is meaningful only to file items.</li>
 * </ul>
 *
 * The DynamoDB partition key is the parent, and the range key is the child.
 *
 * To allow multiple buckets to share the same DynamoDB table, the bucket
 * name is treated as the root directory.
 *
 * For example, assume the consistent store contains metadata representing this
 * file system structure:
 *
 * <pre>
 * s3a://bucket/dir1
 * |-- dir2
 * |   |-- file1
 * |   `-- file2
 * `-- dir3
 *     |-- dir4
 *     |   `-- file3
 *     |-- dir5
 *     |   `-- file4
 *     `-- dir6
 * </pre>
 *
 * This is persisted to a single DynamoDB table as:
 *
 * <pre>
 * ====================================================================================
 * | parent                 | child | is_dir | mod_time | len | etag | ver_id |  ...  |
 * ====================================================================================
 * | /bucket                | dir1  | true   |          |     |      |        |       |
 * | /bucket/dir1           | dir2  | true   |          |     |      |        |       |
 * | /bucket/dir1           | dir3  | true   |          |     |      |        |       |
 * | /bucket/dir1/dir2      | file1 |        |   100    | 111 | abc  |  mno   |       |
 * | /bucket/dir1/dir2      | file2 |        |   200    | 222 | def  |  pqr   |       |
 * | /bucket/dir1/dir3      | dir4  | true   |          |     |      |        |       |
 * | /bucket/dir1/dir3      | dir5  | true   |          |     |      |        |       |
 * | /bucket/dir1/dir3/dir4 | file3 |        |   300    | 333 | ghi  |  stu   |       |
 * | /bucket/dir1/dir3/dir5 | file4 |        |   400    | 444 | jkl  |  vwx   |       |
 * | /bucket/dir1/dir3      | dir6  | true   |          |     |      |        |       |
 * ====================================================================================
 * </pre>
 *
 * This choice of schema is efficient for read access patterns.
 * {@link #get(Path)} can be served from a single item lookup.
 * {@link #listChildren(Path)} can be served from a query against all rows
 * matching the parent (the partition key) and the returned list is guaranteed
 * to be sorted by child (the range key).  Tracking whether or not a path is a
 * directory helps prevent unnecessary queries during traversal of an entire
 * sub-tree.
 *
 * Some mutating operations, notably
 * {@link MetadataStore#deleteSubtree(Path, BulkOperationState)} and
 * {@link MetadataStore#move(Collection, Collection, BulkOperationState)}
 * are less efficient with this schema.
 * They require mutating multiple items in the DynamoDB table.
 *
 * By default, DynamoDB access is performed within the same AWS region as
 * the S3 bucket that hosts the S3A instance.  During initialization, it checks
 * the location of the S3 bucket and creates a DynamoDB client connected to the
 * same region. The region may also be set explicitly by setting the config
 * parameter {@code fs.s3a.s3guard.ddb.region} to the corresponding region.
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DynamoDBMetadataStore implements MetadataStore,
    AWSPolicyProvider {
  public static final Logger LOG = LoggerFactory.getLogger(
      DynamoDBMetadataStore.class);

  /**
   * Name of the operations log.
   */
  public static final String OPERATIONS_LOG_NAME =
      "org.apache.hadoop.fs.s3a.s3guard.Operations";

  /**
   * A log of all state changing operations to the store;
   * only updated at debug level.
   */
  public static final Logger OPERATIONS_LOG = LoggerFactory.getLogger(
      OPERATIONS_LOG_NAME);

  /** parent/child name to use in the version marker. */
  public static final String VERSION_MARKER_ITEM_NAME = "../VERSION";

  /** parent/child name to use in the version marker. */
  public static final String VERSION_MARKER_TAG_NAME = "s3guard_version";

  /** Current version number. */
  public static final int VERSION = 100;

  @VisibleForTesting
  static final String BILLING_MODE
      = "billing-mode";

  @VisibleForTesting
  static final String BILLING_MODE_PER_REQUEST
      = "per-request";

  @VisibleForTesting
  static final String BILLING_MODE_PROVISIONED
      = "provisioned";

  @VisibleForTesting
  static final String DESCRIPTION
      = "S3Guard metadata store in DynamoDB";
  @VisibleForTesting
  static final String READ_CAPACITY = "read-capacity";
  @VisibleForTesting
  static final String WRITE_CAPACITY = "write-capacity";
  @VisibleForTesting
  static final String STATUS = "status";
  @VisibleForTesting
  static final String TABLE = "table";

  @VisibleForTesting
  static final String HINT_DDB_IOPS_TOO_LOW
      = " This may be because the write threshold of DynamoDB is set too low.";

  @VisibleForTesting
  static final String THROTTLING = "Throttling";

  public static final String E_ON_DEMAND_NO_SET_CAPACITY
      = "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST";

  @VisibleForTesting
  static final String E_INCONSISTENT_UPDATE
      = "Duplicate and inconsistent entry in update operation";

  private static final ValueMap DELETE_TRACKING_VALUE_MAP =
      new ValueMap().withBoolean(":false", false);

  /**
   * The maximum number of outstanding operations to submit
   * before blocking to await completion of all the executors.
   * Paging work like this is less efficient, but it ensures that
   * failure (auth, network, etc) are picked up before many more
   * operations are submitted.
   *
   * Arbitrary Choice.
   * Value: {@value}.
   */
  private static final int S3GUARD_DDB_SUBMITTED_TASK_LIMIT = 50;

  private AmazonDynamoDB amazonDynamoDB;
  private DynamoDB dynamoDB;
  private AWSCredentialProviderList credentials;
  private String region;
  private Table table;
  private String tableName;
  private Configuration conf;
  private String username;

  /**
   * This policy is mostly for batched writes, not for processing
   * exceptions in invoke() calls.
   * It also has a role purpose in
   * {@link DynamoDBMetadataStoreTableManager#getVersionMarkerItem()};
   * look at that method for the details.
   */
  private RetryPolicy batchWriteRetryPolicy;

  /**
   * The instrumentation is never null -if/when bound to an owner file system
   * That filesystem statistics will be updated as appropriate.
   */
  private MetastoreInstrumentation instrumentation
      = new MetastoreInstrumentationImpl();

  /** Owner FS: only valid if configured with an owner FS. */
  private S3AFileSystem owner;

  /** Invoker for IO. Until configured properly, use try-once. */
  private Invoker invoker = new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL,
      Invoker.NO_OP
  );

  /** Invoker for read operations. */
  private Invoker readOp;

  /** Invoker for write operations. */
  private Invoker writeOp;

  /** Invoker for scan operations. */
  private Invoker scanOp;

  private final AtomicLong readThrottleEvents = new AtomicLong(0);
  private final AtomicLong writeThrottleEvents = new AtomicLong(0);
  private final AtomicLong scanThrottleEvents = new AtomicLong(0);
  private final AtomicLong batchWriteCapacityExceededEvents = new AtomicLong(0);

  /**
   * Total limit on the number of throttle events after which
   * we stop warning in the log. Keeps the noise down.
   */
  private static final int THROTTLE_EVENT_LOG_LIMIT = 100;

  /**
   * Count of the total number of throttle events; used to crank back logging.
   */
  private AtomicInteger throttleEventCount = new AtomicInteger(0);

  /**
   * Executor for submitting operations.
   */
  private ListeningExecutorService executor;

  /**
   * Time source. This is used during writes when parent
   * entries need to be created.
   */
  private ITtlTimeProvider ttlTimeProvider;

  private DynamoDBMetadataStoreTableManager tableHandler;

  /**
   * A utility function to create DynamoDB instance.
   * @param conf the file system configuration
   * @param s3Region region of the associated S3 bucket (if any).
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param credentials credentials.
   * @return DynamoDB instance.
   * @throws IOException I/O error.
   */
  private DynamoDB createDynamoDB(
      final Configuration conf,
      final String s3Region,
      final String bucket,
      final AWSCredentialsProvider credentials)
      throws IOException {
    if (amazonDynamoDB == null) {
      Preconditions.checkNotNull(conf);
      final Class<? extends DynamoDBClientFactory> cls =
          conf.getClass(S3GUARD_DDB_CLIENT_FACTORY_IMPL,
          S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT, DynamoDBClientFactory.class);
      LOG.debug("Creating DynamoDB client {} with S3 region {}", cls, s3Region);
      amazonDynamoDB = ReflectionUtils.newInstance(cls, conf)
          .createDynamoDBClient(s3Region, bucket, credentials);
    }
    return new DynamoDB(amazonDynamoDB);
  }

  /**
   * {@inheritDoc}.
   * The credentials for authenticating with S3 are requested from the
   * FS via {@link S3AFileSystem#shareCredentials(String)}; this will
   * increment the reference counter of these credentials.
   * @param fs {@code S3AFileSystem} associated with the MetadataStore
   * @param ttlTp the time provider to use for metadata expiry
   * @throws IOException on a failure
   */
  @Override
  @Retries.OnceRaw
  public void initialize(FileSystem fs, ITtlTimeProvider ttlTp)
      throws IOException {
    Preconditions.checkNotNull(fs, "Null filesystem");
    Preconditions.checkArgument(fs instanceof S3AFileSystem,
        "DynamoDBMetadataStore only supports S3A filesystem - not %s",
        fs);
    bindToOwnerFilesystem((S3AFileSystem) fs);
    final String bucket = owner.getBucket();
    String confRegion = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
    if (!StringUtils.isEmpty(confRegion)) {
      region = confRegion;
      LOG.debug("Overriding S3 region with configured DynamoDB region: {}",
          region);
    } else {
      try {
        region = owner.getBucketLocation();
      } catch (AccessDeniedException e) {
        // access denied here == can't call getBucket. Report meaningfully
        URI uri = owner.getUri();
        String message =
            "Failed to get bucket location as client lacks permission "
                + RolePolicies.S3_GET_BUCKET_LOCATION + " for " + uri;
        LOG.error(message);
        throw (IOException)new AccessDeniedException(message).initCause(e);
      }
      LOG.debug("Inferring DynamoDB region from S3 bucket: {}", region);
    }
    credentials = owner.shareCredentials("s3guard");
    dynamoDB = createDynamoDB(conf, region, bucket, credentials);

    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY, bucket);
    initDataAccessRetries(conf);

    this.ttlTimeProvider = ttlTp;

    tableHandler = new DynamoDBMetadataStoreTableManager(
        dynamoDB, tableName, region, amazonDynamoDB, conf, readOp,
        batchWriteRetryPolicy);
    this.table = tableHandler.initTable();

    instrumentation.initialized();
  }

  /**
   * Declare that this table is owned by the specific S3A FS instance.
   * This will bind some fields to the values provided by the owner,
   * including wiring up the instrumentation.
   * @param fs owner filesystem
   */
  @VisibleForTesting
  void bindToOwnerFilesystem(final S3AFileSystem fs) {
    owner = fs;
    conf = owner.getConf();
    StoreContext context = owner.createStoreContext();
    instrumentation = context.getInstrumentation()
        .getS3GuardInstrumentation();
    username = context.getUsername();
    executor = MoreExecutors.listeningDecorator(
        context.createThrottledExecutor());
    ttlTimeProvider = Preconditions.checkNotNull(
        context.getTimeProvider(),
        "ttlTimeProvider must not be null");
  }

  /**
   * Performs one-time initialization of the metadata store via configuration.
   *
   * This initialization depends on the configuration object to get AWS
   * credentials, DynamoDBFactory implementation class, DynamoDB endpoints,
   * DynamoDB table names etc. After initialization, this metadata store does
   * not explicitly relate to any S3 bucket, which be nonexistent.
   *
   * This is used to operate the metadata store directly beyond the scope of the
   * S3AFileSystem integration, e.g. command line tools.
   * Generally, callers should use
   * {@link MetadataStore#initialize(FileSystem, ITtlTimeProvider)}
   * with an initialized {@code S3AFileSystem} instance.
   *
   * Without a filesystem to act as a reference point, the configuration itself
   * must declare the table name and region in the
   * {@link Constants#S3GUARD_DDB_TABLE_NAME_KEY} and
   * {@link Constants#S3GUARD_DDB_REGION_KEY} respectively.
   * It also creates a new credential provider list from the configuration,
   * using the base fs.s3a.* options, as there is no bucket to infer per-bucket
   * settings from.
   *
   * @see MetadataStore#initialize(FileSystem, ITtlTimeProvider)
   * @throws IOException if there is an error
   * @throws IllegalArgumentException if the configuration is incomplete
   */
  @Override
  @Retries.OnceRaw
  public void initialize(Configuration config,
      ITtlTimeProvider ttlTp) throws IOException {
    conf = config;
    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY);

    Preconditions.checkArgument(!StringUtils.isEmpty(tableName),
        "No DynamoDB table name configured");
    region = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
    Preconditions.checkArgument(!StringUtils.isEmpty(region),
        "No DynamoDB region configured");
    // there's no URI here, which complicates life: you cannot
    // create AWS providers here which require one.
    credentials = createAWSCredentialProviderSet(null, conf);
    dynamoDB = createDynamoDB(conf, region, null, credentials);

    username = UserGroupInformation.getCurrentUser().getShortUserName();
    // without an executor from the owner FS, create one using
    // the executor capacity for work.
    int executorCapacity = intOption(conf,
        EXECUTOR_CAPACITY, DEFAULT_EXECUTOR_CAPACITY, 1);
    executor = MoreExecutors.listeningDecorator(
        BlockingThreadPoolExecutorService.newInstance(
            executorCapacity,
            executorCapacity * 2,
              longOption(conf, KEEPALIVE_TIME,
                  DEFAULT_KEEPALIVE_TIME, 0),
                  TimeUnit.SECONDS,
                  "s3a-ddb-" + tableName));
    initDataAccessRetries(conf);
    this.ttlTimeProvider = ttlTp;

    tableHandler = new DynamoDBMetadataStoreTableManager(
        dynamoDB, tableName, region, amazonDynamoDB, conf, readOp,
        batchWriteRetryPolicy);
    this.table = tableHandler.initTable();
  }

  /**
   * Set retry policy. This is driven by the value of
   * {@link Constants#S3GUARD_DDB_MAX_RETRIES} with an exponential backoff
   * between each attempt of {@link Constants#S3GUARD_DDB_THROTTLE_RETRY_INTERVAL}
   * milliseconds.
   * @param config configuration for data access
   */
  private void initDataAccessRetries(Configuration config) {
    batchWriteRetryPolicy = RetryPolicies
        .exponentialBackoffRetry(
            config.getInt(S3GUARD_DDB_MAX_RETRIES,
                S3GUARD_DDB_MAX_RETRIES_DEFAULT),
            conf.getTimeDuration(S3GUARD_DDB_THROTTLE_RETRY_INTERVAL,
                S3GUARD_DDB_THROTTLE_RETRY_INTERVAL_DEFAULT,
                TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS);
    final RetryPolicy throttledRetryRetryPolicy
        = new S3GuardDataAccessRetryPolicy(config);
    readOp = new Invoker(throttledRetryRetryPolicy, this::readRetryEvent);
    writeOp = new Invoker(throttledRetryRetryPolicy, this::writeRetryEvent);
    scanOp = new Invoker(throttledRetryRetryPolicy, this::scanRetryEvent);
  }

  @Override
  @Retries.RetryTranslated
  public void delete(Path path,
      final BulkOperationState operationState)
      throws IOException {
    innerDelete(path, true,
        extractOrCreate(operationState,
            BulkOperationState.OperationType.Delete));
  }

  @Override
  @Retries.RetryTranslated
  public void forgetMetadata(Path path) throws IOException {
    LOG.debug("Forget metadata for {}", path);
    innerDelete(path, false, null);
  }

  /**
   * Inner delete option, action based on the {@code tombstone} flag.
   * No tombstone: delete the entry. Tombstone: create a tombstone entry.
   * There is no check as to whether the entry exists in the table first.
   * @param path path to delete
   * @param tombstone flag to create a tombstone marker
   * @param ancestorState ancestor state for context.
   * @throws IOException I/O error.
   */
  @Retries.RetryTranslated
  private void innerDelete(final Path path,
      final boolean tombstone,
      final AncestorState ancestorState)
      throws IOException {
    checkPath(path);
    LOG.debug("Deleting from table {} in region {}: {}",
        tableName, region, path);

    // deleting nonexistent item consumes 1 write capacity; skip it
    if (path.isRoot()) {
      LOG.debug("Skip deleting root directory as it does not exist in table");
      return;
    }
    // the policy on whether repeating delete operations is based
    // on that of S3A itself
    boolean idempotent = S3AFileSystem.DELETE_CONSIDERED_IDEMPOTENT;
    if (tombstone) {
      Preconditions.checkArgument(ttlTimeProvider != null, "ttlTimeProvider "
          + "must not be null");
      final PathMetadata pmTombstone = PathMetadata.tombstone(path,
          ttlTimeProvider.getNow());
      Item item = PathMetadataDynamoDBTranslation.pathMetadataToItem(
          new DDBPathMetadata(pmTombstone));
      writeOp.retry(
          "Put tombstone",
          path.toString(),
          idempotent,
          () -> {
            logPut(ancestorState, item);
            recordsWritten(1);
            table.putItem(item);
          });
    } else {
      PrimaryKey key = pathToKey(path);
      writeOp.retry(
          "Delete key",
          path.toString(),
          idempotent,
          () -> {
            // record the attempt so even on retry the counter goes up.
            logDelete(ancestorState, key);
            recordsDeleted(1);
            table.deleteItem(key);
          });
    }
  }

  @Override
  @Retries.RetryTranslated
  public void deleteSubtree(Path path,
      final BulkOperationState operationState)
      throws IOException {
    checkPath(path);
    LOG.debug("Deleting subtree from table {} in region {}: {}",
        tableName, region, path);

    final PathMetadata meta = get(path);
    if (meta == null) {
      LOG.debug("Subtree path {} does not exist; this will be a no-op", path);
      return;
    }
    if (meta.isDeleted()) {
      LOG.debug("Subtree path {} is deleted; this will be a no-op", path);
      return;
    }
    deleteEntries(RemoteIterators.mappingRemoteIterator(
        new DescendantsIterator(this, meta),
        FileStatus::getPath),
        operationState);
  }

  @Override
  @Retries.RetryTranslated
  public void deletePaths(Collection<Path> paths,
      final BulkOperationState operationState)
      throws IOException {
    deleteEntries(RemoteIterators.remoteIteratorFromIterable(paths),
        operationState);
  }

  /**
   * Delete the entries under an iterator.
   * There's no attempt to order the paths: they are
   * deleted in the order passed in.
   * @param entries entries to delete.
   * @param operationState Nullable operation state
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private void deleteEntries(RemoteIterator<Path> entries,
      final BulkOperationState operationState)
      throws IOException {
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    AncestorState state = extractOrCreate(operationState,
        BulkOperationState.OperationType.Delete);

    while (entries.hasNext()) {
      final Path pathToDelete = entries.next();
      futures.add(submit(executor, () -> {
        innerDelete(pathToDelete, true, state);
        return null;
      }));
      if (futures.size() > S3GUARD_DDB_SUBMITTED_TASK_LIMIT) {
        // first batch done; block for completion.
        waitForCompletion(futures);
        futures.clear();
      }
    }
    // now wait for the final set.
    waitForCompletion(futures);
  }

  /**
   * Get a consistent view of an item.
   * @param path path to look up in the database
   * @return the result
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private Item getConsistentItem(final Path path) throws IOException {
    PrimaryKey key = pathToKey(path);
    final GetItemSpec spec = new GetItemSpec()
        .withPrimaryKey(key)
        .withConsistentRead(true); // strictly consistent read
    return readOp.retry("get",
        path.toString(),
        true,
        () -> {
          recordsRead(1);
          return table.getItem(spec);
        });
  }

  @Override
  @Retries.RetryTranslated
  public DDBPathMetadata get(Path path) throws IOException {
    return get(path, false);
  }

  @Override
  @Retries.RetryTranslated
  public DDBPathMetadata get(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException {
    checkPath(path);
    LOG.debug("Get from table {} in region {}: {} ; wantEmptyDirectory={}",
        tableName, region, path, wantEmptyDirectoryFlag);
    DDBPathMetadata result = innerGet(path, wantEmptyDirectoryFlag);
    LOG.debug("result of get {} is: {}", path, result);
    return result;
  }

  /**
   * Inner get operation, as invoked in the retry logic.
   * @param path the path to get
   * @param wantEmptyDirectoryFlag Set to true to give a hint to the
   *   MetadataStore that it should try to compute the empty directory flag.
   * @return metadata for {@code path}, {@code null} if not found
   * @throws IOException IO problem
   */
  @Retries.RetryTranslated
  private DDBPathMetadata innerGet(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException {
    final DDBPathMetadata meta;
    if (path.isRoot()) {
      // Root does not persist in the table
      meta =
          new DDBPathMetadata(makeDirStatus(username, path));
    } else {
      final Item item = getConsistentItem(path);
      meta = itemToPathMetadata(item, username);
      LOG.debug("Get from table {} in region {} returning for {}: {}",
          tableName, region, path, meta);
    }

    if (wantEmptyDirectoryFlag && meta != null && !meta.isDeleted()) {
      final FileStatus status = meta.getFileStatus();
      // for a non-deleted directory, we query its direct undeleted children
      // to determine the isEmpty bit. There's no TTL checking going on here.
      if (status.isDirectory()) {
        final QuerySpec spec = new QuerySpec()
            .withHashKey(pathToParentKeyAttribute(path))
            .withConsistentRead(true)
            .withFilterExpression(IS_DELETED + " = :false")
            .withValueMap(DELETE_TRACKING_VALUE_MAP);
        boolean hasChildren = readOp.retry("get/hasChildren",
            path.toString(),
            true,
            () -> {
              // issue the query
              final IteratorSupport<Item, QueryOutcome> it = table.query(
                  spec).iterator();
              // if non empty, log the result to aid with some debugging
              if (it.hasNext()) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Dir {} is non-empty", status.getPath());
                  while(it.hasNext()) {
                    LOG.debug("{}", itemToPathMetadata(it.next(), username));
                  }
                }
                return true;
              } else {
                return false;
              }
          });

        // If directory is authoritative, we can set the empty directory flag
        // to TRUE or FALSE. Otherwise FALSE, or UNKNOWN.
        if (meta.isAuthoritativeDir()) {
          meta.setIsEmptyDirectory(
              hasChildren ? Tristate.FALSE : Tristate.TRUE);
        } else {
          meta.setIsEmptyDirectory(
              hasChildren ? Tristate.FALSE : Tristate.UNKNOWN);
        }
      }
    }

    return meta;
  }

  /**
   * Make a S3AFileStatus object for a directory at given path.
   * The FileStatus only contains what S3A needs, and omits mod time
   * since S3A uses its own implementation which returns current system time.
   * @param dirOwner  username of owner
   * @param path   path to dir
   * @return new S3AFileStatus
   */
  private S3AFileStatus makeDirStatus(String dirOwner, Path path) {
    return new S3AFileStatus(Tristate.UNKNOWN, path, dirOwner);
  }

  @Override
  @Retries.RetryTranslated
  public DirListingMetadata listChildren(final Path path) throws IOException {
    checkPath(path);
    LOG.debug("Listing table {} in region {}: {}", tableName, region, path);

    final QuerySpec spec = new QuerySpec()
        .withHashKey(pathToParentKeyAttribute(path))
        .withConsistentRead(true); // strictly consistent read
    final List<PathMetadata> metas = new ArrayList<>();
    // find the children in the table
    final ItemCollection<QueryOutcome> items = scanOp.retry(
        "listChildren",
        path.toString(),
        true,
        () -> table.query(spec));
    // now wrap the result with retry logic
    try {
      for (Item item : wrapWithRetries(items)) {
        metas.add(itemToPathMetadata(item, username));
      }
    } catch (UncheckedIOException e) {
      // failure in the iterators; unwrap.
      throw e.getCause();
    }

    // Minor race condition here - if the path is deleted between
    // getting the list of items and the directory metadata we might
    // get a null in DDBPathMetadata.
    return getDirListingMetadataFromDirMetaAndList(path, metas,
        get(path));
  }

  DirListingMetadata getDirListingMetadataFromDirMetaAndList(Path path,
      List<PathMetadata> metas, DDBPathMetadata dirPathMeta) {
    boolean isAuthoritative = false;
    if (dirPathMeta != null) {
      isAuthoritative = dirPathMeta.isAuthoritativeDir();
    }

    LOG.trace("Listing table {} in region {} for {} returning {}",
        tableName, region, path, metas);

    if (!metas.isEmpty() && dirPathMeta == null) {
      // We handle this case as the directory is deleted.
      LOG.warn("Directory marker is deleted, but the list of the directory "
          + "elements is not empty: {}. This case is handled as if the "
          + "directory was deleted.", metas);
      return null;
    }

    if(metas.isEmpty() && dirPathMeta == null) {
      return null;
    }

    return new DirListingMetadata(path, metas, isAuthoritative,
        dirPathMeta.getLastUpdated());
  }

  /**
   * Origin of entries in the ancestor map built up in
   * {@link #completeAncestry(Collection, AncestorState)}.
   * This is done to stop generated ancestor entries to overwriting those
   * in the store, while allowing those requested in the API call to do this.
   */
  private enum EntryOrigin {
    Requested,  // requested in method call
    Retrieved,  // retrieved from DDB: do not resubmit
    Generated   // generated ancestor.
  }

  /**
   * Build the list of all parent entries.
   * <p>
   * <b>Thread safety:</b> none. Callers must synchronize access.
   * <p>
   * Callers are required to synchronize on ancestorState.
   * @param pathsToCreate paths to create
   * @param ancestorState ongoing ancestor state.
   * @return the full ancestry paths
   */
  private Collection<DDBPathMetadata> completeAncestry(
      final Collection<DDBPathMetadata> pathsToCreate,
      final AncestorState ancestorState) throws IOException {
    // Key on path to allow fast lookup
    Map<Path, Pair<EntryOrigin, DDBPathMetadata>> ancestry = new HashMap<>();
    LOG.debug("Completing ancestry for {} paths", pathsToCreate.size());
    // we sort the inputs to guarantee that the topmost entries come first.
    // that way if the put request contains both parents and children
    // then the existing parents will not be re-created -they will just
    // be added to the ancestor list first.
    List<DDBPathMetadata> sortedPaths = new ArrayList<>(pathsToCreate);
    sortedPaths.sort(PathOrderComparators.TOPMOST_PM_FIRST);
    // iterate through the paths.
    for (DDBPathMetadata entry : sortedPaths) {
      Preconditions.checkArgument(entry != null);
      Path path = entry.getFileStatus().getPath();
      LOG.debug("Adding entry {}", path);
      if (path.isRoot()) {
        // this is a root entry: do not add it.
        break;
      }
      // add it to the ancestor state, failing if it is already there and
      // of a different type.
      DDBPathMetadata oldEntry = ancestorState.put(path, entry);
      boolean addAncestors = true;
      if (oldEntry != null) {
        // check for and warn if the existing bulk operation has an inconsistent
        // entry.
        // two directories or two files are both allowed.
        // file-over-file can happen in multipart uploaders when the same
        // uploader is overwriting file entries to the same destination as
        // part of its bulk operation.
        boolean oldWasDir = oldEntry.getFileStatus().isDirectory();
        boolean newIsDir = entry.getFileStatus().isDirectory();
        if ((oldWasDir && !newIsDir)
            || (!oldWasDir && newIsDir)) {
          LOG.warn("Overwriting a S3Guard file created in the operation: {}",
              oldEntry);
          LOG.warn("With new entry: {}", entry);
          // restore the old state
          ancestorState.put(path, oldEntry);
          // then raise an exception
          throw new PathIOException(path.toString(),
              String.format("%s old %s new %s",
                  E_INCONSISTENT_UPDATE,
                  oldEntry,
                  entry));
        } else {
          // a directory is already present. Log and continue.
          LOG.debug("Directory at {} being updated with value {}",
              path, entry);
          // and we skip the the subsequent parent scan as we've already been
          // here
          addAncestors = false;
        }
      }
      // add the entry to the ancestry map as an explicitly requested entry.
      ancestry.put(path, Pair.of(EntryOrigin.Requested, entry));
      // now scan up the ancestor tree to see if there are any
      // immediately missing entries.
      Path parent = path.getParent();
      while (addAncestors
          && !parent.isRoot() && !ancestry.containsKey(parent)) {
        if (!ancestorState.findEntry(parent, true)) {
          // there is no entry in the ancestor state.
          // look in the store
          DDBPathMetadata md;
          Pair<EntryOrigin, DDBPathMetadata> newEntry;
          final Item item = getConsistentItem(parent);
          if (item != null && !itemToPathMetadata(item, username).isDeleted()) {
            // This is an undeleted entry found in the database.
            // register it in ancestor state and in the map of entries to create
            // as a retrieved entry
            md = itemToPathMetadata(item, username);
            LOG.debug("Found existing entry for parent: {}", md);
            newEntry = Pair.of(EntryOrigin.Retrieved, md);
            // and we break, assuming that if there is an entry, its parents
            // are valid too.
            addAncestors = false;
          } else {
            // A directory entry was not found in the DB. Create one.
            LOG.debug("auto-create ancestor path {} for child path {}",
                parent, path);
            final S3AFileStatus status = makeDirStatus(parent, username);
            md = new DDBPathMetadata(status, Tristate.FALSE,
                false, false, ttlTimeProvider.getNow());
            // declare to be a generated entry
            newEntry =  Pair.of(EntryOrigin.Generated, md);
          }
          // insert into the ancestor state to avoid further checks
          ancestorState.put(parent, md);
          ancestry.put(parent, newEntry);
        }
        parent = parent.getParent();
      }
    }
    // we now have a list of entries which were not in the operation state.
    // Filter out those which were retrieved, to produce a list of those
    // which must be written to the database.
    // TODO sort in reverse order of existence
    return ancestry.values().stream()
        .filter(p -> p.getLeft() != EntryOrigin.Retrieved)
        .map(Pair::getRight)
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   * <p>
   * The implementation scans all up the directory tree and does a get()
   * for each entry; at each level one is found it is added to the ancestor
   * state.
   * <p>
   * The original implementation would stop on finding the first non-empty
   * parent. This (re) implementation issues a GET for every parent entry
   * and so detects and recovers from a tombstone marker further up the tree
   * (i.e. an inconsistent store is corrected for).
   * <p>
   * if {@code operationState} is not null, when this method returns the
   * operation state will be updated with all new entries created.
   * This ensures that subsequent operations with the same store will not
   * trigger new updates.
   * @param qualifiedPath path to update
   * @param operationState (nullable) operational state for a bulk update
   * @throws IOException on failure.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  @Retries.RetryTranslated
  public void addAncestors(final Path qualifiedPath,
      @Nullable final BulkOperationState operationState) throws IOException {

    Collection<DDBPathMetadata> newDirs = new ArrayList<>();
    final AncestorState ancestorState = extractOrCreate(operationState,
        BulkOperationState.OperationType.Put);
    Path parent = qualifiedPath.getParent();
    boolean entryFound = false;

    // Iterate up the parents.
    // note that only ancestorState get/set operations are synchronized;
    // the DDB read between them is not. As a result, more than one
    // thread may probe the state, find the entry missing, do the database
    // query and add the entry.
    // This is done to avoid making the remote dynamo query part of the
    // synchronized block.
    // If a race does occur, the cost is simply one extra GET and potentially
    // one extra PUT.
    while (!parent.isRoot()) {
      synchronized (ancestorState) {
        if (ancestorState.contains(parent)) {
          // the ancestry map contains the key, so no need to even look for it.
          break;
        }
      }
      // we don't worry about tombstone expiry here as expired or not,
      // a directory entry will go in.
      PathMetadata directory = get(parent);
      if (directory == null || directory.isDeleted()) {
        if (entryFound) {
          LOG.warn("Inconsistent S3Guard table: adding directory {}", parent);
        }
        S3AFileStatus status = makeDirStatus(username, parent);
        LOG.debug("Adding new ancestor entry {}", status);
        DDBPathMetadata meta = new DDBPathMetadata(status, Tristate.FALSE,
            false, ttlTimeProvider.getNow());
        newDirs.add(meta);
        // Do not update ancestor state here, as it
        // will happen in the innerPut() call. Were we to add it
        // here that put operation would actually (mistakenly) skip
        // creating the entry.
      } else {
        // an entry was found. Check its type
        entryFound = true;
        if (directory.getFileStatus().isFile()) {
          throw new PathIOException(parent.toString(),
              "Cannot overwrite parent file: metastore is"
                  + " in an inconsistent state");
        }
        // the directory exists. Add it to the ancestor state for next time.
        synchronized (ancestorState) {
          ancestorState.put(parent, new DDBPathMetadata(directory));
        }
      }
      parent = parent.getParent();
    }
    // the listing of directories to put is all those parents which we know
    // are not in the store or BulkOperationState.
    if (!newDirs.isEmpty()) {
      // patch up the time.
      patchLastUpdated(newDirs, ttlTimeProvider);
      innerPut(newDirs, operationState);
    }
  }

  /**
   * {@inheritDoc}.
   *
   * The DDB implementation sorts all the paths such that new items
   * are ordered highest level entry first; deleted items are ordered
   * lowest entry first.
   *
   * This is to ensure that if a client failed partway through the update,
   * there will no entries in the table which lack parent entries.
   * @param pathsToDelete Collection of all paths that were removed from the
   *                      source directory tree of the move.
   * @param pathsToCreate Collection of all PathMetadata for the new paths
   *                      that were created at the destination of the rename
   *                      ().
   * @param operationState Any ongoing state supplied to the rename tracker
   *                      which is to be passed in with each move operation.
   * @throws IOException if there is an error
   */
  @Override
  @Retries.RetryTranslated
  public void move(@Nullable Collection<Path> pathsToDelete,
      @Nullable Collection<PathMetadata> pathsToCreate,
      @Nullable final BulkOperationState operationState) throws IOException {
    if (pathsToDelete == null && pathsToCreate == null) {
      return;
    }

    LOG.debug("Moving paths of table {} in region {}: {} paths to delete and {}"
        + " paths to create", tableName, region,
        pathsToDelete == null ? 0 : pathsToDelete.size(),
        pathsToCreate == null ? 0 : pathsToCreate.size());
    LOG.trace("move: pathsToDelete = {}, pathsToCreate = {}", pathsToDelete,
        pathsToCreate);

    // In DynamoDBMetadataStore implementation, we assume that if a path
    // exists, all its ancestors will also exist in the table.
    // Following code is to maintain this invariant by putting all ancestor
    // directories of the paths to create.
    // ancestor paths that are not explicitly added to paths to create
    AncestorState ancestorState = extractOrCreate(operationState,
        BulkOperationState.OperationType.Rename);
    List<DDBPathMetadata> newItems = new ArrayList<>();
    if (pathsToCreate != null) {
      // create all parent entries.
      // this is synchronized on the move state so that across both serialized
      // and parallelized renames, duplicate ancestor entries are not created.
      synchronized (ancestorState) {
        newItems.addAll(
            completeAncestry(
                pathMetaToDDBPathMeta(pathsToCreate),
                ancestorState));
      }
    }
    // sort all the new items topmost first.
    newItems.sort(PathOrderComparators.TOPMOST_PM_FIRST);

    // now process the deletions.
    if (pathsToDelete != null) {
      List<DDBPathMetadata> tombstones = new ArrayList<>(pathsToDelete.size());
      for (Path meta : pathsToDelete) {
        Preconditions.checkArgument(ttlTimeProvider != null, "ttlTimeProvider"
            + " must not be null");
        final PathMetadata pmTombstone = PathMetadata.tombstone(meta,
            ttlTimeProvider.getNow());
        tombstones.add(new DDBPathMetadata(pmTombstone));
      }
      // sort all the tombstones lowest first.
      tombstones.sort(TOPMOST_PM_LAST);
      newItems.addAll(tombstones);
    }

    processBatchWriteRequest(ancestorState,
        null, pathMetadataToItem(newItems));
  }

  /**
   * Helper method to issue a batch write request to DynamoDB.
   * <ol>
   *   <li>Keys to delete are processed ahead of writing new items.</li>
   *   <li>No attempt is made to sort the input: the caller must do that</li>
   * </ol>
   * As well as retrying on the operation invocation, incomplete
   * batches are retried until all have been processed.
   *
   * @param ancestorState ancestor state for logging
   * @param keysToDelete primary keys to be deleted; can be null
   * @param itemsToPut new items to be put; can be null
   * @return the number of iterations needed to complete the call.
   */
  @Retries.RetryTranslated("Outstanding batch items are updated with backoff")
  private int processBatchWriteRequest(
      @Nullable AncestorState ancestorState,
      PrimaryKey[] keysToDelete,
      Item[] itemsToPut) throws IOException {
    final int totalToDelete = (keysToDelete == null ? 0 : keysToDelete.length);
    final int totalToPut = (itemsToPut == null ? 0 : itemsToPut.length);
    if (totalToPut == 0 && totalToDelete == 0) {
      LOG.debug("Ignoring empty batch write request");
      return 0;
    }
    int count = 0;
    int batches = 0;
    while (count < totalToDelete + totalToPut) {
      final TableWriteItems writeItems = new TableWriteItems(tableName);
      int numToDelete = 0;
      if (keysToDelete != null
          && count < totalToDelete) {
        numToDelete = Math.min(S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT,
            totalToDelete - count);
        PrimaryKey[] toDelete = Arrays.copyOfRange(keysToDelete,
            count, count + numToDelete);
        LOG.debug("Deleting {} entries: {}", toDelete.length, toDelete);
        writeItems.withPrimaryKeysToDelete(toDelete);
        count += numToDelete;
      }

      if (numToDelete < S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT
          && itemsToPut != null
          && count < totalToDelete + totalToPut) {
        final int numToPut = Math.min(
            S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT - numToDelete,
            totalToDelete + totalToPut - count);
        final int index = count - totalToDelete;
        writeItems.withItemsToPut(
            Arrays.copyOfRange(itemsToPut, index, index + numToPut));
        count += numToPut;
      }

      // if there's a retry and another process updates things then it's not
      // quite idempotent, but this was the case anyway
      batches++;
      BatchWriteItemOutcome res = writeOp.retry(
          "batch write",
          "",
          true,
          () -> dynamoDB.batchWriteItem(writeItems));
      // Check for unprocessed keys in case of exceeding provisioned throughput
      Map<String, List<WriteRequest>> unprocessed = res.getUnprocessedItems();
      int retryCount = 0;
      while (!unprocessed.isEmpty()) {
        batchWriteCapacityExceededEvents.incrementAndGet();
        batches++;
        retryBackoffOnBatchWrite(retryCount++);
        // use a different reference to keep the compiler quiet
        final Map<String, List<WriteRequest>> upx = unprocessed;
        res = writeOp.retry(
            "batch write",
            "",
            true,
            () -> dynamoDB.batchWriteItemUnprocessed(upx));
        unprocessed = res.getUnprocessedItems();
      }
    }
    if (itemsToPut != null) {
      recordsWritten(itemsToPut.length);
      logPut(ancestorState, itemsToPut);
    }
    if (keysToDelete != null) {
      recordsDeleted(keysToDelete.length);
      logDelete(ancestorState, keysToDelete);

    }
    return batches;
  }

  /**
   * Put the current thread to sleep to implement exponential backoff
   * depending on retryCount.  If max retries are exceeded, throws an
   * exception instead.
   *
   * @param retryCount number of retries so far
   * @throws IOException when max retryCount is exceeded.
   */
  private void retryBackoffOnBatchWrite(int retryCount) throws IOException {
    try {
      // Our RetryPolicy ignores everything but retryCount here.
      RetryPolicy.RetryAction action = batchWriteRetryPolicy.shouldRetry(
          null,
          retryCount, 0, true);
      if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
        // Create an AWSServiceThrottledException, with a fake inner cause
        // which we fill in to look like a real exception so
        // error messages look sensible
        AmazonServiceException cause = new AmazonServiceException(
            "Throttling");
        cause.setServiceName("S3Guard");
        cause.setStatusCode(AWSServiceThrottledException.STATUS_CODE);
        cause.setErrorCode(THROTTLING);  // used in real AWS errors
        cause.setErrorType(AmazonServiceException.ErrorType.Service);
        cause.setErrorMessage(THROTTLING);
        cause.setRequestId("n/a");
        throw new AWSServiceThrottledException(
            String.format("Max retries during batch write exceeded"
                    + " (%d) for DynamoDB."
                    + HINT_DDB_IOPS_TOO_LOW,
                retryCount),
            cause);
      } else {
        LOG.debug("Sleeping {} msec before next retry", action.delayMillis);
        Thread.sleep(action.delayMillis);
      }
    } catch (InterruptedException e) {
      throw (IOException)new InterruptedIOException(e.toString()).initCause(e);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Unexpected exception " + e, e);
    }
  }

  @Override
  @Retries.RetryTranslated
  public void put(final PathMetadata meta) throws IOException {
    put(meta, null);
  }

  @Override
  @Retries.RetryTranslated
  public void put(
      final PathMetadata meta,
      @Nullable final BulkOperationState operationState) throws IOException {
    // For a deeply nested path, this method will automatically create the full
    // ancestry and save respective item in DynamoDB table.
    // So after put operation, we maintain the invariant that if a path exists,
    // all its ancestors will also exist in the table.
    // For performance purpose, we generate the full paths to put and use batch
    // write item request to save the items.
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);

    Collection<PathMetadata> wrapper = new ArrayList<>(1);
    wrapper.add(meta);
    put(wrapper, operationState);
  }

  @Override
  @Retries.RetryTranslated
  public void put(
      final Collection<? extends PathMetadata> metas,
      @Nullable final BulkOperationState operationState) throws IOException {
    innerPut(pathMetaToDDBPathMeta(metas), operationState);
  }

  /**
   * Internal put operation.
   * <p>
   * The ancestors to all entries are added to the set of entries to write,
   * provided they are not already stored in any supplied operation state.
   * Both the supplied metadata entries and ancestor entries are sorted
   * so that the topmost entries are written first.
   * This is to ensure that a failure partway through the operation will not
   * create entries in the table without parents.
   * @param metas metadata entries to write.
   * @param operationState (nullable) operational state for a bulk update
   * @throws IOException failure.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Retries.RetryTranslated
  private void innerPut(
      final Collection<DDBPathMetadata> metas,
      @Nullable final BulkOperationState operationState) throws IOException {
    if (metas.isEmpty()) {
      // Happens when someone calls put() with an empty list.
      LOG.debug("Ignoring empty list of entries to put");
      return;
    }
    // always create or retrieve an ancestor state instance, so it can
    // always be used for synchronization.
    final AncestorState ancestorState = extractOrCreate(operationState,
        BulkOperationState.OperationType.Put);

    Item[] items;
    synchronized (ancestorState) {
      items = pathMetadataToItem(
          completeAncestry(metas, ancestorState));
    }
    LOG.debug("Saving batch of {} items to table {}, region {}", items.length,
        tableName, region);
    processBatchWriteRequest(ancestorState, null, items);
  }

  /**
   * Get full path of ancestors that are nonexistent in table.
   *
   * This queries DDB when looking for parents which are not in
   * any supplied ongoing operation state.
   * Updates the operation state with found entries to reduce further checks.
   *
   * @param meta metadata to put
   * @param operationState ongoing bulk state
   * @return a possibly empty list of entries to put.
   * @throws IOException failure
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @VisibleForTesting
  @Retries.RetryTranslated
  List<DDBPathMetadata> fullPathsToPut(DDBPathMetadata meta,
      @Nullable BulkOperationState operationState)
      throws IOException {
    checkPathMetadata(meta);
    final List<DDBPathMetadata> metasToPut = new ArrayList<>();
    // root path is not persisted
    if (!meta.getFileStatus().getPath().isRoot()) {
      metasToPut.add(meta);
    }

    // put all its ancestors if not present; as an optimization we return at its
    // first existent ancestor
    final AncestorState ancestorState = extractOrCreate(operationState,
        BulkOperationState.OperationType.Put);
    Path path = meta.getFileStatus().getPath().getParent();
    while (path != null && !path.isRoot()) {
      synchronized (ancestorState) {
        if (ancestorState.findEntry(path, true)) {
          break;
        }
      }
      final Item item = getConsistentItem(path);
      if (!itemExists(item)) {
        final S3AFileStatus status = makeDirStatus(path, username);
        metasToPut.add(new DDBPathMetadata(status, Tristate.FALSE, false,
            meta.isAuthoritativeDir(), meta.getLastUpdated()));
        path = path.getParent();
      } else {
        // found the entry in the table, so add it to the ancestor state
        synchronized (ancestorState) {
          ancestorState.put(path, itemToPathMetadata(item, username));
        }
        // then break out of the loop.
        break;
      }
    }
    return metasToPut;
  }

  /**
   * Does an item represent an object which exists?
   * @param item item retrieved in a query.
   * @return true iff the item isn't null and, if there is an is_deleted
   * column, that its value is false.
   */
  private static boolean itemExists(Item item) {
    if (item == null) {
      return false;
    }
    if (item.hasAttribute(IS_DELETED) &&
        item.getBoolean(IS_DELETED)) {
      return false;
    }
    return true;
  }

  /**
   * Get the value of an optional boolean attribute, falling back to the
   * default value if the attribute is absent.
   * @param item Item
   * @param attrName Attribute name
   * @param defVal Default value
   * @return The value or the default
   */
  private static boolean getBoolAttribute(Item item,
      String attrName,
      boolean defVal) {
    return item.hasAttribute(attrName) ? item.getBoolean(attrName) : defVal;
  }

  /** Create a directory FileStatus using 0 for the lastUpdated time. */
  static S3AFileStatus makeDirStatus(Path f, String owner) {
    return new S3AFileStatus(Tristate.UNKNOWN, f, owner);
  }

  /**
   * {@inheritDoc}.
   * There is retry around building the list of paths to update, but
   * the call to
   * {@link #processBatchWriteRequest(DynamoDBMetadataStore.AncestorState, PrimaryKey[], Item[])}
   * is only tried once.
   * @param meta Directory listing metadata.
   * @param unchangedEntries unchanged child entry paths
   * @param operationState operational state for a bulk update
   * @throws IOException IO problem
   */
  @Override
  @Retries.RetryTranslated
  public void put(
      final DirListingMetadata meta,
      final List<Path> unchangedEntries,
      @Nullable final BulkOperationState operationState) throws IOException {
    LOG.debug("Saving {} dir meta for {} to table {} in region {}: {}",
        meta.isAuthoritative() ? "auth" : "nonauth",
        meta.getPath(),
        tableName, region, meta);
    // directory path
    Path path = meta.getPath();
    DDBPathMetadata ddbPathMeta =
        new DDBPathMetadata(makeDirStatus(path, username), meta.isEmpty(),
            false, meta.isAuthoritative(), meta.getLastUpdated());
    // put all its ancestors if not present
    final AncestorState ancestorState = extractOrCreate(operationState,
        BulkOperationState.OperationType.Put);
    // First add any missing ancestors...
    final List<DDBPathMetadata> metasToPut = fullPathsToPut(ddbPathMeta,
        ancestorState);

    // next add all changed children of the directory
    // ones that came from the previous listing are left as-is
    final Collection<PathMetadata> children = meta.getListing()
        .stream()
        .filter(e -> !unchangedEntries.contains(e.getFileStatus().getPath()))
        .collect(Collectors.toList());

    metasToPut.addAll(pathMetaToDDBPathMeta(children));

    // sort so highest-level entries are written to the store first.
    // if a sequence fails, no orphan entries will have been written.
    metasToPut.sort(PathOrderComparators.TOPMOST_PM_FIRST);
    processBatchWriteRequest(ancestorState,
        null,
        pathMetadataToItem(metasToPut));
    // and add the ancestors
    synchronized (ancestorState) {
      metasToPut.forEach(ancestorState::put);
    }
  }

  @Override
  public synchronized void close() {
    instrumentation.storeClosed();
    try {
      if (dynamoDB != null) {
        LOG.debug("Shutting down {}", this);
        dynamoDB.shutdown();
        dynamoDB = null;
      }
    } finally {
      closeAutocloseables(LOG, credentials);
      credentials = null;
    }
  }

  @Override
  @Retries.RetryTranslated
  public void destroy() throws IOException {
    tableHandler.destroy();
  }

  @Retries.RetryTranslated
  private ItemCollection<ScanOutcome> expiredFiles(PruneMode pruneMode,
      long cutoff, String keyPrefix) throws IOException {

    String filterExpression;
    String projectionExpression;
    ValueMap map;

    switch (pruneMode) {
    case ALL_BY_MODTIME:
      // filter all files under the given parent older than the modtime.
      // this implicitly skips directories, because they lack a modtime field.
      // however we explicitly exclude directories to make clear that
      // directories are to be excluded and avoid any confusion
      // see: HADOOP-16725.
      // note: files lack the is_dir field entirely, so we use a `not` to
      // filter out the directories.
      filterExpression =
          "mod_time < :mod_time and begins_with(parent, :parent)"
              + " and not is_dir = :is_dir";
      projectionExpression = "parent,child";
      map = new ValueMap()
          .withLong(":mod_time", cutoff)
          .withString(":parent", keyPrefix)
          .withBoolean(":is_dir", true);
      break;
    case TOMBSTONES_BY_LASTUPDATED:
      filterExpression =
          "last_updated < :last_updated and begins_with(parent, :parent) "
              + "and is_deleted = :is_deleted";
      projectionExpression = "parent,child,is_deleted";
      map = new ValueMap()
          .withLong(":last_updated", cutoff)
          .withString(":parent", keyPrefix)
          .withBoolean(":is_deleted", true);
      break;
    default:
      throw new UnsupportedOperationException("Unsupported prune mode: "
          + pruneMode);
    }

    return readOp.retry(
        "scan",
        keyPrefix,
        true,
        () -> table.scan(filterExpression, projectionExpression, null, map));
  }

  @Override
  @Retries.RetryTranslated
  public void prune(PruneMode pruneMode, long cutoff) throws IOException {
    prune(pruneMode, cutoff, "/");
  }

  /**
   * Prune files, in batches. There's optionally a sleep between each batch.
   *
   * @param pruneMode The mode of operation for the prune For details see
   *                  {@link MetadataStore#prune(PruneMode, long)}
   * @param cutoff Oldest modification time to allow
   * @param keyPrefix The prefix for the keys that should be removed
   * @throws IOException Any IO/DDB failure.
   * @throws InterruptedIOException if the prune was interrupted
   * @return count of pruned items.
   */
  @Override
  @Retries.RetryTranslated
  public long prune(PruneMode pruneMode, long cutoff, String keyPrefix)
      throws IOException {
    LOG.debug("Prune {} under {} with age {}",
        pruneMode == PruneMode.ALL_BY_MODTIME
            ? "files and tombstones" : "tombstones",
        keyPrefix, cutoff);
    final ItemCollection<ScanOutcome> items =
        expiredFiles(pruneMode, cutoff, keyPrefix);
    return innerPrune(pruneMode, cutoff, keyPrefix, items);
  }

  /**
   * Prune files, in batches. There's optionally a sleep between each batch.
   *
   * @param pruneMode The mode of operation for the prune For details see
   *                  {@link MetadataStore#prune(PruneMode, long)}
   * @param cutoff Oldest modification time to allow
   * @param keyPrefix The prefix for the keys that should be removed
   * @param items expired items
   * @return count of pruned items.
   * @throws IOException Any IO/DDB failure.
   * @throws InterruptedIOException if the prune was interrupted
   */
  private int innerPrune(
      final PruneMode pruneMode, final long cutoff, final String keyPrefix,
      final ItemCollection<ScanOutcome> items)
      throws IOException {
    int itemCount = 0;
    try (AncestorState state = initiateBulkWrite(
        BulkOperationState.OperationType.Prune, null);
         DurationInfo ignored =
             new DurationInfo(LOG, "Pruning DynamoDB Store")) {
      ArrayList<Path> deletionBatch =
          new ArrayList<>(S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT);
      long delay = conf.getTimeDuration(
          S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
          S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT,
          TimeUnit.MILLISECONDS);
      Set<Path> parentPathSet = new HashSet<>();
      Set<Path> clearedParentPathSet = new HashSet<>();
      // declare the operation to delete a batch as a function so
      // as to keep the code consistent across multiple uses.
      CallableRaisingIOE<Void> deleteBatchOperation =
          () -> {
            // lowest path entries get deleted first.
            deletionBatch.sort(PathOrderComparators.TOPMOST_PATH_LAST);
            processBatchWriteRequest(state, pathToKey(deletionBatch), null);

            // set authoritative false for each pruned dir listing
            // if at least one entry was not a tombstone
            removeAuthoritativeDirFlag(parentPathSet, state);
            // already cleared parent paths.
            clearedParentPathSet.addAll(parentPathSet);
            parentPathSet.clear();
            return null;
          };
      for (Item item : items) {
        DDBPathMetadata md = PathMetadataDynamoDBTranslation
            .itemToPathMetadata(item, username);
        Path path = md.getFileStatus().getPath();
        boolean tombstone = md.isDeleted();
        LOG.debug("Prune entry {}", path);
        deletionBatch.add(path);

        // add parent path of item so it can be marked as non-auth.
        // this is only done if
        // * it has not already been processed
        // * the entry pruned is not a tombstone (no need to update)
        // * the file is not in the root dir
        Path parentPath = path.getParent();
        if (!tombstone
            && parentPath != null
            && !parentPath.isRoot()
            && !clearedParentPathSet.contains(parentPath)) {
          parentPathSet.add(parentPath);
        }

        itemCount++;
        if (deletionBatch.size() == S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT) {
          deleteBatchOperation.apply();
          deletionBatch.clear();
          if (delay > 0) {
            Thread.sleep(delay);
          }
        }
      }
      // final batch of deletes
      if (!deletionBatch.isEmpty()) {
        deleteBatchOperation.apply();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Pruning was interrupted");
    } catch (AmazonDynamoDBException e) {
      throw translateDynamoDBException(keyPrefix,
          "Prune of " + keyPrefix + " failed", e);
    }
    LOG.info("Finished pruning {} items in batches of {}", itemCount,
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT);
    return itemCount;
  }

  /**
   * Remove the Authoritative Directory Marker from a set of paths, if
   * those paths are in the store.
   * <p>
   * This operation is <i>only</i>for pruning; it does not raise an error
   * if, during the prune phase, the table appears inconsistent.
   * This is not unusual as it can happen in a number of ways
   * <ol>
   *   <li>The state of the table changes during a slow prune operation which
   *   deliberately inserts pauses to avoid overloading prepaid IO capacity.
   *   </li>
   *   <li>Tombstone markers have been left in the table after many other
   *   operations have taken place, including deleting/replacing
   *   parents.</li>
   * </ol>
   * <p>
   *
   * If an exception is raised in the get/update process, then the exception
   * is caught and only rethrown after all the other paths are processed.
   * This is to ensure a best-effort attempt to update the store.
   * @param pathSet set of paths.
   * @param state ongoing operation state.
   * @throws IOException only after a best effort is made to update the store.
   */
  private void removeAuthoritativeDirFlag(
      final Set<Path> pathSet,
      final AncestorState state) throws IOException {

    AtomicReference<IOException> rIOException = new AtomicReference<>();

    Set<DDBPathMetadata> metas = pathSet.stream().map(path -> {
      try {
        if (path.isRoot()) {
          LOG.debug("ignoring root path");
          return null;
        }
        if (state != null && state.get(path) != null) {
          // there's already an entry for this path
          LOG.debug("Ignoring update of entry already in the state map");
          return null;
        }
        DDBPathMetadata ddbPathMetadata = get(path);
        if (ddbPathMetadata == null) {
          // there is no entry.
          LOG.debug("No parent {}; skipping", path);
          return null;
        }
        if (ddbPathMetadata.isDeleted()) {
          // the parent itself is deleted
          LOG.debug("Parent has been deleted {}; skipping", path);
          return null;
        }
        if (!ddbPathMetadata.getFileStatus().isDirectory()) {
          // the parent itself is deleted
          LOG.debug("Parent is not a directory {}; skipping", path);
          return null;
        }
        LOG.debug("Setting isAuthoritativeDir==false on {}", ddbPathMetadata);
        ddbPathMetadata.setAuthoritativeDir(false);
        ddbPathMetadata.setLastUpdated(ttlTimeProvider.getNow());
        return ddbPathMetadata;
      } catch (IOException e) {
        String msg = String.format("IOException while getting PathMetadata "
            + "on path: %s.", path);
        LOG.error(msg, e);
        rIOException.set(e);
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toSet());

    try {
      LOG.debug("innerPut on metas: {}", metas);
      if (!metas.isEmpty()) {
        innerPut(metas, state);
      }
    } catch (IOException e) {
      String msg = String.format("IOException while setting false "
          + "authoritative directory flag on: %s.", metas);
      LOG.error(msg, e);
      rIOException.set(e);
    }

    if (rIOException.get() != null) {
      throw rIOException.get();
    }
  }

  @VisibleForTesting
  public AmazonDynamoDB getAmazonDynamoDB() {
    return amazonDynamoDB;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + '{'
        + "region=" + region
        + ", tableName=" + tableName
        + ", tableArn=" + tableHandler.getTableArn()
        + '}';
  }

  /**
   * The administrative policy includes all DDB table operations;
   * application access is restricted to those operations S3Guard operations
   * require when working with data in a guarded bucket.
   * @param access access level desired.
   * @return a possibly empty list of statements.
   */
  @Override
  public List<RoleModel.Statement> listAWSPolicyRules(
      final Set<AccessLevel> access) {
    Preconditions.checkState(tableHandler.getTableArn() != null,
        "TableARN not known");
    if (access.isEmpty()) {
      return Collections.emptyList();
    }
    RoleModel.Statement stat;
    if (access.contains(AccessLevel.ADMIN)) {
      stat = allowAllDynamoDBOperations(tableHandler.getTableArn());
    } else {
      stat = allowS3GuardClientOperations(tableHandler.getTableArn());
    }
    return Lists.newArrayList(stat);
  }

  /**
   * PUT a single item to the table.
   * @param item item to put
   * @return the outcome.
   */
  @Retries.OnceRaw
  private PutItemOutcome putItem(Item item) {
    LOG.debug("Putting item {}", item);
    return table.putItem(item);
  }

  @VisibleForTesting
  Table getTable() {
    return table;
  }

  String getRegion() {
    return region;
  }

  @VisibleForTesting
  public String getTableName() {
    return tableName;
  }

  @VisibleForTesting
  DynamoDB getDynamoDB() {
    return dynamoDB;
  }

  /**
   * Validates a path object; it must be absolute, have an s3a:/// scheme
   * and contain a host (bucket) component.
   * @param path path to check
   * @return the path passed in
   */
  private Path checkPath(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(path.isAbsolute(), "Path %s is not absolute",
        path);
    URI uri = path.toUri();
    Preconditions.checkNotNull(uri.getScheme(), "Path %s missing scheme", path);
    Preconditions.checkArgument(uri.getScheme().equals(Constants.FS_S3A),
        "Path %s scheme must be %s", path, Constants.FS_S3A);
    Preconditions.checkArgument(!StringUtils.isEmpty(uri.getHost()), "Path %s" +
        " is missing bucket.", path);
    return path;
  }

  /**
   * Validates a path meta-data object.
   */
  private static void checkPathMetadata(PathMetadata meta) {
    Preconditions.checkNotNull(meta);
    Preconditions.checkNotNull(meta.getFileStatus());
    Preconditions.checkNotNull(meta.getFileStatus().getPath());
  }

  @Override
  @Retries.OnceRaw
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> map = new TreeMap<>();
    if (table != null) {
      TableDescription desc = getTableDescription(true);
      map.put("name", desc.getTableName());
      map.put(STATUS, desc.getTableStatus());
      map.put("ARN", desc.getTableArn());
      map.put("size", desc.getTableSizeBytes().toString());
      map.put(TABLE, desc.toString());
      ProvisionedThroughputDescription throughput
          = desc.getProvisionedThroughput();
      map.put(READ_CAPACITY, throughput.getReadCapacityUnits().toString());
      map.put(WRITE_CAPACITY, throughput.getWriteCapacityUnits().toString());
      map.put(BILLING_MODE,
          throughput.getWriteCapacityUnits() == 0
              ? BILLING_MODE_PER_REQUEST
              : BILLING_MODE_PROVISIONED);
      map.put("sse", desc.getSSEDescription() == null
          ? "DISABLED"
          : desc.getSSEDescription().toString());
      map.put(MetadataStoreCapabilities.PERSISTS_AUTHORITATIVE_BIT,
          Boolean.toString(true));
    } else {
      map.put("name", "DynamoDB Metadata Store");
      map.put(TABLE, "none");
      map.put(STATUS, "undefined");
    }
    map.put("description", DESCRIPTION);
    map.put("region", region);
    if (batchWriteRetryPolicy != null) {
      map.put("retryPolicy", batchWriteRetryPolicy.toString());
    }
    return map;
  }

  @Retries.OnceRaw
  private TableDescription getTableDescription(boolean forceUpdate) {
    TableDescription desc = table.getDescription();
    if (desc == null || forceUpdate) {
      desc = table.describe();
    }
    return desc;
  }

  @Override
  @Retries.OnceRaw
  public void updateParameters(Map<String, String> parameters)
      throws IOException {
    Preconditions.checkNotNull(table, "Not initialized");
    TableDescription desc = getTableDescription(true);
    ProvisionedThroughputDescription current
        = desc.getProvisionedThroughput();

    long currentRead = current.getReadCapacityUnits();
    long newRead = getLongParam(parameters,
        S3GUARD_DDB_TABLE_CAPACITY_READ_KEY,
        currentRead);
    long currentWrite = current.getWriteCapacityUnits();
    long newWrite = getLongParam(parameters,
            S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY,
            currentWrite);

    if (currentRead == 0 || currentWrite == 0) {
      // table is pay on demand
      throw new IOException(E_ON_DEMAND_NO_SET_CAPACITY);
    }

    if (newRead != currentRead || newWrite != currentWrite) {
      LOG.info("Current table capacity is read: {}, write: {}",
          currentRead, currentWrite);
      LOG.info("Changing capacity of table to read: {}, write: {}",
          newRead, newWrite);
      tableHandler.provisionTableBlocking(newRead, newWrite);
    } else {
      LOG.info("Table capacity unchanged at read: {}, write: {}",
          newRead, newWrite);
    }
  }

  private long getLongParam(Map<String, String> parameters,
      String key,
      long defVal) {
    String k = parameters.get(key);
    if (k != null) {
      return Long.parseLong(k);
    } else {
      return defVal;
    }
  }

  /**
   * Callback on a read operation retried.
   * @param text text of the operation
   * @param ex exception
   * @param attempts number of attempts
   * @param idempotent is the method idempotent (this is assumed to be true)
   */
  void readRetryEvent(
      String text,
      IOException ex,
      int attempts,
      boolean idempotent) {
    readThrottleEvents.incrementAndGet();
    retryEvent(text, ex, attempts, true);
  }

  /**
   * Callback  on a write operation retried.
   * @param text text of the operation
   * @param ex exception
   * @param attempts number of attempts
   * @param idempotent is the method idempotent (this is assumed to be true)
   */
  void writeRetryEvent(
      String text,
      IOException ex,
      int attempts,
      boolean idempotent) {
    writeThrottleEvents.incrementAndGet();
    retryEvent(text, ex, attempts, idempotent);
  }

  /**
   * Callback on a scan operation retried.
   * @param text text of the operation
   * @param ex exception
   * @param attempts number of attempts
   * @param idempotent is the method idempotent (this is assumed to be true)
   */
  void scanRetryEvent(
      String text,
      IOException ex,
      int attempts,
      boolean idempotent) {
    scanThrottleEvents.incrementAndGet();
    retryEvent(text, ex, attempts, idempotent);
  }

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param attempts number of attempts
   * @param idempotent is the method idempotent
   */
  void retryEvent(
      String text,
      IOException ex,
      int attempts,
      boolean idempotent) {
    if (S3AUtils.isThrottleException(ex)) {
      // throttled
      instrumentation.throttled();
      int eventCount = throttleEventCount.addAndGet(1);
      if (attempts == 1 && eventCount < THROTTLE_EVENT_LOG_LIMIT) {
        LOG.warn("DynamoDB IO limits reached in {};"
                + " consider increasing capacity: {}", text, ex.toString());
        LOG.debug("Throttled", ex);
      } else {
        // user has been warned already, log at debug only.
        LOG.debug("DynamoDB IO limits reached in {};"
                + " consider increasing capacity: {}", text, ex.toString());
      }
    } else if (attempts == 1) {
      // not throttled. Log on the first attempt only
      LOG.info("Retrying {}: {}", text, ex.toString());
      LOG.debug("Retrying {}", text, ex);
    }

    // note a retry
    instrumentation.retrying();
    if (owner != null) {
      owner.metastoreOperationRetried(ex, attempts, idempotent);
    }
  }

  /**
   * Get the count of read throttle events.
   * @return the current count of read throttle events.
   */
  @VisibleForTesting
  public long getReadThrottleEventCount() {
    return readThrottleEvents.get();
  }

  /**
   * Get the count of write throttle events.
   * @return the current count of write throttle events.
   */
  @VisibleForTesting
  public long getWriteThrottleEventCount() {
    return writeThrottleEvents.get();
  }

  /**
   * Get the count of scan throttle events.
   * @return the current count of scan throttle events.
   */
  @VisibleForTesting
  public long getScanThrottleEventCount() {
    return scanThrottleEvents.get();
  }

  @VisibleForTesting
  public long getBatchWriteCapacityExceededCount() {
    return batchWriteCapacityExceededEvents.get();
  }

  /**
   * Get the operation invoker for write operations.
   * @return an invoker for retrying mutating operations on a store.
   */
  public Invoker getInvoker() {
    return writeOp;
  }

  /**
   * Wrap an iterator returned from any scan with a retrying one.
   * This includes throttle handling.
   * Retries will update the relevant counters/metrics for scan operations.
   * @param source source iterator
   * @return a retrying iterator.
   */
  public <T> Iterable<T> wrapWithRetries(
      final Iterable<T> source) {
    return new RetryingCollection<>("scan dynamoDB table", scanOp, source);
  }

  /**
   * Record the number of records written.
   * @param count count of records.
   */
  private void recordsWritten(final int count) {
    instrumentation.recordsWritten(count);
  }

  /**
   * Record the number of records read.
   * @param count count of records.
   */
  private void recordsRead(final int count) {
    instrumentation.recordsRead(count);
  }
  /**
   * Record the number of records deleted.
   * @param count count of records.
   */
  private void recordsDeleted(final int count) {
    instrumentation.recordsDeleted(count);
  }

  /**
   * Initiate the rename operation by creating the tracker for the filesystem
   * to keep up to date with state changes in the S3A bucket.
   * @param storeContext store context.
   * @param source source path
   * @param sourceStatus status of the source file/dir
   * @param dest destination path.
   * @return the rename tracker
   */
  @Override
  public RenameTracker initiateRenameOperation(
      final StoreContext storeContext,
      final Path source,
      final S3AFileStatus sourceStatus,
      final Path dest) {
    return new ProgressiveRenameTracker(storeContext, this, source, dest,
        new AncestorState(this, BulkOperationState.OperationType.Rename, dest));
  }

  /**
   * Mark the directories instantiated under the destination path
   * as authoritative. That is: all entries in the
   * operationState (which must be an AncestorState instance),
   * that are under the destination path.
   *
   * The database update synchronized on the operationState, so all other
   * threads trying to update that state will be blocked until completion.
   *
   * This operation is only used in import and at the end of a rename,
   * so this is not considered an issue.
   * @param dest destination path.
   * @param operationState active state.
   * @throws IOException failure.
   * @return the number of directories marked.
   */
  @Override
  public int markAsAuthoritative(
      final Path dest,
      final BulkOperationState operationState) throws IOException {
    if (operationState == null) {
      return 0;
    }
    Preconditions.checkArgument(operationState instanceof AncestorState,
        "Not an AncestorState %s", operationState);
    final AncestorState state = (AncestorState)operationState;
    // only mark paths under the dest as auth
    final String simpleDestKey = pathToParentKey(dest);
    final String destPathKey = simpleDestKey + "/";
    final String opId = AncestorState.stateAsString(state);
    LOG.debug("{}: marking directories under {} as authoritative",
        opId, destPathKey);

    // the list of dirs to build up.
    final List<DDBPathMetadata> dirsToUpdate = new ArrayList<>();
    synchronized (state) {
      for (Map.Entry<Path, DDBPathMetadata> entry :
          state.getAncestry().entrySet()) {
        final Path path = entry.getKey();
        final DDBPathMetadata md = entry.getValue();
        final String key = pathToParentKey(path);
        if (md.getFileStatus().isDirectory()
            && (key.equals(simpleDestKey) || key.startsWith(destPathKey))) {
          // the updated entry is under the destination.
          md.setAuthoritativeDir(true);
          md.setLastUpdated(ttlTimeProvider.getNow());
          LOG.debug("{}: added {}", opId, key);
          dirsToUpdate.add(md);
        }
      }
      processBatchWriteRequest(state,
          null, pathMetadataToItem(dirsToUpdate));
    }
    return dirsToUpdate.size();
  }

  @Override
  public AncestorState initiateBulkWrite(
      final BulkOperationState.OperationType operation,
      final Path dest) {
    return new AncestorState(this, operation, dest);
  }

  @Override
  public void setTtlTimeProvider(ITtlTimeProvider ttlTimeProvider) {
    this.ttlTimeProvider = ttlTimeProvider;
  }

  /**
   * Username.
   * @return the current username
   */
  String getUsername() {
    return username;
  }

  /**
   * Log a PUT into the operations log at debug level.
   * @param state optional ancestor state.
   * @param items items which have been PUT
   */
  private static void logPut(
      @Nullable AncestorState state,
      Item[] items) {
    if (OPERATIONS_LOG.isDebugEnabled()) {
      // log the operations
      String stateStr = AncestorState.stateAsString(state);
      for (Item item : items) {
        boolean tombstone = !itemExists(item);
        boolean isDir = getBoolAttribute(item, IS_DIR, false);
        boolean auth = getBoolAttribute(item, IS_AUTHORITATIVE, false);
        OPERATIONS_LOG.debug("{} {} {}{}{}",
            stateStr,
            tombstone ? "TOMBSTONE" : "PUT",
            itemPrimaryKeyToString(item),
            auth ? " [auth]" : "",
            isDir ? " directory" : "");
      }
    }
  }

  /**
   * Log a PUT into the operations log at debug level.
   * @param state optional ancestor state.
   * @param item item PUT.
   */
  private static void logPut(
      @Nullable AncestorState state,
      Item item) {
    if (OPERATIONS_LOG.isDebugEnabled()) {
      // log the operations
      logPut(state, new Item[]{item});
    }
  }

  /**
   * Log a DELETE into the operations log at debug level.
   * @param state optional ancestor state.
   * @param keysDeleted keys which were deleted.
   */
  private static void logDelete(
      @Nullable AncestorState state,
      PrimaryKey[] keysDeleted) {
    if (OPERATIONS_LOG.isDebugEnabled()) {
      // log the operations
      String stateStr = AncestorState.stateAsString(state);
      for (PrimaryKey key : keysDeleted) {
        OPERATIONS_LOG.debug("{} DELETE {}",
            stateStr, primaryKeyToString(key));
      }
    }
  }

  /**
   * Log a DELETE into the operations log at debug level.
   * @param state optional ancestor state.
   * @param key Deleted key
   */
  private static void logDelete(
      @Nullable AncestorState state,
      PrimaryKey key) {
    if (OPERATIONS_LOG.isDebugEnabled()) {
      logDelete(state, new PrimaryKey[]{key});
    }
  }

  /**
   * Get the move state passed in; create a new one if needed.
   * @param state state.
   * @param operation the type of the operation to use if the state is created.
   * @return the cast or created state.
   */
  private AncestorState extractOrCreate(@Nullable BulkOperationState state,
      BulkOperationState.OperationType operation) {
    if (state != null) {
      return (AncestorState) state;
    } else {
      return new AncestorState(this, operation, null);
    }
  }

  @Override
  public MetastoreInstrumentation getInstrumentation() {
    return instrumentation;
  }

  /**
   * This tracks all the ancestors created,
   * across multiple move/write operations.
   * This is to avoid duplicate creation of ancestors during bulk commits
   * and rename operations managed by a rename tracker.
   *
   * There is no thread safety: callers must synchronize as appropriate.
   */
  @VisibleForTesting
  static final class AncestorState extends BulkOperationState {

    /**
     * Counter of IDs issued.
     */
    private static final AtomicLong ID_COUNTER = new AtomicLong(0);

    /** Owning store. */
    private final DynamoDBMetadataStore store;

    /** The ID of the state; for logging. */
    private final long id;

    /**
     * Map of ancestors.
     */
    private final Map<Path, DDBPathMetadata> ancestry = new HashMap<>();

    /**
     * Destination path.
     */
    private final Path dest;

    /**
     * Create the state.
     * @param store the store, for use in validation.
     * If null: no validation (test only operation)
     * @param operation the type of the operation.
     * @param dest destination path.
     */
    AncestorState(
        @Nullable final DynamoDBMetadataStore store,
        final OperationType operation,
        @Nullable final Path dest) {
      super(operation);
      this.store = store;
      this.dest = dest;
      this.id = ID_COUNTER.addAndGet(1);
    }

    int size() {
      return ancestry.size();
    }

    /**
     * Get the ancestry. Not thread safe.
     * @return the map of ancestors.
     */
    Map<Path, DDBPathMetadata> getAncestry() {
      return ancestry;
    }

    public Path getDest() {
      return dest;
    }

    long getId() {
      return id;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "AncestorState{");
      sb.append("operation=").append(getOperation());
      sb.append("id=").append(id);
      sb.append("; dest=").append(dest);
      sb.append("; size=").append(size());
      sb.append("; paths={")
          .append(StringUtils.join(ancestry.keySet(), " "))
          .append('}');
      sb.append('}');
      return sb.toString();
    }

    /**
     * Does the ancestor state contain a path?
     * @param p path to check
     * @return true if the state has an entry
     */
    boolean contains(Path p) {
      return get(p) != null;
    }

    DDBPathMetadata put(Path p, DDBPathMetadata md) {
      return ancestry.put(p, md);
    }

    DDBPathMetadata put(DDBPathMetadata md) {
      return ancestry.put(md.getFileStatus().getPath(), md);
    }

    DDBPathMetadata get(Path p) {
      return ancestry.get(p);
    }

    /**
     * Find an entry in the ancestor state, warning and optionally
     * raising an exception if there is a file at the path.
     * @param path path to look up
     * @param failOnFile fail if a file was found.
     * @return true iff a directory was found in the ancestor state.
     * @throws PathIOException if there was a file at the path.
     */
    boolean findEntry(
        final Path path,
        final boolean failOnFile) throws PathIOException {
      final DDBPathMetadata ancestor = get(path);
      if (ancestor != null) {
        // there's an entry in the ancestor state
        if (!ancestor.getFileStatus().isDirectory()) {
          // but: its a file, which means this update is now inconsistent.
          final String message = E_INCONSISTENT_UPDATE + " entry is " + ancestor
              .getFileStatus();
          LOG.error(message);
          if (failOnFile) {
            // errors trigger failure
            throw new PathIOException(path.toString(), message);
          }
        }
        return true;
      } else {
        return false;
      }
    }

    /**
     * If debug logging is enabled, this does an audit of the store state.
     * it only logs this; the error messages are created so as they could
     * be turned into exception messages.
     * Audit failures aren't being turned into IOEs is that
     * rename operations delete the source entry and that ends up in the
     * ancestor state as present
     * @throws IOException failure
     */
    @Override
    public void close() throws IOException {
      if (LOG.isDebugEnabled() && store != null) {
        LOG.debug("Auditing {}", stateAsString(this));
        for (Map.Entry<Path, DDBPathMetadata> entry : ancestry
            .entrySet()) {
          Path path = entry.getKey();
          DDBPathMetadata expected = entry.getValue();
          if (expected.isDeleted()) {
            // file was deleted in bulk op; we don't care about it
            // any more
            continue;
          }
          DDBPathMetadata actual;
          try {
            actual = store.get(path);
          } catch (IOException e) {
            LOG.debug("Retrieving {}", path, e);
            // this is for debug; don't be ambitious
            return;
          }
          if (actual == null || actual.isDeleted()) {
            String message = "Metastore entry for path "
                + path + " deleted during bulk "
                + getOperation() + " operation";
            LOG.debug(message);
          } else {
            if (actual.getFileStatus().isDirectory() !=
                expected.getFileStatus().isDirectory()) {
              // the type of the entry has changed
              String message = "Metastore entry for path "
                  + path + " changed during bulk "
                  + getOperation() + " operation"
                  + " from " + expected
                  + " to " + actual;
              LOG.debug(message);
            }
          }

        }
      }
    }

    /**
     * Create a string from the state including operation and ID.
     * @param state state to use -may be null
     * @return a string for logging.
     */
    private static String stateAsString(@Nullable AncestorState state) {
      String stateStr;
      if (state != null) {
        stateStr = String.format("#(%s-%04d)",
            state.getOperation(),
            state.getId());
      } else {
        stateStr = "#()";
      }
      return stateStr;
    }
  }

  protected DynamoDBMetadataStoreTableManager getTableHandler() {
    Preconditions.checkNotNull(tableHandler, "Not initialized");
    return tableHandler;
  }
}
