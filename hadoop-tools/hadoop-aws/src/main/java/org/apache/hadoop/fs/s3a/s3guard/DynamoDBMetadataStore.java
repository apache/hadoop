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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
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
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.AWSServiceThrottledException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.auth.RolePolicies;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.*;
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
 * =========================================================================
 * | parent                 | child | is_dir | mod_time | len |     ...    |
 * =========================================================================
 * | /bucket                | dir1  | true   |          |     |            |
 * | /bucket/dir1           | dir2  | true   |          |     |            |
 * | /bucket/dir1           | dir3  | true   |          |     |            |
 * | /bucket/dir1/dir2      | file1 |        |   100    | 111 |            |
 * | /bucket/dir1/dir2      | file2 |        |   200    | 222 |            |
 * | /bucket/dir1/dir3      | dir4  | true   |          |     |            |
 * | /bucket/dir1/dir3      | dir5  | true   |          |     |            |
 * | /bucket/dir1/dir3/dir4 | file3 |        |   300    | 333 |            |
 * | /bucket/dir1/dir3/dir5 | file4 |        |   400    | 444 |            |
 * | /bucket/dir1/dir3      | dir6  | true   |          |     |            |
 * =========================================================================
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
 * Some mutating operations, notably {@link #deleteSubtree(Path)} and
 * {@link #move(Collection, Collection)}, are less efficient with this schema.
 * They require mutating multiple items in the DynamoDB table.
 *
 * By default, DynamoDB access is performed within the same AWS region as
 * the S3 bucket that hosts the S3A instance.  During initialization, it checks
 * the location of the S3 bucket and creates a DynamoDB client connected to the
 * same region. The region may also be set explicitly by setting the config
 * parameter {@code fs.s3a.s3guard.ddb.region} to the corresponding region.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DynamoDBMetadataStore implements MetadataStore {
  public static final Logger LOG = LoggerFactory.getLogger(
      DynamoDBMetadataStore.class);

  /** parent/child name to use in the version marker. */
  public static final String VERSION_MARKER = "../VERSION";

  /** Current version number. */
  public static final int VERSION = 100;

  /** Error: version marker not found in table. */
  public static final String E_NO_VERSION_MARKER
      = "S3Guard table lacks version marker.";

  /** Error: version mismatch. */
  public static final String E_INCOMPATIBLE_VERSION
      = "Database table is from an incompatible S3Guard version.";

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

  private static ValueMap deleteTrackingValueMap =
      new ValueMap().withBoolean(":false", false);

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
   * It also has a role purpose in {@link #getVersionMarkerItem()};
   * look at that method for the details.
   */
  private RetryPolicy batchWriteRetryPolicy;

  private S3AInstrumentation.S3GuardInstrumentation instrumentation;

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

  private final AtomicLong readThrottleEvents = new AtomicLong(0);
  private final AtomicLong writeThrottleEvents = new AtomicLong(0);
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
   * @throws IOException on a failure
   */
  @Override
  @Retries.OnceRaw
  public void initialize(FileSystem fs) throws IOException {
    Preconditions.checkNotNull(fs, "Null filesystem");
    Preconditions.checkArgument(fs instanceof S3AFileSystem,
        "DynamoDBMetadataStore only supports S3A filesystem.");
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
        LOG.error("Failed to get bucket location from S3 bucket {}",
            uri);
        throw (IOException)new AccessDeniedException(
            "S3 client role lacks permission "
                + RolePolicies.S3_GET_BUCKET_LOCATION + " for " + uri)
            .initCause(e);
      }
      LOG.debug("Inferring DynamoDB region from S3 bucket: {}", region);
    }
    credentials = owner.shareCredentials("s3guard");
    dynamoDB = createDynamoDB(conf, region, bucket, credentials);

    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY, bucket);
    initDataAccessRetries(conf);

    // set up a full retry policy
    invoker = new Invoker(new S3GuardDataAccessRetryPolicy(conf),
        this::retryEvent
    );

    initTable();

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
    instrumentation = owner.getInstrumentation().getS3GuardInstrumentation();
    username = owner.getUsername();
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
   * Generally, callers should use {@link #initialize(FileSystem)}
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
   * @see #initialize(FileSystem)
   * @throws IOException if there is an error
   * @throws IllegalArgumentException if the configuration is incomplete
   */
  @Override
  @Retries.OnceRaw
  public void initialize(Configuration config) throws IOException {
    conf = config;
    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY);
    Preconditions.checkArgument(!StringUtils.isEmpty(tableName),
        "No DynamoDB table name configured");
    region = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
    Preconditions.checkArgument(!StringUtils.isEmpty(region),
        "No DynamoDB region configured");
    credentials = createAWSCredentialProviderSet(null, conf);
    dynamoDB = createDynamoDB(conf, region, null, credentials);

    username = UserGroupInformation.getCurrentUser().getShortUserName();
    initDataAccessRetries(conf);

    initTable();
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
  }

  @Override
  @Retries.RetryTranslated
  public void delete(Path path) throws IOException {
    innerDelete(path, true);
  }

  @Override
  @Retries.RetryTranslated
  public void forgetMetadata(Path path) throws IOException {
    innerDelete(path, false);
  }

  /**
   * Inner delete option, action based on the {@code tombstone} flag.
   * No tombstone: delete the entry. Tombstone: create a tombstone entry.
   * There is no check as to whether the entry exists in the table first.
   * @param path path to delete
   * @param tombstone flag to create a tombstone marker
   * @throws IOException I/O error.
   */
  @Retries.RetryTranslated
  private void innerDelete(final Path path, boolean tombstone)
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
      Item item = PathMetadataDynamoDBTranslation.pathMetadataToItem(
          new DDBPathMetadata(PathMetadata.tombstone(path)));
      writeOp.retry(
          "Put tombstone",
          path.toString(),
          idempotent,
          () -> table.putItem(item));
    } else {
      PrimaryKey key = pathToKey(path);
      writeOp.retry(
          "Delete key",
          path.toString(),
          idempotent,
          () -> table.deleteItem(key));
    }
  }

  @Override
  @Retries.RetryTranslated
  public void deleteSubtree(Path path) throws IOException {
    checkPath(path);
    LOG.debug("Deleting subtree from table {} in region {}: {}",
        tableName, region, path);

    final PathMetadata meta = get(path);
    if (meta == null || meta.isDeleted()) {
      LOG.debug("Subtree path {} does not exist; this will be a no-op", path);
      return;
    }

    for (DescendantsIterator desc = new DescendantsIterator(this, meta);
         desc.hasNext();) {
      innerDelete(desc.next().getPath(), true);
    }
  }

  /**
   * Get a consistent view of an item.
   * @param path path to look up in the database
   * @param path entry
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
        () -> table.getItem(spec));
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
    LOG.debug("Get from table {} in region {}: {}", tableName, region, path);
    return innerGet(path, wantEmptyDirectoryFlag);
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

    if (wantEmptyDirectoryFlag && meta != null) {
      final FileStatus status = meta.getFileStatus();
      // for directory, we query its direct children to determine isEmpty bit
      if (status.isDirectory()) {
        final QuerySpec spec = new QuerySpec()
            .withHashKey(pathToParentKeyAttribute(path))
            .withConsistentRead(true)
            .withFilterExpression(IS_DELETED + " = :false")
            .withValueMap(deleteTrackingValueMap);
        boolean hasChildren = readOp.retry("get/hasChildren",
            path.toString(),
            true,
            () -> table.query(spec).iterator().hasNext());
        // When this class has support for authoritative
        // (fully-cached) directory listings, we may also be able to answer
        // TRUE here.  Until then, we don't know if we have full listing or
        // not, thus the UNKNOWN here:
        meta.setIsEmptyDirectory(
            hasChildren ? Tristate.FALSE : Tristate.UNKNOWN);
      }
    }

    return meta;
  }

  /**
   * Make a FileStatus object for a directory at given path.  The FileStatus
   * only contains what S3A needs, and omits mod time since S3A uses its own
   * implementation which returns current system time.
   * @param owner  username of owner
   * @param path   path to dir
   * @return new FileStatus
   */
  private FileStatus makeDirStatus(String owner, Path path) {
    return new FileStatus(0, true, 1, 0, 0, 0, null,
            owner, null, path);
  }

  @Override
  @Retries.RetryTranslated
  public DirListingMetadata listChildren(final Path path) throws IOException {
    checkPath(path);
    LOG.debug("Listing table {} in region {}: {}", tableName, region, path);

    // find the children in the table
    return readOp.retry(
        "listChildren",
        path.toString(),
        true,
        () -> {
          final QuerySpec spec = new QuerySpec()
              .withHashKey(pathToParentKeyAttribute(path))
              .withConsistentRead(true); // strictly consistent read
          final ItemCollection<QueryOutcome> items = table.query(spec);

          final List<PathMetadata> metas = new ArrayList<>();
          for (Item item : items) {
            DDBPathMetadata meta = itemToPathMetadata(item, username);
            metas.add(meta);
          }

          DDBPathMetadata dirPathMeta = get(path);
          boolean isAuthoritative = false;
          if(dirPathMeta != null) {
            isAuthoritative = dirPathMeta.isAuthoritativeDir();
          }

          LOG.trace("Listing table {} in region {} for {} returning {}",
              tableName, region, path, metas);

          return (metas.isEmpty() && dirPathMeta == null)
              ? null
              : new DirListingMetadata(path, metas, isAuthoritative);
        });
  }

  /**
   * build the list of all parent entries.
   * @param pathsToCreate paths to create
   * @return the full ancestry paths
   */
  Collection<DDBPathMetadata> completeAncestry(
      Collection<DDBPathMetadata> pathsToCreate) {
    // Key on path to allow fast lookup
    Map<Path, DDBPathMetadata> ancestry = new HashMap<>();

    for (DDBPathMetadata meta : pathsToCreate) {
      Preconditions.checkArgument(meta != null);
      Path path = meta.getFileStatus().getPath();
      if (path.isRoot()) {
        break;
      }
      ancestry.put(path, new DDBPathMetadata(meta));
      Path parent = path.getParent();
      while (!parent.isRoot() && !ancestry.containsKey(parent)) {
        LOG.debug("auto-create ancestor path {} for child path {}",
            parent, path);
        final FileStatus status = makeDirStatus(parent, username);
        ancestry.put(parent, new DDBPathMetadata(status, Tristate.FALSE,
            false));
        parent = parent.getParent();
      }
    }
    return ancestry.values();
  }

  @Override
  @Retries.RetryTranslated
  public void move(Collection<Path> pathsToDelete,
      Collection<PathMetadata> pathsToCreate) throws IOException {
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
    Collection<DDBPathMetadata> newItems = new ArrayList<>();
    if (pathsToCreate != null) {
      newItems.addAll(completeAncestry(pathMetaToDDBPathMeta(pathsToCreate)));
    }
    if (pathsToDelete != null) {
      for (Path meta : pathsToDelete) {
        newItems.add(new DDBPathMetadata(PathMetadata.tombstone(meta)));
      }
    }

    processBatchWriteRequest(null, pathMetadataToItem(newItems));
  }

  /**
   * Helper method to issue a batch write request to DynamoDB.
   *
   * As well as retrying on the operation invocation, incomplete
   * batches are retried until all have been deleted.
   * @param keysToDelete primary keys to be deleted; can be null
   * @param itemsToPut new items to be put; can be null
   * @return the number of iterations needed to complete the call.
   */
  @Retries.RetryTranslated("Outstanding batch items are updated with backoff")
  private int processBatchWriteRequest(PrimaryKey[] keysToDelete,
      Item[] itemsToPut) throws IOException {
    final int totalToDelete = (keysToDelete == null ? 0 : keysToDelete.length);
    final int totalToPut = (itemsToPut == null ? 0 : itemsToPut.length);
    int count = 0;
    int batches = 0;
    while (count < totalToDelete + totalToPut) {
      final TableWriteItems writeItems = new TableWriteItems(tableName);
      int numToDelete = 0;
      if (keysToDelete != null
          && count < totalToDelete) {
        numToDelete = Math.min(S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT,
            totalToDelete - count);
        writeItems.withPrimaryKeysToDelete(
            Arrays.copyOfRange(keysToDelete, count, count + numToDelete));
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
  public void put(PathMetadata meta) throws IOException {
    // For a deeply nested path, this method will automatically create the full
    // ancestry and save respective item in DynamoDB table.
    // So after put operation, we maintain the invariant that if a path exists,
    // all its ancestors will also exist in the table.
    // For performance purpose, we generate the full paths to put and use batch
    // write item request to save the items.
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);

    Collection<PathMetadata> wrapper = new ArrayList<>(1);
    wrapper.add(meta);
    put(wrapper);
  }

  @Override
  @Retries.RetryTranslated
  public void put(Collection<PathMetadata> metas) throws IOException {
    innerPut(pathMetaToDDBPathMeta(metas));
  }

  @Retries.OnceRaw
  private void innerPut(Collection<DDBPathMetadata> metas) throws IOException {
    Item[] items = pathMetadataToItem(completeAncestry(metas));
    LOG.debug("Saving batch of {} items to table {}, region {}", items.length,
        tableName, region);
    processBatchWriteRequest(null, items);
  }

  /**
   * Helper method to get full path of ancestors that are nonexistent in table.
   */
  @VisibleForTesting
  @Retries.RetryTranslated
  Collection<DDBPathMetadata> fullPathsToPut(DDBPathMetadata meta)
      throws IOException {
    checkPathMetadata(meta);
    final Collection<DDBPathMetadata> metasToPut = new ArrayList<>();
    // root path is not persisted
    if (!meta.getFileStatus().getPath().isRoot()) {
      metasToPut.add(meta);
    }

    // put all its ancestors if not present; as an optimization we return at its
    // first existent ancestor
    Path path = meta.getFileStatus().getPath().getParent();
    while (path != null && !path.isRoot()) {
      final Item item = getConsistentItem(path);
      if (!itemExists(item)) {
        final FileStatus status = makeDirStatus(path, username);
        metasToPut.add(new DDBPathMetadata(status, Tristate.FALSE, false,
            meta.isAuthoritativeDir()));
        path = path.getParent();
      } else {
        break;
      }
    }
    return metasToPut;
  }

  private boolean itemExists(Item item) {
    if (item == null) {
      return false;
    }
    if (item.hasAttribute(IS_DELETED) &&
        item.getBoolean(IS_DELETED)) {
      return false;
    }
    return true;
  }

  /** Create a directory FileStatus using current system time as mod time. */
  static FileStatus makeDirStatus(Path f, String owner) {
    return  new FileStatus(0, true, 1, 0, System.currentTimeMillis(), 0,
        null, owner, owner, f);
  }

  /**
   * {@inheritDoc}.
   * There is retry around building the list of paths to update, but
   * the call to {@link #processBatchWriteRequest(PrimaryKey[], Item[])}
   * is only tried once.
   * @param meta Directory listing metadata.
   * @throws IOException IO problem
   */
  @Override
  @Retries.RetryTranslated
  public void put(DirListingMetadata meta) throws IOException {
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);

    // directory path
    Path path = meta.getPath();
    DDBPathMetadata ddbPathMeta =
        new DDBPathMetadata(makeDirStatus(path, username), meta.isEmpty(),
            false, meta.isAuthoritative());

    // First add any missing ancestors...
    final Collection<DDBPathMetadata> metasToPut = fullPathsToPut(ddbPathMeta);

    // next add all children of the directory
    metasToPut.addAll(pathMetaToDDBPathMeta(meta.getListing()));

    processBatchWriteRequest(null, pathMetadataToItem(metasToPut));
  }

  @Override
  public synchronized void close() {
    if (instrumentation != null) {
      instrumentation.storeClosed();
    }
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
    if (table == null) {
      LOG.info("In destroy(): no table to delete");
      return;
    }
    LOG.info("Deleting DynamoDB table {} in region {}", tableName, region);
    Preconditions.checkNotNull(dynamoDB, "Not connected to DynamoDB");
    try {
      invoker.retry("delete", null, true,
          () -> table.delete());
      table.waitForDelete();
    } catch (FileNotFoundException rnfe) {
      LOG.info("FileNotFoundException while deleting DynamoDB table {} in "
              + "region {}.  This may indicate that the table does not exist, "
              + "or has been deleted by another concurrent thread or process.",
          tableName, region);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting for DynamoDB table {} being deleted",
          tableName, ie);
      throw new InterruptedIOException("Table " + tableName
          + " in region " + region + " has not been deleted");
    }
  }

  @Retries.RetryTranslated
  private ItemCollection<ScanOutcome> expiredFiles(long modTime,
      String keyPrefix) throws IOException {
    String filterExpression =
        "mod_time < :mod_time and begins_with(parent, :parent)";
    String projectionExpression = "parent,child";
    ValueMap map = new ValueMap()
        .withLong(":mod_time", modTime)
        .withString(":parent", keyPrefix);
    return readOp.retry(
        "scan",
        keyPrefix,
        true,
        () -> table.scan(filterExpression, projectionExpression, null, map));
  }

  @Override
  @Retries.RetryTranslated
  public void prune(long modTime) throws IOException {
    prune(modTime, "/");
  }

  /**
   * Prune files, in batches. There's a sleep between each batch.
   * @param modTime Oldest modification time to allow
   * @param keyPrefix The prefix for the keys that should be removed
   * @throws IOException Any IO/DDB failure.
   * @throws InterruptedIOException if the prune was interrupted
   */
  @Override
  @Retries.RetryTranslated
  public void prune(long modTime, String keyPrefix) throws IOException {
    int itemCount = 0;
    try {
      Collection<Path> deletionBatch =
          new ArrayList<>(S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT);
      long delay = conf.getTimeDuration(
          S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
          S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT,
          TimeUnit.MILLISECONDS);
      Set<Path> parentPathSet = new HashSet<>();
      for (Item item : expiredFiles(modTime, keyPrefix)) {
        DDBPathMetadata md = PathMetadataDynamoDBTranslation
            .itemToPathMetadata(item, username);
        Path path = md.getFileStatus().getPath();
        deletionBatch.add(path);

        // add parent path of what we remove
        Path parentPath = path.getParent();
        if (parentPath != null) {
          parentPathSet.add(parentPath);
        }

        itemCount++;
        if (deletionBatch.size() == S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT) {
          Thread.sleep(delay);
          processBatchWriteRequest(pathToKey(deletionBatch), null);

          // set authoritative false for each pruned dir listing
          removeAuthoritativeDirFlag(parentPathSet);
          parentPathSet.clear();

          deletionBatch.clear();
        }
      }
      // final batch of deletes
      if (!deletionBatch.isEmpty()) {
        Thread.sleep(delay);
        processBatchWriteRequest(pathToKey(deletionBatch), null);

        // set authoritative false for each pruned dir listing
        removeAuthoritativeDirFlag(parentPathSet);
        parentPathSet.clear();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Pruning was interrupted");
    }
    LOG.info("Finished pruning {} items in batches of {}", itemCount,
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT);
  }

  private void removeAuthoritativeDirFlag(Set<Path> pathSet)
      throws IOException {
    AtomicReference<IOException> rIOException = new AtomicReference<>();

    Set<DDBPathMetadata> metas = pathSet.stream().map(path -> {
      try {
        DDBPathMetadata ddbPathMetadata = get(path);
        if(ddbPathMetadata == null) {
          return null;
        }
        LOG.debug("Setting false isAuthoritativeDir on {}", ddbPathMetadata);
        ddbPathMetadata.setAuthoritativeDir(false);
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
      innerPut(metas);
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

  /**
   *  Add tags from configuration to the existing DynamoDB table.
   */
  @Retries.OnceRaw
  public void tagTable() {
    List<Tag> tags = new ArrayList<>();
    Map <String, String> tagProperties =
        conf.getPropsWithPrefix(S3GUARD_DDB_TABLE_TAG);
    for (Map.Entry<String, String> tagMapEntry : tagProperties.entrySet()) {
      Tag tag = new Tag().withKey(tagMapEntry.getKey())
          .withValue(tagMapEntry.getValue());
      tags.add(tag);
    }
    if (tags.isEmpty()) {
      return;
    }

    TagResourceRequest tagResourceRequest = new TagResourceRequest()
        .withResourceArn(table.getDescription().getTableArn())
        .withTags(tags);
    getAmazonDynamoDB().tagResource(tagResourceRequest);
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
        + '}';
  }

  /**
   * Create a table if it does not exist and wait for it to become active.
   *
   * If a table with the intended name already exists, then it uses that table.
   * Otherwise, it will automatically create the table if the config
   * {@link org.apache.hadoop.fs.s3a.Constants#S3GUARD_DDB_TABLE_CREATE_KEY} is
   * enabled. The DynamoDB table creation API is asynchronous.  This method wait
   * for the table to become active after sending the creation request, so
   * overall, this method is synchronous, and the table is guaranteed to exist
   * after this method returns successfully.
   *
   * The wait for a table becoming active is Retry+Translated; it can fail
   * while a table is not yet ready.
   *
   * @throws IOException if table does not exist and auto-creation is disabled;
   * or table is being deleted, or any other I/O exception occurred.
   */
  @VisibleForTesting
  @Retries.OnceRaw
  void initTable() throws IOException {
    table = dynamoDB.getTable(tableName);
    try {
      try {
        LOG.debug("Binding to table {}", tableName);
        TableDescription description = table.describe();
        LOG.debug("Table state: {}", description);
        final String status = description.getTableStatus();
        switch (status) {
        case "CREATING":
          LOG.debug("Table {} in region {} is being created/updated. This may"
                  + " indicate that the table is being operated by another "
                  + "concurrent thread or process. Waiting for active...",
              tableName, region);
          waitForTableActive(table);
          break;
        case "DELETING":
          throw new FileNotFoundException("DynamoDB table "
              + "'" + tableName + "' is being "
              + "deleted in region " + region);
        case "UPDATING":
          // table being updated; it can still be used.
          LOG.debug("Table is being updated.");
          break;
        case "ACTIVE":
          break;
        default:
          throw new IOException("Unknown DynamoDB table status " + status
              + ": tableName='" + tableName + "', region=" + region);
        }

        final Item versionMarker = getVersionMarkerItem();
        verifyVersionCompatibility(tableName, versionMarker);
        Long created = extractCreationTimeFromMarker(versionMarker);
        LOG.debug("Using existing DynamoDB table {} in region {} created {}",
            tableName, region, (created != null) ? new Date(created) : null);
      } catch (ResourceNotFoundException rnfe) {
        if (conf.getBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, false)) {
          final ProvisionedThroughput capacity = new ProvisionedThroughput(
              conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY,
                  S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT),
              conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY,
                  S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT));

          createTable(capacity);
        } else {
          throw (FileNotFoundException)new FileNotFoundException(
              "DynamoDB table '" + tableName + "' does not "
              + "exist in region " + region + "; auto-creation is turned off")
              .initCause(rnfe);
        }
      }

    } catch (AmazonClientException e) {
      throw translateException("initTable", tableName, e);
    }
  }

  /**
   * Get the version mark item in the existing DynamoDB table.
   *
   * As the version marker item may be created by another concurrent thread or
   * process, we sleep and retry a limited number times if the lookup returns
   * with a null value.
   * DDB throttling is always retried.
   */
  @VisibleForTesting
  @Retries.RetryTranslated
  Item getVersionMarkerItem() throws IOException {
    final PrimaryKey versionMarkerKey =
        createVersionMarkerPrimaryKey(VERSION_MARKER);
    int retryCount = 0;
    // look for a version marker, with usual throttling/failure retries.
    Item versionMarker = queryVersionMarker(versionMarkerKey);
    while (versionMarker == null) {
      // The marker was null.
      // Two possibilities
      // 1. This isn't a S3Guard table.
      // 2. This is a S3Guard table in construction; another thread/process
      //    is about to write/actively writing the version marker.
      // So that state #2 is handled, batchWriteRetryPolicy is used to manage
      // retries.
      // This will mean that if the cause is actually #1, failure will not
      // be immediate. As this will ultimately result in a failure to
      // init S3Guard and the S3A FS, this isn't going to be a performance
      // bottleneck -simply a slightly slower failure report than would otherwise
      // be seen.
      // "if your settings are broken, performance is not your main issue"
      try {
        RetryPolicy.RetryAction action = batchWriteRetryPolicy.shouldRetry(null,
            retryCount, 0, true);
        if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
          break;
        } else {
          LOG.debug("Sleeping {} ms before next retry", action.delayMillis);
          Thread.sleep(action.delayMillis);
        }
      } catch (Exception e) {
        throw new IOException("initTable: Unexpected exception " + e, e);
      }
      retryCount++;
      versionMarker = queryVersionMarker(versionMarkerKey);
    }
    return versionMarker;
  }

  /**
   * Issue the query to get the version marker, with throttling for overloaded
   * DDB tables.
   * @param versionMarkerKey key to look up
   * @return the marker
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private Item queryVersionMarker(final PrimaryKey versionMarkerKey)
      throws IOException {
    return readOp.retry("getVersionMarkerItem",
        VERSION_MARKER, true,
        () -> table.getItem(versionMarkerKey));
  }

  /**
   * Verify that a table version is compatible with this S3Guard client.
   * @param tableName name of the table (for error messages)
   * @param versionMarker the version marker retrieved from the table
   * @throws IOException on any incompatibility
   */
  @VisibleForTesting
  static void verifyVersionCompatibility(String tableName,
      Item versionMarker) throws IOException {
    if (versionMarker == null) {
      LOG.warn("Table {} contains no version marker", tableName);
      throw new IOException(E_NO_VERSION_MARKER
      + " Table: " + tableName);
    } else {
      final int version = extractVersionFromMarker(versionMarker);
      if (VERSION != version) {
        // version mismatch. Unless/until there is support for
        // upgrading versions, treat this as an incompatible change
        // and fail.
        throw new IOException(E_INCOMPATIBLE_VERSION
            + " Table "+  tableName
            + " Expected version " + VERSION + " actual " + version);
      }
    }
  }

  /**
   * Wait for table being active.
   * @param t table to block on.
   * @throws IOException IO problems
   * @throws InterruptedIOException if the wait was interrupted
   * @throws IllegalArgumentException if an exception was raised in the waiter
   */
  @Retries.RetryTranslated
  private void waitForTableActive(Table t) throws IOException {
    invoker.retry("Waiting for active state of table " + tableName,
        null,
        true,
        () -> {
          try {
            t.waitForActive();
          } catch (IllegalArgumentException ex) {
            throw translateTableWaitFailure(tableName, ex);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for table {} in region {}"
                    + " active",
                tableName, region, e);
            Thread.currentThread().interrupt();
            throw (InterruptedIOException)
                new InterruptedIOException("DynamoDB table '"
                    + tableName + "' is not active yet in region " + region)
                    .initCause(e);
          }
        });
  }

  /**
   * Create a table, wait for it to become active, then add the version
   * marker.
   * Creating an setting up the table isn't wrapped by any retry operations;
   * the wait for a table to become available is RetryTranslated.
   * @param capacity capacity to provision
   * @throws IOException on any failure.
   * @throws InterruptedIOException if the wait was interrupted
   */
  @Retries.OnceRaw
  private void createTable(ProvisionedThroughput capacity) throws IOException {
    try {
      LOG.info("Creating non-existent DynamoDB table {} in region {}",
          tableName, region);
      table = dynamoDB.createTable(new CreateTableRequest()
          .withTableName(tableName)
          .withKeySchema(keySchema())
          .withAttributeDefinitions(attributeDefinitions())
          .withProvisionedThroughput(capacity));
      LOG.debug("Awaiting table becoming active");
    } catch (ResourceInUseException e) {
      LOG.warn("ResourceInUseException while creating DynamoDB table {} "
              + "in region {}.  This may indicate that the table was "
              + "created by another concurrent thread or process.",
          tableName, region);
    }
    waitForTableActive(table);
    final Item marker = createVersionMarker(VERSION_MARKER, VERSION,
        System.currentTimeMillis());
    putItem(marker);
    tagTable();
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

  /**
   * Provision the table with given read and write capacity units.
   * Call will fail if the table is busy, or the new values match the current
   * ones.
   * @param readCapacity read units
   * @param writeCapacity write units
   * @throws IOException on a failure
   */
  @Retries.RetryTranslated
  void provisionTable(Long readCapacity, Long writeCapacity)
      throws IOException {
    final ProvisionedThroughput toProvision = new ProvisionedThroughput()
        .withReadCapacityUnits(readCapacity)
        .withWriteCapacityUnits(writeCapacity);
    invoker.retry("ProvisionTable", tableName, true,
        () -> {
          final ProvisionedThroughputDescription p =
              table.updateTable(toProvision).getProvisionedThroughput();
          LOG.info("Provision table {} in region {}: readCapacityUnits={}, "
                  + "writeCapacityUnits={}",
              tableName, region, p.getReadCapacityUnits(),
              p.getWriteCapacityUnits());
        });
  }

  @Retries.RetryTranslated
  @VisibleForTesting
  void provisionTableBlocking(Long readCapacity, Long writeCapacity)
      throws IOException {
    provisionTable(readCapacity, writeCapacity);
    waitForTableActive(table);
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
      map.put(TABLE, desc.toString());
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

    if (newRead != currentRead || newWrite != currentWrite) {
      LOG.info("Current table capacity is read: {}, write: {}",
          currentRead, currentWrite);
      LOG.info("Changing capacity of table to read: {}, write: {}",
          newRead, newWrite);
      provisionTableBlocking(newRead, newWrite);
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
      if (instrumentation != null) {
        instrumentation.throttled();
      }
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

    if (instrumentation != null) {
      // note a retry
      instrumentation.retrying();
    }
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

  @VisibleForTesting
  public long getBatchWriteCapacityExceededCount() {
    return batchWriteCapacityExceededEvents.get();
  }

  @VisibleForTesting
  public Invoker getInvoker() {
    return invoker;
  }

  /**
   * Take an {@code IllegalArgumentException} raised by a DDB operation
   * and if it contains an inner SDK exception, unwrap it.
   * @param ex exception.
   * @return the inner AWS exception or null.
   */
  public static SdkBaseException extractInnerException(
      IllegalArgumentException ex) {
    if (ex.getCause() instanceof  SdkBaseException) {
      return (SdkBaseException) ex.getCause();
    } else {
      return null;
    }
  }

  /**
   * Handle a table wait failure by extracting any inner cause and
   * converting it, or, if unconvertable by wrapping
   * the IllegalArgumentException in an IOE.
   *
   * @param name name of the table
   * @param e exception
   * @return an IOE to raise.
   */
  @VisibleForTesting
  static IOException translateTableWaitFailure(
      final String name, IllegalArgumentException e) {
    final SdkBaseException ex = extractInnerException(e);
    if (ex != null) {
      if (ex instanceof WaiterTimedOutException) {
        // a timeout waiting for state change: extract the
        // message from the outer exception, but translate
        // the inner one for the throttle policy.
        return new AWSClientIOException(e.getMessage(), ex);
      } else {
        return translateException(e.getMessage(), name, ex);
      }
    } else {
      return new IOException(e);
    }
  }
}
