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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.*;

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
 * parameter fs.s3a.s3guard.ddb.endpoint with the corresponding endpoint.
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

  private DynamoDB dynamoDB;
  private String region;
  private Table table;
  private String tableName;
  private Configuration conf;
  private String username;

  /**
   * A utility function to create DynamoDB instance.
   * @param fs S3A file system.
   * @return DynamoDB instance.
   */
  @VisibleForTesting
  static DynamoDB createDynamoDB(S3AFileSystem fs) throws IOException {
    Preconditions.checkNotNull(fs);
    String region;
    try {
      region = fs.getAmazonS3Client().getBucketLocation(fs.getBucket());
    } catch (AmazonClientException e) {
      throw translateException("Determining bucket location",
          fs.getUri().toString(), e);
    }
    return createDynamoDB(fs, region);
  }

  /**
   * A utility function to create DynamoDB instance.
   * @param fs S3A file system.
   * @param region region of the S3A file system.
   * @return DynamoDB instance.
   */
  private static DynamoDB createDynamoDB(S3AFileSystem fs, String region)
      throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(region);
    final Configuration conf = fs.getConf();
    Class<? extends DynamoDBClientFactory> cls = conf.getClass(
        S3GUARD_DDB_CLIENT_FACTORY_IMPL,
        S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT,
        DynamoDBClientFactory.class);
    LOG.debug("Creating dynamo DB client {}", cls);
    AmazonDynamoDBClient dynamoDBClient = ReflectionUtils.newInstance(cls, conf)
        .createDynamoDBClient(fs.getUri(), region);
    return new DynamoDB(dynamoDBClient);
  }

  @Override
  public void initialize(FileSystem fs) throws IOException {
    Preconditions.checkArgument(fs instanceof S3AFileSystem,
        "DynamoDBMetadataStore only supports S3A filesystem.");
    final S3AFileSystem s3afs = (S3AFileSystem) fs;
    final String bucket = s3afs.getBucket();
    try {
      region = s3afs.getAmazonS3Client().getBucketLocation(bucket);
    } catch (AmazonClientException e) {
      throw translateException("Determining bucket location",
          fs.getUri().toString(), e);
    }

    username = s3afs.getUsername();
    conf = s3afs.getConf();
    Class<? extends DynamoDBClientFactory> cls = conf.getClass(
        S3GUARD_DDB_CLIENT_FACTORY_IMPL,
        S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT,
        DynamoDBClientFactory.class);
    AmazonDynamoDBClient dynamoDBClient = ReflectionUtils.newInstance(cls, conf)
        .createDynamoDBClient(s3afs.getUri(), region);
    dynamoDB = new DynamoDB(dynamoDBClient);

    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY, bucket);

    initTable();
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
   * S3AFileSystem integration, e.g. command line tools. Generally you should
   * use {@link #initialize(FileSystem)} if given an initialized S3 file system.
   *
   * @see #initialize(FileSystem)
   * @throws IOException if there is an error
   */
  @Override
  public void initialize(Configuration config) throws IOException {
    conf = config;
    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY);
    Preconditions.checkArgument(!StringUtils.isEmpty(tableName),
        "No DynamoDB table name configured!");
    username = UserGroupInformation.getCurrentUser().getShortUserName();

    Class<? extends DynamoDBClientFactory> clsDdb = conf.getClass(
        S3GUARD_DDB_CLIENT_FACTORY_IMPL,
        S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT,
        DynamoDBClientFactory.class);
    LOG.debug("Creating dynamo DB client {}", clsDdb);
    AmazonDynamoDBClient dynamoDBClient =
        ReflectionUtils.newInstance(clsDdb, conf)
            .createDynamoDBClient(conf);
    dynamoDB = new DynamoDB(dynamoDBClient);
    region = dynamoDBClient.getEndpointPrefix();

    initTable();
  }

  @Override
  public void delete(Path path) throws IOException {
    path = checkPath(path);
    LOG.debug("Deleting from table {} in region {}: {}",
        tableName, region, path);

    // deleting nonexistent item consumes 1 write capacity; skip it
    if (path.isRoot()) {
      LOG.debug("Skip deleting root directory as it does not exist in table");
      return;
    }

    try {
      table.deleteItem(pathToKey(path));
    } catch (AmazonClientException e) {
      throw translateException("delete", path, e);
    }
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    path = checkPath(path);
    LOG.debug("Deleting subtree from table {} in region {}: {}",
        tableName, region, path);

    final PathMetadata meta = get(path);
    if (meta == null) {
      LOG.debug("Subtree path {} does not exist; this will be a no-op", path);
      return;
    }

    for (DescendantsIterator desc = new DescendantsIterator(this, meta);
         desc.hasNext();) {
      delete(desc.next().getFileStatus().getPath());
    }
  }

  @Override
  public PathMetadata get(Path path) throws IOException {
    path = checkPath(path);
    LOG.debug("Get from table {} in region {}: {}", tableName, region, path);

    try {
      final PathMetadata meta;
      if (path.isRoot()) {
        // Root does not persist in the table
        meta = new PathMetadata(new S3AFileStatus(true, path, username));
      } else {
        final GetItemSpec spec = new GetItemSpec()
            .withPrimaryKey(pathToKey(path))
            .withConsistentRead(true); // strictly consistent read
        final Item item = table.getItem(spec);
        meta = itemToPathMetadata(item, username);
        LOG.debug("Get from table {} in region {} returning for {}: {}",
            tableName, region, path, meta);
      }

      if (meta != null) {
        final S3AFileStatus status = (S3AFileStatus) meta.getFileStatus();
        // for directory, we query its direct children to determine isEmpty bit
        if (status.isDirectory()) {
          final QuerySpec spec = new QuerySpec()
              .withHashKey(pathToParentKeyAttribute(path))
              .withConsistentRead(true)
              .withMaxResultSize(1); // limit 1
          final ItemCollection<QueryOutcome> items = table.query(spec);
          status.setIsEmptyDirectory(!(items.iterator().hasNext()));
        }
      }

      return meta;
    } catch (AmazonClientException e) {
      throw translateException("get", path, e);
    }
  }

  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    path = checkPath(path);
    LOG.debug("Listing table {} in region {}: {}", tableName, region, path);

    try {
      final QuerySpec spec = new QuerySpec()
          .withHashKey(pathToParentKeyAttribute(path))
          .withConsistentRead(true); // strictly consistent read
      final ItemCollection<QueryOutcome> items = table.query(spec);

      final List<PathMetadata> metas = new ArrayList<>();
      for (Item item : items) {
        metas.add(itemToPathMetadata(item, username));
      }
      LOG.trace("Listing table {} in region {} for {} returning {}",
          tableName, region, path, metas);

      return (metas.isEmpty() && get(path) == null)
          ? null
          : new DirListingMetadata(path, metas, false);
    } catch (AmazonClientException e) {
      throw translateException("listChildren", path, e);
    }
  }

  @Override
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
    try {
      processBatchWriteRequest(pathToKey(pathsToDelete),
          pathMetadataToItem(pathsToCreate));
    } catch (AmazonClientException e) {
      throw translateException("move", (String) null, e);
    }
  }

  /**
   * Helper method to issue a batch write request to DynamoDB.
   *
   * Callers of this method should catch the {@link AmazonClientException} and
   * translate it for better error report and easier debugging.
   * @param keysToDelete primary keys to be deleted; can be null
   * @param itemsToPut new items to be put; can be null
   */
  private void processBatchWriteRequest(PrimaryKey[] keysToDelete,
      Item[] itemsToPut) {
    final int totalToDelete = (keysToDelete == null ? 0 : keysToDelete.length);
    final int totalToPut = (itemsToPut == null ? 0 : itemsToPut.length);
    int count = 0;
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

      BatchWriteItemOutcome res = dynamoDB.batchWriteItem(writeItems);
      // Check for unprocessed keys in case of exceeding provisioned throughput
      Map<String, List<WriteRequest>> unprocessed = res.getUnprocessedItems();
      while (unprocessed.size() > 0) {
        res = dynamoDB.batchWriteItemUnprocessed(unprocessed);
        unprocessed = res.getUnprocessedItems();
      }
    }
  }

  @Override
  public void put(PathMetadata meta) throws IOException {
    // For a deeply nested path, this method will automatically create the full
    // ancestry and save respective item in DynamoDB table.
    // So after put operation, we maintain the invariant that if a path exists,
    // all its ancestors will also exist in the table.
    // For performance purpose, we generate the full paths to put and use batch
    // write item request to save the items.
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);
    processBatchWriteRequest(null, pathMetadataToItem(fullPathsToPut(meta)));
  }

  /**
   * Helper method to get full path of ancestors that are nonexistent in table.
   */
  private Collection<PathMetadata> fullPathsToPut(PathMetadata meta)
      throws IOException {
    checkPathMetadata(meta);
    final Collection<PathMetadata> metasToPut = new ArrayList<>();
    // root path is not persisted
    if (!meta.getFileStatus().getPath().isRoot()) {
      metasToPut.add(meta);
    }

    // put all its ancestors if not present; as an optimization we return at its
    // first existent ancestor
    Path path = meta.getFileStatus().getPath().getParent();
    while (path != null && !path.isRoot()) {
      final GetItemSpec spec = new GetItemSpec()
          .withPrimaryKey(pathToKey(path))
          .withConsistentRead(true); // strictly consistent read
      final Item item = table.getItem(spec);
      if (item == null) {
        final S3AFileStatus status = new S3AFileStatus(false, path, username);
        metasToPut.add(new PathMetadata(status));
        path = path.getParent();
      } else {
        break;
      }
    }
    return metasToPut;
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);

    // directory path
    final Collection<PathMetadata> metasToPut = fullPathsToPut(
        new PathMetadata(new S3AFileStatus(false, meta.getPath(), username)));
    // all children of the directory
    metasToPut.addAll(meta.getListing());

    try {
      processBatchWriteRequest(null, pathMetadataToItem(metasToPut));
    } catch (AmazonClientException e) {
      throw translateException("put", (String) null, e);
    }
  }

  @Override
  public synchronized void close() {
    if (dynamoDB != null) {
      LOG.debug("Shutting down {}", this);
      dynamoDB.shutdown();
      dynamoDB = null;
    }
  }

  @Override
  public void destroy() throws IOException {
    if (table == null) {
      LOG.info("In destroy(): no table to delete");
      return;
    }
    LOG.info("Deleting DynamoDB table {} in region {}", tableName, region);
    Preconditions.checkNotNull(dynamoDB, "Not connected to Dynamo");
    try {
      table.delete();
      table.waitForDelete();
    } catch (ResourceNotFoundException rnfe) {
      LOG.info("ResourceNotFoundException while deleting DynamoDB table {} in "
              + "region {}.  This may indicate that the table does not exist, "
              + "or has been deleted by another concurrent thread or process.",
          tableName, region);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting for DynamoDB table {} being deleted",
          tableName, ie);
      throw new InterruptedIOException("Table " + tableName
          + " in region " + region + " has not been deleted");
    } catch (AmazonClientException e) {
      throw translateException("destroy", (String) null, e);
    }
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
   * @throws IOException if table does not exist and auto-creation is disabled;
   * or any other I/O exception occurred.
   */
  @VisibleForTesting
  void initTable() throws IOException {
    table = dynamoDB.getTable(tableName);
    try {
      try {
        LOG.debug("Binding to table {}", tableName);
        table.describe();
        final Item versionMarker = table.getItem(
            createVersionMarkerPrimaryKey(VERSION_MARKER));
        verifyVersionCompatibility(tableName, versionMarker);
        Long created = extractCreationTimeFromMarker(versionMarker);
        LOG.debug("Using existing DynamoDB table {} in region {} created {}",
            tableName, region,
            created != null ? new Date(created) : null);

      } catch (ResourceNotFoundException rnfe) {
        if (conf.getBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, false)) {
          final ProvisionedThroughput capacity = new ProvisionedThroughput(
              conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY,
                  S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT),
              conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY,
                  S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT));

          createTable(capacity);
        } else {
          throw new IOException("DynamoDB table '" + tableName + "' does not "
              + "exist in region " + region + "; auto-creation is turned off");
        }
      }

    } catch (AmazonClientException e) {
      throw translateException("initTable", (String) null, e);
    }
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
   * Create a table, wait for it to become active, then add the version
   * marker.
   * @param capacity capacity to provision
   * @throws IOException on an failure.
   */
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
      table.waitForActive();
    } catch (ResourceInUseException e) {
      LOG.warn("ResourceInUseException while creating DynamoDB table {} "
              + "in region {}.  This may indicate that the table was "
              + "created by another concurrent thread or process.",
          tableName, region);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for DynamoDB table {} active",
          tableName, e);
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException(
          "DynamoDB table '" + tableName + "' "
              + "is not active yet in region " + region).initCause(e);
    }
    final Item marker = createVersionMarker(VERSION_MARKER, VERSION,
        System.currentTimeMillis());
    putItem(marker);
  }

  /**
   * PUT a single item to the table.
   * @param item item to put
   * @return the outcome.
   */
  PutItemOutcome putItem(Item item) {
    LOG.debug("Putting item {}", item);
    return table.putItem(item);
  }

  /**
   * Provision the table with given read and write capacity units.
   */
  void provisionTable(Long readCapacity, Long writeCapacity)
      throws IOException {
    final ProvisionedThroughput toProvision = new ProvisionedThroughput()
        .withReadCapacityUnits(readCapacity)
        .withWriteCapacityUnits(writeCapacity);
    try {
      final ProvisionedThroughputDescription p =
          table.updateTable(toProvision).getProvisionedThroughput();
      LOG.info("Provision table {} in region {}: readCapacityUnits={}, "
              + "writeCapacityUnits={}",
          tableName, region, p.getReadCapacityUnits(),
          p.getWriteCapacityUnits());
    } catch (AmazonClientException e) {
      throw translateException("provisionTable", (String) null, e);
    }
  }

  Table getTable() {
    return table;
  }

  String getRegion() {
    return region;
  }

  @VisibleForTesting
  DynamoDB getDynamoDB() {
    return dynamoDB;
  }

  /**
   * Validates a path object; it must be absolute, and contain a host
   * (bucket) component.
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

}
