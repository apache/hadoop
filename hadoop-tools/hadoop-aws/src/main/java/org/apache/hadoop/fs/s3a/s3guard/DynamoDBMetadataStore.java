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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
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
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
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
 * <li> parent (absolute path of the parent). </li>
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
 * Root is a special case.  It has no parent, so it cannot be split into
 * separate parent and child attributes.  To avoid confusion in the DynamoDB
 * table, we simply do not persist root and instead treat it as a special case
 * path that always exists.
 *
 * For example, assume the consistent store contains metadata representing this
 * file system structure:
 *
 * <pre>
 * /dir1
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
 * ==================================================================
 * | parent          | child | is_dir | mod_time | len |     ...    |
 * ==================================================================
 * | /               | dir1  | true   |          |     |            |
 * | /dir1           | dir2  | true   |          |     |            |
 * | /dir1           | dir3  | true   |          |     |            |
 * | /dir1/dir2      | file1 |        |   100    | 111 |            |
 * | /dir1/dir2      | file2 |        |   200    | 222 |            |
 * | /dir1/dir3      | dir4  | true   |          |     |            |
 * | /dir1/dir3      | dir5  | true   |          |     |            |
 * | /dir1/dir3/dir4 | file3 |        |   300    | 333 |            |
 * | /dir1/dir3/dir5 | file4 |        |   400    | 444 |            |
 * | /dir1/dir3      | dir6  | true   |          |     |            |
 * ==================================================================
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
 * All DynamoDB access is performed within the same AWS region as the S3 bucket
 * that hosts the S3A instance.  During initialization, it checks the location
 * of the S3 bucket and creates a DynamoDB client connected to the same region.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DynamoDBMetadataStore implements MetadataStore {
  public static final Logger LOG = LoggerFactory.getLogger(
      DynamoDBMetadataStore.class);

  private DynamoDB dynamoDB;
  private String region;
  private Table table;
  private String tableName;
  private S3AFileSystem s3afs;
  private String username;

  @Override
  public void initialize(FileSystem fs) throws IOException {
    Preconditions.checkArgument(fs instanceof S3AFileSystem,
        "DynamoDBMetadataStore only supports S3A filesystem.");
    s3afs = (S3AFileSystem) fs;
    final String bucket = s3afs.getBucket();
    try {
      region = s3afs.getAmazonS3Client().getBucketLocation(bucket);
    } catch (AmazonClientException e) {
      throw translateException("Determining bucket location",
          fs.getUri().toString(), e);
    }

    username = s3afs.getUsername();

    final Configuration conf = s3afs.getConf();
    Class<? extends DynamoDBClientFactory> cls = conf.getClass(
        S3GUARD_DDB_CLIENT_FACTORY_IMPL,
        S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT,
        DynamoDBClientFactory.class);
    AmazonDynamoDBClient dynamoDBClient = ReflectionUtils.newInstance(cls, conf)
        .createDynamoDBClient(s3afs.getUri(), region);
    dynamoDB = new DynamoDB(dynamoDBClient);

    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY, bucket);

    // create the table unless it's explicitly told not to do so
    if (conf.getBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, true)) {
      createTable();
    }
  }

  /**
   * Performs one-time initialization of the metadata store via configuration.
   *
   * This initialization depends on the configuration object to get DEFAULT
   * S3AFileSystem URI, AWS credentials, S3ClientFactory implementation class,
   * DynamoDBFactor implementation class, DynamoDB endpoints, metadata table
   * names etc. Generally you should use {@link #initialize(FileSystem)} instead
   * given an initialized S3 file system.
   *
   * @see #initialize(FileSystem)
   * @throws IOException if there is an error
   */
  void initialize(Configuration conf) throws IOException {
    final FileSystem defautFs = FileSystem.get(conf);
    Preconditions.checkArgument(defautFs instanceof S3AFileSystem,
        "DynamoDBMetadataStore only supports S3A filesystem.");
    s3afs = (S3AFileSystem) defautFs;

    // use the bucket as the DynamoDB table name if not specified in config
    tableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY);
    Preconditions.checkNotNull(tableName, "No DynamoDB table name configured!");

    final Class<? extends S3ClientFactory> clsS3 = conf.getClass(
        S3_CLIENT_FACTORY_IMPL,
        DEFAULT_S3_CLIENT_FACTORY_IMPL,
        S3ClientFactory.class);
    final S3ClientFactory factory = ReflectionUtils.newInstance(clsS3, conf);
    AmazonS3 s3 = factory.createS3Client(s3afs.getUri(), s3afs.getUri());
    try {
      region = s3.getBucketLocation(tableName);
    } catch (AmazonClientException e) {
      throw new IOException("Can not find location for bucket " + tableName, e);
    }

    Class<? extends DynamoDBClientFactory> clsDdb = conf.getClass(
        S3GUARD_DDB_CLIENT_FACTORY_IMPL,
        S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT,
        DynamoDBClientFactory.class);
    LOG.debug("Creating dynamo DB client {}", clsDdb);
    AmazonDynamoDBClient dynamoDBClient =
        ReflectionUtils.newInstance(clsDdb, conf)
            .createDynamoDBClient(s3afs.getUri(), region);
    dynamoDB = new DynamoDB(dynamoDBClient);

    createTable();
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
        meta = itemToPathMetadata(s3afs.getUri(), item, username);
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
        metas.add(itemToPathMetadata(s3afs.getUri(), item, username));
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
    final TableWriteItems writeItems = new TableWriteItems(tableName)
        .withItemsToPut(pathMetadataToItem(pathsToCreate))
        .withPrimaryKeysToDelete(pathToKey(pathsToDelete));
    try {
      BatchWriteItemOutcome res = dynamoDB.batchWriteItem(writeItems);

      // Check for unprocessed keys in case of exceeding provisioned throughput
      Map<String, List<WriteRequest>> unprocessed = res.getUnprocessedItems();
      while (unprocessed.size() > 0) {
        res = dynamoDB.batchWriteItemUnprocessed(unprocessed);
        unprocessed = res.getUnprocessedItems();
      }
    } catch (AmazonClientException e) {
      throw translateException("createTable", (String) null, e);
    }
  }

  @Override
  public void put(PathMetadata meta) throws IOException {
    checkPathMetadata(meta);
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);
    innerPut(meta);

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
        innerPut(new PathMetadata(status));
        path = path.getParent();
      } else {
        break;
      }
    }
  }

  private void innerPut(PathMetadata meta) throws IOException {
    final Path path = meta.getFileStatus().getPath();
    if (path.isRoot()) {
      LOG.debug("Root path / is not persisted");
      return;
    }

    try {
      table.putItem(pathMetadataToItem(meta));
    } catch (AmazonClientException e) {
      throw translateException("put", path, e);
    }
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {
    LOG.debug("Saving to table {} in region {}: {}", tableName, region, meta);

    for (PathMetadata pathMetadata : meta.getListing()) {
      put(pathMetadata);
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
      LOG.debug("In destroy(): no table to delete");
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
   * If a table with the intended name already exists, then it logs the
   * {@link ResourceInUseException} and uses that table. The DynamoDB table
   * creation API is asynchronous.  This method wait for the table to become
   * active after sending the creation request, so overall, this method is
   * synchronous, and the table is guaranteed to exist after this method
   * returns successfully.
   */
  @VisibleForTesting
  void createTable() throws IOException {
    final Configuration conf = s3afs.getConf();
    final ProvisionedThroughput capacity = new ProvisionedThroughput(
        conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY,
            S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT),
        conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY,
            S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT));

    try {
      LOG.info("Creating DynamoDB table {} in region {}", tableName, region);
      table = dynamoDB.createTable(new CreateTableRequest()
          .withTableName(tableName)
          .withKeySchema(keySchema())
          .withAttributeDefinitions(attributeDefinitions())
          .withProvisionedThroughput(capacity));
    } catch (ResourceInUseException e) {
      LOG.info("ResourceInUseException while creating DynamoDB table {} in "
              + "region {}.  This may indicate that the table was created by "
              + "another concurrent thread or process.",
          tableName, region);
      table = dynamoDB.getTable(tableName);
    }

    try {
      table.waitForActive();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for DynamoDB table {} active",
          tableName, e);
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("DynamoDB table '" + tableName + "'" +
          " is not active yet in region " + region);
    } catch (AmazonClientException e) {
      throw translateException("createTable", (String) null, e);
    }
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
   * Validates a path object; and make it qualified if it's not.
   */
  private Path checkPath(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(path.isAbsolute(),
        "Path '" + path + "' is not absolute!");
    return path.makeQualified(s3afs.getUri(), null);
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
