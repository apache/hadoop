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

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CAPACITY_READ_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CREATE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_TAG;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.E_INCOMPATIBLE_VERSION;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.E_NO_VERSION_MARKER;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.E_ON_DEMAND_NO_SET_CAPACITY;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.VERSION;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.VERSION_MARKER;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.attributeDefinitions;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.createVersionMarker;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.createVersionMarkerPrimaryKey;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.extractCreationTimeFromMarker;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.extractVersionFromMarker;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.keySchema;

public class DynamoDBMetadataStoreTableHandler {
  public static final Logger LOG = LoggerFactory.getLogger(
      DynamoDBMetadataStoreTableHandler.class);

  /** Invoker for IO. Until configured properly, use try-once. */
  private Invoker invoker = new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL,
      Invoker.NO_OP
  );

  private AmazonDynamoDB amazonDynamoDB;
  final private DynamoDB dynamoDB;
  final private String tableName;
  private Table table;
  private String region;
  private Configuration conf;
  private final Invoker readOp;
  private final RetryPolicy batchWriteRetryPolicy;

  private String tableArn;

  public DynamoDBMetadataStoreTableHandler(DynamoDB dynamoDB,
      String tableName,
      String region,
      AmazonDynamoDB amazonDynamoDB,
      Configuration conf,
      Invoker readOp,
      RetryPolicy batchWriteCapacityExceededEvents) {
    this.dynamoDB = dynamoDB;
    this.amazonDynamoDB = amazonDynamoDB;
    this.tableName = tableName;
    this.region = region;
    this.conf = conf;
    this.readOp = readOp;
    this.batchWriteRetryPolicy = batchWriteCapacityExceededEvents;
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
  Table initTable() throws IOException {
    table = dynamoDB.getTable(tableName);
    try {
      try {
        LOG.debug("Binding to table {}", tableName);
        TableDescription description = table.describe();
        LOG.debug("Table state: {}", description);
        tableArn = description.getTableArn();
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
          long readCapacity = conf.getLong(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY,
              S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT);
          long writeCapacity = conf.getLong(
              S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY,
              S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT);
          ProvisionedThroughput capacity;
          if (readCapacity > 0 && writeCapacity > 0) {
            capacity = new ProvisionedThroughput(
                readCapacity,
                writeCapacity);
          } else {
            // at least one capacity value is <= 0
            // verify they are both exactly zero
            Preconditions.checkArgument(
                readCapacity == 0 && writeCapacity == 0,
                "S3Guard table read capacity %d and and write capacity %d"
                    + " are inconsistent", readCapacity, writeCapacity);
            // and set the capacity to null for per-request billing.
            capacity = null;
          }

          createTable(capacity);
        } else {
          throw (FileNotFoundException) new FileNotFoundException(
              "DynamoDB table '" + tableName + "' does not "
                  + "exist in region " + region +
                  "; auto-creation is turned off")
              .initCause(rnfe);
        }
      }

    } catch (AmazonClientException e) {
      throw translateException("initTable", tableName, e);
    }

    return table;
  }

  private void addVersionMarkerToEmptyTable(String tableName)
      throws IOException {
    final ScanResult result = readOp.retry(
        "scan",
        null,
        true,
        () -> {
          final ScanRequest req = new ScanRequest().withTableName(
              tableName).withLimit(1);
          return amazonDynamoDB.scan(req);
        }
    );
    boolean isEmptyTable = result.getCount() == 0;

    if (!isEmptyTable) {
      // the table is not empty, do nothing.
    } else {
      // the table is empty, add version marker
      putVersionMarkerToTable();
    }
  }

  /**
   * Create a table, wait for it to become active, then add the version
   * marker.
   * Creating an setting up the table isn't wrapped by any retry operations;
   * the wait for a table to become available is RetryTranslated.
   * @param capacity capacity to provision. If null: create a per-request
   * table.
   * @throws IOException on any failure.
   * @throws InterruptedIOException if the wait was interrupted
   */
  @Retries.OnceRaw
  private void createTable(ProvisionedThroughput capacity) throws IOException {
    try {
      String mode;
      CreateTableRequest request = new CreateTableRequest()
          .withTableName(tableName)
          .withKeySchema(keySchema())
          .withAttributeDefinitions(attributeDefinitions());
      if (capacity != null) {
        mode = String.format("with provisioned read capacity %d and"
                + " write capacity %s",
            capacity.getReadCapacityUnits(), capacity.getWriteCapacityUnits());
        request.withProvisionedThroughput(capacity);
      } else {
        mode = "with pay-per-request billing";
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);
      }
      LOG.info("Creating non-existent DynamoDB table {} in region {} {}",
          tableName, region, mode);
      table = dynamoDB.createTable(request);
      LOG.debug("Awaiting table becoming active");
    } catch (ResourceInUseException e) {
      LOG.warn("ResourceInUseException while creating DynamoDB table {} "
              + "in region {}.  This may indicate that the table was "
              + "created by another concurrent thread or process.",
          tableName, region);
    }
    waitForTableActive(table);
    putVersionMarkerToTable();
    tagTable();
  }

  /**
   *  Add tags from configuration to the existing DynamoDB table.
   */
  @Retries.OnceRaw
  public void tagTable() {
    List<Tag> tags = new ArrayList<>();
    Map<String, String> tagProperties =
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
    amazonDynamoDB.tagResource(tagResourceRequest);
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
            + " Table " + tableName
            + " Expected version " + VERSION + " actual " + version);
      }
    }
  }

  /**
   * Add version marker to the dynamo table
   */
  @Retries.OnceRaw
  private void putVersionMarkerToTable() {
    final Item marker = createVersionMarker(VERSION_MARKER, VERSION,
        System.currentTimeMillis());
    putItem(marker);
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

  /**
   * Take an {@code IllegalArgumentException} raised by a DDB operation
   * and if it contains an inner SDK exception, unwrap it.
   * @param ex exception.
   * @return the inner AWS exception or null.
   */
  public static SdkBaseException extractInnerException(
      IllegalArgumentException ex) {
    if (ex.getCause() instanceof SdkBaseException) {
      return (SdkBaseException) ex.getCause();
    } else {
      return null;
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
   * <p>
   * Until the AWS SDK lets us switch a table to on-demand, an attempt to
   * set the I/O capacity to zero will fail.
   * @param readCapacity read units: must be greater than zero
   * @param writeCapacity write units: must be greater than zero
   * @throws IOException on a failure
   */
  @Retries.RetryTranslated
  void provisionTable(Long readCapacity, Long writeCapacity)
      throws IOException {

    if (readCapacity == 0 || writeCapacity == 0) {
      // table is pay on demand
      throw new IOException(E_ON_DEMAND_NO_SET_CAPACITY);
    }
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

  public String getTableName() {
    return tableName;
  }

  public Table getTable() {
    return table;
  }

  public String getTableArn() {
    return tableArn;
  }
}
