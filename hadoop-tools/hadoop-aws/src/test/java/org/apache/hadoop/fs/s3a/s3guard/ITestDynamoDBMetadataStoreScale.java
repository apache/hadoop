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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AWSServiceThrottledException;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.scale.AbstractITestS3AMetadataStoreScale;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.s3guard.MetadataStoreTestBase.basicFileStatus;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.PARENT;
import static org.junit.Assume.*;

/**
 * Scale test for DynamoDBMetadataStore.
 *
 * The throttle tests aren't quite trying to verify that throttling can
 * be recovered from, because that makes for very slow tests: you have
 * to overload the system and them have them back of until they finally complete.
 * <p>
 * With DDB on demand, throttling is very unlikely.
 * Here the tests simply run to completion, so act as regression tests of
 * parallel invocations on the metastore APIs
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestDynamoDBMetadataStoreScale
    extends AbstractITestS3AMetadataStoreScale {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestDynamoDBMetadataStoreScale.class);

  private static final long BATCH_SIZE = 25;

  /**
   * IO Units for batch size; this sets the size to use for IO capacity.
   * Value: {@value}.
   */
  private static final long MAXIMUM_READ_CAPACITY = 10;
  private static final long MAXIMUM_WRITE_CAPACITY = 15;

  private DynamoDBMetadataStore ddbms;

  private DynamoDB ddb;

  private Table table;

  private String tableName;

  /** was the provisioning changed in test_001_limitCapacity()? */
  private boolean isOverProvisionedForTest;

  private ProvisionedThroughputDescription originalCapacity;

  private static final int THREADS = 40;

  private static final int OPERATIONS_PER_THREAD = 50;

  private boolean isOnDemandTable;

  /**
   * Create the metadata store. The table and region are determined from
   * the attributes of the FS used in the tests.
   * @return a new metadata store instance
   * @throws IOException failure to instantiate
   * @throws AssumptionViolatedException if the FS isn't running S3Guard + DDB/
   */
  @Override
  public MetadataStore createMetadataStore() throws IOException {
    S3AFileSystem fs = getFileSystem();
    assumeTrue("S3Guard is disabled for " + fs.getUri(),
        fs.hasMetadataStore());
    MetadataStore store = fs.getMetadataStore();
    assumeTrue("Metadata store for " + fs.getUri() + " is " + store
            + " -not DynamoDBMetadataStore",
        store instanceof DynamoDBMetadataStore);
    DDBCapacities capacities = DDBCapacities.extractCapacities(
        store.getDiagnostics());
    isOnDemandTable = capacities.isOnDemandTable();

    DynamoDBMetadataStore fsStore = (DynamoDBMetadataStore) store;
    Configuration conf = new Configuration(fs.getConf());

    tableName = fsStore.getTableName();
    assertTrue("Null/Empty tablename in " + fsStore,
        StringUtils.isNotEmpty(tableName));
    String region = fsStore.getRegion();
    assertTrue("Null/Empty region in " + fsStore,
        StringUtils.isNotEmpty(region));
    // create a new metastore configured to fail fast if throttling
    // happens.
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    conf.set(S3GUARD_DDB_REGION_KEY, region);
    conf.set(S3GUARD_DDB_THROTTLE_RETRY_INTERVAL, "50ms");
    conf.set(S3GUARD_DDB_MAX_RETRIES, "2");
    conf.set(MAX_ERROR_RETRIES, "1");
    conf.set(S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY, "5ms");

    DynamoDBMetadataStore ms = new DynamoDBMetadataStore();
    ms.initialize(conf, new S3Guard.TtlTimeProvider(conf));
    // wire up the owner FS so that we can make assertions about throttle
    // events
    ms.bindToOwnerFilesystem(fs);
    return ms;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    ddbms = (DynamoDBMetadataStore) createMetadataStore();
    tableName = ddbms.getTableName();
    assertNotNull("table has no name", tableName);
    ddb = ddbms.getDynamoDB();
    table = ddb.getTable(tableName);
    originalCapacity = table.describe().getProvisionedThroughput();

    // If you set the same provisioned I/O as already set it throws an
    // exception, avoid that.
    isOverProvisionedForTest = (
        originalCapacity.getReadCapacityUnits() > MAXIMUM_READ_CAPACITY
            || originalCapacity.getWriteCapacityUnits() > MAXIMUM_WRITE_CAPACITY);
  }

  @Override
  public void teardown() throws Exception {
    if (ddbms != null) {
      S3GuardTableAccess tableAccess = new S3GuardTableAccess(ddbms);
      ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
      builder.withCondition(
          ExpressionSpecBuilder.S(PARENT).beginsWith("/test/"));

      Iterable<DDBPathMetadata> entries = tableAccess.scanMetadata(builder);
      List<Path> list = new ArrayList<>();
      entries.iterator().forEachRemaining(e -> {
        Path p = e.getFileStatus().getPath();
        LOG.info("Deleting {}", p);
        list.add(p);
      });
      tableAccess.delete(list);
    }
    IOUtils.cleanupWithLogger(LOG, ddbms);
    super.teardown();
  }

  private boolean expectThrottling() {
    return !isOverProvisionedForTest && !isOnDemandTable;
  }

  /**
   * The subclass expects the superclass to be throttled; sometimes it is.
   */
  @Test
  @Override
  public void test_020_Moves() throws Throwable {
    ThrottleTracker tracker = new ThrottleTracker(ddbms);
    try {
      // if this doesn't throttle, all is well.
      super.test_020_Moves();
    } catch (AWSServiceThrottledException ex) {
      // if the service was throttled, we expect the exception text
      GenericTestUtils.assertExceptionContains(
          DynamoDBMetadataStore.HINT_DDB_IOPS_TOO_LOW,
          ex,
          "Expected throttling message");
    } finally {
      LOG.info("Statistics {}", tracker);
    }
  }

  /**
   * Though the AWS SDK claims in documentation to handle retries and
   * exponential backoff, we have witnessed
   * com.amazonaws...dynamodbv2.model.ProvisionedThroughputExceededException
   * (Status Code: 400; Error Code: ProvisionedThroughputExceededException)
   * Hypothesis:
   * Happens when the size of a batched write is bigger than the number of
   * provisioned write units.  This test ensures we handle the case
   * correctly, retrying w/ smaller batch instead of surfacing exceptions.
   */
  @Test
  public void test_030_BatchedWrite() throws Exception {

    final int iterations = 15;
    final ArrayList<PathMetadata> toCleanup = new ArrayList<>();
    toCleanup.ensureCapacity(THREADS * iterations);

    // Fail if someone changes a constant we depend on
    assertTrue("Maximum batch size must big enough to run this test",
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT >= BATCH_SIZE);


    // We know the dynamodb metadata store will expand a put of a path
    // of depth N into a batch of N writes (all ancestors are written
    // separately up to the root).  (Ab)use this for an easy way to write
    // a batch of stuff that is bigger than the provisioned write units
    try {
      describe("Running %d iterations of batched put, size %d", iterations,
          BATCH_SIZE);
      Path base = path(getMethodName());
      final String pathKey = base.toUri().getPath();

      ThrottleTracker result = execute("prune",
          1,
          expectThrottling(),
          () -> {
            ThrottleTracker tracker = new ThrottleTracker(ddbms);
            long pruneItems = 0;
            for (long i = 0; i < iterations; i++) {
              Path longPath = pathOfDepth(BATCH_SIZE,
                  pathKey, String.valueOf(i));
              S3AFileStatus status = basicFileStatus(longPath, 0, false,
                  12345);
              PathMetadata pm = new PathMetadata(status);
              synchronized (toCleanup) {
                toCleanup.add(pm);
              }

              ddbms.put(pm, null);

              pruneItems++;

              if (pruneItems == BATCH_SIZE) {
                describe("pruning files");
                ddbms.prune(MetadataStore.PruneMode.ALL_BY_MODTIME,
                    Long.MAX_VALUE, pathKey);
                pruneItems = 0;
              }
              if (tracker.probe()) {
                // fail fast
                break;
              }
            }
          });
      if (expectThrottling()) {
        assertNotEquals("No batch retries in " + result,
            0, result.getBatchThrottles());
      }
    } finally {
      describe("Cleaning up table %s", tableName);
      for (PathMetadata pm : toCleanup) {
        cleanupMetadata(ddbms, pm);
      }
    }
  }

  /**
   * Test Get throttling including using
   * {@link MetadataStore#get(Path, boolean)},
   * as that stresses more of the code.
   */
  @Test
  public void test_040_get() throws Throwable {
    // attempt to create many many get requests in parallel.
    Path path = new Path("s3a://example.org/get");
    S3AFileStatus status = new S3AFileStatus(true, path, "alice");
    PathMetadata metadata = new PathMetadata(status);
    ddbms.put(metadata, null);
    try {
      execute("get",
          OPERATIONS_PER_THREAD,
          expectThrottling(),
          () -> ddbms.get(path, true)
      );
    } finally {
      retryingDelete(path);
    }
  }

  /**
   * Ask for the version marker, which is where table init can be overloaded.
   */
  @Test
  public void test_050_getVersionMarkerItem() throws Throwable {
    execute("get",
        OPERATIONS_PER_THREAD * 2,
        expectThrottling(),
        () -> ddbms.getVersionMarkerItem()
    );
  }

  /**
   * Cleanup with an extra bit of retry logic around it, in case things
   * are still over the limit.
   * @param path path
   */
  private void retryingDelete(final Path path) {
    try {
      ddbms.getInvoker().retry("Delete ", path.toString(), true,
          () -> ddbms.delete(path));
    } catch (IOException e) {
      LOG.warn("Failed to delete {}: ", path, e);
    }
  }

  @Test
  public void test_060_list() throws Throwable {
    // attempt to create many many get requests in parallel.
    Path path = new Path("s3a://example.org/list");
    S3AFileStatus status = new S3AFileStatus(true, path, "alice");
    PathMetadata metadata = new PathMetadata(status);
    ddbms.put(metadata, null);
    try {
      Path parent = path.getParent();
      execute("list",
          OPERATIONS_PER_THREAD,
          expectThrottling(),
          () -> ddbms.listChildren(parent)
      );
    } finally {
      retryingDelete(path);
    }
  }

  @Test
  public void test_070_putDirMarker() throws Throwable {
    // attempt to create many many get requests in parallel.
    Path path = new Path("s3a://example.org/putDirMarker");
    S3AFileStatus status = new S3AFileStatus(true, path, "alice");
    PathMetadata metadata = new PathMetadata(status);
    ddbms.put(metadata, null);
    DirListingMetadata children = ddbms.listChildren(path.getParent());
    try (DynamoDBMetadataStore.AncestorState state =
             ddbms.initiateBulkWrite(
                 BulkOperationState.OperationType.Put,
                 path)) {
      execute("list",
          OPERATIONS_PER_THREAD,
          expectThrottling(),
          () -> ddbms.put(children, state));
    } finally {
      retryingDelete(path);
    }
  }

  @Test
  public void test_080_fullPathsToPut() throws Throwable {
    // attempt to create many many get requests in parallel.
    Path base = new Path("s3a://example.org/test_080_fullPathsToPut");
    Path child = new Path(base, "child");
    List<PathMetadata> pms = new ArrayList<>();
    try {
      try (BulkOperationState bulkUpdate
              = ddbms.initiateBulkWrite(
                  BulkOperationState.OperationType.Put, child)) {
        ddbms.put(new PathMetadata(makeDirStatus(base)), bulkUpdate);
        ddbms.put(new PathMetadata(makeDirStatus(child)), bulkUpdate);
        ddbms.getInvoker().retry("set up directory tree",
            base.toString(),
            true,
            () -> ddbms.put(pms, bulkUpdate));
      }
      try (BulkOperationState bulkUpdate
              = ddbms.initiateBulkWrite(
                  BulkOperationState.OperationType.Put, child)) {
        DDBPathMetadata dirData = ddbms.get(child, true);
        execute("put",
            OPERATIONS_PER_THREAD,
            expectThrottling(),
            () -> ddbms.fullPathsToPut(dirData, bulkUpdate)
        );
      }
    } finally {
      ddbms.forgetMetadata(child);
      ddbms.forgetMetadata(base);
    }
  }

  /**
   * Try many deletes in parallel; this will create tombstones.
   */
  @Test
  public void test_090_delete() throws Throwable {
    Path path = new Path("s3a://example.org/delete");
    S3AFileStatus status = new S3AFileStatus(true, path, "alice");
    PathMetadata metadata = new PathMetadata(status);
    ddbms.put(metadata, null);
    ITtlTimeProvider time = checkNotNull(getTtlTimeProvider(), "time provider");

    try (DurationInfo ignored = new DurationInfo(LOG, true, "delete")) {
      execute("delete",
          OPERATIONS_PER_THREAD,
          expectThrottling(),
          () -> {
            ddbms.delete(path);
          });
    }
  }

  /**
   * Forget Metadata: delete entries without tombstones.
   */
  @Test
  public void test_100_forgetMetadata() throws Throwable {
    Path path = new Path("s3a://example.org/delete");
    try (DurationInfo ignored = new DurationInfo(LOG, true, "delete")) {
      execute("delete",
          OPERATIONS_PER_THREAD,
          expectThrottling(),
          () -> ddbms.forgetMetadata(path)
      );
    }
  }

  @Test
  public void test_900_instrumentation() throws Throwable {
    describe("verify the owner FS gets updated after throttling events");
    Assume.assumeTrue("No throttling expected", expectThrottling());
    // we rely on the FS being shared
    S3AFileSystem fs = getFileSystem();
    String fsSummary = fs.toString();

    S3AStorageStatistics statistics = fs.getStorageStatistics();
    for (StorageStatistics.LongStatistic statistic : statistics) {
      LOG.info("{}", statistic.toString());
    }
    String retryKey = Statistic.S3GUARD_METADATASTORE_RETRY.getSymbol();
    assertTrue("No increment of " + retryKey + " in " + fsSummary,
        statistics.getLong(retryKey) > 0);
    String throttledKey = Statistic.S3GUARD_METADATASTORE_THROTTLED.getSymbol();
    assertTrue("No increment of " + throttledKey + " in " + fsSummary,
        statistics.getLong(throttledKey) > 0);
  }

  /**
   * Execute a set of operations in parallel, collect throttling statistics
   * and return them.
   * This execution will complete as soon as throttling is detected.
   * This ensures that the tests do not run for longer than they should.
   * @param operation string for messages.
   * @param operationsPerThread number of times per thread to invoke the action.
   * @param expectThrottling is throttling expected (and to be asserted on?)
   * @param action action to invoke.
   * @return the throttle statistics
   */
  public ThrottleTracker execute(String operation,
      int operationsPerThread,
      final boolean expectThrottling,
      LambdaTestUtils.VoidCallable action)
      throws Exception {

    final ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    final ThrottleTracker tracker = new ThrottleTracker(ddbms);
    final ExecutorService executorService = Executors.newFixedThreadPool(
        THREADS);
    final List<Callable<ExecutionOutcome>> tasks = new ArrayList<>(THREADS);

    final AtomicInteger throttleExceptions = new AtomicInteger(0);
    for (int i = 0; i < THREADS; i++) {
      tasks.add(
          () -> {
            final ExecutionOutcome outcome = new ExecutionOutcome();
            final ContractTestUtils.NanoTimer t
                = new ContractTestUtils.NanoTimer();
            for (int j = 0; j < operationsPerThread; j++) {
              if (tracker.isThrottlingDetected()) {
                outcome.skipped = true;
                return outcome;
              }
              try {
                action.call();
                outcome.completed++;
              } catch (AWSServiceThrottledException e) {
                // this is possibly OK
                LOG.info("Operation [{}] raised a throttled exception " + e, j, e);
                LOG.debug(e.toString(), e);
                throttleExceptions.incrementAndGet();
                // consider it completed
                outcome.throttleExceptions.add(e);
                outcome.throttled++;
              } catch (Exception e) {
                LOG.error("Failed to execute {}", operation, e);
                outcome.exceptions.add(e);
                break;
              }
              tracker.probe();
            }
            LOG.info("Thread completed {} with in {} ms with outcome {}: {}",
                operation, t.elapsedTimeMs(), outcome, tracker);
            return outcome;
          }
      );
    }
    final List<Future<ExecutionOutcome>> futures =
        executorService.invokeAll(tasks,
        getTestTimeoutMillis(), TimeUnit.MILLISECONDS);
    long elapsedMs = timer.elapsedTimeMs();
    LOG.info("Completed {} with {}", operation, tracker);
    LOG.info("time to execute: {} millis", elapsedMs);

    for (Future<ExecutionOutcome> future : futures) {
      assertTrue("Future timed out", future.isDone());
    }
    tracker.probe();

    if (expectThrottling) {
      tracker.assertThrottlingDetected();
    }
    for (Future<ExecutionOutcome> future : futures) {

      ExecutionOutcome outcome = future.get();
      if (!outcome.exceptions.isEmpty()) {
        throw outcome.exceptions.get(0);
      }
      if (!outcome.skipped) {
        assertEquals("Future did not complete all operations",
            operationsPerThread, outcome.completed + outcome.throttled);
      }
    }

    return tracker;
  }

  /**
   * Attempt to delete metadata, suppressing any errors, and retrying on
   * throttle events just in case some are still surfacing.
   * @param ms store
   * @param pm path to clean up
   */
  private void cleanupMetadata(DynamoDBMetadataStore ms, PathMetadata pm) {
    Path path = pm.getFileStatus().getPath();
    try {
      ITestDynamoDBMetadataStore.deleteMetadataUnderPath(ms, path, true);
    } catch (IOException ioe) {
      // Ignore.
      LOG.info("Ignoring error while cleaning up {} in database", path, ioe);
    }
  }

  private Path pathOfDepth(long n,
      String name, @Nullable String fileSuffix) {
    StringBuilder sb = new StringBuilder();
    for (long i = 0; i < n; i++) {
      sb.append(i == 0 ? "/" + name : String.format("level-%03d", i));
      if (i == n - 1 && fileSuffix != null) {
        sb.append(fileSuffix);
      }
      sb.append("/");
    }
    return new Path(getFileSystem().getUri().toString(), sb.toString());
  }

  /**
   * Outcome of a thread's execution operation.
   */
  private static class ExecutionOutcome {
    private int completed;
    private int throttled;
    private boolean skipped;
    private final List<Exception> exceptions = new ArrayList<>(1);
    private final List<Exception> throttleExceptions = new ArrayList<>(1);

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ExecutionOutcome{");
      sb.append("completed=").append(completed);
      sb.append(", skipped=").append(skipped);
      sb.append(", throttled=").append(throttled);
      sb.append(", exception count=").append(exceptions.size());
      sb.append('}');
      return sb.toString();
    }
  }
}
