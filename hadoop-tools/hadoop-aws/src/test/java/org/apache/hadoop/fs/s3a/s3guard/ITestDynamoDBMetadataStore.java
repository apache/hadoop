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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.SSEDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.UntagResourceRequest;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DurationInfo;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import static java.lang.String.valueOf;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.clearBucketOption;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStoreTableManager.E_INCOMPATIBLE_ITEM_VERSION;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStoreTableManager.E_INCOMPATIBLE_TAG_VERSION;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStoreTableManager.E_NO_VERSION_MARKER_AND_NOT_EMPTY;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStoreTableManager.getVersionMarkerFromTags;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.*;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test that {@link DynamoDBMetadataStore} implements {@link MetadataStore}.
 *
 * In this integration test, we use a real AWS DynamoDB. A
 * {@link DynamoDBMetadataStore} object is created in the @BeforeClass method,
 * and shared for all test in the @BeforeClass method. You will be charged
 * bills for AWS S3 or DynamoDB when you run these tests.
 *
 * According to the base class, every test case will have independent contract
 * to create a new {@link S3AFileSystem} instance and initializes it.
 * A table will be created and shared between the tests; some tests also
 * create their own.
 *
 * Important: Any new test which creates a table must do the following
 * <ol>
 *   <li>Enable on-demand pricing.</li>
 *   <li>Always destroy the table, even if an assertion fails.</li>
 * </ol>
 * This is needed to avoid "leaking" DDB tables and running up bills.
 */
public class ITestDynamoDBMetadataStore extends MetadataStoreTestBase {

  public static final int MINUTE = 60_000;

  public ITestDynamoDBMetadataStore() {
    super();
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestDynamoDBMetadataStore.class);
  public static final PrimaryKey
      VERSION_MARKER_PRIMARY_KEY = createVersionMarkerPrimaryKey(
      DynamoDBMetadataStore.VERSION_MARKER_ITEM_NAME);

  private S3AFileSystem fileSystem;
  private S3AContract s3AContract;
  private DynamoDBMetadataStoreTableManager tableHandler;

  private URI fsUri;

  private String bucket;

  @SuppressWarnings("StaticNonFinalField")
  private static DynamoDBMetadataStore ddbmsStatic;

  @SuppressWarnings("StaticNonFinalField")
  private static String testDynamoDBTableName;

  private static final List<Path> UNCHANGED_ENTRIES = Collections.emptyList();

  /**
   * Create a path under the test path provided by
   * the FS contract.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   */
  protected Path path(String filepath) {
    return getFileSystem().makeQualified(
        new Path(s3AContract.getTestPath(), filepath));
  }

  @Override
  public void setUp() throws Exception {
    Configuration conf = prepareTestConfiguration(new Configuration());
    assumeThatDynamoMetadataStoreImpl(conf);
    Assume.assumeTrue("Test DynamoDB table name should be set to run "
            + "integration tests.", testDynamoDBTableName != null);
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, testDynamoDBTableName);
    enableOnDemand(conf);
    s3AContract = new S3AContract(conf);
    s3AContract.init();

    fileSystem = (S3AFileSystem) s3AContract.getTestFileSystem();
    assume("No test filesystem", s3AContract.isEnabled());
    assertNotNull("No test filesystem", fileSystem);
    fsUri = fileSystem.getUri();
    bucket = fileSystem.getBucket();

    try{
      super.setUp();
    } catch (FileNotFoundException e){
      LOG.warn("MetadataStoreTestBase setup failed. Waiting for table to be "
          + "deleted before trying again.", e);
      try {
        ddbmsStatic.getTable().waitForDelete();
      } catch (IllegalArgumentException | InterruptedException ex) {
        LOG.warn("When awaiting a table to be cleaned up", e);
      }
      super.setUp();
    }
    tableHandler = getDynamoMetadataStore().getTableHandler();
  }

  @BeforeClass
  public static void beforeClassSetup() throws IOException {
    Configuration conf = prepareTestConfiguration(new Configuration());
    assumeThatDynamoMetadataStoreImpl(conf);
    // S3GUARD_DDB_TEST_TABLE_NAME_KEY and S3GUARD_DDB_TABLE_NAME_KEY should
    // be configured to use this test.
    testDynamoDBTableName = conf.get(
        S3ATestConstants.S3GUARD_DDB_TEST_TABLE_NAME_KEY);
    String dynamoDbTableName = conf.getTrimmed(S3GUARD_DDB_TABLE_NAME_KEY);
    Assume.assumeTrue("No DynamoDB table name configured in "
            + S3GUARD_DDB_TABLE_NAME_KEY,
        !StringUtils.isEmpty(dynamoDbTableName));

    // We should assert that the table name is configured, so the test should
    // fail if it's not configured.
    assertNotNull("Test DynamoDB table name '"
        + S3ATestConstants.S3GUARD_DDB_TEST_TABLE_NAME_KEY + "'"
        + " should be set to run integration tests.",
        testDynamoDBTableName);

    // We should assert that the test table is not the same as the production
    // table, as the test table could be modified and destroyed multiple
    // times during the test.
    assertNotEquals("Test DynamoDB table name: "
            + "'" + S3ATestConstants.S3GUARD_DDB_TEST_TABLE_NAME_KEY + "'"
            + " and production table name: "
            + "'" + S3GUARD_DDB_TABLE_NAME_KEY + "' can not be the same.",
        testDynamoDBTableName, conf.get(S3GUARD_DDB_TABLE_NAME_KEY));

    // We can use that table in the test if these assertions are valid
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, testDynamoDBTableName);

    // remove some prune delays
    conf.setInt(S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY, 0);

    // clear all table tagging config before this test
    conf.getPropsWithPrefix(S3GUARD_DDB_TABLE_TAG).keySet().forEach(
        propKey -> conf.unset(S3GUARD_DDB_TABLE_TAG + propKey)
    );

    // set the tags on the table so that it can be tested later.
    Map<String, String> tagMap = createTagMap();
    for (Map.Entry<String, String> tagEntry : tagMap.entrySet()) {
      conf.set(S3GUARD_DDB_TABLE_TAG + tagEntry.getKey(), tagEntry.getValue());
    }
    LOG.debug("Creating static ddbms which will be shared between tests.");
    enableOnDemand(conf);

    ddbmsStatic = new DynamoDBMetadataStore();
    ddbmsStatic.initialize(conf, new S3Guard.TtlTimeProvider(conf));
  }

  @AfterClass
  public static void afterClassTeardown() {
    LOG.debug("Destroying static DynamoDBMetadataStore.");
    destroy(ddbmsStatic);
    ddbmsStatic = null;
  }

  /**
   * Destroy and then close() a metastore instance.
   * Exceptions are caught and logged at debug.
   * @param ddbms store -may be null.
   */
  private static void destroy(final DynamoDBMetadataStore ddbms) {
    if (ddbms != null) {
      try {
        ddbms.destroy();
        IOUtils.closeStream(ddbms);
      } catch (IOException e) {
        LOG.debug("On ddbms shutdown", e);
      }
    }
  }

  private static void assumeThatDynamoMetadataStoreImpl(Configuration conf){
    Assume.assumeTrue("Test only applies when DynamoDB is used for S3Guard",
        conf.get(Constants.S3_METADATA_STORE_IMPL).equals(
            Constants.S3GUARD_METASTORE_DYNAMO));
  }

  /**
   * This teardown does not call super.teardown() so as to avoid the DDMBS table
   * from being destroyed.
   * <p>
   * This is potentially quite slow, depending on DDB IO Capacity and number
   * of entries to forget.
   */
  @Override
  public void tearDown() throws Exception {
    LOG.info("Removing data from ddbms table in teardown.");
    Thread.currentThread().setName("Teardown");
    // The following is a way to be sure the table will be cleared and there
    // will be no leftovers after the test.
    try {
      deleteAllMetadata();
    } finally {
      IOUtils.cleanupWithLogger(LOG, fileSystem);
    }
  }

  /**
   * Forget all metadata in the store.
   * This originally did an iterate and forget, but using prune() hands off the
   * bulk IO into the metastore itself; the forgetting is used
   * to purge anything which wasn't pruned.
   */
  private void deleteAllMetadata() throws IOException {
    // The following is a way to be sure the table will be cleared and there
    // will be no leftovers after the test.
    // only executed if there is a filesystem, as failure during test setup
    // means that strToPath will NPE.
    if (getContract() != null && getContract().getFileSystem() != null) {
      deleteMetadataUnderPath(ddbmsStatic, strToPath("/"), true);
    }
  }

  /**
   * Delete all metadata under a path.
   * Attempt to use prune first as it scales slightly better.
   * @param ms store
   * @param path path to prune under
   * @param suppressErrors should errors be suppressed?
   * @throws IOException if there is a failure and suppressErrors == false
   */
  public static void deleteMetadataUnderPath(final DynamoDBMetadataStore ms,
      final Path path, final boolean suppressErrors) throws IOException {
    ThrottleTracker throttleTracker = new ThrottleTracker(ms);
    int forgotten = 0;
    try (DurationInfo ignored = new DurationInfo(LOG, true, "forget")) {
      PathMetadata meta = ms.get(path);
      if (meta != null) {
        for (DescendantsIterator desc = new DescendantsIterator(ms,
            meta);
            desc.hasNext();) {
          forgotten++;
          ms.forgetMetadata(desc.next().getPath());
        }
        LOG.info("Forgot {} entries", forgotten);
      }
    } catch (FileNotFoundException fnfe) {
      // there is no table.
      return;
    } catch (IOException ioe) {
      LOG.warn("Failed to forget entries under {}", path, ioe);
      if (!suppressErrors) {
        throw ioe;
      }
    }
    LOG.info("Throttle statistics: {}", throttleTracker);
  }

  @Override protected String getPathStringForPrune(String path)
      throws Exception {
    String b = getTestBucketName(getContract().getFileSystem().getConf());
    return "/" + b + "/dir2";
  }

  /**
   * Each contract has its own S3AFileSystem and DynamoDBMetadataStore objects.
   */
  private class DynamoDBMSContract extends AbstractMSContract {

    DynamoDBMSContract(Configuration conf) {
    }

    DynamoDBMSContract() {
      this(new Configuration());
    }

    @Override
    public S3AFileSystem getFileSystem() {
      return ITestDynamoDBMetadataStore.this.fileSystem;
    }

    @Override
    public DynamoDBMetadataStore getMetadataStore() {
      return ITestDynamoDBMetadataStore.ddbmsStatic;
    }
  }

  @Override
  public DynamoDBMSContract createContract() {
    return new DynamoDBMSContract();
  }

  @Override
  public DynamoDBMSContract createContract(Configuration conf) {
    return new DynamoDBMSContract(conf);
  }

  @Override
  protected S3AFileStatus basicFileStatus(Path path, int size, boolean isDir)
      throws IOException {
    String owner = UserGroupInformation.getCurrentUser().getShortUserName();
    return isDir
        ? new S3AFileStatus(true, path, owner)
        : new S3AFileStatus(size, getModTime(), path, BLOCK_SIZE, owner,
            null, null);
  }

  /**
   * Create a directory status entry.
   * @param dir directory.
   * @return the status
   */
  private S3AFileStatus dirStatus(Path dir) throws IOException {
    return basicFileStatus(dir, 0, true);
  }

  private DynamoDBMetadataStore getDynamoMetadataStore() throws IOException {
    return (DynamoDBMetadataStore) getContract().getMetadataStore();
  }

  private S3AFileSystem getFileSystem() {
    return this.fileSystem;
  }

  /**
   * Force the configuration into DDB on demand, so that
   * even if a test bucket isn't cleaned up, the cost is $0.
   * @param conf configuration to patch.
   */
  public static void enableOnDemand(Configuration conf) {
    conf.setInt(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY, 0);
    conf.setInt(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY, 0);
  }

  /**
   * Get the configuration needed to create a table; extracts
   * it from the filesystem then always patches it to be on demand.
   * Why the patch? It means even if a cached FS has brought in
   * some provisioned values, they get reset.
   * @return a new configuration
   */
  private Configuration getTableCreationConfig() {
    Configuration conf = new Configuration(getFileSystem().getConf());
    enableOnDemand(conf);
    return conf;
  }

  /**
   * This tests that after initialize() using an S3AFileSystem object, the
   * instance should have been initialized successfully, and tables are ACTIVE.
   */
  @Test
  public void testInitialize() throws IOException {
    final S3AFileSystem s3afs = this.fileSystem;
    final String tableName =
        getTestTableName("testInitialize");
    Configuration conf = getFileSystem().getConf();
    enableOnDemand(conf);
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(s3afs, new S3Guard.TtlTimeProvider(conf));
      Table table = verifyTableInitialized(tableName, ddbms.getDynamoDB());
      verifyTableSse(conf, table.getDescription());
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());

      String expectedRegion = conf.get(S3GUARD_DDB_REGION_KEY,
          s3afs.getBucketLocation(bucket));
      assertEquals("DynamoDB table should be in configured region or the same" +
              " region as S3 bucket",
          expectedRegion,
          ddbms.getRegion());
    } finally {
      destroy(ddbms);
    }
  }

  /**
   * This tests that after initialize() using a Configuration object, the
   * instance should have been initialized successfully, and tables are ACTIVE.
   */
  @Test
  public void testInitializeWithConfiguration() throws IOException {
    final String tableName =
        getTestTableName("testInitializeWithConfiguration");
    final Configuration conf = getTableCreationConfig();
    conf.unset(S3GUARD_DDB_TABLE_NAME_KEY);
    String savedRegion = conf.get(S3GUARD_DDB_REGION_KEY,
        getFileSystem().getBucketLocation());
    conf.unset(S3GUARD_DDB_REGION_KEY);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(conf,  new S3Guard.TtlTimeProvider(conf));
      fail("Should have failed because the table name is not set!");
    } catch (IllegalArgumentException ignored) {
    }

    // config table name
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(conf, new S3Guard.TtlTimeProvider(conf));
      fail("Should have failed because as the region is not set!");
    } catch (IllegalArgumentException ignored) {
    }

    // config region
    conf.set(S3GUARD_DDB_REGION_KEY, savedRegion);
    doTestInitializeWithConfiguration(conf, tableName);

    // config table server side encryption (SSE)
    conf.setBoolean(S3GUARD_DDB_TABLE_SSE_ENABLED, true);
    doTestInitializeWithConfiguration(conf, tableName);
  }

  /**
   * Test initialize() using a Configuration object successfully.
   */
  private void doTestInitializeWithConfiguration(Configuration conf,
      String tableName) throws IOException {
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf, new S3Guard.TtlTimeProvider(conf));
      Table table = verifyTableInitialized(tableName, ddbms.getDynamoDB());
      verifyTableSse(conf, table.getDescription());
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());
      assertEquals("Unexpected key schema found!",
          keySchema(),
          ddbms.getTable().describe().getKeySchema());
    } finally {
      destroy(ddbms);
    }
  }

  /**
   * This should really drive a parameterized test run of 5^2 entries, but it
   * would require a major refactoring to set things up.
   * For now, each source test has its own entry, with the destination written
   * to.
   * This seems to be enough to stop DDB throttling from triggering test
   * timeouts.
   */
  private static final int[] NUM_METAS_TO_DELETE_OR_PUT = {
      -1, // null
      0, // empty collection
      1, // one path
      S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT, // exact limit of a batch request
      S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT + 1 // limit + 1
  };

  @Test
  public void testBatchWrite00() throws IOException {
    doBatchWriteForOneSet(0);
  }

  @Test
  public void testBatchWrite01() throws IOException {
    doBatchWriteForOneSet(1);
  }

  @Test
  public void testBatchWrite02() throws IOException {
    doBatchWriteForOneSet(2);
  }

  @Test
  public void testBatchWrite03() throws IOException {
    doBatchWriteForOneSet(3);
  }

  @Test
  public void testBatchWrite04() throws IOException {
    doBatchWriteForOneSet(4);
  }

  /**
   * Test that for a large batch write request, the limit is handled correctly.
   * With cleanup afterwards.
   */
  private void doBatchWriteForOneSet(int index) throws IOException {
    for (int numNewMetas : NUM_METAS_TO_DELETE_OR_PUT) {
      doTestBatchWrite(NUM_METAS_TO_DELETE_OR_PUT[index],
          numNewMetas,
          getDynamoMetadataStore());
    }
    // The following is a way to be sure the table will be cleared and there
    // will be no leftovers after the test.
    deleteMetadataUnderPath(ddbmsStatic, strToPath("/"), false);
  }

  /**
   * Test that for a large batch write request, the limit is handled correctly.
   */
  private void doTestBatchWrite(int numDelete, int numPut,
      DynamoDBMetadataStore ms) throws IOException {
    Path path = new Path(
        "/ITestDynamoDBMetadataStore_testBatchWrite_" + numDelete + '_'
            + numPut);
    final Path root = fileSystem.makeQualified(path);
    final Path oldDir = new Path(root, "oldDir");
    final Path newDir = new Path(root, "newDir");
    LOG.info("doTestBatchWrite: oldDir={}, newDir={}", oldDir, newDir);
    Thread.currentThread()
        .setName(String.format("Bulk put=%d; delete=%d", numPut, numDelete));

    AncestorState putState = checkNotNull(ms.initiateBulkWrite(
        BulkOperationState.OperationType.Put, newDir),
        "No state from initiateBulkWrite()");
    ms.put(new PathMetadata(dirStatus(oldDir)), putState);
    ms.put(new PathMetadata(dirStatus(newDir)), putState);

    final List<PathMetadata> oldMetas = numDelete < 0 ? null :
        new ArrayList<>(numDelete);
    for (int i = 0; i < numDelete; i++) {
      oldMetas.add(new PathMetadata(
          basicFileStatus(new Path(oldDir, "child" + i), i, false)));
    }
    final List<PathMetadata> newMetas = numPut < 0 ? null :
        new ArrayList<>(numPut);
    for (int i = 0; i < numPut; i++) {
      newMetas.add(new PathMetadata(
          basicFileStatus(new Path(newDir, "child" + i), i, false)));
    }

    Collection<Path> pathsToDelete = null;
    if (oldMetas != null) {
      // put all metadata of old paths and verify
      ms.put(new DirListingMetadata(oldDir, oldMetas, false), UNCHANGED_ENTRIES,
          putState);
      assertEquals("Child count",
          0, ms.listChildren(newDir).withoutTombstones().numEntries());
      Assertions.assertThat(ms.listChildren(oldDir).getListing())
          .describedAs("Old Directory listing")
          .containsExactlyInAnyOrderElementsOf(oldMetas);

      assertTrue(CollectionUtils
          .isEqualCollection(oldMetas, ms.listChildren(oldDir).getListing()));

      pathsToDelete = new ArrayList<>(oldMetas.size());
      for (PathMetadata meta : oldMetas) {
        pathsToDelete.add(meta.getFileStatus().getPath());
      }
    }

    // move the old paths to new paths and verify
    AncestorState state = checkNotNull(ms.initiateBulkWrite(
        BulkOperationState.OperationType.Put, newDir),
        "No state from initiateBulkWrite()");
    assertEquals("bulk write destination", newDir, state.getDest());

    ThrottleTracker throttleTracker = new ThrottleTracker(ms);
    try(DurationInfo ignored = new DurationInfo(LOG, true,
        "Move")) {
      ms.move(pathsToDelete, newMetas, state);
    }
    LOG.info("Throttle status {}", throttleTracker);
    assertEquals("Number of children in source directory",
        0, ms.listChildren(oldDir).withoutTombstones().numEntries());
    if (newMetas != null) {
      Assertions.assertThat(ms.listChildren(newDir).getListing())
          .describedAs("Directory listing")
          .containsAll(newMetas);
      if (!newMetas.isEmpty()) {
        Assertions.assertThat(state.size())
            .describedAs("Size of ancestor state")
            .isGreaterThan(newMetas.size());
      }
    }
  }

  @Test
  public void testInitExistingTable() throws IOException {
    final DynamoDBMetadataStore ddbms = getDynamoMetadataStore();
    final String tableName = ddbms.getTable().getTableName();
    verifyTableInitialized(tableName, ddbms.getDynamoDB());
    // create existing table
    tableHandler.initTable();
    verifyTableInitialized(tableName, ddbms.getDynamoDB());
  }

  /**
   * Test versioning handling.
   * <ol>
   *   <li>Create the table.</li>
   *   <li>Verify tag propagation.</li>
   *   <li>Delete the version marker -verify failure.</li>
   *   <li>Reinstate a different version marker -verify failure</li>
   * </ol>
   * Delete the version marker and verify that table init fails.
   * This also includes the checks for tagging, which goes against all
   * principles of unit tests.
   * However, merging the routines saves
   */
  @Test
  public void testTableVersioning() throws Exception {
    String tableName = getTestTableName("testTableVersionRequired");
    Configuration conf = getTableCreationConfig();
    int maxRetries = conf.getInt(S3GUARD_DDB_MAX_RETRIES,
        S3GUARD_DDB_MAX_RETRIES_DEFAULT);
    conf.setInt(S3GUARD_DDB_MAX_RETRIES, 3);
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    tagConfiguration(conf);
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf, new S3Guard.TtlTimeProvider(conf));
      DynamoDBMetadataStoreTableManager localTableHandler =
          ddbms.getTableHandler();

      Table table = verifyTableInitialized(tableName, ddbms.getDynamoDB());
      // check the tagging
      verifyStoreTags(createTagMap(), ddbms);
      // check version compatibility
      checkVerifyVersionMarkerCompatibility(localTableHandler, table);

      conf.setInt(S3GUARD_DDB_MAX_RETRIES, maxRetries);
    } finally {
      destroy(ddbms);
    }
  }

  private void checkVerifyVersionMarkerCompatibility(
      DynamoDBMetadataStoreTableManager localTableHandler, Table table)
      throws Exception {
    final AmazonDynamoDB addb
        = getDynamoMetadataStore().getAmazonDynamoDB();
    Item originalVersionMarker = table.getItem(VERSION_MARKER_PRIMARY_KEY);

    LOG.info("1/6: remove version marker and tags from table " +
        "the table is empty, so it should be initialized after the call");
    deleteVersionMarkerItem(table);
    removeVersionMarkerTag(table, addb);
    localTableHandler.initTable();

    final int versionFromItem = extractVersionFromMarker(
        localTableHandler.getVersionMarkerItem());
    final int versionFromTag = extractVersionFromMarker(
        getVersionMarkerFromTags(table, addb));
    assertEquals("Table should be tagged with the right version.",
        VERSION, versionFromTag);
    assertEquals("Table should have the right version marker.",
        VERSION, versionFromItem);

    LOG.info("2/6: if the table is not empty and there's no version marker " +
        "it should fail");
    deleteVersionMarkerItem(table);
    removeVersionMarkerTag(table, addb);
    String testKey = "coffee";
    Item wrongItem =
        createVersionMarker(testKey, VERSION * 2, 0);
    table.putItem(wrongItem);
    intercept(IOException.class, E_NO_VERSION_MARKER_AND_NOT_EMPTY,
        () -> localTableHandler.initTable());

    LOG.info("3/6: table has only version marker item then it will be tagged");
    table.putItem(originalVersionMarker);
    localTableHandler.initTable();
    final int versionFromTag2 = extractVersionFromMarker(
        getVersionMarkerFromTags(table, addb));
    assertEquals("Table should have the right version marker tag " +
        "if there was a version item.", VERSION, versionFromTag2);

    LOG.info("4/6: table has only version marker tag then the version marker " +
        "item will be created.");
    deleteVersionMarkerItem(table);
    removeVersionMarkerTag(table, addb);
    localTableHandler.tagTableWithVersionMarker();
    localTableHandler.initTable();
    final int versionFromItem2 = extractVersionFromMarker(
        localTableHandler.getVersionMarkerItem());
    assertEquals("Table should have the right version marker item " +
        "if there was a version tag.", VERSION, versionFromItem2);

    LOG.info("5/6: add a different marker tag to the table: init should fail");
    deleteVersionMarkerItem(table);
    removeVersionMarkerTag(table, addb);
    Item v200 = createVersionMarker(VERSION_MARKER_ITEM_NAME, VERSION * 2, 0);
    table.putItem(v200);
    intercept(IOException.class, E_INCOMPATIBLE_ITEM_VERSION,
        () -> localTableHandler.initTable());

    LOG.info("6/6: add a different marker item to the table: init should fail");
    deleteVersionMarkerItem(table);
    removeVersionMarkerTag(table, addb);
    int wrongVersion = VERSION + 3;
    tagTableWithCustomVersion(table, addb, wrongVersion);
    intercept(IOException.class, E_INCOMPATIBLE_TAG_VERSION,
        () -> localTableHandler.initTable());

    // CLEANUP
    table.putItem(originalVersionMarker);
    localTableHandler.tagTableWithVersionMarker();
    localTableHandler.initTable();
  }

  private void tagTableWithCustomVersion(Table table,
      AmazonDynamoDB addb,
      int wrongVersion) {
    final Tag vmTag = new Tag().withKey(VERSION_MARKER_TAG_NAME)
        .withValue(valueOf(wrongVersion));
    TagResourceRequest tagResourceRequest = new TagResourceRequest()
        .withResourceArn(table.getDescription().getTableArn())
        .withTags(vmTag);
    addb.tagResource(tagResourceRequest);
  }

  private void removeVersionMarkerTag(Table table, AmazonDynamoDB addb) {
    addb.untagResource(new UntagResourceRequest()
        .withResourceArn(table.describe().getTableArn())
        .withTagKeys(VERSION_MARKER_TAG_NAME));
  }

  /**
   * Deletes a version marker; spins briefly to await it disappearing.
   * @param table table to delete the key
   * @throws Exception failure
   */
  private void deleteVersionMarkerItem(Table table) throws Exception {
    table.deleteItem(VERSION_MARKER_PRIMARY_KEY);
    eventually(30_000, 1_0, () ->
        assertNull("Version marker should be null after deleting it " +
            "from the table.", table.getItem(VERSION_MARKER_PRIMARY_KEY)));
  }

  /**
   * Test that initTable fails with IOException when table does not exist and
   * table auto-creation is disabled.
   */
  @Test
  public void testFailNonexistentTable() throws IOException {
    final String tableName =
        getTestTableName("testFailNonexistentTable");
    final S3AFileSystem s3afs = getFileSystem();
    final Configuration conf = s3afs.getConf();
    enableOnDemand(conf);
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    String b = fsUri.getHost();
    clearBucketOption(conf, b, S3GUARD_DDB_TABLE_CREATE_KEY);
    clearBucketOption(conf, b, S3_METADATA_STORE_IMPL);
    clearBucketOption(conf, b, S3GUARD_DDB_TABLE_NAME_KEY);
    conf.unset(S3GUARD_DDB_TABLE_CREATE_KEY);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(s3afs, new S3Guard.TtlTimeProvider(conf));
      // if an exception was not raised, a table was created.
      // So destroy it before failing.
      ddbms.destroy();
      fail("Should have failed as table does not exist and table auto-creation"
          + " is disabled");
    } catch (IOException ignored) {
    }
  }

  /**
   * Test cases about root directory as it is not in the DynamoDB table.
   */
  @Test
  public void testRootDirectory() throws IOException {
    final DynamoDBMetadataStore ddbms = getDynamoMetadataStore();
    Path rootPath = new Path(new Path(fsUri), "/");
    verifyRootDirectory(ddbms.get(rootPath), true);

    ddbms.put(new PathMetadata(new S3AFileStatus(true,
        new Path(rootPath, "foo"),
        UserGroupInformation.getCurrentUser().getShortUserName())),
        null);
    verifyRootDirectory(ddbms.get(rootPath), false);
  }

  private void verifyRootDirectory(PathMetadata rootMeta, boolean isEmpty) {
    assertNotNull(rootMeta);
    final FileStatus status = rootMeta.getFileStatus();
    assertNotNull(status);
    assertTrue(status.isDirectory());
    // UNKNOWN is always a valid option, but true / false should not contradict
    if (isEmpty) {
      assertNotSame("Should not be marked non-empty",
          Tristate.FALSE,
          rootMeta.isEmptyDirectory());
    } else {
      assertNotSame("Should not be marked empty",
          Tristate.TRUE,
          rootMeta.isEmptyDirectory());
    }
  }

  /**
   * Test that when moving nested paths, all its ancestors up to destination
   * root will also be created.
   * Here is the directory tree before move:
   * <pre>
   * testMovePopulateAncestors
   * ├── a
   * │   └── b
   * │       └── src
   * │           ├── dir1
   * │           │   └── dir2
   * │           └── file1.txt
   * └── c
   *     └── d
   *         └── dest
   *</pre>
   * As part of rename(a/b/src, d/c/dest), S3A will enumerate the subtree at
   * a/b/src.  This test verifies that after the move, the new subtree at
   * 'dest' is reachable from the root (i.e. c/ and c/d exist in the table.
   * DynamoDBMetadataStore depends on this property to do recursive delete
   * without a full table scan.
   */
  @Test
  public void testMovePopulatesAncestors() throws IOException {
    final DynamoDBMetadataStore ddbms = getDynamoMetadataStore();
    final String testRoot = "/testMovePopulatesAncestors";
    final String srcRoot = testRoot + "/a/b/src";
    final String destRoot = testRoot + "/c/d/e/dest";

    final Path nestedPath1 = strToPath(srcRoot + "/file1.txt");
    AncestorState bulkWrite = ddbms.initiateBulkWrite(
        BulkOperationState.OperationType.Put, nestedPath1);
    ddbms.put(new PathMetadata(basicFileStatus(nestedPath1, 1024, false)),
        bulkWrite);
    final Path nestedPath2 = strToPath(srcRoot + "/dir1/dir2");
    ddbms.put(new PathMetadata(basicFileStatus(nestedPath2, 0, true)),
        bulkWrite);

    // We don't put the destRoot path here, since put() would create ancestor
    // entries, and we want to ensure that move() does it, instead.

    // Build enumeration of src / dest paths and do the move()
    final Collection<Path> fullSourcePaths = Lists.newArrayList(
        strToPath(srcRoot),
        strToPath(srcRoot + "/dir1"),
        strToPath(srcRoot + "/dir1/dir2"),
        strToPath(srcRoot + "/file1.txt"));
    final String finalFile = destRoot + "/file1.txt";
    final Collection<PathMetadata> pathsToCreate = Lists.newArrayList(
        new PathMetadata(basicFileStatus(strToPath(destRoot),
            0, true)),
        new PathMetadata(basicFileStatus(strToPath(destRoot + "/dir1"),
            0, true)),
        new PathMetadata(basicFileStatus(strToPath(destRoot + "/dir1/dir2"),
            0, true)),
        new PathMetadata(basicFileStatus(strToPath(finalFile),
            1024, false))
    );

    ddbms.move(fullSourcePaths, pathsToCreate, bulkWrite);
    bulkWrite.close();
    // assert that all the ancestors should have been populated automatically
    List<String> paths = Lists.newArrayList(
        testRoot + "/c", testRoot + "/c/d", testRoot + "/c/d/e", destRoot,
        destRoot + "/dir1", destRoot + "/dir1/dir2");
    for (String p : paths) {
      assertCached(p);
      verifyInAncestor(bulkWrite, p, true);
    }
    // Also check moved files while we're at it
    assertCached(finalFile);
    verifyInAncestor(bulkWrite, finalFile, false);
  }

  @Test
  public void testAncestorOverwriteConflict() throws Throwable {
    final DynamoDBMetadataStore ddbms = getDynamoMetadataStore();
    String testRoot = "/" + getMethodName();
    String parent = testRoot + "/parent";
    Path parentPath = strToPath(parent);
    String child = parent + "/child";
    Path childPath = strToPath(child);
    String grandchild = child + "/grandchild";
    Path grandchildPath = strToPath(grandchild);
    String child2 = parent + "/child2";
    String grandchild2 = child2 + "/grandchild2";
    Path grandchild2Path = strToPath(grandchild2);
    AncestorState bulkWrite = ddbms.initiateBulkWrite(
        BulkOperationState.OperationType.Put, parentPath);

    // writing a child creates ancestors
    ddbms.put(
        new PathMetadata(basicFileStatus(childPath, 1024, false)),
        bulkWrite);
    verifyInAncestor(bulkWrite, child, false);
    verifyInAncestor(bulkWrite, parent, true);

    // overwrite an ancestor with a file entry in the same operation
    // is an error.
    intercept(PathIOException.class, E_INCONSISTENT_UPDATE,
        () -> ddbms.put(
            new PathMetadata(basicFileStatus(parentPath, 1024, false)),
            bulkWrite));

    // now put a file under the child and expect the put operation
    // to fail fast, because the ancestor state includes a file at a parent.

    intercept(PathIOException.class, E_INCONSISTENT_UPDATE,
        () -> ddbms.put(
            new PathMetadata(basicFileStatus(grandchildPath, 1024, false)),
            bulkWrite));

    // and expect a failure for directory update under the child
    DirListingMetadata grandchildListing = new DirListingMetadata(
        grandchildPath,
        new ArrayList<>(), false);
    intercept(PathIOException.class, E_INCONSISTENT_UPDATE,
        () -> ddbms.put(grandchildListing, UNCHANGED_ENTRIES, bulkWrite));

    // but a directory update under another path is fine
    DirListingMetadata grandchild2Listing = new DirListingMetadata(
        grandchild2Path,
        new ArrayList<>(), false);
    ddbms.put(grandchild2Listing, UNCHANGED_ENTRIES, bulkWrite);
    // and it creates a new entry for its parent
    verifyInAncestor(bulkWrite, child2, true);
  }

  /**
   * Assert that a path has an entry in the ancestor state.
   * @param state ancestor state
   * @param path path to look for
   * @param isDirectory is it a directory
   * @return the value
   * @throws IOException IO failure
   * @throws AssertionError assertion failure.
   */
  private DDBPathMetadata verifyInAncestor(AncestorState state,
      String path,
      final boolean isDirectory)
      throws IOException {
    final Path p = strToPath(path);
    assertTrue("Path " + p + " not found in ancestor state", state.contains(p));
    final DDBPathMetadata md = state.get(p);
    assertTrue("Ancestor value for "+  path,
        isDirectory
            ? md.getFileStatus().isDirectory()
            : md.getFileStatus().isFile());
    return md;
  }

  @Test
  public void testDeleteTable() throws Exception {
    final String tableName = getTestTableName("testDeleteTable");
    Path testPath = new Path(new Path(fsUri), "/" + tableName);
    final S3AFileSystem s3afs = getFileSystem();
    // patch the filesystem config as this is one read in initialize()
    final Configuration conf =  s3afs.getConf();
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    enableOnDemand(conf);
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(s3afs, new S3Guard.TtlTimeProvider(conf));
      // we can list the empty table
      ddbms.listChildren(testPath);
      DynamoDB dynamoDB = ddbms.getDynamoDB();
      ddbms.destroy();
      verifyTableNotExist(tableName, dynamoDB);

      // delete table once more; the ResourceNotFoundException swallowed
      // silently
      ddbms.destroy();
      verifyTableNotExist(tableName, dynamoDB);
      intercept(IOException.class, "",
          "Should have failed after the table is destroyed!",
          () -> ddbms.listChildren(testPath));
      ddbms.destroy();
      intercept(FileNotFoundException.class, "",
          "Destroyed table should raise FileNotFoundException when pruned",
          () -> ddbms.prune(PruneMode.ALL_BY_MODTIME, 0));
    } finally {
      destroy(ddbms);
    }
  }

  protected void verifyStoreTags(final Map<String, String> tagMap,
      final DynamoDBMetadataStore store) {
    List<Tag> tags = listTagsOfStore(store);
    Map<String, String> actual = new HashMap<>();
    tags.forEach(t -> actual.put(t.getKey(), t.getValue()));
    Assertions.assertThat(actual)
        .describedAs("Tags from DDB table")
        .containsAllEntriesOf(tagMap);

    // The version marker is always there in the tags.
    // We have a plus one in tags we expect.
    assertEquals(tagMap.size() + 1, tags.size());
  }

  protected List<Tag> listTagsOfStore(final DynamoDBMetadataStore store) {
    ListTagsOfResourceRequest listTagsOfResourceRequest =
        new ListTagsOfResourceRequest()
            .withResourceArn(store.getTable().getDescription()
                .getTableArn());
    return store.getAmazonDynamoDB()
        .listTagsOfResource(listTagsOfResourceRequest).getTags();
  }

  private static Map<String, String> createTagMap() {
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("hello", "dynamo");
    tagMap.put("tag", "youre it");
    return tagMap;
  }

  private static void tagConfiguration(Configuration conf) {
    // set the tags on the table so that it can be tested later.
    Map<String, String> tagMap = createTagMap();
    for (Map.Entry<String, String> tagEntry : tagMap.entrySet()) {
      conf.set(S3GUARD_DDB_TABLE_TAG + tagEntry.getKey(), tagEntry.getValue());
    }
  }

  @Test
  public void testGetEmptyDirFlagCanSetTrue() throws IOException {
    boolean authoritativeDirectoryListing = true;
    testGetEmptyDirFlagCanSetTrueOrUnknown(authoritativeDirectoryListing);
  }

  @Test
  public void testGetEmptyDirFlagCanSetUnknown() throws IOException {
    boolean authoritativeDirectoryListing = false;
    testGetEmptyDirFlagCanSetTrueOrUnknown(authoritativeDirectoryListing);
  }

  private void testGetEmptyDirFlagCanSetTrueOrUnknown(boolean auth)
      throws IOException {
    // setup
    final DynamoDBMetadataStore ms = getDynamoMetadataStore();
    String rootPath = "/testAuthoritativeEmptyDirFlag-" + UUID.randomUUID();
    String filePath = rootPath + "/file1";
    final Path dirToPut = fileSystem.makeQualified(new Path(rootPath));
    final Path fileToPut = fileSystem.makeQualified(new Path(filePath));

    // Create non-auth DirListingMetadata
    DirListingMetadata dlm =
        new DirListingMetadata(dirToPut, new ArrayList<>(), auth);
    if(auth){
      assertEquals(Tristate.TRUE, dlm.isEmpty());
    } else {
      assertEquals(Tristate.UNKNOWN, dlm.isEmpty());
    }
    assertEquals(auth, dlm.isAuthoritative());

    // Test with non-authoritative listing, empty dir
    ms.put(dlm, UNCHANGED_ENTRIES, null);
    final PathMetadata pmdResultEmpty = ms.get(dirToPut, true);
    if(auth){
      assertEquals(Tristate.TRUE, pmdResultEmpty.isEmptyDirectory());
    } else {
      assertEquals(Tristate.UNKNOWN, pmdResultEmpty.isEmptyDirectory());
    }

    // Test with non-authoritative listing, non-empty dir
    dlm.put(new PathMetadata(basicFileStatus(fileToPut, 1, false)));
    ms.put(dlm, UNCHANGED_ENTRIES, null);
    final PathMetadata pmdResultNotEmpty = ms.get(dirToPut, true);
    assertEquals(Tristate.FALSE, pmdResultNotEmpty.isEmptyDirectory());
  }

  /**
   * This validates the table is created and ACTIVE in DynamoDB.
   *
   * This should not rely on the {@link DynamoDBMetadataStore} implementation.
   * Return the table
   */
  private Table verifyTableInitialized(String tableName, DynamoDB dynamoDB) {
    final Table table = dynamoDB.getTable(tableName);
    final TableDescription td = table.describe();
    assertEquals(tableName, td.getTableName());
    assertEquals("ACTIVE", td.getTableStatus());
    return table;
  }

  /**
   * Verify the table is created with correct server side encryption (SSE).
   */
  private void verifyTableSse(Configuration conf, TableDescription td) {
    SSEDescription sseDescription = td.getSSEDescription();
    if (conf.getBoolean(S3GUARD_DDB_TABLE_SSE_ENABLED, false)) {
      assertNotNull(sseDescription);
      assertEquals("ENABLED", sseDescription.getStatus());
      assertEquals("KMS", sseDescription.getSSEType());
      // We do not test key ARN is the same as configured value,
      // because in configuration, the ARN can be specified by alias.
      assertNotNull(sseDescription.getKMSMasterKeyArn());
    } else {
      if (sseDescription != null) {
        assertEquals("DISABLED", sseDescription.getStatus());
      }
    }
  }

  /**
   * This validates the table is not found in DynamoDB.
   *
   * This should not rely on the {@link DynamoDBMetadataStore} implementation.
   */
  private void verifyTableNotExist(String tableName, DynamoDB dynamoDB) throws
      Exception{
    intercept(ResourceNotFoundException.class,
        () -> dynamoDB.getTable(tableName).describe());
  }

  private String getTestTableName(String suffix) {
    return getTestDynamoTablePrefix(s3AContract.getConf()) + suffix;
  }

  @Test
  public void testPruneAgainstInvalidTable() throws Throwable {
    describe("Create an Invalid listing and prune it");
    DynamoDBMetadataStore ms
        = ITestDynamoDBMetadataStore.ddbmsStatic;
    String base = "/" + getMethodName();
    String subdir = base + "/subdir";
    Path subDirPath = strToPath(subdir);
    createNewDirs(base, subdir);

    String subFile = subdir + "/file1";
    Path subFilePath = strToPath(subFile);
    putListStatusFiles(subdir, true,
        subFile);
    final DDBPathMetadata subDirMetadataOrig = ms.get(subDirPath);
    Assertions.assertThat(subDirMetadataOrig.isAuthoritativeDir())
        .describedAs("Subdirectory %s", subDirMetadataOrig)
        .isTrue();

    // now let's corrupt the graph by putting a file
    // over the subdirectory

    long now = getTime();
    long oldTime = now - MINUTE;
    putFile(subdir, oldTime, null);
    getFile(subdir);

    Path basePath = strToPath(base);
    DirListingMetadata listing = ms.listChildren(basePath);
    String childText = listing.prettyPrint();
    LOG.info("Listing {}", childText);
    Collection<PathMetadata> childList = listing.getListing();
    Assertions.assertThat(childList)
        .as("listing of %s with %s", basePath, childText)
        .hasSize(1);
    PathMetadata[] pm = new PathMetadata[0];
    S3AFileStatus status = childList.toArray(pm)[0]
        .getFileStatus();
    Assertions.assertThat(status.isFile())
        .as("Entry %s", (Object)pm)
        .isTrue();
    getNonNull(subFile);

    LOG.info("Pruning");
    // now prune
    ms.prune(PruneMode.ALL_BY_MODTIME,
        now + MINUTE, subdir);
    ms.get(subFilePath);

    final PathMetadata subDirMetadataFinal = getNonNull(subdir);

    Assertions.assertThat(subDirMetadataFinal.getFileStatus().isFile())
        .describedAs("Subdirectory entry %s is still a file",
            subDirMetadataFinal)
        .isTrue();
  }

  @Test
  public void testPutFileDirectlyUnderTombstone() throws Throwable {
    describe("Put a file under a tombstone; verify the tombstone");
    String base = "/" + getMethodName();
    long now = getTime();
    putTombstone(base, now, null);
    PathMetadata baseMeta1 = get(base);
    Assertions.assertThat(baseMeta1.isDeleted())
        .as("Metadata %s", baseMeta1)
        .isTrue();
    String child = base + "/file";
    putFile(child, now, null);
    getDirectory(base);
  }

  @Test
  public void testPruneTombstoneUnderTombstone() throws Throwable {
    describe("Put a tombsteone under a tombstone, prune the pair");
    String base = "/" + getMethodName();
    long now = getTime();
    String dir = base + "/dir";
    putTombstone(dir, now, null);
    assertIsTombstone(dir);
    // parent dir is created
    assertCached(base);
    String child = dir + "/file";
    String child2 = dir + "/file2";

    // this will actually mark the parent as a dir,
    // so that lists of that dir will pick up the tombstone
    putTombstone(child, now, null);
    getDirectory(dir);
    // tombstone the dir
    putTombstone(dir, now, null);
    // add another child entry; this will update the dir entry from being
    // tombstone to dir
    putFile(child2, now, null);
    getDirectory(dir);

    // put a tombstone over the directory again
    putTombstone(dir, now, null);
    // verify
    assertIsTombstone(dir);

    //prune all tombstones
    getDynamoMetadataStore().prune(PruneMode.TOMBSTONES_BY_LASTUPDATED,
        now + MINUTE);

    // the child is gone
    assertNotFound(child);

    // *AND* the parent dir has not been created
    assertNotFound(dir);

    // the child2 entry is still there, though it's now orphan (the store isn't
    // meeting the rule "all entries must have a parent which exists"
    getFile(child2);
    // a full prune will still find and delete it, as this
    // doesn't walk the tree
    getDynamoMetadataStore().prune(PruneMode.ALL_BY_MODTIME,
        now + MINUTE);
    assertNotFound(child2);
    assertNotFound(dir);
  }

  @Test
  public void testPruneFileUnderTombstone() throws Throwable {
    describe("Put a file under a tombstone, prune the pair");
    String base = "/" + getMethodName();
    long now = getTime();
    String dir = base + "/dir";
    putTombstone(dir, now, null);
    assertIsTombstone(dir);
    // parent dir is created
    assertCached(base);
    String child = dir + "/file";

    // this will actually mark the parent as a dir,
    // so that lists of that dir will pick up the tombstone
    putFile(child, now, null);
    // dir is reinstated
    getDirectory(dir);

    // put a tombstone
    putTombstone(dir, now, null);
    // prune all entries
    getDynamoMetadataStore().prune(PruneMode.ALL_BY_MODTIME,
        now + MINUTE);
    // the child is gone
    assertNotFound(child);

    // *AND* the parent dir has not been created
    assertNotFound(dir);
  }


  @Test
  public void testPruneFilesNotDirs() throws Throwable {
    describe("HADOOP-16725: directories cannot be pruned");
    String base = "/" + getMethodName();
    final long now = getTime();
    // round it off for ease of interpreting results
    final long t0 = now - (now % 100_000);
    long interval = 1_000;
    long t1 = t0 + interval;
    long t2 = t1 + interval;
    String dir = base + "/dir";
    String dir2 = base + "/dir2";
    String child1 = dir + "/file1";
    String child2 = dir + "/file2";
    final Path basePath = strToPath(base);
    // put the dir at age t0
    final DynamoDBMetadataStore ms = getDynamoMetadataStore();
    final AncestorState ancestorState
        = ms.initiateBulkWrite(
            BulkOperationState.OperationType.Put,
            basePath);
    putDir(base, t0, ancestorState);
    assertLastUpdated(base, t0);

    putDir(dir, t0, ancestorState);
    assertLastUpdated(dir, t0);
    // base dir is unchanged
    assertLastUpdated(base, t0);

    // this directory will not have any children, so
    // will be excluded from any ancestor re-creation
    putDir(dir2, t0, ancestorState);

    // child1 has age t0 and so will be pruned
    putFile(child1, t0, ancestorState);

    // child2 has age t2
    putFile(child2, t2, ancestorState);

    // close the ancestor state
    ancestorState.close();

    // make some assertions about state before the prune
    assertLastUpdated(base, t0);
    assertLastUpdated(dir, t0);
    assertLastUpdated(dir2, t0);
    assertLastUpdated(child1, t0);
    assertLastUpdated(child2, t2);

    // prune all entries older than t1 must delete child1 but
    // not the directory, even though it is of the same age
    LOG.info("Starting prune of all entries older than {}", t1);
    ms.prune(PruneMode.ALL_BY_MODTIME, t1);
    // child1 is gone
    assertNotFound(child1);

    // *AND* the parent dir has not been created
    assertCached(dir);
    assertCached(child2);
    assertCached(dir2);

  }

  /**
   * A cert that there is an entry for the given key and that its
   * last updated timestamp matches that passed in.
   * @param key Key to look up.
   * @param lastUpdated Timestamp to expect.
   * @throws IOException I/O failure.
   */
  protected void assertLastUpdated(final String key, final long lastUpdated)
      throws IOException {
    PathMetadata dirMD = verifyCached(key);
    assertEquals("Last updated timestamp in MD " + dirMD,
        lastUpdated, dirMD.getLastUpdated());
  }

  /**
   * Keep in sync with code changes in S3AFileSystem.finishedWrite() so that
   * the production code can be tested here.
   */
  @Test
  public void testPutFileDeepUnderTombstone() throws Throwable {
    describe("Put a file two levels under a tombstone");
    String base = "/" + getMethodName();
    String dir = base + "/dir";
    long now = getTime();
    // creating a file MUST create its parents
    String child = dir + "/file";
    Path childPath = strToPath(child);
    putFile(child, now, null);
    getFile(child);
    getDirectory(dir);
    getDirectory(base);

    // now put the tombstone
    putTombstone(base, now, null);
    assertIsTombstone(base);

    /*- --------------------------------------------*/
    /* Begin S3FileSystem.finishedWrite() sequence. */
    /* ---------------------------------------------*/
    AncestorState ancestorState = getDynamoMetadataStore()
        .initiateBulkWrite(BulkOperationState.OperationType.Put,
            childPath);
    S3Guard.addAncestors(getDynamoMetadataStore(),
        childPath,
        getTtlTimeProvider(),
        ancestorState);
    // now write the file again.
    putFile(child, now, ancestorState);
    /* -------------------------------------------*/
    /* End S3FileSystem.finishedWrite() sequence. */
    /* -------------------------------------------*/

    getFile(child);
    // the ancestor will now exist.
    getDirectory(dir);
    getDirectory(base);
  }

  @Test
  public void testDumpTable() throws Throwable {
    describe("Dump the table contents, but not the S3 Store");
    String target = System.getProperty("test.build.dir", "target");
    File buildDir = new File(target).getAbsoluteFile();
    String name = "ITestDynamoDBMetadataStore";
    File destFile = new File(buildDir, name);
    DumpS3GuardDynamoTable.dumpStore(
        null,
        ddbmsStatic,
        getFileSystem().getConf(),
        destFile,
        fsUri);
    File storeFile = new File(buildDir, name + DumpS3GuardDynamoTable.SCAN_CSV);
    try (BufferedReader in = new BufferedReader(new InputStreamReader(
        new FileInputStream(storeFile), Charset.forName("UTF-8")))) {
      for (String line : org.apache.commons.io.IOUtils.readLines(in)) {
        LOG.info(line);
      }
    }
  }

  @Test
  public void testPurgeTableNoForce() throws Throwable {
    describe("Purge the table");

    putTombstone("/" + getMethodName(), getTime(), null);
    Pair<Long, Long> r = PurgeS3GuardDynamoTable.purgeStore(
        null,
        ddbmsStatic,
        getFileSystem().getConf(),
        fsUri,
        false);

    Assertions.assertThat(r.getLeft()).
        describedAs("entries found in %s", r)
        .isGreaterThanOrEqualTo(1);
    Assertions.assertThat(r.getRight()).
        describedAs("entries deleted in %s", r)
        .isZero();
  }

  @Test
  public void testPurgeTableForce() throws Throwable {
    describe("Purge the table -force");
    putTombstone("/" + getMethodName(), getTime(), null);
    Pair<Long, Long> r = PurgeS3GuardDynamoTable.purgeStore(
        null,
        ddbmsStatic,
        getFileSystem().getConf(),
        fsUri,
        true);
    Assertions.assertThat(r.getLeft()).
        describedAs("entries found in %s", r)
        .isGreaterThanOrEqualTo(1);
    Assertions.assertThat(r.getRight()).
        describedAs("entries deleted in %s", r)
        .isEqualTo(r.getLeft());

    // second iteration will have zero entries

    r = PurgeS3GuardDynamoTable.purgeStore(
        null,
        ddbmsStatic,
        getFileSystem().getConf(),
        fsUri,
        true);
    Assertions.assertThat(r.getLeft()).
        describedAs("entries found in %s", r)
        .isZero();
    Assertions.assertThat(r.getRight()).
        describedAs("entries deleted in %s", r)
        .isZero();
  }

  /**
   * Assert that an entry exists and is a directory.
   * @param pathStr path
   * @throws IOException IO failure.
   */
  protected DDBPathMetadata verifyAuthDirStatus(String pathStr,
      boolean authDirFlag)
      throws IOException {
    DDBPathMetadata md = (DDBPathMetadata) getDirectory(pathStr);
    assertEquals("isAuthoritativeDir() mismatch in " + md,
        authDirFlag,
        md.isAuthoritativeDir());
    return md;
  }
}
