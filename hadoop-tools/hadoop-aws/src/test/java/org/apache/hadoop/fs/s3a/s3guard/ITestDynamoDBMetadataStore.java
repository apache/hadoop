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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import com.amazonaws.services.dynamodbv2.model.Tag;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.Tristate;

import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
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

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.clearBucketOption;
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

  public ITestDynamoDBMetadataStore() {
    super();
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestDynamoDBMetadataStore.class);
  public static final PrimaryKey
      VERSION_MARKER_PRIMARY_KEY = createVersionMarkerPrimaryKey(
      DynamoDBMetadataStore.VERSION_MARKER);

  private S3AFileSystem fileSystem;
  private S3AContract s3AContract;

  private URI fsUri;

  private String bucket;

  private static DynamoDBMetadataStore ddbmsStatic;

  private static String testDynamoDBTableName;

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
          + "deleted before trying again.");
      ddbmsStatic.getTable().waitForDelete();
      super.setUp();
    }
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
    Assume.assumeTrue("No DynamoDB table name configured",
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

    LOG.debug("Creating static ddbms which will be shared between tests.");
    enableOnDemand(conf);

    ddbmsStatic = new DynamoDBMetadataStore();
    ddbmsStatic.initialize(conf);
  }

  @AfterClass
  public static void afterClassTeardown() {
    LOG.debug("Destroying static DynamoDBMetadataStore.");
    if (ddbmsStatic != null) {
      try {
        ddbmsStatic.destroy();
      } catch (Exception e) {
        LOG.warn("Failed to destroy tables in teardown", e);
      }
      IOUtils.closeStream(ddbmsStatic);
      ddbmsStatic = null;
    }
  }

  private static void assumeThatDynamoMetadataStoreImpl(Configuration conf){
    Assume.assumeTrue("Test only applies when DynamoDB is used for S3Guard",
        conf.get(Constants.S3_METADATA_STORE_IMPL).equals(
            Constants.S3GUARD_METASTORE_DYNAMO));
  }


  @Override
  public void tearDown() throws Exception {
    try {
      if (ddbmsStatic != null) {
        LOG.info("Removing data from ddbms table in teardown.");
        // The following is a way to be sure the table will be cleared and there
        // will be no leftovers after the test.
        PathMetadata meta = ddbmsStatic.get(strToPath("/"));
        if (meta != null){
          for (DescendantsIterator desc =
              new DescendantsIterator(ddbmsStatic, meta);
              desc.hasNext();) {
            ddbmsStatic.forgetMetadata(desc.next().getPath());
          }
        }
      }
    } catch (IOException ignored) {
    }
    IOUtils.cleanupWithLogger(LOG, fileSystem);
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
  S3AFileStatus basicFileStatus(Path path, int size, boolean isDir)
      throws IOException {
    String owner = UserGroupInformation.getCurrentUser().getShortUserName();
    return isDir
        ? new S3AFileStatus(true, path, owner)
        : new S3AFileStatus(size, getModTime(), path, BLOCK_SIZE, owner,
            null, null);
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
      ddbms.initialize(s3afs);
      verifyTableInitialized(tableName, ddbms.getDynamoDB());
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());
      String expectedRegion = conf.get(S3GUARD_DDB_REGION_KEY,
          s3afs.getBucketLocation(bucket));
      assertEquals("DynamoDB table should be in configured region or the same" +
              " region as S3 bucket",
          expectedRegion,
          ddbms.getRegion());
    } finally {
      ddbms.destroy();
      ddbms.close();
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
      ddbms.initialize(conf);
      fail("Should have failed because the table name is not set!");
    } catch (IllegalArgumentException ignored) {
    }
    // config table name
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(conf);
      fail("Should have failed because as the region is not set!");
    } catch (IllegalArgumentException ignored) {
    }
    // config region
    conf.set(S3GUARD_DDB_REGION_KEY, savedRegion);
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf);
      verifyTableInitialized(tableName, ddbms.getDynamoDB());
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());
      assertEquals("Unexpected key schema found!",
          keySchema(),
          ddbms.getTable().describe().getKeySchema());
    } finally {
      ddbms.destroy();
      ddbms.close();
    }
  }

  /**
   * Test that for a large batch write request, the limit is handled correctly.
   */
  @Test
  public void testBatchWrite() throws IOException {
    final int[] numMetasToDeleteOrPut = {
        -1, // null
        0, // empty collection
        1, // one path
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT, // exact limit of a batch request
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT + 1 // limit + 1
    };
    DynamoDBMetadataStore ms = getDynamoMetadataStore();
    for (int numOldMetas : numMetasToDeleteOrPut) {
      for (int numNewMetas : numMetasToDeleteOrPut) {
        doTestBatchWrite(numOldMetas, numNewMetas, ms);
      }
    }
  }

  private void doTestBatchWrite(int numDelete, int numPut,
      DynamoDBMetadataStore ms) throws IOException {
    Path path = new Path(
        "/ITestDynamoDBMetadataStore_testBatchWrite_" + numDelete + '_'
            + numPut);
    final Path root = fileSystem.makeQualified(path);
    final Path oldDir = new Path(root, "oldDir");
    final Path newDir = new Path(root, "newDir");
    LOG.info("doTestBatchWrite: oldDir={}, newDir={}", oldDir, newDir);

    ms.put(new PathMetadata(basicFileStatus(oldDir, 0, true)));
    ms.put(new PathMetadata(basicFileStatus(newDir, 0, true)));

    final List<PathMetadata> oldMetas = numDelete < 0 ? null :
        new ArrayList<>(numDelete);
    for (int i = 0; i < numDelete; i++) {
      oldMetas.add(new PathMetadata(
          basicFileStatus(new Path(oldDir, "child" + i), i, true)));
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
      ms.put(new DirListingMetadata(oldDir, oldMetas, false));
      assertEquals(0, ms.listChildren(newDir).withoutTombstones().numEntries());
      assertTrue(CollectionUtils
          .isEqualCollection(oldMetas, ms.listChildren(oldDir).getListing()));

      pathsToDelete = new ArrayList<>(oldMetas.size());
      for (PathMetadata meta : oldMetas) {
        pathsToDelete.add(meta.getFileStatus().getPath());
      }
    }

    // move the old paths to new paths and verify
    ms.move(pathsToDelete, newMetas);
    assertEquals(0, ms.listChildren(oldDir).withoutTombstones().numEntries());
    if (newMetas != null) {
      assertTrue(CollectionUtils
          .isEqualCollection(newMetas, ms.listChildren(newDir).getListing()));
    }
  }

  @Test
  public void testInitExistingTable() throws IOException {
    final DynamoDBMetadataStore ddbms = getDynamoMetadataStore();
    final String tableName = ddbms.getTable().getTableName();
    verifyTableInitialized(tableName, ddbms.getDynamoDB());
    // create existing table
    ddbms.initTable();
    verifyTableInitialized(tableName, ddbms.getDynamoDB());
  }

  /**
   * Test the low level version check code.
   */
  @Test
  public void testItemVersionCompatibility() throws Throwable {
    verifyVersionCompatibility("table",
        createVersionMarker(VERSION_MARKER, VERSION, 0));
  }

  /**
   * Test that a version marker entry without the version number field
   * is rejected as incompatible with a meaningful error message.
   */
  @Test
  public void testItemLacksVersion() throws Throwable {
    intercept(IOException.class, E_NOT_VERSION_MARKER,
        () -> verifyVersionCompatibility("table",
            new Item().withPrimaryKey(
                createVersionMarkerPrimaryKey(VERSION_MARKER))));
  }

  /**
   * Delete the version marker and verify that table init fails.
   */
  @Test
  public void testTableVersionRequired() throws Exception {
    String tableName = getTestTableName("testTableVersionRequired");
    Configuration conf = getTableCreationConfig();
    int maxRetries = conf.getInt(S3GUARD_DDB_MAX_RETRIES,
        S3GUARD_DDB_MAX_RETRIES_DEFAULT);
    conf.setInt(S3GUARD_DDB_MAX_RETRIES, 3);
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);

    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf);
      Table table = verifyTableInitialized(tableName, ddbms.getDynamoDB());
      table.deleteItem(VERSION_MARKER_PRIMARY_KEY);

      // create existing table
      intercept(IOException.class, E_NO_VERSION_MARKER,
          () -> ddbms.initTable());

      conf.setInt(S3GUARD_DDB_MAX_RETRIES, maxRetries);
    } finally {
      ddbms.destroy();
      ddbms.close();
    }
  }

  /**
   * Set the version value to a different number and verify that
   * table init fails.
   */
  @Test
  public void testTableVersionMismatch() throws Exception {
    String tableName = getTestTableName("testTableVersionMismatch");
    Configuration conf = getTableCreationConfig();
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);

    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf);
      Table table = verifyTableInitialized(tableName, ddbms.getDynamoDB());
      table.deleteItem(VERSION_MARKER_PRIMARY_KEY);
      Item v200 = createVersionMarker(VERSION_MARKER, 200, 0);
      table.putItem(v200);

      // create existing table
      intercept(IOException.class, E_INCOMPATIBLE_VERSION,
          () -> ddbms.initTable());
    } finally {
      ddbms.destroy();
      ddbms.close();
    }
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
      ddbms.initialize(s3afs);
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
        UserGroupInformation.getCurrentUser().getShortUserName())));
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
    ddbms.put(new PathMetadata(basicFileStatus(nestedPath1, 1024, false)));
    final Path nestedPath2 = strToPath(srcRoot + "/dir1/dir2");
    ddbms.put(new PathMetadata(basicFileStatus(nestedPath2, 0, true)));

    // We don't put the destRoot path here, since put() would create ancestor
    // entries, and we want to ensure that move() does it, instead.

    // Build enumeration of src / dest paths and do the move()
    final Collection<Path> fullSourcePaths = Lists.newArrayList(
        strToPath(srcRoot),
        strToPath(srcRoot + "/dir1"),
        strToPath(srcRoot + "/dir1/dir2"),
        strToPath(srcRoot + "/file1.txt")
    );
    final Collection<PathMetadata> pathsToCreate = Lists.newArrayList(
        new PathMetadata(basicFileStatus(strToPath(destRoot),
            0, true)),
        new PathMetadata(basicFileStatus(strToPath(destRoot + "/dir1"),
            0, true)),
        new PathMetadata(basicFileStatus(strToPath(destRoot + "/dir1/dir2"),
            0, true)),
        new PathMetadata(basicFileStatus(strToPath(destRoot + "/file1.txt"),
            1024, false))
    );

    ddbms.move(fullSourcePaths, pathsToCreate);

    // assert that all the ancestors should have been populated automatically
    assertCached(testRoot + "/c");
    assertCached(testRoot + "/c/d");
    assertCached(testRoot + "/c/d/e");
    assertCached(destRoot /* /c/d/e/dest */);

    // Also check moved files while we're at it
    assertCached(destRoot + "/dir1");
    assertCached(destRoot + "/dir1/dir2");
    assertCached(destRoot + "/file1.txt");
  }

  @Test
  public void testProvisionTable() throws Exception {
    final String tableName
        = getTestTableName("testProvisionTable-" + UUID.randomUUID());
    final Configuration conf = getTableCreationConfig();
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    conf.setInt(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY, 2);
    conf.setInt(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY, 2);
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf);
      DynamoDB dynamoDB = ddbms.getDynamoDB();
      final DDBCapacities oldProvision = DDBCapacities.extractCapacities(
          dynamoDB.getTable(tableName).describe().getProvisionedThroughput());
      Assume.assumeFalse("Table is on-demand", oldProvision.isOnDemandTable());
      long desiredReadCapacity = oldProvision.getRead() - 1;
      long desiredWriteCapacity = oldProvision.getWrite() - 1;
      ddbms.provisionTable(desiredReadCapacity,
          desiredWriteCapacity);
      ddbms.initTable();
      // we have to wait until the provisioning settings are applied,
      // so until the table is ACTIVE again and not in UPDATING
      ddbms.getTable().waitForActive();
      final DDBCapacities newProvision = DDBCapacities.extractCapacities(
          dynamoDB.getTable(tableName).describe().getProvisionedThroughput());
      assertEquals("Check newly provisioned table read capacity units.",
          desiredReadCapacity,
          newProvision.getRead());
      assertEquals("Check newly provisioned table write capacity units.",
          desiredWriteCapacity,
          newProvision.getWrite());
    } finally {
      ddbms.destroy();
      ddbms.close();
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    final String tableName = getTestTableName("testDeleteTable");
    Path testPath = new Path(new Path(fsUri), "/" + tableName);
    final S3AFileSystem s3afs = getFileSystem();
    final Configuration conf = getTableCreationConfig();
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    enableOnDemand(conf);
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(s3afs);
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
    } finally {
      ddbms.destroy();
      ddbms.close();
    }
  }

  @Test
  public void testTableTagging() throws IOException {
    final Configuration conf = getTableCreationConfig();
    // clear all table tagging config before this test
    conf.getPropsWithPrefix(S3GUARD_DDB_TABLE_TAG).keySet().forEach(
        propKey -> conf.unset(S3GUARD_DDB_TABLE_TAG + propKey)
    );

    String tableName =
        getTestTableName("testTableTagging-" + UUID.randomUUID());
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    conf.set(S3GUARD_DDB_TABLE_CREATE_KEY, "true");

    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("hello", "dynamo");
    tagMap.put("tag", "youre it");
    for (Map.Entry<String, String> tagEntry : tagMap.entrySet()) {
      conf.set(S3GUARD_DDB_TABLE_TAG + tagEntry.getKey(), tagEntry.getValue());
    }

    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    try {
      ddbms.initialize(conf);
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());
      ListTagsOfResourceRequest listTagsOfResourceRequest =
          new ListTagsOfResourceRequest()
              .withResourceArn(ddbms.getTable().getDescription().getTableArn());
      List<Tag> tags = ddbms.getAmazonDynamoDB()
          .listTagsOfResource(listTagsOfResourceRequest).getTags();
      assertEquals(tagMap.size(), tags.size());
      for (Tag tag : tags) {
        Assert.assertEquals(tagMap.get(tag.getKey()), tag.getValue());
      }
    } finally {
      ddbms.destroy();
      ddbms.close();
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
    String rootPath = "/testAuthoritativeEmptyDirFlag"+ UUID.randomUUID();
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
    ms.put(dlm);
    final PathMetadata pmdResultEmpty = ms.get(dirToPut, true);
    if(auth){
      assertEquals(Tristate.TRUE, pmdResultEmpty.isEmptyDirectory());
    } else {
      assertEquals(Tristate.UNKNOWN, pmdResultEmpty.isEmptyDirectory());
    }

    // Test with non-authoritative listing, non-empty dir
    dlm.put(basicFileStatus(fileToPut, 1, false));
    ms.put(dlm);
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
}
