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
import java.net.URI;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.MockS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.keySchema;

/**
 * Test that {@link DynamoDBMetadataStore} implements {@link MetadataStore}.
 *
 * In this unit test, we create an in-memory DynamoDBLocal server instance for
 * all unit test cases.  You won't be charged bills for DynamoDB requests when
 * you run this test.  An {@link S3AFileSystem} object is created and shared for
 * initializing {@link DynamoDBMetadataStore} objects.  There are no real S3
 * request issued as the underlying AWS S3Client is mocked.
 *
 * According to the base class, every test case will have independent contract
 * to create a new {@link DynamoDBMetadataStore} instance and initializes it.
 * A table will be created for each test by the test contract, and will be
 * destroyed after the test case finishes.
 */
public class TestDynamoDBMetadataStore extends MetadataStoreTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDynamoDBMetadataStore.class);
  private static final String BUCKET = "TestDynamoDBMetadataStore";

  /** The DynamoDBLocal dynamoDBLocalServer instance for testing. */
  private static DynamoDBProxyServer dynamoDBLocalServer;
  private static String ddbEndpoint;
  /** The DynamoDB instance that can issue requests directly to server. */
  private static DynamoDB dynamoDB;

  @Rule
  public final Timeout timeout = new Timeout(60 * 1000);

  /**
   * Sets up the in-memory DynamoDBLocal server and initializes s3 file system.
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    GenericTestUtils.setLogLevel(DynamoDBMetadataStore.LOG, Level.ALL);
    // sqlite4java library should have been copied to target/native-libs
    System.setProperty("sqlite4java.library.path", "target/native-libs");

    // Set up the in-memory local DynamoDB instance for all test cases
    final String port = String.valueOf(ServerSocketUtil.getPort(0, 100));
    dynamoDBLocalServer = ServerRunner.createServerFromCommandLineArgs(
        new String[] {"-inMemory", "-port", port});
    dynamoDBLocalServer.start();
    ddbEndpoint = "http://localhost:" + port;
    LOG.info("DynamoDBLocal for test was started at {}", ddbEndpoint);

    try {
      dynamoDB = new DynamoDBMSContract().getMetadataStore().getDynamoDB();
    } catch (AmazonServiceException e) {
      final String msg = "Cannot initialize a DynamoDBMetadataStore instance "
          + "against the local DynamoDB server. Perhaps the DynamoDBLocal "
          + "server is not configured correctly. ";
      LOG.error(msg, e);
      // fail fast if the DynamoDBLocal server can not work
      fail(msg + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (dynamoDB != null) {
      dynamoDB.shutdown();
    }
    if (dynamoDBLocalServer != null) {
      LOG.info("Shutting down the in-memory local DynamoDB server");
      try {
        dynamoDBLocalServer.stop();
      } catch (Exception e) {
        final String msg = "Got exception to stop the DynamoDBLocal server. ";
        LOG.error(msg, e);
        fail(msg + e.getLocalizedMessage());
      }
    }
  }

  /**
   * Each contract has its own S3AFileSystem and DynamoDBMetadataStore objects.
   */
  private static class DynamoDBMSContract extends AbstractMSContract {
    private final S3AFileSystem s3afs;
    private final DynamoDBMetadataStore ms = new DynamoDBMetadataStore();

    DynamoDBMSContract() throws IOException {
      final Configuration conf = new Configuration();
      // using mocked S3 clients
      conf.setClass(S3_CLIENT_FACTORY_IMPL, MockS3ClientFactory.class,
          S3ClientFactory.class);
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
          URI.create(Constants.FS_S3A + "://" + BUCKET).toString());
      // setting config for creating a DynamoDBClient against local server
      conf.set(Constants.ACCESS_KEY, "dummy-access-key");
      conf.set(Constants.SECRET_KEY, "dummy-secret-key");
      conf.set(Constants.S3GUARD_DDB_ENDPOINT_KEY, ddbEndpoint);
      conf.setBoolean(Constants.S3GUARD_DDB_TABLE_CREATE_KEY, true);

      // always create new file system object for a test contract
      s3afs = (S3AFileSystem) FileSystem.newInstance(conf);
      ms.initialize(s3afs);
    }

    @Override
    public S3AFileSystem getFileSystem() {
      return s3afs;
    }

    @Override
    public DynamoDBMetadataStore getMetadataStore() {
      return ms;
    }
  }

  @Override
  public DynamoDBMSContract createContract() throws IOException {
    return new DynamoDBMSContract();
  }

  @Override
  FileStatus basicFileStatus(Path path, int size, boolean isDir)
      throws IOException {
    String owner = UserGroupInformation.getCurrentUser().getShortUserName();
    return isDir
        ? new S3AFileStatus(false, path, owner)
        : new S3AFileStatus(size, getModTime(), path, BLOCK_SIZE, owner);
  }

  /**
   * This tests that after initialize() using an S3AFileSystem object, the
   * instance should have been initialized successfully, and tables are ACTIVE.
   */
  @Test
  public void testInitialize() throws IOException {
    final String tableName = "testInitializeWithFileSystem";
    final S3AFileSystem s3afs = createContract().getFileSystem();
    final Configuration conf = s3afs.getConf();
    conf.set(Constants.S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(s3afs);
      verifyTableInitialized(tableName);
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());
      assertEquals("DynamoDB table should be in the same region as S3 bucket",
          s3afs.getAmazonS3Client().getBucketLocation(tableName),
          ddbms.getRegion());
    }
  }

  /**
   * This tests that after initialize() using a Configuration object, the
   * instance should have been initialized successfully, and tables are ACTIVE.
   */
  @Test
  public void testInitializeWithConfiguration() throws IOException {
    final String tableName = "testInitializeWithConfiguration";
    final Configuration conf = createContract().getFileSystem().getConf();
    conf.set(Constants.S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(conf);
      verifyTableInitialized(tableName);
      assertNotNull(ddbms.getTable());
      assertEquals(tableName, ddbms.getTable().getTableName());
      assertEquals("Unexpected key schema found!",
          keySchema(),
          ddbms.getTable().describe().getKeySchema());
    }
  }

  @Test
  public void testCreateExistingTable() throws IOException {
    final DynamoDBMetadataStore ddbms = createContract().getMetadataStore();
    verifyTableInitialized(BUCKET);
    // create existing table
    ddbms.createTable();
    verifyTableInitialized(BUCKET);
  }

  /**
   * Test cases about root directory as it is not in the DynamoDB table.
   */
  @Test
  public void testRootDirectory() throws IOException {
    final DynamoDBMetadataStore ddbms = createContract().getMetadataStore();
    verifyRootDirectory(ddbms.get(new Path("/")), true);

    ddbms.put(new PathMetadata(new S3AFileStatus(true,
        new Path("/foo"),
        UserGroupInformation.getCurrentUser().getShortUserName())));
    verifyRootDirectory(ddbms.get(new Path("/")), false);
  }

  private void verifyRootDirectory(PathMetadata rootMeta, boolean isEmpty) {
    assertNotNull(rootMeta);
    final S3AFileStatus status = (S3AFileStatus) rootMeta.getFileStatus();
    assertNotNull(status);
    assertTrue(status.isDirectory());
    assertEquals(isEmpty, status.isEmptyDirectory());
  }

  @Test
  public void testProvisionTable() throws IOException {
    final DynamoDBMetadataStore ddbms = createContract().getMetadataStore();
    final ProvisionedThroughputDescription oldProvision =
        dynamoDB.getTable(BUCKET).describe().getProvisionedThroughput();
    ddbms.provisionTable(oldProvision.getReadCapacityUnits() * 2,
        oldProvision.getWriteCapacityUnits() * 2);
    final ProvisionedThroughputDescription newProvision =
        dynamoDB.getTable(BUCKET).describe().getProvisionedThroughput();
    LOG.info("Old provision = {}, new provision = {}",
        oldProvision, newProvision);
    assertEquals(oldProvision.getReadCapacityUnits() * 2,
        newProvision.getReadCapacityUnits().longValue());
    assertEquals(oldProvision.getWriteCapacityUnits() * 2,
        newProvision.getWriteCapacityUnits().longValue());
  }

  @Test
  public void testDeleteTable() throws IOException {
    final String tableName = "testDeleteTable";
    final S3AFileSystem s3afs = createContract().getFileSystem();
    final Configuration conf = s3afs.getConf();
    conf.set(Constants.S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(s3afs);
      // we can list the empty table
      ddbms.listChildren(new Path("/"));

      ddbms.destroy();
      verifyTableNotExist(tableName);

      // delete table once more; be ResourceNotFoundException swallowed silently
      ddbms.destroy();
      verifyTableNotExist(tableName);

      try {
        // we can no longer list the destroyed table
        ddbms.listChildren(new Path("/"));
        fail("Should have failed after the table is destroyed!");
      } catch (IOException ignored) {
      }
    }
  }

  /**
   * This validates the table is created and ACTIVE in DynamoDB.
   *
   * This should not rely on the {@link DynamoDBMetadataStore} implementation.
   */
  private static void verifyTableInitialized(String tableName) {
    final Table table = dynamoDB.getTable(tableName);
    final TableDescription td = table.describe();
    assertEquals(tableName, td.getTableName());
    assertEquals("ACTIVE", td.getTableStatus());
  }

  /**
   * This validates the table is not found in DynamoDB.
   *
   * This should not rely on the {@link DynamoDBMetadataStore} implementation.
   */
  private static void verifyTableNotExist(String tableName) {
    final Table table = dynamoDB.getTable(tableName);
    try {
      table.describe();
      fail("Expecting ResourceNotFoundException for table '" + tableName + "'");
    } catch (ResourceNotFoundException ignored) {
    }
  }

}
