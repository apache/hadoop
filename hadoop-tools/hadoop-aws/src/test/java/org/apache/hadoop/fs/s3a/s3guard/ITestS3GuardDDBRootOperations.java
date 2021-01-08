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

import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3AUtils.applyLocatedFiles;

/**
 * This test run against the root of the FS, and operations which span the DDB
 * table and the filesystem.
 * For this reason, these tests are executed in the sequential phase of the
 * integration tests.
 * <p>
 * The tests only run if DynamoDB is the metastore.
 * <p></p>
 * The marker policy is fixed to "delete"
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestS3GuardDDBRootOperations extends AbstractS3ATestBase {

  private StoreContext storeContext;

  private String fsUriStr;

  private DynamoDBMetadataStore metastore;

  private String metastoreUriStr;

  // this is a switch you can change in your IDE to enable
  // or disable those tests which clean up the metastore.
  private final boolean cleaning = true;

  /**
   * The test timeout is increased in case previous tests have created
   * many tombstone markers which now need to be purged.
   * @return the test timeout.
   */
  @Override
  protected int getTestTimeoutMillis() {
    return SCALE_TEST_TIMEOUT_SECONDS * 1000;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    disableFilesystemCaching(conf);

    removeBucketOverrides(bucketName, conf,
        S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
        ENABLE_MULTI_DELETE,
        DIRECTORY_MARKER_POLICY);
    conf.set(DIRECTORY_MARKER_POLICY,
        DIRECTORY_MARKER_POLICY_DELETE);
    // set a sleep time of 0 on pruning, for speedier test runs.
    conf.setTimeDuration(
        S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
        0,
        TimeUnit.MILLISECONDS);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    S3ATestUtils.assumeS3GuardState(true, conf);
    storeContext = fs.createStoreContext();
    assume("Filesystem isn't running DDB",
        storeContext.getMetadataStore() instanceof DynamoDBMetadataStore);
    metastore = (DynamoDBMetadataStore) storeContext.getMetadataStore();
    URI fsURI = storeContext.getFsURI();
    fsUriStr = fsURI.toString();
    if (!fsUriStr.endsWith("/")) {
      fsUriStr = fsUriStr + "/";
    }
    metastoreUriStr = "dynamodb://" + metastore.getTableName() + "/";
  }

  @Override
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    super.teardown();
  }

  private void assumeCleaningOperation() {
    assume("Cleaning operation skipped", cleaning);
  }

  @Test
  @Ignore
  public void test_050_dump_metastore() throws Throwable {
    File destFile = calculateDumpFileBase();
    describe("Dumping S3Guard store under %s", destFile);
    DumpS3GuardDynamoTable.dumpStore(
        null,
        metastore,
        getConfiguration(),
        destFile,
        getFileSystem().getUri());
  }

  @Test
  public void test_060_dump_metastore_and_s3() throws Throwable {
    File destFile = calculateDumpFileBase();
    describe("Dumping S3Guard store under %s", destFile);
    DumpS3GuardDynamoTable.dumpStore(
        getFileSystem(),
        metastore,
        getConfiguration(),
        destFile,
        getFileSystem().getUri());
  }

  @Test
  public void test_100_FilesystemPrune() throws Throwable {
    describe("Execute prune against a filesystem URI");
    assumeCleaningOperation();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    int result = S3GuardTool.run(conf,
        S3GuardTool.Prune.NAME,
        "-seconds", "1",
        fsUriStr);
    Assertions.assertThat(result)
        .describedAs("Result of prune %s", fsUriStr)
        .isEqualTo(0);
  }


  @Test
  public void test_200_MetastorePruneTombstones() throws Throwable {
    describe("Execute prune against a dynamo URL");
    assumeCleaningOperation();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    int result = S3GuardTool.run(conf,
        S3GuardTool.Prune.NAME,
        "-tombstone",
        "-meta", checkNotNull(metastoreUriStr),
        "-seconds", "1",
        fs.qualify(new Path("/")).toString());
    Assertions.assertThat(result)
        .describedAs("Result of prune %s", fsUriStr)
        .isEqualTo(0);
  }

  @Test
  public void test_300_MetastorePrune() throws Throwable {
    describe("Execute prune against a dynamo URL");
    assumeCleaningOperation();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    int result = S3GuardTool.run(conf,
        S3GuardTool.Prune.NAME,
        "-meta", checkNotNull(metastoreUriStr),
        "-region", fs.getBucketLocation(),
        "-seconds", "1");
    Assertions.assertThat(result)
        .describedAs("Result of prune %s", fsUriStr)
        .isEqualTo(0);
  }

  @Test
  public void test_400_rm_root_recursive() throws Throwable {
    describe("Remove the root directory");
    assumeCleaningOperation();
    S3AFileSystem fs = getFileSystem();
    Path root = new Path("/");
    Path file = new Path("/test_400_rm_root_recursive-01");
    Path file2 = new Path("/test_400_rm_root_recursive-02");
    // recursive treewalk to delete all files
    // does not delete directories.
    applyLocatedFiles(fs.listFilesAndEmptyDirectories(root, true),
        f -> {
          Path p = f.getPath();
          fs.delete(p, true);
          assertPathDoesNotExist("expected file to be deleted", p);
        });
    ContractTestUtils.deleteChildren(fs, root, true);
    // everything must be done by now
    StringBuffer sb = new StringBuffer();
    AtomicInteger foundFile = new AtomicInteger(0);
    applyLocatedFiles(fs.listFilesAndEmptyDirectories(root, true),
        f -> {
          foundFile.addAndGet(1);
          Path p = f.getPath();
          sb.append(f.isDirectory()
              ? "Dir  "
              : "File ")
            .append(p);
          if (!f.isDirectory()) {
            sb.append("[").append(f.getLen()).append("]");
          }

          fs.delete(p, true);
        });

    assertEquals("Remaining files " + sb,
        0, foundFile.get());
    try {
      ContractTestUtils.touch(fs, file);
      assertDeleted(file, false);


      assertFalse("Root directory delete failed",
          fs.delete(root, true));

      ContractTestUtils.touch(fs, file2);
      assertFalse("Root directory delete should have failed",
          fs.delete(root, true));
    } finally {
      fs.delete(file, false);
      fs.delete(file2, false);
    }
  }

  @Test
  @Ignore
  public void test_600_dump_metastore() throws Throwable {
    File destFile = calculateDumpFileBase();
    describe("Dumping S3Guard store under %s", destFile);
    DumpS3GuardDynamoTable.dumpStore(
        getFileSystem(),
        metastore,
        getConfiguration(),
        destFile,
        getFileSystem().getUri());
  }

  protected File calculateDumpFileBase() {
    String target = System.getProperty("test.build.dir", "target");
    File buildDir = new File(target,
        this.getClass().getSimpleName()).getAbsoluteFile();
    buildDir.mkdirs();
    return new File(buildDir, getMethodName());
  }
}
