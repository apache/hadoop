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

package org.apache.hadoop.fs.contract.s3a;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.fs.test.formats.AbstractIcebergDeleteTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathsDoNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertSuccessfulBulkDelete;
import static org.apache.hadoop.fs.contract.ContractTestUtils.getFileStatusOrNull;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_PERFORMANCE_FLAGS;
import static org.apache.hadoop.fs.s3a.Constants.PERFORMANCE_FLAGS;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.FS_S3A_IMPL_DISABLE_CACHE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.createFiles;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.io.wrappedio.WrappedIO.bulkDelete_delete;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Iceberg Bulk Delete API.
 * <p>
 * Parameterized on Iceberg bulk delete enabled/disabled and
 * s3a multipart delete enabled/disabled.
 */
@RunWith(Parameterized.class)
public class ITestIcebergBulkDelete extends AbstractIcebergDeleteTest {

  private static final Logger LOG = LoggerFactory.getLogger(ITestIcebergBulkDelete.class);

  /**
   * Parallelism when using the classic multi-thread bulk delete.
   */
  private static final String ICEBERG_DELETE_FILE_PARALLELISM =
      "iceberg.hadoop.delete-file-parallelism";

  /** Is bulk delete enabled on hadoop runtimes with API support: {@value}. */
  public static final String ICEBERG_BULK_DELETE_ENABLED = "iceberg.hadoop.bulk.delete.enabled";

  private static final int DELETE_PAGE_SIZE = 3;

  private static final int DELETE_FILE_COUNT = 7;

  @Parameterized.Parameters(name = "multiobjectdelete-{0}-usebulk-{1}")
  public static Iterable<Object[]> enableMultiObjectDelete() {
    return Arrays.asList(new Object[][]{
        {true, true},
        {true, false},
        {false, true},
        {false, false}
    });
  }

  /**
   * Enable s3a multi object delete.
   */
  private final boolean enableMultiObjectDelete;

  /**
   * Enable bulk delete in iceberg.
   */
  private final boolean useBulk;

  public ITestIcebergBulkDelete(boolean enableMultiObjectDelete, final boolean useBulk) {
    this.enableMultiObjectDelete = enableMultiObjectDelete;
    this.useBulk = useBulk;
  }

  @Override
  public void setup() throws Exception {
    // close all filesystems.
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());

    // the create the single new one
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf = propagateBucketOptions(conf, getTestBucketName(conf));
    removeBaseAndBucketOverrides(conf,
        BULK_DELETE_PAGE_SIZE);
    // turn the caching on else every call refreshes the cache
    conf.setBoolean(FS_S3A_IMPL_DISABLE_CACHE, false);
    conf.setInt(BULK_DELETE_PAGE_SIZE, DELETE_PAGE_SIZE);

    // skip this test run if multi-delete is explicitly disabled;
    // this is needed to test against third party stores
    // which do not support it.
    if (enableMultiObjectDelete) {
      skipIfNotEnabled(conf, ENABLE_MULTI_DELETE, "multi object delete is disabled");
    }
    conf.setBoolean(ENABLE_MULTI_DELETE, enableMultiObjectDelete);
    conf.setBoolean(ICEBERG_BULK_DELETE_ENABLED, useBulk);
    conf.setInt(ICEBERG_DELETE_FILE_PARALLELISM, 5);
    // speed up file/dir creation
    conf.set(FS_S3A_PERFORMANCE_FLAGS, PERFORMANCE_FLAGS);
    return conf;
  }


  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(createConfiguration());
  }

  @Override
  protected int getExpectedPageSize() {
    return enableMultiObjectDelete
        ? 1
        : DELETE_PAGE_SIZE;
  }

  /**
   * Create file IO; includes an assert that bulk delete is enabled.
   * @return a file iO
   */
  private HadoopFileIO createFileIO() {
    final Configuration conf = getFileSystem().getConf();

    final HadoopFileIO fileIO = new HadoopFileIO(conf);
    // assert that bulk delete loaded.
    Assertions.assertThat(fileIO.isBulkDeleteApiUsed())
        .describedAs("is HadoopFileIO able to load Hadoop bulk delete")
        .isEqualTo(useBulk);
    return fileIO;
  }

  /**
   * Delete a single file using the bulk delete API.
   */
  @Test
  public void testDeleteSingleFile() throws Throwable {
    Path path = new Path(methodPath(), "../single");
    try (HadoopFileIO fileIO = createFileIO()) {
      final List<String> filename = stringList(path);
      LOG.info("Deleting empty path");
      fileIO.deleteFiles(filename);
      // now one file
      final FileSystem fs = getFileSystem();
      touch(fs, path);
      LOG.info("Deleting file at {}", filename);
      fileIO.deleteFiles(filename);
      assertPathsDoNotExist(fs, "should have been deleted", path);
    }
  }

  /**
   * A directory is not deleted through the bulk delete API,
   * but does not report a failure.
   * The classic invocation mechanism reports a failure.
   */
  @Test
  public void testDeleteDirectory() throws Throwable {
    Path path = methodPath();

    try (HadoopFileIO fileIO = createFileIO()) {
      final List<String> filename = stringList(path);

      // create a directory and a child underneath
      Path child = new Path(path, "child");
      final FileSystem fs = getFileSystem();
      fs.mkdirs(path);
      touch(fs, child);

      LOG.info("Deleting path to directory");
      if (useBulk) {
        fileIO.deleteFiles(filename);
      } else {
        final BulkDeletionFailureException ex =
            intercept(BulkDeletionFailureException.class, () ->
                fileIO.deleteFiles(filename));
        Assertions.assertThat(ex.numberFailedObjects())
            .describedAs("Failure count in %s", ex)
            .isEqualTo(1);
      }
      // Reported failure or not, the directory is still found
      assertPathExists("directory was not deleted", path);
    }
  }

  /**
   * A directory is not deleted through the bulk delete API,
   * it is through the classic single file delete.
   * The assertions match this behavior.
   * <p>
   * Note that the semantics of FileSystem.delete(path, nonrecursive)
   * have special handling of deleting an empty directory, where
   * it is allowed (as with unix cli rm), so a child file
   * is created to force stricter semantics.
   */
  @Test
  public void testDeleteDirectoryDirect() throws Throwable {
    //Path path = new Path(methodPath(), "../single");
    Path path = methodPath();
    try (HadoopFileIO fileIO = createFileIO()) {

      // create a directory and a child underneath
      Path child = new Path(path, "child");
      final FileSystem fs = getFileSystem();

      fs.mkdirs(path);
      touch(fs, child);

      LOG.info("Deleting path to directory via deleteFile");
      intercept(RuntimeIOException.class, () ->
      {
        final String s = toString(path);
        fileIO.deleteFile(s);
        final FileStatus st = getFileStatusOrNull(fs, path);
        return String.format("Expected failure deleting %s but none raised. Path status: %s",
            path, st);
      });
    }
  }

  @Test
  public void testDeleteManyFiles() throws Throwable {
    Path path = methodPath();
    final FileSystem fs = getFileSystem();
    final List<Path> files = createFiles(fs, path, 1, DELETE_FILE_COUNT, 0);
    try (HadoopFileIO fileIO = createFileIO()) {
      fileIO.deleteFiles(stringList(files));
      for (Path p : files) {
        assertPathDoesNotExist("expected deletion", p);
      }
    }
  }

  /**
   * Use a more complex filename.
   * This validates that any conversions to URI/string
   * when passing to an object store is correct.
   */
  @Test
  public void testDeleteComplexFilename() throws Exception {
    Path path = new Path(basePath, "child[=comple]x");
    List<Path> paths = new ArrayList<>();
    paths.add(path);
    // bulk delete call doesn't verify if a path exists or not before deleting.
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  public static List<String> stringList(List<Path> files) {
    return files.stream().map(p -> toString(p)).collect(Collectors.toList());
  }

  public static List<String> stringList(Path path) {
    return Lists.newArrayList(toString(path));
  }

  /**
   * Use the chosen algorithm to convert the path to the string to
   * use in Iceberg APIs.
   * <p>
   * This must match whatever HadoopFileIO itself uses to map from a Path
   * to a String, which is currently {@link Path#toString()}.
   * @param path path to stringify
   * @return transformed string.
   */
  private static String toString(final Path path) {
    return path.toString();
  }


}
