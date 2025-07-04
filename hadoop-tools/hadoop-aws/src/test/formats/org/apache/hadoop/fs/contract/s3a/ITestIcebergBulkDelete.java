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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileInfo;

import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.createFiles;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_BULK_DELETE;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_OBJECTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_ALL_PROBES;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.NO_HEAD_OR_LIST;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Iceberg Bulk Delete API.
 * <p>
 * Parameterized on s3a multipart delete enabled/disabled.
 */
@ParameterizedClass
@MethodSource("params")
public class ITestIcebergBulkDelete extends AbstractS3ACostTest {

  private static final Logger LOG = LoggerFactory.getLogger(ITestIcebergBulkDelete.class);

  /**
   * Size of the delete thread pool.
   */
  private static final String ICEBERG_DELETE_FILE_PARALLELISM =
      "iceberg.hadoop.delete-file-parallelism";

  /**
   * Page size for the FS bulk delete; small to make validating multi-page
   * deletes possible.
   */
  private static final int DELETE_PAGE_SIZE = 3;

  /**
   * A count of large files to create which must be at least 2 greater than the modulus of
   * the page size. ie {@code (value % DELETE_PAGE_SIZE) >= 2)}.
   * This guarantees that even the tail request will be a bulk request.
   */
  private static final int DELETE_FILE_COUNT = 8;

  public static final int ICEBERG_EXECUTORS = 5;

  /**
   * The HadoopFileIO instance used to perform all Iceberg IO;
   * created in setup.
   */
  private HadoopFileIO fileIO;

  /**
   * This test suite is parameterized for single/multiple
   * delete options.
   * @return a list of test parameters.
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false},
        {true}
    });
  }


  /**
   * Enable s3a multi object delete.
   */
  private final boolean enableMultiObjectDelete;

  public ITestIcebergBulkDelete(boolean enableMultiObjectDelete) {
    this.enableMultiObjectDelete = enableMultiObjectDelete;
  }

  /**
   * Setup.
   * To avoid iceberg creating filesystem instances repeatedly, caching
   * must be enabled. Test setup needs to delete any existing
   * cached ones to avoid contamination.
   */
  @Override
  @BeforeEach
  public void setup() throws Exception {
    // close all filesystems.
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());

    // then create the single new one
    super.setup();
    assertThat(getContract())
        .describedAs("FS Contract is null")
        .isNotNull();
    fileIO = createFileIO();
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf = propagateBucketOptions(conf, getTestBucketName(conf));
    removeBaseAndBucketOverrides(conf,
        BULK_DELETE_PAGE_SIZE);
    // turn the caching on else every call refreshes the cache
    conf.setBoolean(FS_S3A_IMPL_DISABLE_CACHE, false);

    // skip this test run if multi-delete is explicitly disabled;
    // this is needed to test against third party stores
    // which do not support it.
    if (enableMultiObjectDelete) {
      skipIfNotEnabled(conf, ENABLE_MULTI_DELETE, "multi object delete is disabled");
    }
    conf.setBoolean(ENABLE_MULTI_DELETE, enableMultiObjectDelete);
    conf.setInt(BULK_DELETE_PAGE_SIZE, DELETE_PAGE_SIZE);
    conf.setInt(ICEBERG_DELETE_FILE_PARALLELISM, ICEBERG_EXECUTORS);
    return conf;
  }

  private int pageSize() {
    return enableMultiObjectDelete ? DELETE_PAGE_SIZE : 1;
  }

  /**
   * Create file IO for the current filesystem.
   * @return a file iO
   */
  private HadoopFileIO createFileIO() {
    return new HadoopFileIO(getFileSystem().getConf());
  }


  /**
   * Delete a single file using the bulk delete API.
   * There's no probe and a simple DELETE request is issued.
   */
  @Test
  public void testDeletePathWithNoFile() throws Throwable {
    Path path = new Path(methodPath(), "../missing");
    final List<String> filename = stringList(path);
    LOG.info("Deleting empty path");
    verifyMetrics(() -> deleteFiles(filename),
        always(NO_HEAD_OR_LIST),
        with(INVOCATION_BULK_DELETE, 1),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 1));
    assertDoesNotExist("was never there", path);
  }

  /**
   * Delete a single file using the bulk delete API.
   */
  @Test
  public void testDeleteSingleFile() throws Throwable {
    Path path = new Path(methodPath(), "../single");
    final List<String> filename = stringList(path);
    // one file
    touchfile(path);
    LOG.info("Deleting file at {}", filename);
    verifyMetrics(() -> deleteFiles(filename),
        always(NO_HEAD_OR_LIST),
        with(INVOCATION_BULK_DELETE, 1),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 1));
    assertDoesNotExist("should have been deleted", path);
  }

  /**
   * Create a file in the filesystem through the Iceberg FileIO API.
   * @param path path to create
   * @return the string representation of the path
   * @throws Throwable any exception raised
   */
  public String touchfile(Path path) throws Throwable {
    final String name = toString(path);
    fileIO.newOutputFile(name)
        .createOrOverwrite()
        .close();
    return name;
  }


  private String deleteFiles(List<String> filenames) {
    fileIO.deleteFiles(filenames);
    return  "deleted " + filenames.size() + " file(s); bulk delete =" + isBulkDelete();
  }

  private String deleteFile(String filename) {
    fileIO.deleteFile(filename);
    return  "deleted " + filename;
  }


  /**
   * Probe for the existence of a file.
   * @param path path to probe for existence
   * @return true if a file was found or dir inferred.
   * @throws Throwable failure
   */
  private boolean exists(Path path) throws Throwable {
    return fileIO.newInputFile(toString(path)).exists();
  }

  /**
   * Assert that a path exists.
   * @param message message to print if the assertion fails
   * @param path path to check
   * @throws Throwable any exception raised
   */
  private void assertExists(String message, Path path) throws Throwable {
    assertThat(exists(path))
        .as(message + ": " + path)
        .isTrue();
  }

  /**
   * Assert that a path does not exist.
   * @param message message to print if the assertion fails
   * @param path path to check
   * @throws Throwable any exception raised
   */
  private void assertDoesNotExist(String message, Path path) throws Throwable {
    assertThat(exists(path))
        .as(message + ": " + path)
        .isFalse();
  }

  /**
   * A directory is not deleted through the bulk delete API,
   * but does not report a failure.
   * The classic invocation mechanism reports a failure.
   */
  @Test
  public void testBulkDeleteDirectory() throws Throwable {
    Path path = methodPath();
    Path child = new Path(path, "child+=comple]x");
    final FileSystem fs = getFileSystem();
    final List<String> dir = stringList(path);

    // create a directory and a child underneath

    fs.mkdirs(path);
    final String childname = touchfile(child);

    LOG.info("Deleting path to directory");
    verifyMetrics(() -> deleteFiles(dir),
        always(NO_HEAD_OR_LIST),
        with(INVOCATION_BULK_DELETE, 1),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 1));
    // The directory is still found, as is the child.
    assertExists("directory was unexpectedly deleted", path);
    assertExists("child was unexpectedly deleted", child);
  }

  /**
   * A directory is not deleted through the bulk delete API,
   * but does not report a failure.
   * The classic invocation mechanism reports a failure.
   */
  @Test
  public void testDeleteDirectorySimpleAPI() throws Throwable {
    final Path base = methodPath();
    Path path = new Path(base, "subdir");
    Path child = new Path(path, "child+=comple]x");
    final FileSystem fs = getFileSystem();
    final String pathname = toString(path);

    final List<String> filename = stringList(path);

    // create a directory and a child underneath
    fs.mkdirs(path);
    final String childname = touchfile(child);
    assertThat(listPaths(base))
        .as("directory should be empty after deletion")
        .hasSize(1)
        .element(0)
        .satisfies(fileInfo -> fileInfo.location().equals(childname));


    // Through the HadoopFileIO.deleteFile API.
    // this is rejected by S3A as it is not a file nor an empty directory,
    intercept(RuntimeIOException.class,
        "Failed to delete",
        () -> deleteFile(pathname));

    assertExists("directory was unexpectedly deleted", path);
    assertExists("child was unexpectedly deleted", child);

  }

  /**
   * Create a file and delete through the simple file API.
   */
  @Test
  public void testDeleteFileSimpleAPI() throws Throwable {
    LOG.info("Deleting file via deleteFile(String)");
    final Path base = methodPath();
    Path path = new Path(base, "subdir");
    Path child = new Path(path, "child+=comple]x");
    final FileSystem fs = getFileSystem();
    final List<String> filename = stringList(path);

    // create a directory and a child underneath
    // this creates two objects: marker and file.
    fs.mkdirs(path);
    final String childname = touchfile(child);

    // Single file API maps to delete(file)
    verifyMetrics(() -> deleteFile(childname),
        always(FILE_STATUS_ALL_PROBES),
        with(INVOCATION_BULK_DELETE, 0),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_OBJECTS, 1));

    assertDoesNotExist("child should have been deleted", child);
    deleteFile(toString(path));
    assertDoesNotExist("empty directory should have been deleted", path);
    assertThat(listPaths(base))
        .as("directory should be empty after deletion")
        .isEmpty();
  }

  /**
   * Delete many files through bulk delete API.
   * <p>
   * No existence probes; when multidelete is enabled a single request is made.
   * single object delete is mapped to a series of DELETE calls.
   * <p>
   * Iceberg code queues by page size.
   * When multidelete is enabled, the #of requests is as many as can fit the file count.
   * If disabled, it is #of files.
   */
  @Test
  public void testDeleteManyFiles() throws Throwable {
    LOG.info("Deleting many files via the bulk delete API");
    Path path = methodPath();
    final FileSystem fs = getFileSystem();
    int expectedInvocationCount;
    if (!isBulkDelete()) {
      expectedInvocationCount = DELETE_FILE_COUNT;
    } else {
      expectedInvocationCount = DELETE_FILE_COUNT / pageSize() + 1;
    }
    final List<Path> files = createFiles(fs, path, 1, DELETE_FILE_COUNT, 0);
    verifyMetrics(() -> deleteFiles(stringList(files)),
        always(NO_HEAD_OR_LIST),
        with(INVOCATION_BULK_DELETE, expectedInvocationCount),
        with(OBJECT_DELETE_OBJECTS, DELETE_FILE_COUNT),
        // bulk delete: bulk delete calls to S3 store only
        probe(isBulkDelete(), OBJECT_BULK_DELETE_REQUEST, expectedInvocationCount),
        probe(isBulkDelete(), OBJECT_DELETE_REQUEST, 0),
        // single delete, one per file
        probe(!isBulkDelete(), OBJECT_BULK_DELETE_REQUEST, 0),
        probe(!isBulkDelete(), OBJECT_DELETE_REQUEST, DELETE_FILE_COUNT));
    for (Path p : files) {
      assertPathDoesNotExist("expected deletion", p);
    }
  }

  /**
   * Convert a list of paths to a list of strings.
   * @param files files to convert
   * @return the list of strings
   */
  public static List<String> stringList(List<Path> files) {
    return files.stream().map(p -> toString(p)).collect(Collectors.toList());
  }

  /**
   * Convert a single path to a list of strings.
   * @param path path to convert
   * @return the list of strings
   */
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

  /**
   * List the files under a given path.
   * Directories are not reported.
   * @param path path to list
   * @return the list of files
   * @throws Throwable any exception raised
   */
  private List<FileInfo> listPaths(Path path) throws Throwable {
    return Lists.newArrayList(fileIO.listPrefix(toString(path)));
  }

}
