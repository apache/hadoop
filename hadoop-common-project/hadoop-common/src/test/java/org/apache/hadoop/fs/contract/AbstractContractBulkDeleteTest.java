/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.wrappedio.WrappedIO;
import org.apache.hadoop.io.wrappedio.impl.DynamicWrappedIO;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.io.wrappedio.WrappedIO.bulkDelete_delete;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Contract tests for bulk delete operation.
 * Many of these tests use {@link WrappedIO} wrappers through reflection,
 * to validate the codepath we expect libraries designed to work with
 * multiple versions to use.
 */
public abstract class AbstractContractBulkDeleteTest extends AbstractFSContractTestBase {

  private static final Logger LOG =
          LoggerFactory.getLogger(AbstractContractBulkDeleteTest.class);

  /**
   * Page size for bulk delete. This is calculated based
   * on the store implementation.
   */
  protected int pageSize;

  /**
   * Base path for the bulk delete tests.
   * All the paths to be deleted should be under this base path.
   */
  protected Path basePath;

  /**
   * Test file system.
   */
  protected FileSystem fs;

  /**
   * Reflection support.
   */
  private DynamicWrappedIO dynamicWrappedIO;

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    fs = getFileSystem();
    basePath = path(getClass().getName());
    dynamicWrappedIO = new DynamicWrappedIO();
    pageSize = dynamicWrappedIO.bulkDelete_pageSize(fs, basePath);
    fs.mkdirs(basePath);
  }

  public Path getBasePath() {
    return basePath;
  }

  protected int getExpectedPageSize() {
    return 1;
  }

  /**
   * Validate the page size for bulk delete operation. Different stores can have different
   * implementations for bulk delete operation thus different page size.
   */
  @Test
  public void validatePageSize() throws Exception {
    Assertions.assertThat(pageSize)
            .describedAs("Page size should be 1 by default for all stores")
            .isEqualTo(getExpectedPageSize());
  }

  @Test
  public void testPathsSizeEqualsPageSizePrecondition() throws Exception {
    List<Path> listOfPaths = createListOfPaths(pageSize, basePath);
    // Bulk delete call should pass with no exception.
    bulkDelete_delete(getFileSystem(), basePath, listOfPaths);
  }

  @Test
  public void testPathsSizeGreaterThanPageSizePrecondition() throws Exception {
    List<Path> listOfPaths = createListOfPaths(pageSize + 1, basePath);
    intercept(IllegalArgumentException.class, () ->
        dynamicWrappedIO.bulkDelete_delete(getFileSystem(), basePath, listOfPaths));
  }

  @Test
  public void testPathsSizeLessThanPageSizePrecondition() throws Exception {
    List<Path> listOfPaths = createListOfPaths(pageSize - 1, basePath);
    // Bulk delete call should pass with no exception.
    dynamicWrappedIO.bulkDelete_delete(getFileSystem(), basePath, listOfPaths);
  }

  @Test
  public void testBulkDeleteSuccessful() throws Exception {
    runBulkDelete(false);
  }

  @Test
  public void testBulkDeleteSuccessfulUsingDirectFS() throws Exception {
    runBulkDelete(true);
  }

  private void runBulkDelete(boolean useDirectFS) throws IOException {
    List<Path> listOfPaths = createListOfPaths(pageSize, basePath);
    for (Path path : listOfPaths) {
      touch(fs, path);
    }
    FileStatus[] fileStatuses = fs.listStatus(basePath);
    Assertions.assertThat(fileStatuses)
            .describedAs("File count after create")
            .hasSize(pageSize);
    if (useDirectFS) {
      assertSuccessfulBulkDelete(
              fs.createBulkDelete(basePath).bulkDelete(listOfPaths));
    } else {
      // Using WrappedIO to call bulk delete.
      assertSuccessfulBulkDelete(
              bulkDelete_delete(getFileSystem(), basePath, listOfPaths));
    }

    FileStatus[] fileStatusesAfterDelete = fs.listStatus(basePath);
    Assertions.assertThat(fileStatusesAfterDelete)
            .describedAs("File statuses should be empty after delete")
            .isEmpty();
  }


  @Test
  public void validatePathCapabilityDeclared() throws Exception {
    Assertions.assertThat(fs.hasPathCapability(basePath, CommonPathCapabilities.BULK_DELETE))
            .describedAs("Path capability BULK_DELETE should be declared")
            .isTrue();
  }

  /**
   * This test should fail as path is not under the base path.
   */
  @Test
  public void testDeletePathsNotUnderBase() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path pathNotUnderBase = path("not-under-base");
    paths.add(pathNotUnderBase);
    intercept(IllegalArgumentException.class,
            () -> bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  /**
   * We should be able to delete the base path itself
   * using bulk delete operation.
   */
  @Test
  public void testDeletePathSameAsBasePath() throws Exception {
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(),
            basePath,
            Arrays.asList(basePath)));
  }

  /**
   * This test should fail as path is not absolute.
   */
  @Test
  public void testDeletePathsNotAbsolute() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path pathNotAbsolute = new Path("not-absolute");
    paths.add(pathNotAbsolute);
    intercept(IllegalArgumentException.class,
            () -> bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  @Test
  public void testDeletePathsNotExists() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path pathNotExists = new Path(basePath, "not-exists");
    paths.add(pathNotExists);
    // bulk delete call doesn't verify if a path exist or not before deleting.
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  @Test
  public void testDeletePathsDirectory() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path dirPath = new Path(basePath, "dir");
    paths.add(dirPath);
    Path filePath = new Path(dirPath, "file");
    paths.add(filePath);
    pageSizePreconditionForTest(paths.size());
    fs.mkdirs(dirPath);
    touch(fs, filePath);
    // Outcome is undefined. But call shouldn't fail.
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  @Test
  public void testBulkDeleteParentDirectoryWithDirectories() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path dirPath = new Path(basePath, "dir");
    fs.mkdirs(dirPath);
    Path subDir = new Path(dirPath, "subdir");
    fs.mkdirs(subDir);
    // adding parent directory to the list of paths.
    paths.add(dirPath);
    List<Map.Entry<Path, String>> entries = bulkDelete_delete(getFileSystem(), basePath, paths);
    Assertions.assertThat(entries)
            .describedAs("Parent non empty directory should not be deleted")
            .hasSize(1);
    // During the bulk delete operation, the non-empty directories are not deleted in default implementation.
    assertIsDirectory(dirPath);
  }

  @Test
  public void testBulkDeleteParentDirectoryWithFiles() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path dirPath = new Path(basePath, "dir");
    fs.mkdirs(dirPath);
    Path file = new Path(dirPath, "file");
    touch(fs, file);
    // adding parent directory to the list of paths.
    paths.add(dirPath);
    List<Map.Entry<Path, String>> entries = bulkDelete_delete(getFileSystem(), basePath, paths);
    Assertions.assertThat(entries)
            .describedAs("Parent non empty directory should not be deleted")
            .hasSize(1);
    // During the bulk delete operation, the non-empty directories are not deleted in default implementation.
    assertIsDirectory(dirPath);
  }


  @Test
  public void testDeleteEmptyDirectory() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path emptyDirPath = new Path(basePath, "empty-dir");
    fs.mkdirs(emptyDirPath);
    paths.add(emptyDirPath);
    // Should pass as empty directory.
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  @Test
  public void testDeleteEmptyList() throws Exception {
    List<Path> paths = new ArrayList<>();
    // Empty list should pass.
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  @Test
  public void testDeleteSamePathsMoreThanOnce() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path path = new Path(basePath, "file");
    paths.add(path);
    paths.add(path);
    Path another = new Path(basePath, "another-file");
    paths.add(another);
    pageSizePreconditionForTest(paths.size());
    touch(fs, path);
    touch(fs, another);
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }

  /**
   * Skip test if paths size is greater than page size.
   */
  protected void pageSizePreconditionForTest(int size) {
    if (size > pageSize) {
      skip("Test requires paths size less than or equal to page size: "
          + pageSize
          + "; actual size is " + size);
    }
  }

  /**
   * This test validates that files to be deleted don't have
   * to be direct children of the base path.
   */
  @Test
  public void testDeepDirectoryFilesDelete() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path dir1 = new Path(basePath, "dir1");
    Path dir2 = new Path(dir1, "dir2");
    Path dir3 = new Path(dir2, "dir3");
    fs.mkdirs(dir3);
    Path file1 = new Path(dir3, "file1");
    touch(fs, file1);
    paths.add(file1);
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }


  @Test
  public void testChildPaths() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path dirPath = new Path(basePath, "dir");
    fs.mkdirs(dirPath);
    paths.add(dirPath);
    Path filePath = new Path(dirPath, "file");
    touch(fs, filePath);
    paths.add(filePath);
    pageSizePreconditionForTest(paths.size());
    // Should pass as both paths are under the base path.
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
  }


  /**
   * Assert on returned entries after bulk delete operation.
   * Entries should be empty after successful delete.
   */
  public static void assertSuccessfulBulkDelete(List<Map.Entry<Path, String>> entries) {
    Assertions.assertThat(entries)
            .describedAs("Bulk delete failed, " +
                    "return entries should be empty after successful delete")
            .isEmpty();
  }

  /**
   * Create a list of paths with the given count
   * under the given base path.
   */
  private List<Path> createListOfPaths(int count, Path basePath) {
    List<Path> paths = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Path path = new Path(basePath, "file-" + i);
      paths.add(path);
    }
    return paths;
  }
}
