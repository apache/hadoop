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

package org.apache.hadoop.fs.contract.s3a;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractBulkDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.test.tags.IntegrationTest;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.STORE_IO_RATE_LIMITED_DURATION;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.io.wrappedio.WrappedIO.bulkDelete_delete;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Contract tests for bulk delete operation for S3A Implementation.
 */
@IntegrationTest
@ParameterizedClass(name = "enableMultiObjectDelete={0}")
@MethodSource("enableMultiObjectDelete")
public class ITestS3AContractBulkDelete extends AbstractContractBulkDeleteTest {

  private static final Logger LOG = LoggerFactory.getLogger(ITestS3AContractBulkDelete.class);

  /**
   * Delete Page size: {@value}.
   * This is the default page size for bulk delete operation for this contract test.
   * All the tests in this class should pass number of paths equal to or less than
   * this page size during the bulk delete operation.
   */
  private static final int DELETE_PAGE_SIZE = 20;

  private final boolean enableMultiObjectDelete;

  public static Iterable<Object[]> enableMultiObjectDelete() {
    return Arrays.asList(new Object[][]{
            {true},
            {false}
    });
  }

  public ITestS3AContractBulkDelete(boolean enableMultiObjectDelete) {
    this.enableMultiObjectDelete = enableMultiObjectDelete;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf = propagateBucketOptions(conf, getTestBucketName(conf));
    if (enableMultiObjectDelete) {
      // if multi-object delete is disabled, skip the test.
      skipIfNotEnabled(conf, Constants.ENABLE_MULTI_DELETE,
              "Bulk delete is explicitly disabled for this bucket");
    }
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
            Constants.BULK_DELETE_PAGE_SIZE);
    conf.setInt(Constants.BULK_DELETE_PAGE_SIZE, DELETE_PAGE_SIZE);
    conf.setBoolean(Constants.ENABLE_MULTI_DELETE, enableMultiObjectDelete);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(createConfiguration());
  }

  @Override
  protected int getExpectedPageSize() {
    if (!enableMultiObjectDelete) {
      // if multi-object delete is disabled, page size should be 1.
      return 1;
    }
    return DELETE_PAGE_SIZE;
  }

  @Override
  public void validatePageSize() throws Exception {
    Assertions.assertThat(pageSize)
            .describedAs("Page size should match the configured page size")
            .isEqualTo(getExpectedPageSize());
  }

  @Test
  public void testBulkDeleteZeroPageSizePrecondition() throws Exception {
    if (!enableMultiObjectDelete) {
      // if multi-object delete is disabled, skip this test as
      // page size is always 1.
      skip("Multi-object delete is disabled");
    }
    Configuration conf = getContract().getConf();
    conf.setInt(Constants.BULK_DELETE_PAGE_SIZE, 0);
    Path testPath = path(getMethodName());
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      intercept(IllegalArgumentException.class,
              () -> fs.createBulkDelete(testPath));
    }
  }

  @Test
  public void testPageSizeWhenMultiObjectsDisabled() throws Exception {
    Configuration conf = getContract().getConf();
    conf.setBoolean(Constants.ENABLE_MULTI_DELETE, false);
    Path testPath = path(getMethodName());
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      BulkDelete bulkDelete = fs.createBulkDelete(testPath);
      Assertions.assertThat(bulkDelete.pageSize())
              .describedAs("Page size should be 1 when multi-object delete is disabled")
              .isEqualTo(1);
    }
  }

  @Override
  @Test
  public void testDeletePathsDirectory() throws Exception {
    List<Path> paths = new ArrayList<>();
    Path dirPath = new Path(basePath, "dir");
    fs.mkdirs(dirPath);
    paths.add(dirPath);
    Path filePath = new Path(dirPath, "file");
    touch(fs, filePath);
    if (enableMultiObjectDelete) {
      // Adding more paths only if multi-object delete is enabled.
      paths.add(filePath);
    }
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
    // During the bulk delete operation, the directories are not deleted in S3A.
    assertIsDirectory(dirPath);
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
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
    // During the bulk delete operation, the directories are not deleted in S3A.
    assertIsDirectory(dirPath);
    assertIsDirectory(subDir);
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
    assertSuccessfulBulkDelete(bulkDelete_delete(getFileSystem(), basePath, paths));
    // During the bulk delete operation,
    // the directories are not deleted in S3A.
    assertIsDirectory(dirPath);
  }


  @Test
  public void testRateLimiting() throws Exception {
    if (!enableMultiObjectDelete) {
      skip("Multi-object delete is disabled so hard to trigger rate limiting");
    }
    Configuration conf = getContract().getConf();
    conf.setInt(Constants.S3A_IO_RATE_LIMIT, 5);
    Path basePath = path(getMethodName());
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      createFiles(fs, basePath, 1, 20, 0);
      FileStatus[] fileStatuses = fs.listStatus(basePath);
      List<Path> paths = Arrays.stream(fileStatuses)
              .map(FileStatus::getPath)
              .collect(toList());
      pageSizePreconditionForTest(paths.size());
      BulkDelete bulkDelete = fs.createBulkDelete(basePath);
      bulkDelete.bulkDelete(paths);
      MeanStatistic meanStatisticBefore = lookupMeanStatistic(fs.getIOStatistics(),
              STORE_IO_RATE_LIMITED_DURATION + SUFFIX_MEAN);
      Assertions.assertThat(meanStatisticBefore.mean())
              .describedAs("Rate limiting should not have happened during first delete call")
              .isEqualTo(0.0);
      bulkDelete.bulkDelete(paths);
      bulkDelete.bulkDelete(paths);
      bulkDelete.bulkDelete(paths);
      MeanStatistic meanStatisticAfter = lookupMeanStatistic(fs.getIOStatistics(),
              STORE_IO_RATE_LIMITED_DURATION + SUFFIX_MEAN);
      Assertions.assertThat(meanStatisticAfter.mean())
              .describedAs("Rate limiting should have happened during multiple delete calls")
              .isGreaterThan(0.0);
    }
  }
}
