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

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.test.TestName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.tags.IntegrationTest;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.isCreatePerformanceEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 *  Tests a live S3 system. If your keys and bucket aren't specified, all tests
 *  are marked as passed.
 */
@IntegrationTest
public class ITestS3AFileSystemContract extends FileSystemContractBaseTest {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileSystemContract.class);

  private Path basePath;

  @RegisterExtension
  private TestName methodName = new TestName();

  private void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  @Override
  protected int getGlobalTimeout() {
    return S3ATestConstants.S3A_TEST_TIMEOUT;
  }

  @BeforeEach
  public void setUp() throws Exception {
    nameThread();
    Configuration conf = setPerformanceFlags(
        new Configuration(),
        "");

    fs = S3ATestUtils.createTestFileSystem(conf);
    assumeTrue(fs != null);
    basePath = fs.makeQualified(
        S3ATestUtils.createTestPath(new Path("s3afilesystemcontract")));
  }

  @Override
  public Path getTestBaseDir() {
    return basePath;
  }

  @Test
  public void testMkdirsWithUmask() throws Exception {
    skip("Not supported");
  }

  @Test
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    assumeTrue(renameSupported());

    Path src = path("testRenameDirectoryAsExisting/dir");
    fs.mkdirs(src);
    createFile(path(src + "/file1"));
    createFile(path(src + "/subdir/file2"));

    Path dst = path("testRenameDirectoryAsExistingNew/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertFalse(fs.exists(path(src + "/file1")),
        "Nested file1 exists");
    assertFalse(fs.exists(path(src + "/subdir/file2")),
        "Nested file2 exists");
    assertTrue(fs.exists(path(dst + "/file1")),
        "Renamed nested file1 exists");
    assertTrue(fs.exists(path(dst + "/subdir/file2")),
        "Renamed nested exists");
  }

  @Test
  public void testRenameDirectoryAsExistingFile() throws Exception {
    assumeTrue(renameSupported());

    Path src = path("testRenameDirectoryAsExistingFile/dir");
    fs.mkdirs(src);
    Path dst = path("testRenameDirectoryAsExistingFileNew/newfile");
    createFile(dst);
    intercept(FileAlreadyExistsException.class,
        () -> rename(src, dst, false, true, true));
  }

  @Test
  public void testRenameDirectoryMoveToNonExistentDirectory()
      throws Exception {
    skip("does not fail");
  }

  @Test
  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    skip("does not fail");
  }

  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    intercept(FileAlreadyExistsException.class,
        () -> super.testRenameFileAsExistingFile());
  }

  @Test
  public void testRenameNonExistentPath() throws Exception {
    intercept(FileNotFoundException.class,
        () -> super.testRenameNonExistentPath());

  }

  @Override
  public void testOverwrite() throws IOException {
    boolean createPerformance = isCreatePerformanceEnabled(fs);
    try {
      super.testOverwrite();
      assertThat(createPerformance)
          .describedAs("create performance enabled")
          .isFalse();
    } catch (AssertionError e) {
      // swallow the exception if create performance is enabled,
      // else rethrow
      if (!createPerformance) {
        throw e;
      }
    }
  }

  @Override
  public void testOverWriteAndRead() throws Exception {
    // Currently analytics accelerator does not support reading of files that have been overwritten.
    // This is because the analytics accelerator library caches metadata, and when a file is
    // overwritten, the old metadata continues to be used, until it is removed from the cache over
    // time. This will be fixed in https://github.com/awslabs/analytics-accelerator-s3/issues/218.
    skipIfAnalyticsAcceleratorEnabled(fs.getConf(),
        "Analytics Accelerator currently does not support reading of over written files");
    super.testOverWriteAndRead();
  }
}
