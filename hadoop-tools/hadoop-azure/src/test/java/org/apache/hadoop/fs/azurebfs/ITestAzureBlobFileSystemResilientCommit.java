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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.output.ResilientCommitByRenameHelper;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_RENAME_RAISES_EXCEPTIONS;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toAsciiByteArray;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the commit helper; parameterized on whether or not the FS
 * raises exceptions on rename failures.
 * The outcome must be the same through the commit helper;
 * exceptions and error messages will be different.
 */
@RunWith(Parameterized.class)
public class ITestAzureBlobFileSystemResilientCommit
    extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAzureBlobFileSystemResilientCommit.class);
  private static final byte[] DATA = toAsciiByteArray("hello");
  private static final byte[] DATA2 = toAsciiByteArray("world");

  private final boolean raiseExceptions;

  /**
   * error keyword from azure storage when exceptions are being
   * raised.
   */
  public static final String E_NO_SOURCE = "SourcePathNotFound";

  public ITestAzureBlobFileSystemResilientCommit(
      final boolean raiseExceptions) throws Exception {
    this.raiseExceptions = raiseExceptions;
  }

  /**
   * Does FS raise exceptions?
   * @return test params
   */
  @Parameterized.Parameters(name = "raising-{0}")
  public static Collection getParameters() {
    // -1 is covered in separate test case
    return Arrays.asList(true, false);
  }

  /**
   * FS raising exceptions on rename.
   */
  private AzureBlobFileSystem targetFS;
  private Path outputPath;
  private ResilientCommitByRenameHelper commitHelper;
  private Path sourcePath;
  private Path destPath;

  @Override
  public void setup() throws Exception {
    super.setup();
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration conf = new Configuration(this.getRawConfiguration());
    conf.setBoolean(FS_AZURE_RENAME_RAISES_EXCEPTIONS, raiseExceptions);

    targetFS = (AzureBlobFileSystem) FileSystem.newInstance(
        currentFs.getUri(),
        conf);
    Assertions.assertThat(
            targetFS.getConf().getBoolean(FS_AZURE_RENAME_RAISES_EXCEPTIONS, false))
        .describedAs("FS raises exceptions on rename %s", targetFS)
        .isEqualTo(raiseExceptions);
    outputPath = path(getMethodName());
    sourcePath = new Path(outputPath, "source");
    destPath = new Path(outputPath, "dest");
    targetFS.mkdirs(outputPath);

    commitHelper = new ResilientCommitByRenameHelper(
        targetFS,
        outputPath, true);

  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, targetFS);
    super.teardown();
  }

  /**
   * Create a file; return the status.
   * @param path file path
   * @param data text of file
   * @return the status
   * @throws IOException creation failure
   */
  FileStatus file(Path path, byte[] data) throws IOException {
    ContractTestUtils.createFile(targetFS, path, true,
        data);
    return targetFS.getFileStatus(path);
  }

  /**
   * make sure the filesystem resilience matches the text
   * expectations.
   */
  @Test
  public void testVerifyResilient() {
    Assertions.assertThat(commitHelper.isRenameRecoveryAvailable())
        .describedAs("recovery availability of %s", commitHelper)
        .isTrue();
  }

  @Test
  public void testSimpleRename() throws Throwable {
    describe("simple rename succeeds and then fails");
    file(sourcePath, DATA);
    targetFS.rename(sourcePath, destPath);
    ContractTestUtils.verifyFileContents(targetFS, destPath, DATA);
    ContractTestUtils.assertPathDoesNotExist(targetFS,
        "source", sourcePath);
    // attempt 2 fails differently depending
    // on the FS settings
    if (raiseExceptions) {
      intercept(FileNotFoundException.class,
          E_NO_SOURCE,
          () -> targetFS.rename(sourcePath, destPath));
    } else {

      Assertions.assertThat(targetFS.rename(sourcePath, destPath))
          .describedAs("return value of rename")
          .isFalse();
    }
  }

  @Test
  public void testSimpleRenameNoSource() throws Throwable {
    describe("simple rename fails when there is no source file");
    // attempt fails differently depending
    // on the FS settings
    if (raiseExceptions) {
      intercept(FileNotFoundException.class,
          E_NO_SOURCE,
          () -> targetFS.rename(sourcePath, destPath));
    } else {

      Assertions.assertThat(targetFS.rename(sourcePath, destPath))
          .describedAs("return value of rename")
          .isFalse();
    }
  }

  /**
   * commit a file twice.
   * the second time the source file is missing but the dest file
   * has the same etag. as a result, this is considered a success.
   */
  @Test
  public void testDoubleCommitTriggersRecovery() throws Throwable {
    describe("commit a file twice; expect the second to be recovery");
    final FileStatus status = file(sourcePath, DATA);
    commit(status, false);
    ContractTestUtils.verifyFileContents(targetFS, destPath, DATA);

    // try again and as the status holds, expect recovery
    commit(status, true);
    Assertions.assertThat(commitHelper.getRecoveryCount())
        .describedAs("recovery count of %s", commitHelper)
        .isEqualTo(1);
  }

  /**
   * commit a file twice with a status entry with no etag;
   * the second attempt will fail.
   */
  @Test
  public void testDoubleCommitSourceHasNoEtag() throws Throwable {
    describe("commit a file without an etag; expect the second to fail");
    final FileStatus status = file(sourcePath, DATA);
    FileStatus st2 = new FileStatus(status);
    commit(st2, false);
    // try again and as the status has no tag, expect failure.
    intercept(IOException.class, () ->
        commit(st2, false));
  }

  /**
   * overwrite a file before trying to commit it again.
   */
  @Test
  public void testDoubleCommitDifferentFiles() throws Throwable {
    describe("commit two different files; no recovery allowed");
    final FileStatus status = file(sourcePath, DATA);
    commit(status, false);
    file(sourcePath, DATA2);

    // ioe raised; type will depend on whether or not FS
    // is raising exceptions.
    intercept(IOException.class, () ->
        commit(status, false));
  }

  /**
   * commit a file, then
   * expectone with a filestatus with a different source etag,
   * to fail
   */
  @Test
  public void testDoubleCommitDifferentFiles2() throws Throwable {
    describe("commit two different files; no recovery allowed");

    // create a file to the source path with different data
    // its status will not match that of the dest
    final FileStatus status2 = file(sourcePath, DATA2);

    final FileStatus status = file(sourcePath, DATA);
    commit(status, false);

    // ioe raised; type will depend on whether or not FS
    // is raising exceptions.
    intercept(IOException.class, () ->
        commit(status2, false));
  }

  /**
   * commit a file twice.
   * the second time the source file is missing but the dest file
   * has the same etag. as a result, this is considered a success.
   */
  @Test
  public void testCommitMissingDestDir() throws Throwable {
    describe("commit a file twice; expect the second to be recovery");
    final FileStatus status = file(sourcePath, DATA);
    final Path subpath = new Path(destPath, "subpath");
    intercept(IOException.class, () ->
        commitHelper.commitFile(status, subpath));
  }

  @Test
  public void testCommitNoSource() throws Throwable {
    describe("delete the source file, expect commit to fail");

    final FileStatus status = file(sourcePath, DATA);
    targetFS.delete(sourcePath, true);
    intercept(FileNotFoundException.class,
        raiseExceptions ? E_NO_SOURCE : "",
        () -> commit(status, false));
  }

  private ResilientCommitByRenameHelper.CommitOutcome commit(final FileStatus status,
      boolean expectRecovery)
      throws IOException {
    final ResilientCommitByRenameHelper.CommitOutcome outcome = commitHelper.commitFile(
        status, destPath);
    Assertions.assertThat(outcome.isRenameFailureResolvedThroughEtags())
        .describedAs("resolution of %s", outcome)
        .isEqualTo(expectRecovery);
    return outcome;
  }

}
