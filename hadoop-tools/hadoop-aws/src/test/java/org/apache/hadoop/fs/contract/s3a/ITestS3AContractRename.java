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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.S3A_TEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;

/**
 * S3A contract tests covering rename.
 * Parameterized for auth mode as testRenameWithNonEmptySubDir was failing
 * during HADOOP-16697 development; this lets us ensure that when S3Guard
 * is enabled, both auth and nonauth paths work
 */
@RunWith(Parameterized.class)
public class ITestS3AContractRename extends AbstractContractRenameTest {

  public static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AContractRename.class);

  private final boolean authoritative;

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "auth={0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false},
        {true}
    });
  }

  public ITestS3AContractRename(boolean authoritative) {
    this.authoritative = authoritative;
  }

  @Override
  protected int getTestTimeoutMillis() {
    return S3A_TEST_TIMEOUT;
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, authoritative);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        "Skipping auth mode tests when the FS doesn't have a metastore",
        !authoritative || ((S3AFileSystem) getFileSystem()).hasMetadataStore());
  }

  @Override
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("S3A rename into an existing directory returns false");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source-256.txt");
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");

    Path destFilePath = new Path(destDir, "dest-512.txt");
    byte[] destDataset = dataset(512, 'A', 'Z');
    writeDataset(fs, destFilePath, destDataset, destDataset.length, 1024,
        false);
    assertIsFile(destFilePath);

    boolean rename = fs.rename(srcDir, destDir);
    assertFalse("s3a doesn't support rename to non-empty directory", rename);
  }

  /**
   * Test that after renaming, the nested file is moved along with all its
   * ancestors. It is similar to {@link #testRenamePopulatesDirectoryAncestors}.
   *
   * This is an extension testRenamePopulatesFileAncestors
   * of the superclass version which does better
   * logging of the state of the store before the assertions.
   */
  @Test
  public void testRenamePopulatesFileAncestors2() throws Exception {
    final S3AFileSystem fs = (S3AFileSystem) getFileSystem();
    Path base = path("testRenamePopulatesFileAncestors2");
    final Path src = new Path(base, "src");
    Path dest = new Path(base, "dest");
    fs.mkdirs(src);
    final String nestedFile = "/dir1/dir2/dir3/fileA";
    // size of file to create
    int filesize = 16 * 1024;
    byte[] srcDataset = dataset(filesize, 'a', 'z');
    Path srcFile = path(src + nestedFile);
    Path destFile = path(dest + nestedFile);
    writeDataset(fs, srcFile, srcDataset, srcDataset.length,
        1024, false);

    S3ATestUtils.MetricDiff fileCopyDiff = new S3ATestUtils.MetricDiff(fs,
        Statistic.FILES_COPIED);
    S3ATestUtils.MetricDiff fileCopyBytes = new S3ATestUtils.MetricDiff(fs,
        Statistic.FILES_COPIED_BYTES);

    rename(src, dest);

    describe("Rename has completed, examining data under " + base);
    fileCopyDiff.assertDiffEquals("Number of files copied", 1);
    fileCopyBytes.assertDiffEquals("Number of bytes copied", filesize);
    // log everything in the base directory.
    S3ATestUtils.lsR(fs, base, true);
    // look at the data.
    verifyFileContents(fs, destFile, srcDataset);
    describe("validating results");
    validateAncestorsMoved(src, dest, nestedFile);

  }

  @Override
  public void testRenameFileUnderFileSubdir() throws Exception {
    skip("Rename deep paths under files is allowed");
  }
}
