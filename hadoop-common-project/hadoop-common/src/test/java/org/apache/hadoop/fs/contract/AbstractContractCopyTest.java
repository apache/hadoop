/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonPathCapabilities.FS_NATIVE_COPY;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

/**
 * Test native copy - if supported.
 */
public abstract class AbstractContractCopyTest extends
    AbstractFSContractTestBase {

  protected Path srcDir;
  protected Path srcFile;
  protected Path zeroByteFile;
  protected Path dstDir;
  protected Path dstFile;
  protected FileSystem fs;

  protected byte[] srcData;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_NATIVE_COPY);

    srcDir = absolutepath("source");
    dstDir = absolutepath("target");

    srcFile = new Path(srcDir, "source.txt");
    dstFile = new Path(dstDir, "dst.txt");
    zeroByteFile = new Path(srcDir, "zero.txt");

    srcData = dataset(TEST_FILE_LEN, 0, 255);
    createFile(getFileSystem(), srcFile, true, srcData);
    touch(getFileSystem(), zeroByteFile);

    fs = getFileSystem();
  }

  @Override
  public void teardown() throws Exception {
    describe("\nTeardown\n");
    super.teardown();
  }


  @Test
  public void testBasicCopy() throws Throwable {
    describe("Checks whether S3A FS supports native copy"
        + " and copies a file from one location to another."
        + " File deletion taken care in teardown.");

    assertPathExists(srcFile + " does not exist", srcFile);

    fs.delete(dstFile, false);
    // initiate copy
    fs.copyFile(srcFile.toUri(), dstFile.toUri()).get();

    // Check if file is available and verify its contents
    assertIsFile(srcFile);
    assertIsFile(dstFile);
    verifyFileContents(fs, dstFile, srcData);
  }

  @Test
  public void testZeroByteFile() throws Throwable {
    describe("Copy 0 byte file.");

    assertPathExists(zeroByteFile + " does not exist", zeroByteFile);

    fs.delete(dstFile, false);
    // initiate copy
    fs.copyFile(zeroByteFile.toUri(), dstFile.toUri()).get();
    // Check if file is available
    assertIsFile(zeroByteFile);
  }

  @Test
  public void testFolderCopy() throws Throwable {
    describe("Test folder copy. Recursive copy is not supported yet.");
    // initiate copy
    handleExpectedException(intercept(Exception.class,
        () -> fs.copyFile(srcDir.toUri(), dstDir.toUri()).get()));
  }

  @Test
  public void testOverwrite() throws Throwable {
    describe("Test copying a file which is already present");

    fs.delete(dstFile, false);
    // initiate copy
    fs.copyFile(srcFile.toUri(), dstFile.toUri()).get();

    // Recopy, this would overwrite the contents
    fs.copyFile(srcFile.toUri(), dstFile.toUri()).get();

    // Check if file is available and verify its contents
    assertIsFile(srcFile);
    assertIsFile(dstFile);
    verifyFileContents(fs, dstFile, srcData);
  }

  @Test
  public void testNonExistingParentFolder() throws Throwable {
    describe("Test copying a file to destination folder "
        + "which is not present");

    Path nonExistingFolder = new Path(dstDir.getParent(),
        Long.toString(System.currentTimeMillis()));
    Path dFile = new Path(nonExistingFolder, "testfile");

    handleExpectedException(interceptFuture(Exception.class, "",
        fs.copyFile(zeroByteFile.toUri(), dFile.toUri())));
  }

  @Test
  public void testNonExistingFileCopy() throws Throwable {
    describe("Test copying a non-existing file to destination folder");

    Path srcFilePath = new Path(srcDir, "non-existing-file.txt");
    handleExpectedException(intercept(Exception.class,
        () -> fs.copyFile(srcFilePath.toUri(), dstDir.toUri()).get()));
  }

  @Test
  public void testNativeCopyCapability() throws Throwable {
    describe("Test native copy capability");

    // Check for native copy support
    boolean nativeCopySupported = fs.hasPathCapability(srcFile, FS_NATIVE_COPY);
    assertFalse("FileSystem does not have support for native file copy",
        nativeCopySupported);
  }

}
