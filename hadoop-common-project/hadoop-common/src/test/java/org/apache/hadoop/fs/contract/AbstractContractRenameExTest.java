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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertListStatusFinds;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the rename(path, path, options) operations.
 */
public abstract class AbstractContractRenameExTest extends
    AbstractFSContractTestBase {

  protected void renameEx(
      final Path src,
      final Path dst)
      throws IOException {
    getFileSystem().rename(src, dst, Options.Rename.NONE);
  }

  protected void renameEx(
      final Path src,
      final Path dst,
      final Options.Rename... options)
      throws IOException {
    getFileSystem().rename(src, dst, options);
  }

  @Test
  public void testRenameNewFileSameDir() throws Throwable {
    describe("rename a file into a new file in the same directory");
    Path renameSrc = path("rename_src");
    Path renameTarget = path("rename_dest");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), renameSrc,
        data, data.length, 1024 * 1024, false);
    boolean rename = rename(renameSrc, renameTarget);
    assertTrue("rename("+renameSrc+", "+ renameTarget+") returned false",
        rename);
    assertListStatusFinds(getFileSystem(),
        renameTarget.getParent(), renameTarget);
    verifyFileContents(getFileSystem(), renameTarget, data);
  }

  @Test
  public void testRenameNonexistentFile() throws Throwable {
    describe("rename a file which does not exist");
    final Path missing = path("testRenameNonexistentFileSrc");
    final Path target = path("testRenameNonexistentFileDest");
    mkdirs(missing.getParent());
    IOException e = intercept(IOException.class,
        () -> renameEx(missing, target));
    if (e instanceof FileNotFoundException) {
      handleExpectedException(e);
    } else {
      handleRelaxedException("rename nonexistent file",
          "FileNotFoundException", e);
    }
    assertPathDoesNotExist("rename nonexistent file created a destination file",
        target);
  }

  /**
   * Rename test -handles filesystems that will overwrite the destination
   * as well as those that do not (i.e. HDFS).
   */
  @Test
  public void testRenameFileOverExistingFile() throws Throwable {
    describe("Verify renaming a file onto an existing file matches expectations");
    final Path srcFile = path("source-256.txt");
    final byte[] srcData = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), srcFile, srcData, srcData.length, 1024, false);
    final Path destFile = path("dest-512.txt");
    final byte[] destData = dataset(512, 'A', 'Z');
    writeDataset(getFileSystem(), destFile, destData, destData.length, 1024, false);
    assertIsFile(destFile);
    IOException e = intercept(IOException.class,
        () -> renameEx(srcFile, destFile));
    if (e instanceof FileAlreadyExistsException) {
      handleExpectedException(e);
    } else {
      handleRelaxedException("rename over file",
          "FileAlreadyExistsException", e);
    }
    // verify that the destination file is as expected based on the expected
    // outcome
    verifyFileContents(getFileSystem(), destFile, destData);
    verifyFileContents(getFileSystem(), srcFile, srcData);
    // now rename with overwrite
    renameEx(srcFile, destFile, Options.Rename.OVERWRITE);
    verifyFileContents(getFileSystem(), destFile, srcData);
    assertPathDoesNotExist("Source still found", srcFile);
  }

  /**
   * Rename a file onto itself.
   */
  @Test
  public void testRenameFileOverSelf() throws Throwable {
    describe("Verify renaming a file onto itself does not lose the data");
    final Path srcFile = path("source-256.txt");
    final byte[] srcData = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), srcFile, srcData, srcData.length, 1024, false);
    intercept(FileAlreadyExistsException.class,
        () -> renameEx(srcFile, srcFile));
    verifyFileContents(getFileSystem(), srcFile, srcData);
    intercept(FileAlreadyExistsException.class,
        () -> renameEx(srcFile, srcFile, Options.Rename.OVERWRITE));
    verifyFileContents(getFileSystem(), srcFile, srcData);
  }

  /**
   * Rename a dir onto itself.
   */
  @Test
  public void testRenameDirOverSelf() throws Throwable {
    final Path src = path("testRenameDirOverSelf");
    getFileSystem().mkdirs(src);
    intercept(FileAlreadyExistsException.class,
        () -> renameEx(src, src));
    assertIsDirectory(src );
  }

  @Test
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts it underneath"
             +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    String sourceName = "source-256.txt";
    Path srcFilePath = new Path(srcDir, sourceName);
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");

    Path destFilePath = new Path(destDir, "dest-512.txt");
    byte[] destDateset = dataset(512, 'A', 'Z');
    writeDataset(fs, destFilePath, destDateset, destDateset.length, 1024, false);
    assertIsFile(destFilePath);
    // no overwrite: fail
    intercept(FileAlreadyExistsException.class,
        () -> renameEx(srcDir, destDir));
    // overwrite fails if dest dir has a file
    intercept(IOException.class,
        () -> renameEx(srcDir, destDir, Options.Rename.OVERWRITE));
    // overwrite is good
    // delete the dest file and all will be well
    assertDeleted(destFilePath, false);
    renameEx(srcDir, destDir, Options.Rename.OVERWRITE);
    verifyFileContents(fs, new Path(destDir, sourceName), srcDataset);
    assertPathDoesNotExist("Source still found", srcFilePath);
  }

  @Test
  public void testRenameFileNonexistentDir() throws Throwable {
    describe("rename a file into a nonexistent directory");
    final Path renameSrc = path("testRenameSrc");
    final Path renameTarget = path("subdir/testRenameTarget");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), renameSrc, data, data.length, 1024 * 1024,
        false);
    intercept(FileNotFoundException.class,
        () -> renameEx(renameSrc, renameTarget));
    verifyFileContents(getFileSystem(), renameSrc, data);
  }

  @Test
  public void testRenameWithNonEmptySubDir() throws Throwable {
    final Path renameTestDir = path("testRenameWithNonEmptySubDir");
    final Path srcDir = new Path(renameTestDir, "src1");
    final Path srcSubDir = new Path(srcDir, "sub");
    final Path finalDir = new Path(renameTestDir, "dest");
    FileSystem fs = getFileSystem();
    boolean renameRemoveEmptyDest = isSupported(RENAME_REMOVE_DEST_IF_EMPTY_DIR);
    rm(fs, renameTestDir, true, false);

    fs.mkdirs(srcDir);
    fs.mkdirs(finalDir);
    String sourceTextName = "source.txt";
    Path sourceTextPath = new Path(srcDir, sourceTextName);
    writeTextFile(fs, sourceTextPath,
        "this is the file in src dir", false);
    Path subfileTxt = new Path(srcSubDir, "subfile.txt");
    writeTextFile(fs, subfileTxt,
        "this is the file in src/sub dir", false);

    assertPathExists("not created in src dir",
        sourceTextPath);
    assertPathExists("not created in src/sub dir",
        subfileTxt);

    // no overwrite: fail
    intercept(FileAlreadyExistsException.class,
        () -> renameEx(srcDir, finalDir));
    // now overwrite
    renameEx(srcDir, finalDir, Options.Rename.OVERWRITE);
    // Accept both POSIX rename behavior and CLI rename behavior
    // POSIX rename behavior
    assertPathExists("not renamed into dest dir",
        new Path(finalDir, sourceTextName));
    assertPathExists("not renamed into dest/sub dir",
        new Path(finalDir, "sub/subfile.txt"));
    assertPathDoesNotExist("not deleted",
        sourceTextPath);
  }

  /**
   * Test that after renaming, the nested subdirectory is moved along with all
   * its ancestors.
   */
  @Test
  public void testRenamePopulatesDirectoryAncestors() throws IOException {
    final FileSystem fs = getFileSystem();
    final Path src = path("testRenamePopulatesDirectoryAncestors/source");
    fs.mkdirs(src);
    final String nestedDir = "/dir1/dir2/dir3/dir4";
    fs.mkdirs(path(src + nestedDir));

    Path dst = path("testRenamePopulatesDirectoryAncestorsNew");

    renameEx(src, dst);
    validateAncestorsMoved(src, dst, nestedDir);
  }

  /**
   * Test that after renaming, the nested file is moved along with all its
   * ancestors. It is similar to {@link #testRenamePopulatesDirectoryAncestors}.
   */
  @Test
  public void testRenamePopulatesFileAncestors() throws IOException {
    final FileSystem fs = getFileSystem();
    final Path src = path("testRenamePopulatesFileAncestors/source");
    fs.mkdirs(src);
    final String nestedFile = "/dir1/dir2/dir3/file4";
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, path(src + nestedFile), srcDataset, srcDataset.length,
        1024, false);

    Path dst = path("testRenamePopulatesFileAncestorsNew");

    renameEx(src, dst);
    validateAncestorsMoved(src, dst, nestedFile);
  }

  /**
   * Validate that the nested path and its ancestors should have been moved.
   *
   * @param src the source root to move
   * @param dst the destination root to move
   * @param nestedPath the nested path to move
   */
  private void validateAncestorsMoved(Path src, Path dst, String nestedPath)
      throws IOException {
    assertIsDirectory(dst);
    assertPathDoesNotExist("src path should not exist", path(src + nestedPath));
    assertPathExists("dst path should exist", path(dst + nestedPath));

    Path path = new Path(nestedPath).getParent();
    while (path != null && !path.isRoot()) {
      final Path parentSrc = path(src + path.toString());
      assertPathDoesNotExist(parentSrc + " is not deleted", parentSrc);
      final Path parentDst = path(dst + path.toString());
      assertPathExists(parentDst + " should exist after rename", parentDst);
      assertIsDirectory(parentDst);
      path = path.getParent();
    }
  }

}
