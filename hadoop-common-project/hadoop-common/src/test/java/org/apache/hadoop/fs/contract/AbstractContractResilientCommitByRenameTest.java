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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.ResilientCommitByRename;
import org.apache.hadoop.fs.impl.ResilientCommitByRenameHelper;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertListStatusFinds;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.impl.ResilientCommitByRename.RESILIENT_COMMIT_BY_RENAME_PATH_CAPABILITY;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link ResilientCommitByRename}.
 */
public abstract class AbstractContractResilientCommitByRenameTest extends
    AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractResilientCommitByRenameTest.class);

  private ResilientCommitByRenameHelper committer;

  @Override
  public void setup() throws Exception {
    super.setup();
    final FileSystem fs = getFileSystem();
    final Path path = methodPath();
    Assertions.assertThat(fs)
        .describedAs("FS %s", fs)
        .isInstanceOf(ResilientCommitByRename.class);
    Assertions.assertThat(fs.hasPathCapability(path,
            RESILIENT_COMMIT_BY_RENAME_PATH_CAPABILITY))
        .describedAs("FS %s path capability %s", fs, RESILIENT_COMMIT_BY_RENAME_PATH_CAPABILITY)
        .isTrue();

    committer = new ResilientCommitByRenameHelper(fs);
    Assertions.assertThat(committer.resilientCommitAvailable(path))
        .describedAs("resilient commit available")
        .isTrue();
  }

  private void commit(final Path src, final Path dst) throws IOException {
    committer.commitFile(getFileSystem().getFileStatus(src), dst);
  }

  @Test
  public void testCommitNewFileSameDir() throws Throwable {
    describe("rename a file into a new file in the same directory");
    Path source = path("rename_src");
    Path target = path("rename_dest");
    byte[] data = dataset(256, 'a', 'z');
    final FileSystem fs = getFileSystem();
    writeDataset(fs, source,
        data, data.length, 1024 * 1024, false);
    commit(source, target);

    assertListStatusFinds(fs,
        target.getParent(), target);
    verifyFileContents(fs, target, data);
  }

  @Test
  public void testCommitNonexistentFile() throws Throwable {
    describe("trying to commit a nonexistent file raises FNFE");
    Path source = path("testCommitNonexistentFileSrc");
    final FileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, source);
    final FileStatus status = fs.getFileStatus(source);
    fs.delete(source, false);
    Path dest = path("testCommitNonexistentFileDest");
    intercept(FileNotFoundException.class,
        () -> committer.commitFile(status, dest));
    assertPathDoesNotExist("rename nonexistent file created a destination file",
        dest);
  }

  /**
   * Its an error if the destination exists.
   */
  @Test
  public void testCommitFileOverExistingFile() throws Throwable {
    describe("overwrite in commit is forbidden");
    Path source = path("source-256.txt");
    byte[] sourceData = dataset(256, 'a', 'z');
    final FileSystem fs = getFileSystem();
    writeDataset(fs, source, sourceData, sourceData.length, 1024, false);
    Path dest = path("dest-512.txt");
    byte[] destData = dataset(512, 'A', 'Z');
    writeDataset(fs, dest, destData, destData.length, 1024, false);
    assertIsFile(dest);
    final IOException exception = intercept(FileAlreadyExistsException.class, () ->
        commit(source, dest));
    LOG.info("caught exception", exception);

  }


  @Test
  public void testCommitParentIsFile() throws Exception {
    String action = "commit where the parent is a file";
    describe(action);
    Path base = methodPath();
    Path grandparent = new Path(base, "file");
    expectCommitUnderFileFails(action,
        grandparent,
        new Path(base, "testCommitSrc"),
        new Path(grandparent, "testCommitTarget"));
  }

  @Test
  public void testCommitGrandparentIsFile() throws Exception {
    String action = "commit where the grandparent is a file";
    describe(action);
    Path base = methodPath();
    Path grandparent = new Path(base, "file");
    Path parent = new Path(grandparent, "parent");
    expectCommitUnderFileFails(action,
        grandparent,
        new Path(base, "testCommitSrc"),
        new Path(parent, "testCommitTarget"));
  }

  protected void expectCommitUnderFileFails(String action,
      Path file, Path renameSrc, Path renameTarget)
      throws Exception {
    byte[] data = dataset(256, 'a', 'z');
    FileSystem fs = getFileSystem();
    writeDataset(fs, file, data, data.length, 1024 * 1024,
        true);
    writeDataset(fs, renameSrc, data, data.length, 1024 * 1024,
        true);

    IOException exception = intercept(IOException.class, () -> commit(renameSrc, renameTarget));
    LOG.info("caught exception", exception);
    assertPathDoesNotExist("after failure ", renameTarget);
    assertPathExists(action, renameSrc);
  }

}
