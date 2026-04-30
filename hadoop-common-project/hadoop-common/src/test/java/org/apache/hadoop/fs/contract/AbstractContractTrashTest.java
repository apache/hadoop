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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the {@link TrashPolicy} returned by {@link FileSystem#getTrashPolicy}
 * results in a consistent trash behavior.
 */
public abstract class AbstractContractTrashTest extends AbstractFSContractTestBase {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractTrashTest.class);

  @BeforeEach
  @Override
  public void setup() throws Exception {
    FileSystem.closeAll();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // Enable trash with 12 seconds deletes and 6 seconds checkpoints
    conf.set(FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
    conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds
    return conf;
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    Exception cleanupException = null;
    try {
      final FileSystem fs = getFileSystem();
      Collection<FileStatus> trashRoots = fs.getTrashRoots(true);
      for (FileStatus trashRoot : trashRoots) {
        fs.delete(trashRoot.getPath(), true);
      }
    } catch (Exception e) {
      cleanupException = e;
    }
    try {
      super.teardown();
    } catch (Exception e) {
      if (cleanupException != null) {
        e.addSuppressed(cleanupException);
      }
      throw e;
    }
    if (cleanupException != null) {
      throw cleanupException;
    }
  }

  @Test
  public void testTrashPolicy() throws Throwable {
    final FileSystem fs = getFileSystem();

    final TrashPolicy trashPolicy = fs.getTrashPolicy(new Path("/"), getContract().getConf());
    assertTrue(trashPolicy.isEnabled());
    assertTrue(trashPolicy.getDeletionInterval() > 0);

    // Check that the current directory is still empty even if checkpoints operation is run
    assertPathDoesNotExist("trash current directory should not exist before moveToTrash",
        trashPolicy.getCurrentTrashDir());
    trashPolicy.createCheckpoint();
    assertPathDoesNotExist("trash current directory should not exist before moveToTrash",
        trashPolicy.getCurrentTrashDir());
    trashPolicy.deleteCheckpoint();
    assertPathDoesNotExist("trash current directory should not exist before moveToTrash",
        trashPolicy.getCurrentTrashDir());
    trashPolicy.deleteCheckpointsImmediately();
    assertPathDoesNotExist("trash current directory should not exist before moveToTrash",
        trashPolicy.getCurrentTrashDir());

    // TrashPolicy#moveToTrash should move the file to the current trash directory
    Path base = methodPath();
    mkdirs(base);
    Path fileToDelete = new Path(base, "testFile");
    byte[] data = ContractTestUtils.dataset(256, 'a', 'z');
    ContractTestUtils.writeDataset(fs, fileToDelete, data, data.length, 1024 * 1024, false);

    assertMovedToTrash(trashPolicy, fileToDelete);
    assertPathExists("trash current directory should exist after moveToTrash",
        trashPolicy.getCurrentTrashDir());
    Path expectedCurrentTrashPath = Path.mergePaths(trashPolicy.getCurrentTrashDir(fileToDelete), fileToDelete);;
    ContractTestUtils.verifyFileContents(fs, expectedCurrentTrashPath, data);
    // Calling TrashPolicy#moveToTrash on the key in path should return false
    // and the file remains unchanged
    assertFalse(trashPolicy.moveToTrash(expectedCurrentTrashPath));
    ContractTestUtils.verifyFileContents(fs, expectedCurrentTrashPath, data);

    // Calling TrashPolicy#deleteCheckpoint or TrashPolicy#deleteCheckpointImmediately has no effect on the
    // current trash directory.
    trashPolicy.deleteCheckpoint();
    trashPolicy.deleteCheckpointsImmediately();
    ContractTestUtils.verifyFileContents(fs, expectedCurrentTrashPath, data);

    // TrashPolicy#createCheckpoint rename the current trash directory to a new directory
    trashPolicy.createCheckpoint();
    assertPathDoesNotExist("trash current directory should not exist after checkpoint",
        trashPolicy.getCurrentTrashDir(fileToDelete));
    assertPathDoesNotExist("the path under current trash directory should not exist after checkpoint",
        expectedCurrentTrashPath);
    FileStatus[] trashRootChildren = ContractTestUtils.listChildren(fs, fs.getTrashRoot(fileToDelete));
    assertThat(trashRootChildren).hasSize(1);
    FileStatus trashCheckpointDir = trashRootChildren[0];
    Path expectedCheckpointTrashPath = Path.mergePaths(trashCheckpointDir.getPath(), fileToDelete);
    ContractTestUtils.verifyFileContents(fs, expectedCheckpointTrashPath, data);

    // TrashPolicy#deleteCheckpoint
    Thread.sleep(12000); // This should be the time set as deletion interval
    trashPolicy.deleteCheckpoint();
    assertPathDoesNotExist("the path under checkpoint directory should be deleted",
        expectedCheckpointTrashPath);
    trashRootChildren = ContractTestUtils.listChildren(fs, fs.getTrashRoot(fileToDelete));
    assertThat(trashRootChildren).hasSize(0);
  }

  @Test
  public void testEmptier() throws Throwable {
    // Adapted from TestTrash#testTrashEmptier.
    final FileSystem fs = getFileSystem();

    // Start Emptier in background
    final TrashPolicy trashPolicy = fs.getTrashPolicy(new Path("/"), getContract().getConf());

    Runnable emptier = trashPolicy.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    // First create a new directory with mkdirs
    Path base = methodPath();
    mkdirs(base);
    int fileIndex = 0;
    Set<String> checkpoints = new HashSet<>();
    while (true) {
      // Create a file with a new name
      Path myFile = new Path(base, "myFile" + fileIndex);
      ContractTestUtils.writeTextFile(fs, myFile, "file" + fileIndex, false);
      fileIndex++;

      // Move the files to trash
      assertMovedToTrash(trashPolicy, myFile);

      Path trashDir = trashPolicy.getCurrentTrashDir(myFile);
      FileStatus files[] = fs.listStatus(trashDir.getParent());
      // Scan files in .Trash and add them to set of checkpoints
      for (FileStatus file : files) {
        String fileName = file.getPath().getName();
        checkpoints.add(fileName);
      }
      // If checkpoints has 4 objects it is Current + 3 checkpoint directories
      if (checkpoints.size() == 4) {
        // The actual contents should be smaller since the last checkpoint
        // should've been deleted and Current might not have been recreated yet
        assertThat(checkpoints).hasSizeGreaterThan(files.length);
        break;
      }
      Thread.sleep(5000);
    }
    emptierThread.interrupt();
    emptierThread.join();
  }

  @Test
  public void testTrash() throws Throwable {
    // Adapted from TestTrash#testTrash. There are some tests that are excluded,
    // such as checkpoint format tests since the trash does not specify the trash
    // checkpoint requirements
    final FileSystem fs = getFileSystem();
    Trash trash = new Trash(fs, getContract().getConf());

    // First create a new directory with mkdirs
    Path baseDir = methodPath();
    mkdirs(baseDir);

    // Create a file in that directory
    Path myFile = new Path(baseDir, "myFile");
    ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

    // Verify that expunge without Trash directory will not throw Exception
    trash.expunge();

    // Verify that we succeed in removing the file we created
    // This should go into Trash.
    {
      assertMovedToTrash(trash, myFile);
      Path currenTrashDir = trash.getCurrentTrashDir(myFile);
      Path expectedCurrentTrashFile = Path.mergePaths(currenTrashDir, myFile);
      assertPathExists("File should be moved to trash", expectedCurrentTrashFile);
    }

    // Verify that we can recreate the file
    ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

    // Verify that we succeed in removing the file we re-created
    assertMovedToTrash(trash, myFile);

    // Verify that we can recreated the file
    ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

    // Verify that we succeed in removing the whole directory
    // along with the file inside it.
    assertMovedToTrash(trash, baseDir);
    assertPathDoesNotExist("The deleted directory should not exist", baseDir);
    assertPathDoesNotExist("The file under deleted directory should not exist", myFile);

    // recreate directory
    mkdirs(baseDir);

    // Verify that we succeed in removing the whole directory
    assertMovedToTrash(trash, baseDir);

    // Check that we can delete a file from the trash
    {
      Path currentTrashDir = trash.getCurrentTrashDir(null);
      Path toErase = new Path(currentTrashDir, "toErase");
      ContractTestUtils.writeTextFile(fs, toErase, "toEraseContent", false);

      assertTrue(fs.delete(toErase, false));
      assertPathDoesNotExist("The deleted file in trash should not exist", toErase);
    }

    // Simulate trash removal
    {
      Path currentTrashDir = trash.getCurrentTrashDir(myFile);
      Path trashFilePath = Path.mergePaths(currentTrashDir, myFile);
      assertPathExists("Trash file should exist before expunge", trashFilePath);
      trash.expunge();
      trash.checkpoint();
      // Verify that after expunging the Trash, it really goes away
      assertPathDoesNotExist("Trash file should be deleted after trash has " +
          "been expunge", trashFilePath);
    }

    // Recreate directory and file
    mkdirs(baseDir);
    ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

    // remove the file first, then remove directory
    {
      assertMovedToTrash(trash, myFile);
      Path currentTrashDir = trash.getCurrentTrashDir(myFile);
      Path trashFilePath = Path.mergePaths(currentTrashDir, myFile);
      assertPathExists("Trash file should exist", trashFilePath);

      assertMovedToTrash(trash, baseDir);
      Path trashDirPath = Path.mergePaths(currentTrashDir, baseDir);
      assertPathExists("Trash directory should exist", trashDirPath);
    }

    // attempt to remove parent of trash
    {
      Path currentTrashDir = trash.getCurrentTrashDir(myFile);
      Path trashRootParent = currentTrashDir.getParent().getParent();

      assertThrows(IOException.class, () -> trash.moveToTrash(trashRootParent));
      assertPathExists("Trash root should still exist", currentTrashDir);
    }

    // deleting same file multiple times
    {
      mkdirs(baseDir);
      trash.expungeImmediately();

      int numRuns = 10;
      for (int i = 0; i < numRuns; i++) {
        // create file
        ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

        // move file to trash
        assertMovedToTrash(trash, myFile);
      }

      // current trash directory
      Path trashDir = Path.mergePaths(trash.getCurrentTrashDir(myFile),
          new Path(myFile.getParent().toUri().getPath()));

      // count the number of files in the current trash directory
      final String prefix = myFile.getName();

      // filter that matches all the files that start with fileName*
      PathFilter pf = new PathFilter() {
        @Override
        public boolean accept(Path file) {
          return file.getName().startsWith(prefix);
        }
      };
      FileStatus [] fss = fs.listStatus(trashDir, pf);

      assertEquals(numRuns, fss.length, "Count should have returned " + numRuns);
    }

    // verify expungeImmediately removes all checkpoints and current folder
    {
      mkdirs(baseDir);

      // moveToTrash thrice, create checkpoint after the first two
      ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);
      assertMovedToTrash(trash, myFile);
      trash.checkpoint();
      ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);
      assertMovedToTrash(trash, myFile);
      trash.checkpoint();
      ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);
      assertMovedToTrash(trash, myFile);

      // There should be two trash checkpoint directories and one current directory
      Path trashRootPath = trash.getCurrentTrashDir(myFile);
      Path trashRootParent = trashRootPath.getParent();
      FileStatus[] fss = fs.listStatus(trashRootParent);
      assertEquals(3, fss.length);

      // Clear out trash
      trash.expungeImmediately();

      // Now the trash folder should be empty
      fss = fs.listStatus(trashRootParent);
      assertEquals(0, fss.length);
    }
  }

  private void assertMovedToTrash(Trash trash, Path path) throws IOException {
    assertTrue(trash.moveToTrash(path),
        "Failed to move " + path + " to trash");
  }

  private void assertMovedToTrash(TrashPolicy trashPolicy, Path path) throws IOException {
    assertTrue(trashPolicy.moveToTrash(path),
        "Failed to move " + path + " to trash");
  }
}
