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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

/**
 * Test the {@link TrashPolicy} returned by {@link FileSystem#getTrashPolicy}
 * results in a consistent trash behavior.
 * <p>
 *   Consistent trash behavior means that invoking the {@link TrashPolicy} methods in the
 *   following order should not result in unexpected results such as files in trash that
 *   will never be deleted by trash mechanism.
 *   <ol>
 *   <li>
 *     {@link TrashPolicy#getDeletionInterval()} should return 0 before
 *     {@link TrashPolicy#initialize(Configuration, FileSystem)} is invoked.
 *     The deletion interval should not return negative value. Zero value implies
 *     that trash is disabled, which means {@link TrashPolicy#isEnabled()} should
 *     return false.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#initialize(Configuration, FileSystem)} should be implemented
 *     and ensure that the subsequent {@link TrashPolicy} operations should work properly
 *   </li>
 *   <li>
 *     {@link TrashPolicy#isEnabled()} should return true after
 *     {@link TrashPolicy#initialize(Configuration, FileSystem)} is invoked and
 *     initialize the deletion interval to positive value. {@link TrashPolicy#isEnabled()}
 *     should remain false if the {@link TrashPolicy#getDeletionInterval()}} returns 0 even after
 *     {@link TrashPolicy#initialize(Configuration, FileSystem)} has been invoked.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#moveToTrash(Path)} should move a file or directory to the
 *     current trash directory defined {@link TrashPolicy#getCurrentTrashDir(Path)}
 *     if it's not already in the trash. This implies that
 *     the {@link FileSystem#exists(Path)} should return false for the original path, but
 *     should return true for the current trash directory.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#moveToTrash(Path)} should return false if {@link TrashPolicy#isEnabled()} is false or
 *     the path is already under {@link FileSystem#getTrashRoot(Path)}. There should not be any side
 *     effect when {@link TrashPolicy#moveToTrash(Path)} returns false.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#createCheckpoint()} should create rename the current trash directory to
 *     another trash directory which is not equal to {@link TrashPolicy#getCurrentTrashDir(Path)}.
 *     {@link TrashPolicy#createCheckpoint()} is a no-op if there is no current trash directory.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#deleteCheckpoint()} should cleanup the all the current
 *     and checkpoint directories under {@link FileSystem#getTrashRoots(boolean)} created before
 *     {@link TrashPolicy#getDeletionInterval()} minutes ago.
 *     Note that the current trash directory {@link TrashPolicy#getCurrentTrashDir()} should not be deleted.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#deleteCheckpointsImmediately()} should cleanup the checkpoint directories under
 *     {@link FileSystem#getTrashRoots(boolean)} regardless of the checkpoint timestamp.
 *     Note that the current trash directory {@link TrashPolicy#getCurrentTrashDir()} should not be deleted.
 *   </li>
 *   <li>
 *     {@link TrashPolicy#getEmptier()} returns a runnable that will empty the trash.
 *     The effective trash emptier interval should be [0, {@link TrashPolicy#getDeletionInterval()}].
 *     Zero interval means that {@link Runnable#run()} is a no-op and returns immediately.
 *     Non-zero trash emptier interval means that the {@link Runnable#run()} keeps running for
 *     each interval (unless it is interrupted). For each interval, the trash emptier carry out the
 *     following operations:
 *     <ol>
 *       It checks all the trash root directories through {@link FileSystem#getTrashRoots(boolean)} for all users.
 *     </ol>
 *     <ol>
 *       For each trash root directory, it deletes the trash checkpoint directory with checkpoint time older than
 *       {@link TrashPolicy#getDeletionInterval()}. Afterward, it creates a new trash checkpoint through
 *       {@link TrashPolicy#createCheckpoint()}. Note that existing checkpoints which has not expired, will not
 *       have any change.
 *     </ol>
 *   </li>
 *   </ol>
 * </p>
 */
public abstract class AbstractContractTrashTest extends AbstractFSContractTestBase {

  @BeforeEach
  @Override
  public void setup() throws Exception {
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
    final FileSystem fs = getFileSystem();
    Collection<FileStatus> trashRoots = fs.getTrashRoots(true);
    for (FileStatus trashRoot : trashRoots) {
      fs.delete(trashRoot.getPath(), true);
    }
    super.teardown();
  }

  @Test
  public void testTrashPolicy() throws Throwable {
    final FileSystem fs = getFileSystem();

    // TrashPolicy needs to be initialized with non-zero deletion interval before
    // TrashPolicy#isEnabled returns true
    final TrashPolicy trashPolicy = fs.getTrashPolicy(getContract().getConf());
    assertFalse(trashPolicy.isEnabled());
    assertEquals(0, trashPolicy.getDeletionInterval());
    assertFalse(trashPolicy.moveToTrash(new Path("randomFile")));
    trashPolicy.initialize(getContract().getConf(), fs);
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

    assertTrue(trashPolicy.moveToTrash(fileToDelete));
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
    assertEquals(1, trashRootChildren.length);
    FileStatus trashCheckpointDir = trashRootChildren[0];
    Path expectedCheckpointTrashPath = Path.mergePaths(trashCheckpointDir.getPath(), fileToDelete);
    ContractTestUtils.verifyFileContents(fs, expectedCheckpointTrashPath, data);

    // TrashPolicy#deleteCheckpoint
    Thread.sleep(12000); // This should be the time set as deletion interval
    trashPolicy.deleteCheckpoint();
    assertPathDoesNotExist("the path under checkpoint directory should be deleted",
        expectedCheckpointTrashPath);
    trashRootChildren = ContractTestUtils.listChildren(fs, fs.getTrashRoot(fileToDelete));
    assertEquals(0, trashRootChildren.length);
  }

  @Test
  public void testEmptier() throws Throwable {
    // Adapted from TestTrash#testTrashEmptier.
    final FileSystem fs = getFileSystem();

    // Start Emptier in background
    final TrashPolicy trashPolicy = fs.getTrashPolicy(getContract().getConf());
    trashPolicy.initialize(getContract().getConf(), fs);

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
      assertTrue(trashPolicy.moveToTrash(myFile));

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
        assertTrue(checkpoints.size() > files.length);
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
      assertTrue(trash.moveToTrash(myFile));
      Path currenTrashDir = trash.getCurrentTrashDir(myFile);
      Path expectedCurrentTrashFile = Path.mergePaths(currenTrashDir, myFile);
      assertPathExists("File should be moved to trash", expectedCurrentTrashFile);
    }

    // Verify that we can recreate the file
    ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

    // Verify that we succeed in removing the file we re-created
    assertTrue(trash.moveToTrash(myFile));

    // Verify that we can recreated the file
    ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);

    // Verify that we succeed in removing the whole directory
    // along with the file inside it.
    assertTrue(trash.moveToTrash(baseDir));
    assertPathDoesNotExist("The deleted directory should not exist", baseDir);
    assertPathDoesNotExist("The file under deleted directory should not exist", myFile);

    // recreate directory
    mkdirs(baseDir);

    // Verify that we succeed in removing the whole directory
    assertTrue(trash.moveToTrash(baseDir));

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
      assertTrue(trash.moveToTrash(myFile));
      Path currentTrashDir = trash.getCurrentTrashDir(myFile);
      Path trashFilePath = Path.mergePaths(currentTrashDir, myFile);
      assertPathExists("Trash file should exist", trashFilePath);

      assertTrue(trash.moveToTrash(baseDir));
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
        assertTrue(trash.moveToTrash(myFile));
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
      assertTrue(trash.moveToTrash(myFile));
      trash.checkpoint();
      ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);
      assertTrue(trash.moveToTrash(myFile));
      trash.checkpoint();
      ContractTestUtils.writeTextFile(fs, myFile, "myFileContent", false);
      assertTrue(trash.moveToTrash(myFile));

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
}
