/**
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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Testing random FileSystem operations with random Snapshot operations.
 */
public class TestRandomOpsWithSnapshots {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRandomOpsWithSnapshots.class);

  private static final short REPL = 3;
  private static final long BLOCKSIZE = 1024;

  private static final int TOTAL_FILECOUNT = 250;
  private static final int MAX_NUM_ITERATIONS = 10;
  private static final int MAX_NUM_FILESYSTEM_OPERATIONS = 50;
  private static final int MAX_NUM_SNAPSHOT_OPERATIONS = 50;
  private static final int MAX_NUM_SUB_DIRECTORIES_LEVEL = 10;
  private static final int MAX_NUM_FILE_LENGTH = 100;
  private static final int MIN_NUM_OPERATIONS = 25;

  private static final String TESTDIRSTRING = "/testDir";
  private static final String WITNESSDIRSTRING = "/WITNESSDIR";
  private static final Path TESTDIR = new Path(TESTDIRSTRING);
  private static final Path WITNESSDIR = new Path(WITNESSDIRSTRING);
  private static List<Path> snapshottableDirectories = new ArrayList<Path>();
  private static Map<Path, ArrayList<String>> pathToSnapshotsMap =
      new HashMap<Path, ArrayList<String>>();

  private static final Configuration CONFIG = new Configuration();
  private MiniDFSCluster cluster;
  private DistributedFileSystem hdfs;
  private static Random generator = null;

  private int numberFileCreated = 0;
  private int numberFileDeleted = 0;
  private int numberFileRenamed = 0;

  private int numberDirectoryCreated = 0;
  private int numberDirectoryDeleted = 0;
  private int numberDirectoryRenamed = 0;

  private int numberSnapshotCreated = 0;
  private int numberSnapshotDeleted = 0;
  private int numberSnapshotRenamed = 0;

  // Operation directories
  private enum OperationDirectories {
    TestDir,
    WitnessDir;
  }

  // Operation type
  private enum OperationType {
    FileSystem,
    Snapshot;
  }

  // FileSystem & Snapshot operation
  private enum Operations {
    FileSystem_CreateFile(2 /*operation weight*/, OperationType.FileSystem),
    FileSystem_DeleteFile(2, OperationType.FileSystem),
    FileSystem_RenameFile(2, OperationType.FileSystem),
    FileSystem_CreateDir(1, OperationType.FileSystem),
    FileSystem_DeleteDir(1, OperationType.FileSystem),
    FileSystem_RenameDir(2, OperationType.FileSystem),

    Snapshot_CreateSnapshot(5, OperationType.Snapshot),
    Snapshot_DeleteSnapshot(3, OperationType.Snapshot),
    Snapshot_RenameSnapshot(2, OperationType.Snapshot);

    private int weight;
    private OperationType operationType;

    Operations(int weight, OperationType type) {
      this.weight = weight;
      this.operationType = type;
    }

    private int getWeight() {
      return weight;
    }

    private static final Operations[] VALUES = values();

    private static int sumWeights(OperationType type) {
      int sum = 0;
      for (Operations value: VALUES) {
        if (value.operationType == type) {
          sum += value.getWeight();
        }
      }
      return sum;
    }

    private static final int TOTAL_WEIGHT_FILESYSTEM =
        sumWeights(OperationType.FileSystem);

    private static final int TOTAL_WEIGHT_SNAPSHOT =
        sumWeights(OperationType.Snapshot);

    public static Operations getRandomOperation(OperationType type) {
      int randomNum = 0;
      Operations randomOperation = null;
      switch (type) {
      case FileSystem:
        randomNum = generator.nextInt(TOTAL_WEIGHT_FILESYSTEM);
        break;
      case Snapshot:
        randomNum = generator.nextInt(TOTAL_WEIGHT_SNAPSHOT);
        break;
      default:
        break;
      }
      int currentWeightSum = 0;
      for (Operations currentValue: VALUES) {
        if (currentValue.operationType == type) {
          if (randomNum <= (currentWeightSum + currentValue.getWeight())) {
            randomOperation = currentValue;
            break;
          }
          currentWeightSum += currentValue.getWeight();
        }
      }
      return randomOperation;
    }
  }

  @Before
  public void setUp() throws Exception {
    CONFIG.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(CONFIG).numDataNodes(REPL).
        format(true).build();
    cluster.waitActive();

    hdfs = cluster.getFileSystem();
    hdfs.mkdirs(TESTDIR);
    hdfs.mkdirs(WITNESSDIR);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /*
   * Random file system operations with snapshot operations in between.
   */
  @Test(timeout = 900000)
  public void testRandomOperationsWithSnapshots()
          throws IOException, InterruptedException, TimeoutException {
    // Set
    long seed = System.currentTimeMillis();
    LOG.info("testRandomOperationsWithSnapshots, seed to be used: " + seed);
    generator = new Random(seed);

    int fileLen = generator.nextInt(MAX_NUM_FILE_LENGTH);
    createFiles(TESTDIRSTRING, fileLen);

    // Get list of snapshottable directories
    SnapshottableDirectoryStatus[] snapshottableDirectoryStatus =
        hdfs.getSnapshottableDirListing();
    for (SnapshottableDirectoryStatus ssds : snapshottableDirectoryStatus) {
      snapshottableDirectories.add(ssds.getFullPath());
    }

    if (snapshottableDirectories.size() == 0) {
      hdfs.allowSnapshot(hdfs.getHomeDirectory());
      snapshottableDirectories.add(hdfs.getHomeDirectory());
    }

    int numberOfIterations = generator.nextInt(MAX_NUM_ITERATIONS);
    LOG.info("Number of iterations: " + numberOfIterations);

    int numberFileSystemOperations = generator.nextInt(
        MAX_NUM_FILESYSTEM_OPERATIONS-MIN_NUM_OPERATIONS+1)+MIN_NUM_OPERATIONS;
    LOG.info("Number of FileSystem operations: "+ numberFileSystemOperations);

    int numberSnapshotOperations = generator.nextInt(
        MAX_NUM_SNAPSHOT_OPERATIONS-MIN_NUM_OPERATIONS)+MIN_NUM_OPERATIONS;
    LOG.info("Number of Snapshot operations: " + numberSnapshotOperations);

    // Act && Verify
    randomOperationsWithSnapshots(numberOfIterations,
        numberFileSystemOperations, numberSnapshotOperations);
  }

  /*
   * Based on input we're performing:
   *   random number of file system operations
   *   random number of snapshot operations
   *   restart name node making sure fsimage can be loaded successfully.
   */
  public void randomOperationsWithSnapshots(int numberOfIterations,
                                            int numberFileSystemOperations,
                                            int numberSnapshotOperations)
          throws IOException, InterruptedException, TimeoutException {
    // random number of iterations
    for (int i = 0; i < numberOfIterations; i++) {
      // random number of FileSystem operations
      for (int j = 0; j < numberFileSystemOperations; j++) {
        Operations fsOperation =
            Operations.getRandomOperation(OperationType.FileSystem);
        LOG.info("fsOperation: " + fsOperation);
        switch (fsOperation) {
        case FileSystem_CreateDir:
          createTestDir();
          break;

        case FileSystem_DeleteDir:
          deleteTestDir();
          break;

        case FileSystem_RenameDir:
          renameTestDir();
          break;

        case FileSystem_CreateFile:
          createTestFile();
          break;

        case FileSystem_DeleteFile:
          deleteTestFile();
          break;

        case FileSystem_RenameFile:
          renameTestFile();
          break;

        default:
          assertNull("Invalid FileSystem operation", fsOperation);
          break;
        }
      }

      // random number of Snapshot operations
      for (int k = 0; k < numberSnapshotOperations; k++) {
        Operations snapshotOperation =
            Operations.getRandomOperation(OperationType.Snapshot);
        LOG.info("snapshotOperation: " + snapshotOperation);

        switch (snapshotOperation) {
        case Snapshot_CreateSnapshot:
          createSnapshot();
          break;

        case Snapshot_DeleteSnapshot:
          deleteSnapshot();
          break;

        case Snapshot_RenameSnapshot:
          renameSnapshot();
          break;

        default:
          assertNull("Invalid Snapshot operation", snapshotOperation);
          break;
        }
      }
      // Verification
      checkClusterHealth();
    }
  }

  /* Create a new test directory. */
  private void createTestDir() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path parentDir = snapshottableDirectories.get(index);
      Path newDir = new Path(parentDir, "createTestDir_" +
          UUID.randomUUID().toString());

      for (OperationDirectories dir : OperationDirectories.values()) {
        if (dir == OperationDirectories.WitnessDir) {
          newDir = new Path(getNewPathString(newDir.toString(),
              TESTDIRSTRING, WITNESSDIRSTRING));
        }
        hdfs.mkdirs(newDir);
        assertTrue("Directory exists", hdfs.exists(newDir));
        LOG.info("Directory created: " + newDir);
        numberDirectoryCreated++;
      }
    }
  }

  /* Delete an existing test directory. */
  private void deleteTestDir() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path deleteDir = snapshottableDirectories.get(index);

      if (!pathToSnapshotsMap.containsKey(deleteDir)) {
        boolean isWitnessDir = false;
        for (OperationDirectories dir : OperationDirectories.values()) {
          if (dir == OperationDirectories.WitnessDir) {
            isWitnessDir = true;
            deleteDir = new Path(getNewPathString(deleteDir.toString(),
                TESTDIRSTRING, WITNESSDIRSTRING));
          }
          hdfs.delete(deleteDir, true);
          assertFalse("Directory does not exist", hdfs.exists(deleteDir));
          if (!isWitnessDir) {
            snapshottableDirectories.remove(deleteDir);
          }
          LOG.info("Directory removed: " + deleteDir);
          numberDirectoryDeleted++;
        }
      }
    }
  }

  /* Rename an existing test directory. */
  private void renameTestDir() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path oldDir = snapshottableDirectories.get(index);

      if (!pathToSnapshotsMap.containsKey(oldDir)) {
        Path newDir = oldDir.suffix("_renameDir+"+UUID.randomUUID().toString());
        for (OperationDirectories dir : OperationDirectories.values()) {
          if (dir == OperationDirectories.WitnessDir) {
            oldDir = new Path(getNewPathString(oldDir.toString(),
                TESTDIRSTRING, WITNESSDIRSTRING));
            newDir = new Path(getNewPathString(newDir.toString(),
                TESTDIRSTRING, WITNESSDIRSTRING));
          }
          hdfs.rename(oldDir, newDir, Options.Rename.OVERWRITE);
          assertTrue("Target directory exists", hdfs.exists(newDir));
          assertFalse("Source directory does not exist",
              hdfs.exists(oldDir));

          if (dir == OperationDirectories.TestDir) {
            snapshottableDirectories.remove(oldDir);
            snapshottableDirectories.add(newDir);
          }
          LOG.info("Renamed directory:" + oldDir + " to directory: " + newDir);
          numberDirectoryRenamed++;
        }
      }
    }
  }

  /* Create a new snapshot. */
  private void createSnapshot() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path randomDir = snapshottableDirectories.get(index);
      String snapshotName = Integer.toString(generator.nextInt()) + ".ss";
      hdfs.createSnapshot(randomDir, snapshotName);
      LOG.info("createSnapshot, directory: " + randomDir +
          ", snapshot name: " + snapshotName);
      numberSnapshotCreated++;

      if (pathToSnapshotsMap.containsKey(randomDir)) {
        pathToSnapshotsMap.get(randomDir).add(snapshotName);
      } else {
        pathToSnapshotsMap.put(randomDir,
            new ArrayList<String>(Arrays.asList(snapshotName)));
      }
    }
  }

  /* Delete an existing snapshot. */
  private void deleteSnapshot() throws IOException {
    if (!pathToSnapshotsMap.isEmpty()) {
      int index = generator.nextInt(pathToSnapshotsMap.size());
      Object[] snapshotPaths = pathToSnapshotsMap.keySet().toArray();
      Path snapshotPath = (Path)snapshotPaths[index];
      ArrayList<String> snapshotNameList=pathToSnapshotsMap.get(snapshotPath);

      String snapshotNameToBeDeleted = snapshotNameList.get(
          generator.nextInt(snapshotNameList.size()));
      hdfs.deleteSnapshot(snapshotPath, snapshotNameToBeDeleted);
      LOG.info("deleteSnapshot, directory: " + snapshotPath +
          ", snapshot name: " + snapshotNameToBeDeleted);
      numberSnapshotDeleted++;

      // Adjust pathToSnapshotsMap after snapshot deletion
      if (snapshotNameList.size() == 1) {
        pathToSnapshotsMap.remove(snapshotPath);
      } else {
        pathToSnapshotsMap.get(snapshotPath).remove(snapshotNameToBeDeleted);
      }
    }
  }

  /* Rename an existing snapshot. */
  private void renameSnapshot() throws IOException {
    if (!pathToSnapshotsMap.isEmpty()) {
      int index = generator.nextInt(pathToSnapshotsMap.size());
      Object[] snapshotPaths = pathToSnapshotsMap.keySet().toArray();
      Path snapshotPath = (Path)snapshotPaths[index];
      ArrayList<String> snapshotNameList =
          pathToSnapshotsMap.get(snapshotPath);

      String snapshotOldName = snapshotNameList.get(
          generator.nextInt(snapshotNameList.size()));

      String snapshotOldNameNoExt = snapshotOldName.substring(0,
          snapshotOldName.lastIndexOf('.')-1);

      String snapshotNewName = snapshotOldNameNoExt + "_rename.ss";
      hdfs.renameSnapshot(snapshotPath, snapshotOldName, snapshotNewName);
      LOG.info("renameSnapshot, directory:" + snapshotPath + ", snapshot name:"
          + snapshotOldName + " to " + snapshotNewName);
      numberSnapshotRenamed++;

      // Adjust pathToSnapshotsMap after snapshot deletion
      pathToSnapshotsMap.get(snapshotPath).remove(snapshotOldName);
      pathToSnapshotsMap.get(snapshotPath).add(snapshotNewName);
    }
  }

  /* Create a new test file. */
  private void createTestFile() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path randomDir = snapshottableDirectories.get(index);
      if (!randomDir.isRoot()) {
        randomDir = randomDir.getParent();
      }

      Path newFile = new Path(randomDir, "createTestFile.log");
      for (OperationDirectories dir : OperationDirectories.values()) {
        if (dir == OperationDirectories.WitnessDir) {
          newFile = new Path(getNewPathString(newFile.toString(),
              TESTDIRSTRING, WITNESSDIRSTRING));
        }
        hdfs.createNewFile(newFile);
        assertTrue("File exists", hdfs.exists(newFile));
        LOG.info("createTestFile, file created: " + newFile);
        numberFileCreated++;
      }
    }
  }

  /* Delete an existing test file. */
  private void deleteTestFile() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path randomDir = snapshottableDirectories.get(index);

      FileStatus[] fileStatusList = hdfs.listStatus(randomDir);
      for (FileStatus fsEntry : fileStatusList) {
        if (fsEntry.isFile()) {
          Path deleteFile = fsEntry.getPath();
          for (OperationDirectories dir : OperationDirectories.values()) {
            if (dir == OperationDirectories.WitnessDir) {
              deleteFile = new Path(getNewPathString(deleteFile.toString(),
                  TESTDIRSTRING, WITNESSDIRSTRING));
            }
            hdfs.delete(deleteFile, false);
            assertFalse("File does not exists",
                hdfs.exists(deleteFile));
            LOG.info("deleteTestFile, file deleted: " + deleteFile);
            numberFileDeleted++;
          }
          break;
        }
      }
    }
  }

  /* Rename an existing test file. */
  private void renameTestFile() throws IOException {
    if (snapshottableDirectories.size() > 0) {
      int index = generator.nextInt(snapshottableDirectories.size());
      Path randomDir = snapshottableDirectories.get(index);

      FileStatus[] fileStatusList = hdfs.listStatus(randomDir);
      for (FileStatus fsEntry : fileStatusList) {
        if (fsEntry.isFile()) {
          Path oldFile = fsEntry.getPath();
          Path newFile = oldFile.suffix("_renameFile");
          for (OperationDirectories dir : OperationDirectories.values()) {
            if (dir == OperationDirectories.WitnessDir) {
              oldFile = new Path(getNewPathString(oldFile.toString(),
                  TESTDIRSTRING, WITNESSDIRSTRING));
              newFile = new Path(getNewPathString(newFile.toString(),
                  TESTDIRSTRING, WITNESSDIRSTRING));
            }

            hdfs.rename(oldFile, newFile, Options.Rename.OVERWRITE);
            assertTrue("Target file exists", hdfs.exists(newFile));
            assertFalse("Source file does not exist", hdfs.exists(oldFile));
            LOG.info("Renamed file: " + oldFile + " to file: " + newFile);
            numberFileRenamed++;
          }
          break;
        }
      }
    }
  }

  /* Check cluster health. */
  private void checkClusterHealth() throws IOException, InterruptedException,
      TimeoutException {
    // 1. FileStatus comparison between test and witness directory
    FileStatus[] testDirStatus = hdfs.listStatus(TESTDIR);
    FileStatus[] witnessDirStatus = hdfs.listStatus(WITNESSDIR);
    assertEquals(witnessDirStatus.length, testDirStatus.length);
    LOG.info("checkClusterHealth, number of entries verified.");

    Arrays.sort(testDirStatus);
    Arrays.sort(witnessDirStatus);
    for (int i = 0; i < testDirStatus.length; i++) {
      assertEquals(witnessDirStatus[i].getPermission(),
          testDirStatus[i].getPermission());
      assertEquals(witnessDirStatus[i].getOwner(),
          testDirStatus[i].getOwner());
      assertEquals(witnessDirStatus[i].getGroup(),
          testDirStatus[i].getGroup());
      assertEquals(witnessDirStatus[i].getLen(), testDirStatus[i].getLen());
      assertEquals(witnessDirStatus[i].getBlockSize(),
          testDirStatus[i].getBlockSize());
      assertEquals(witnessDirStatus[i].hasAcl(), testDirStatus[i].hasAcl());
      assertEquals(witnessDirStatus[i].isEncrypted(),
          testDirStatus[i].isEncrypted());
      assertEquals(witnessDirStatus[i].isErasureCoded(),
          testDirStatus[i].isErasureCoded());
      assertEquals(witnessDirStatus[i].isDirectory(),
          testDirStatus[i].isDirectory());
      assertEquals(witnessDirStatus[i].isFile(), testDirStatus[i].isFile());
    }
    LOG.info("checkClusterHealth, metadata verified.");

    // Randomly decide whether we want to do a check point
    if (generator.nextBoolean()) {
      LOG.info("checkClusterHealth, doing a checkpoint on NN.");
      hdfs.setSafeMode(SafeModeAction.ENTER);
      hdfs.saveNamespace();
      hdfs.setSafeMode(SafeModeAction.LEAVE);
    }

    /** Restart name node making sure loading from image successfully */
    LOG.info("checkClusterHealth, restarting NN.");
    cluster.restartNameNodes();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !cluster.getNameNode().isInSafeMode();
      }
    }, 10, 100000);

    assertTrue("NameNode is up", cluster.getNameNode().isActiveState());
    assertTrue("DataNode is up and running", cluster.isDataNodeUp());
    assertTrue("Cluster is up and running", cluster.isClusterUp());
    LOG.info("checkClusterHealth, cluster is healthy.");

    printOperationStats();
  }

  /* Helper: create a file with a length of filelen. */
  private void createFile(final String fileName, final long filelen,
                          boolean enableSnapshot) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, filelen, (short) 1, 0);

    // Randomly allow snapshot on parent directly
    if (enableSnapshot && generator.nextBoolean()) {
      try {
        hdfs.allowSnapshot(filePath.getParent());
      } catch (SnapshotException e) {
        LOG.info("createFile, exception setting snapshotable directory: " +
            e.getMessage());
      }
    }
  }

  /* Helper: create a large number of directories and files. */
  private void createFiles(String rootDir, int fileLength) throws IOException {
    if (!rootDir.endsWith("/")) {
      rootDir += "/";
    }

    // Create files in a directory with random depth, ranging from 0-10.
    for (int i = 0; i < TOTAL_FILECOUNT; i++) {
      String filename = rootDir;
      int dirs = generator.nextInt(MAX_NUM_SUB_DIRECTORIES_LEVEL);

      for (int j=i; j >=(i-dirs); j--) {
        filename += j + "/";
      }
      filename += "file" + i;
      createFile(filename, fileLength, true);
      assertTrue("Test file created", hdfs.exists(new Path(filename)));
      LOG.info("createFiles, file: " + filename + "was created");

      String witnessFile =
          filename.replaceAll(TESTDIRSTRING, WITNESSDIRSTRING);
      createFile(witnessFile, fileLength, false);
      assertTrue("Witness file exists",
          hdfs.exists(new Path(witnessFile)));
      LOG.info("createFiles, file: " + witnessFile + "was created");
    }
  }

  /* Helper: replace all target string with replacement string in original
   * string. */
  private String getNewPathString(String originalString, String targetString,
                                  String replacementString) {
    String str =  originalString.replaceAll(targetString, replacementString);
    LOG.info("Original string: " + originalString);
    LOG.info("New string: " + str);
    return str;
  }

  private void printOperationStats() {
    LOG.info("Operation statistics for this iteration: ");

    LOG.info("Number of files created: " + numberFileCreated);
    LOG.info("Number of files deleted: " + numberFileDeleted);
    LOG.info("Number of files renamed: " + numberFileRenamed);

    LOG.info("Number of directories created: " + numberDirectoryCreated);
    LOG.info("Number of directories deleted: " + numberDirectoryDeleted);
    LOG.info("Number of directories renamed: " + numberDirectoryRenamed);

    LOG.info("Number of snapshots created: " + numberSnapshotCreated);
    LOG.info("Number of snapshots deleted: " + numberSnapshotDeleted);
    LOG.info("Number of snapshots renamed: " + numberSnapshotRenamed);

    numberFileCreated = 0;
    numberFileDeleted = 0;
    numberFileRenamed = 0;

    numberDirectoryCreated = 0;
    numberDirectoryDeleted = 0;
    numberDirectoryRenamed = 0;

    numberSnapshotCreated = 0;
    numberSnapshotDeleted = 0;
    numberSnapshotRenamed = 0;
  }
}