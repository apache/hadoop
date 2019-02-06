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

package org.apache.hadoop.tools.contract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.mapred.CopyMapper;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contract test suite covering a file system's integration with DistCp.  The
 * tests coordinate two file system instances: one "local", which is the local
 * file system, and the other "remote", which is the file system implementation
 * under test.  The tests in the suite cover both copying from local to remote
 * (e.g. a backup use case) and copying from remote to local (e.g. a restore use
 * case).
 */
public abstract class AbstractContractDistCpTest
    extends AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractDistCpTest.class);

  public static final String SCALE_TEST_DISTCP_FILE_SIZE_KB
      = "scale.test.distcp.file.size.kb";

  public static final int DEFAULT_DISTCP_SIZE_KB = 1024;

  protected static final int MB = 1024 * 1024;

  @Rule
  public TestName testName = new TestName();

  /**
   * The timeout value is extended over the default so that large updates
   * are allowed to take time, especially to remote stores.
   * @return the current test timeout
   */
  protected int getTestTimeoutMillis() {
    return 15  * 60 * 1000;
  }

  private Configuration conf;
  private FileSystem localFS, remoteFS;
  private Path localDir, remoteDir;

  private Path inputDir;

  private Path inputSubDir1;

  private Path inputSubDir2;

  private Path inputSubDir4;

  private Path inputFile1;

  private Path inputFile2;

  private Path inputFile3;

  private Path inputFile4;

  private Path inputFile5;

  private Path outputDir;

  private Path outputSubDir1;

  private Path outputSubDir2;

  private Path outputSubDir4;

  private Path outputFile1;

  private Path outputFile2;

  private Path outputFile3;

  private Path outputFile4;

  private Path outputFile5;

  private Path inputDirUnderOutputDir;

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = new Configuration();
    newConf.set("mapred.job.tracker", "local");
    return newConf;
  }

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
    conf = getContract().getConf();
    localFS = FileSystem.getLocal(conf);
    remoteFS = getFileSystem();
    // Test paths are isolated by concrete subclass name and test method name.
    // All paths are fully qualified including scheme (not taking advantage of
    // default file system), so if something fails, the messages will make it
    // clear which paths are local and which paths are remote.
    String className = getClass().getSimpleName();
    String testSubDir = className + "/" + testName.getMethodName();
    localDir =
        localFS.makeQualified(new Path(new Path(
        GenericTestUtils.getTestDir().toURI()), testSubDir + "/local"));
    mkdirs(localFS, localDir);
    remoteDir = path(testSubDir + "/remote");
    mkdirs(remoteFS, remoteDir);
    // test teardown does this, but IDE-based test debugging can skip
    // that teardown; this guarantees the initial state is clean
    remoteFS.delete(remoteDir, true);
    localFS.delete(localDir, true);
  }

  /**
   * Set up both input and output fields.
   * @param src source tree
   * @param dest dest tree
   */
  protected void initPathFields(final Path src, final Path dest) {
    initInputFields(src);
    initOutputFields(dest);
  }

  /**
   * Output field setup.
   * @param path path to set up
   */
  protected void initOutputFields(final Path path) {
    outputDir = new Path(path, "outputDir");
    inputDirUnderOutputDir = new Path(outputDir, "inputDir");
    outputFile1 = new Path(inputDirUnderOutputDir, "file1");
    outputSubDir1 = new Path(inputDirUnderOutputDir, "subDir1");
    outputFile2 = new Path(outputSubDir1, "file2");
    outputSubDir2 = new Path(inputDirUnderOutputDir, "subDir2/subDir2");
    outputFile3 = new Path(outputSubDir2, "file3");
    outputSubDir4 = new Path(inputDirUnderOutputDir, "subDir4/subDir4");
    outputFile4 = new Path(outputSubDir4, "file4");
    outputFile5 = new Path(outputSubDir4, "file5");
  }

  /**
   * this path setup is used across different methods (copy, update, track)
   * so they are set up as fields.
   * @param srcDir source directory for these to go under.
   */
  protected void initInputFields(final Path srcDir) {
    inputDir = new Path(srcDir, "inputDir");
    inputFile1 = new Path(inputDir, "file1");
    inputSubDir1 = new Path(inputDir, "subDir1");
    inputFile2 = new Path(inputSubDir1, "file2");
    inputSubDir2 = new Path(inputDir, "subDir2/subDir2");
    inputFile3 = new Path(inputSubDir2, "file3");
    inputSubDir4 = new Path(inputDir, "subDir4/subDir4");
    inputFile4 = new Path(inputSubDir4, "file4");
    inputFile5 = new Path(inputSubDir4, "file5");
  }

  protected FileSystem getLocalFS() {
    return localFS;
  }

  protected FileSystem getRemoteFS() {
    return remoteFS;
  }

  protected Path getLocalDir() {
    return localDir;
  }

  protected Path getRemoteDir() {
    return remoteDir;
  }

  @Test
  public void testUpdateDeepDirectoryStructureToRemote() throws Exception {
    describe("update a deep directory structure from local to remote");
    distCpDeepDirectoryStructure(localFS, localDir, remoteFS, remoteDir);
    distCpUpdateDeepDirectoryStructure(inputDirUnderOutputDir);
  }

  @Test
  public void testUpdateDeepDirectoryStructureNoChange() throws Exception {
    describe("update an unchanged directory structure"
        + " from local to remote; expect no copy");
    Path target = distCpDeepDirectoryStructure(localFS, localDir, remoteFS,
        remoteDir);
    describe("\nExecuting Update\n");
    Job job = distCpUpdate(localDir, target);
    assertCounterInRange(job, CopyMapper.Counter.SKIP, 1, -1);
    assertCounterInRange(job, CopyMapper.Counter.BYTESCOPIED, 0, 0);
  }

  /**
   * Assert that a counter is in a range; min and max values are inclusive.
   * @param job job to query
   * @param counter counter to examine
   * @param min min value, if negative "no minimum"
   * @param max max value, if negative "no maximum"
   * @throws IOException IO problem
   */
  void assertCounterInRange(Job job, Enum<?> counter, long min, long max)
      throws IOException {
    Counter c = job.getCounters().findCounter(counter);
    long value = c.getValue();
    String description =
        String.format("%s value %s", c.getDisplayName(), value, false);

    if (min >= 0) {
      assertTrue(description + " too below minimum " + min,
          value >= min);
    }
    if (max >= 0) {
      assertTrue(description + " above maximum " + max,
          value <= max);
    }
  }

  /**
   * Do a distcp from the local source to the destination filesystem.
   * This is executed as part of
   * {@link #testUpdateDeepDirectoryStructureToRemote()}; it's designed to be
   * overidden or wrapped by subclasses which wish to add more assertions.
   *
   * Life is complicated here by the way that the src/dest paths
   * on a distcp is different with -update.
   * @param destDir output directory used by the initial distcp
   * @return the distcp job
   */
  protected Job distCpUpdateDeepDirectoryStructure(final Path destDir)
      throws Exception {
    describe("Now do an incremental update with deletion of missing files");
    Path srcDir = inputDir;
    LOG.info("Source directory = {}, dest={}", srcDir, destDir);

    ContractTestUtils.assertPathsExist(localFS,
        "Paths for test are wrong",
        inputFile1, inputFile2, inputFile3, inputFile4, inputFile5);

    modifySourceDirectories();

    Job job = distCpUpdate(srcDir, destDir);

    Path outputFileNew1 = new Path(outputSubDir2, "newfile1");

    lsR("Updated Remote", remoteFS, destDir);

    ContractTestUtils.assertPathDoesNotExist(remoteFS,
        " deleted from " + inputFile1, outputFile1);
    ContractTestUtils.assertIsFile(remoteFS, outputFileNew1);
    ContractTestUtils.assertPathsDoNotExist(remoteFS,
        "DistCP should have deleted",
        outputFile3, outputFile4, outputSubDir4);
    assertCounterInRange(job, CopyMapper.Counter.COPY, 1, 1);
    assertCounterInRange(job, CopyMapper.Counter.SKIP, 1, -1);
    return job;
  }

  /**
   * Run distcp -update srcDir destDir.
   * @param srcDir local source directory
   * @param destDir remote destination directory.
   * @return the completed job
   * @throws Exception any failure.
   */
  private Job distCpUpdate(final Path srcDir, final Path destDir)
      throws Exception {
    describe("\nDistcp -update from " + srcDir + " to " + destDir);
    lsR("Local to update", localFS, srcDir);
    lsR("Remote before update", remoteFS, destDir);
    return runDistCp(buildWithStandardOptions(
        new DistCpOptions.Builder(
            Collections.singletonList(srcDir), destDir)
            .withDeleteMissing(true)
            .withSyncFolder(true)
            .withCRC(true)
            .withOverwrite(false)));
  }

  /**
   * Update the source directories as various tests expect,
   * including adding a new file.
   * @return the path to the newly created file
   * @throws IOException IO failure
   */
  private Path modifySourceDirectories() throws IOException {
    localFS.delete(inputFile1, false);
    localFS.delete(inputFile3, false);
    // delete all of subdir4, so input/output file 4 & 5 will go
    localFS.delete(inputSubDir4, true);
    // add one new file
    Path inputFileNew1 = new Path(inputSubDir2, "newfile1");
    ContractTestUtils.touch(localFS, inputFileNew1);
    return inputFileNew1;
  }


  @Test
  public void testTrackDeepDirectoryStructureToRemote() throws Exception {
    describe("copy a deep directory structure from local to remote");

    Path destDir = distCpDeepDirectoryStructure(localFS, localDir, remoteFS,
        remoteDir);
    ContractTestUtils.assertIsDirectory(remoteFS, destDir);

    describe("Now do an incremental update and save of missing files");
    Path srcDir = inputDir;
    // same path setup as in deepDirectoryStructure()
    Path trackDir = new Path(localDir, "trackDir");


    describe("\nDirectories\n");
    lsR("Local to update", localFS, srcDir);
    lsR("Remote before update", remoteFS, destDir);


    ContractTestUtils.assertPathsExist(localFS,
        "Paths for test are wrong",
        inputFile2, inputFile3, inputFile4, inputFile5);

    Path inputFileNew1 = modifySourceDirectories();

    // Distcp set to track but not delete
    runDistCp(buildWithStandardOptions(
        new DistCpOptions.Builder(
            Collections.singletonList(srcDir),
            inputDirUnderOutputDir)
            .withTrackMissing(trackDir)
            .withSyncFolder(true)
            .withOverwrite(false)));

    lsR("tracked udpate", remoteFS, destDir);
    // new file went over
    Path outputFileNew1 = new Path(outputSubDir2, "newfile1");
    ContractTestUtils.assertIsFile(remoteFS, outputFileNew1);

    ContractTestUtils.assertPathExists(localFS, "tracking directory",
        trackDir);

    // now read in the listings
    Path sortedSourceListing = new Path(trackDir,
        DistCpConstants.SOURCE_SORTED_FILE);
    ContractTestUtils.assertIsFile(localFS, sortedSourceListing);
    Path sortedTargetListing = new Path(trackDir,
        DistCpConstants.TARGET_SORTED_FILE);
    ContractTestUtils.assertIsFile(localFS, sortedTargetListing);
    // deletion didn't happen
    ContractTestUtils.assertPathsExist(remoteFS,
        "DistCP should have retained",
        outputFile2, outputFile3, outputFile4, outputSubDir4);

    // now scan the table and see that things are there.
    Map<String, Path> sourceFiles = new HashMap<>(10);
    Map<String, Path> targetFiles = new HashMap<>(10);

    try (SequenceFile.Reader sourceReader = new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(sortedSourceListing));
         SequenceFile.Reader targetReader = new SequenceFile.Reader(conf,
             SequenceFile.Reader.file(sortedTargetListing))) {
      CopyListingFileStatus copyStatus = new CopyListingFileStatus();
      Text name = new Text();
      while(sourceReader.next(name, copyStatus)) {
        String key = name.toString();
        Path path = copyStatus.getPath();
        LOG.info("{}: {}", key, path);
        sourceFiles.put(key, path);
      }
      while(targetReader.next(name, copyStatus)) {
        String key = name.toString();
        Path path = copyStatus.getPath();
        LOG.info("{}: {}", key, path);
        targetFiles.put(name.toString(), copyStatus.getPath());
      }
    }

    // look for the new file in both lists
    assertTrue("No " + outputFileNew1 + " in source listing",
        sourceFiles.containsValue(inputFileNew1));
    assertTrue("No " + outputFileNew1 + " in target listing",
        targetFiles.containsValue(outputFileNew1));
    assertTrue("No " + outputSubDir4 + " in target listing",
        targetFiles.containsValue(outputSubDir4));
    assertFalse("Found " + inputSubDir4 + " in source listing",
        sourceFiles.containsValue(inputSubDir4));

  }

  public void lsR(final String description,
      final FileSystem fs,
      final Path dir) throws IOException {
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(dir, true);
    LOG.info("{}: {}:", description, dir);
    StringBuilder sb = new StringBuilder();
    while(files.hasNext()) {
      LocatedFileStatus status = files.next();
      sb.append(String.format("  %s; type=%s; length=%d",
          status.getPath(),
          status.isDirectory()? "dir" : "file",
          status.getLen()));
    }
    LOG.info("{}", sb);
  }

  @Test
  public void largeFilesToRemote() throws Exception {
    describe("copy multiple large files from local to remote");
    largeFiles(localFS, localDir, remoteFS, remoteDir);
  }

  @Test
  public void testDeepDirectoryStructureFromRemote() throws Exception {
    describe("copy a deep directory structure from remote to local");
    distCpDeepDirectoryStructure(remoteFS, remoteDir, localFS, localDir);
  }

  @Test
  public void testLargeFilesFromRemote() throws Exception {
    describe("copy multiple large files from remote to local");
    largeFiles(remoteFS, remoteDir, localFS, localDir);
  }

  /**
   * Executes a DistCp using a file system sub-tree with multiple nesting
   * levels.
   * The filenames are those of the fields initialized in setup.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @return the target directory of the copy
   * @throws Exception if there is a failure
   */
  private Path distCpDeepDirectoryStructure(FileSystem srcFS,
      Path srcDir,
      FileSystem dstFS,
      Path dstDir) throws Exception {
    initPathFields(srcDir, dstDir);

    mkdirs(srcFS, inputSubDir1);
    mkdirs(srcFS, inputSubDir2);
    byte[] data1 = dataset(100, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset(200, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    byte[] data3 = dataset(300, 53, 63);
    createFile(srcFS, inputFile3, true, data3);
    createFile(srcFS, inputFile4, true, dataset(400, 53, 63));
    createFile(srcFS, inputFile5, true, dataset(500, 53, 63));
    Path target = new Path(dstDir, "outputDir");
    runDistCp(inputDir, target);
    ContractTestUtils.assertIsDirectory(dstFS, target);
    lsR("Destination tree after distcp", dstFS, target);
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS,
        new Path(target, "inputDir/subDir1/file2"), data2);
    verifyFileContents(dstFS,
        new Path(target, "inputDir/subDir2/subDir2/file3"), data3);
    return target;
  }

  /**
   * Executes a test using multiple large files.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @throws Exception if there is a failure
   */
  private void largeFiles(FileSystem srcFS, Path srcDir, FileSystem dstFS,
      Path dstDir) throws Exception {
    initPathFields(srcDir, dstDir);
    Path largeFile1 = new Path(inputDir, "file1");
    Path largeFile2 = new Path(inputDir, "file2");
    Path largeFile3 = new Path(inputDir, "file3");
    mkdirs(srcFS, inputDir);
    int fileSizeKb = conf.getInt(SCALE_TEST_DISTCP_FILE_SIZE_KB,
        DEFAULT_DISTCP_SIZE_KB);
    int fileSizeMb = fileSizeKb / 1024;
    getLogger().info("{} with file size {}", testName.getMethodName(), fileSizeMb);
    byte[] data1 = dataset((fileSizeMb + 1) * MB, 33, 43);
    createFile(srcFS, largeFile1, true, data1);
    byte[] data2 = dataset((fileSizeMb + 2) * MB, 43, 53);
    createFile(srcFS, largeFile2, true, data2);
    byte[] data3 = dataset((fileSizeMb + 3) * MB, 53, 63);
    createFile(srcFS, largeFile3, true, data3);
    Path target = new Path(dstDir, "outputDir");
    runDistCp(inputDir, target);
    ContractTestUtils.assertIsDirectory(dstFS, target);
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS, new Path(target, "inputDir/file2"), data2);
    verifyFileContents(dstFS, new Path(target, "inputDir/file3"), data3);
  }

  /**
   * Executes DistCp and asserts that the job finished successfully.
   *
   * @param src source path
   * @param dst destination path
   * @throws Exception if there is a failure
   */
  private void runDistCp(Path src, Path dst) throws Exception {
    runDistCp(buildWithStandardOptions(
        new DistCpOptions.Builder(Collections.singletonList(src), dst)));
  }

  /**
   * Run the distcp job.
   * @param optons distcp options
   * @return the job. It will have already completed.
   * @throws Exception failure
   */
  private Job runDistCp(final DistCpOptions options) throws Exception {
    Job job = new DistCp(conf, options).execute();
    assertNotNull("Unexpected null job returned from DistCp execution.", job);
    assertTrue("DistCp job did not complete.", job.isComplete());
    assertTrue("DistCp job did not complete successfully.", job.isSuccessful());
    return job;
  }

  /**
   * Add any standard options and then build.
   * @param builder DistCp option builder
   * @return the build options
   */
  private DistCpOptions buildWithStandardOptions(
      DistCpOptions.Builder builder) {
    return builder
        .withNumListstatusThreads(DistCpOptions.MAX_NUM_LISTSTATUS_THREADS)
        .build();
  }

  /**
   * Creates a directory and any ancestor directories required.
   *
   * @param fs FileSystem in which to create directories
   * @param dir path of directory to create
   * @throws Exception if there is a failure
   */
  private static void mkdirs(FileSystem fs, Path dir) throws Exception {
    assertTrue("Failed to mkdir " + dir, fs.mkdirs(dir));
  }
}
