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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_INFO;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtLevel;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_DISTCP_JOB_ID;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.tools.SimpleCopyListing;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.hadoop.tools.util.DistCpTestUtils;
import org.apache.hadoop.util.functional.RemoteIterators;

import org.assertj.core.api.Assertions;
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
 * The HDFS contract test needs to be run explicitly.
 */
public abstract class AbstractContractDistCpTest
    extends AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractDistCpTest.class);

  /** Using offset to change modification time in tests. */
  private static final long MODIFICATION_TIME_OFFSET = 10000;

  public static final String SCALE_TEST_DISTCP_FILE_SIZE_KB
      = "scale.test.distcp.file.size.kb";

  public static final int DEFAULT_DISTCP_SIZE_KB = 1024;

  protected static final int MB = 1024 * 1024;

  /**
   * Default depth for a directory tree: {@value}.
   */
  protected static final int DEFAULT_DEPTH = 3;

  /**
   * Default width for a directory tree: {@value}.
   * Total dir size is
   * <pre>
   *   DEFAULT_WITH^DEFAULT_DEPTH
   * </pre>
   * So the duration of a test run grows rapidly with this value.
   * This has very significant consequences for object storage runs.
   */
  protected static final int DEFAULT_WIDTH = 2;

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
    localFS.delete(localDir, true);
    mkdirs(localFS, localDir);
    Path testSubPath = path(testSubDir);
    remoteDir = new Path(testSubPath, "remote");
    // test teardown does this, but IDE-based test debugging can skip
    // that teardown; this guarantees the initial state is clean
    remoteFS.delete(remoteDir, true);
  }

  @Override
  public void teardown() throws Exception {
    // if remote FS supports IOStatistics log it.
    logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, getRemoteFS());
    super.teardown();
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
            .withSkipCRC(true)
            .withDirectWrite(shouldUseDirectWrite())
            .withOverwrite(false)));
  }

  /**
   * Run distcp -update srcDir destDir.
   * @param srcDir local source directory
   * @param destDir remote destination directory.
   * @return the completed job
   * @throws Exception any failure.
   */
  private Job distCpUpdateWithFs(final Path srcDir, final Path destDir,
      FileSystem sourceFs, FileSystem targetFs)
      throws Exception {
    describe("\nDistcp -update from " + srcDir + " to " + destDir);
    lsR("Source Fs to update", sourceFs, srcDir);
    lsR("Target Fs before update", targetFs, destDir);
    return runDistCp(buildWithStandardOptions(
        new DistCpOptions.Builder(
            Collections.singletonList(srcDir), destDir)
            .withDeleteMissing(true)
            .withSyncFolder(true)
            .withSkipCRC(false)
            .withDirectWrite(shouldUseDirectWrite())
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
            .withDirectWrite(shouldUseDirectWrite())
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

  @Test
  public void testSetJobId() throws Exception {
    describe("check jobId is set in the conf");
    remoteFS.create(new Path(remoteDir, "file1")).close();
    DistCpTestUtils
        .assertRunDistCp(DistCpConstants.SUCCESS, remoteDir.toString(),
            localDir.toString(), getDefaultCLIOptionsOrNull(), conf);
    assertNotNull("DistCp job id isn't set",
        conf.get(CONF_LABEL_DISTCP_JOB_ID));
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
    int fileSizeKb = conf.getInt(SCALE_TEST_DISTCP_FILE_SIZE_KB,
        getDefaultDistCPSizeKb());
    if (fileSizeKb < 1) {
      skip("File size in " + SCALE_TEST_DISTCP_FILE_SIZE_KB + " is zero");
    }
    initPathFields(srcDir, dstDir);
    Path largeFile1 = new Path(inputDir, "file1");
    Path largeFile2 = new Path(inputDir, "file2");
    Path largeFile3 = new Path(inputDir, "file3");
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
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS, new Path(target, "inputDir/file2"), data2);
    verifyFileContents(dstFS, new Path(target, "inputDir/file3"), data3);
  }

  /**
   * Override point. What is the default distcp size
   * for large files if not overridden by
   * {@link #SCALE_TEST_DISTCP_FILE_SIZE_KB}.
   * If 0 then, unless overridden in the configuration,
   * the large file tests will not run.
   * @return file size.
   */
  protected int getDefaultDistCPSizeKb() {
    return DEFAULT_DISTCP_SIZE_KB;
  }

  /**
   * Executes DistCp and asserts that the job finished successfully.
   * The choice of direct/indirect is based on the value of
   *  {@link #shouldUseDirectWrite()}.
   * @param src source path
   * @param dst destination path
   * @throws Exception if there is a failure
   */
  private void runDistCp(Path src, Path dst) throws Exception {
    if (shouldUseDirectWrite()) {
      runDistCpDirectWrite(src, dst);
    } else {
      runDistCpWithRename(src, dst);
    }
  }

  /**
   * Run the distcp job.
   * @param options distcp options
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

  @Test
  public void testDirectWrite() throws Exception {
    describe("copy file from local to remote using direct write option");
    if (shouldUseDirectWrite()) {
      skip("not needed as all other tests use the -direct option.");
    }
    directWrite(localFS, localDir, remoteFS, remoteDir, true);
  }

  @Test
  public void testNonDirectWrite() throws Exception {
    describe("copy file from local to remote without using direct write " +
        "option");
    directWrite(localFS, localDir, remoteFS, remoteDir, false);
  }

  @Test
  public void testDistCpWithIterator() throws Exception {
    describe("Build listing in distCp using the iterator option.");
    Path source = new Path(remoteDir, "src");
    Path dest = new Path(localDir, "dest");
    dest = localFS.makeQualified(dest);

    GenericTestUtils
        .createFiles(remoteFS, source, getDepth(), getWidth(), getWidth());

    GenericTestUtils.LogCapturer log =
        GenericTestUtils.LogCapturer.captureLogs(SimpleCopyListing.LOG);

    String options = "-useiterator -update -delete" + getDefaultCLIOptions();
    DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, source.toString(),
        dest.toString(), options, conf);

    // Check the target listing was also done using iterator.
    Assertions.assertThat(log.getOutput()).contains(
        "Building listing using iterator mode for " + dest.toString());

    Assertions.assertThat(RemoteIterators.toList(localFS.listFiles(dest, true)))
        .describedAs("files").hasSize(getTotalFiles());
  }

  public int getDepth() {
    return DEFAULT_DEPTH;
  }

  public int getWidth() {
    return DEFAULT_WIDTH;
  }

  private int getTotalFiles() {
    int totalFiles = 0;
    for (int i = 1; i <= getDepth(); i++) {
      totalFiles += Math.pow(getWidth(), i);
    }
    return totalFiles;
  }

  /**
   * Override point: should direct write always be used?
   * false by default; enable for stores where rename is slow.
   * @return true if direct write should be used in all tests.
   */
  protected boolean shouldUseDirectWrite() {
    return false;
  }

  /**
   * Return the default options for distcp, including,
   * if {@link #shouldUseDirectWrite()} is true,
   * the -direct option.
   * Append or prepend this to string CLIs.
   * @return default options.
   */
  protected String getDefaultCLIOptions() {
    return shouldUseDirectWrite()
        ? " -direct "
        : "";
  }

  /**
   * Return the default options for distcp, including,
   * if {@link #shouldUseDirectWrite()} is true,
   * the -direct option, null if there are no
   * defaults.
   * @return default options.
   */
  protected String getDefaultCLIOptionsOrNull() {
    return shouldUseDirectWrite()
        ? " -direct "
        : null;
  }

  /**
   * Executes a test with support for using direct write option.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @param directWrite whether to use -directwrite option
   * @throws Exception if there is a failure
   */
  private void directWrite(FileSystem srcFS, Path srcDir, FileSystem dstFS,
          Path dstDir, boolean directWrite) throws Exception {
    initPathFields(srcDir, dstDir);

    // Create 2 test files
    mkdirs(srcFS, inputSubDir1);
    byte[] data1 = dataset(64, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset(200, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    Path target = new Path(dstDir, "outputDir");
    if (directWrite) {
      runDistCpDirectWrite(inputDir, target);
    } else {
      runDistCpWithRename(inputDir, target);
    }
    ContractTestUtils.assertIsDirectory(dstFS, target);
    lsR("Destination tree after distcp", dstFS, target);

    // Verify copied file contents
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS, new Path(target, "inputDir/subDir1/file2"),
        data2);
  }

  /**
   * Run distcp -direct srcDir destDir.
   * @param srcDir local source directory
   * @param destDir remote destination directory
   * @return the completed job
   * @throws Exception any failure.
   */
  private Job runDistCpDirectWrite(final Path srcDir, final Path destDir)
          throws Exception {
    describe("\nDistcp -direct from " + srcDir + " to " + destDir);
    return runDistCp(buildWithStandardOptions(
            new DistCpOptions.Builder(
                    Collections.singletonList(srcDir), destDir)
                    .withDirectWrite(true)));
  }
  /**
   * Run distcp srcDir destDir.
   * @param srcDir local source directory
   * @param destDir remote destination directory
   * @return the completed job
   * @throws Exception any failure.
   */
  private Job runDistCpWithRename(Path srcDir, final Path destDir)
          throws Exception {
    describe("\nDistcp from " + srcDir + " to " + destDir);
    return runDistCp(buildWithStandardOptions(
            new DistCpOptions.Builder(
                    Collections.singletonList(srcDir), destDir)
                    .withDirectWrite(false)));
  }

  @Test
  public void testDistCpWithFile() throws Exception {
    describe("Distcp only file");

    Path source = new Path(remoteDir, "file");
    Path dest = new Path(localDir, "file");
    dest = localFS.makeQualified(dest);

    mkdirs(localFS, localDir);

    int len = 4;
    int base = 0x40;
    byte[] block = dataset(len, base, base + len);
    ContractTestUtils.createFile(remoteFS, source, true, block);
    verifyPathExists(remoteFS, "", source);
    verifyPathExists(localFS, "", localDir);

    DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, source.toString(),
        dest.toString(), getDefaultCLIOptionsOrNull(), conf);

    Assertions
        .assertThat(RemoteIterators.toList(localFS.listFiles(dest, true)))
        .describedAs("files").hasSize(1);
    verifyFileContents(localFS, dest, block);
  }

  @Test
  public void testDistCpWithUpdateExistFile() throws Exception {
    describe("Now update an existing file.");

    Path source = new Path(remoteDir, "file");
    Path dest = new Path(localDir, "file");
    dest = localFS.makeQualified(dest);

    int len = 4;
    int base = 0x40;
    byte[] block = dataset(len, base, base + len);
    byte[] destBlock = dataset(len, base, base + len + 1);
    ContractTestUtils.createFile(remoteFS, source, true, block);
    ContractTestUtils.createFile(localFS, dest, true, destBlock);

    verifyPathExists(remoteFS, "", source);
    verifyPathExists(localFS, "", dest);
    DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, source.toString(),
        dest.toString(), "-delete -update" + getDefaultCLIOptions(), conf);

    Assertions.assertThat(RemoteIterators.toList(localFS.listFiles(dest, true)))
        .hasSize(1);
    verifyFileContents(localFS, dest, block);
  }

  @Test
  public void testDistCpUpdateCheckFileSkip() throws Exception {
    describe("Distcp update to check file skips.");

    Path source = new Path(remoteDir, "file");
    Path dest = new Path(localDir, "file");

    Path source0byte = new Path(remoteDir, "file_0byte");
    Path dest0byte = new Path(localDir, "file_0byte");
    dest = localFS.makeQualified(dest);
    dest0byte = localFS.makeQualified(dest0byte);

    // Creating a source file with certain dataset.
    byte[] sourceBlock = dataset(10, 'a', 'z');

    // Write the dataset.
    ContractTestUtils
        .writeDataset(remoteFS, source, sourceBlock, sourceBlock.length,
            1024, true);

    // Create 0 byte source and target files.
    ContractTestUtils.createFile(remoteFS, source0byte, true, new byte[0]);
    ContractTestUtils.createFile(localFS, dest0byte, true, new byte[0]);

    // Execute the distcp -update job.
    Job job = distCpUpdateWithFs(remoteDir, localDir, remoteFS, localFS);

    // First distcp -update would normally copy the source to dest.
    verifyFileContents(localFS, dest, sourceBlock);
    // Verify 1 file was skipped in the distcp -update (The 0 byte file).
    // Verify 1 file was copied in the distcp -update (The new source file).
    verifySkipAndCopyCounter(job, 1, 1);

    // Remove the source file and replace with a file with same name and size
    // but different content.
    remoteFS.delete(source, false);
    Path updatedSource = new Path(remoteDir, "file");
    byte[] updatedSourceBlock = dataset(10, 'b', 'z');
    ContractTestUtils.writeDataset(remoteFS, updatedSource,
        updatedSourceBlock, updatedSourceBlock.length, 1024, true);

    // For testing purposes we would take the modification time of the
    // updated Source file and add an offset or subtract the offset and set
    // that time as the modification time for target file, this way we can
    // ensure that our test can emulate a scenario where source is either more
    // recently changed after -update so that copy takes place or target file
    // is more recently changed which would skip the copying since the source
    // has not been recently updated.
    FileStatus fsSourceUpd = remoteFS.getFileStatus(updatedSource);
    long modTimeSourceUpd = fsSourceUpd.getModificationTime();

    // Add by an offset which would ensure enough gap for the test to
    // not fail due to race conditions.
    long newTargetModTimeNew = modTimeSourceUpd + MODIFICATION_TIME_OFFSET;
    localFS.setTimes(dest, newTargetModTimeNew, -1);

    // Execute the distcp -update job.
    Job updatedSourceJobOldSrc =
        distCpUpdateWithFs(remoteDir, localDir, remoteFS,
            localFS);

    // File contents should remain same since the mod time for target is
    // newer than the updatedSource which indicates that the sync happened
    // more recently and there is no update.
    verifyFileContents(localFS, dest, sourceBlock);
    // Skipped both 0 byte file and sourceFile (since mod time of target is
    // older than the source it is perceived that source is of older version
    // and we can skip it's copy).
    verifySkipAndCopyCounter(updatedSourceJobOldSrc, 2, 0);

    // Subtract by an offset which would ensure enough gap for the test to
    // not fail due to race conditions.
    long newTargetModTimeOld =
        Math.min(modTimeSourceUpd - MODIFICATION_TIME_OFFSET, 0);
    localFS.setTimes(dest, newTargetModTimeOld, -1);

    // Execute the distcp -update job.
    Job updatedSourceJobNewSrc = distCpUpdateWithFs(remoteDir, localDir,
        remoteFS,
        localFS);

    // Verifying the target directory have both 0 byte file and the content
    // file.
    Assertions
        .assertThat(RemoteIterators.toList(localFS.listFiles(localDir, true)))
        .hasSize(2);
    // Now the copy should take place and the file contents should change
    // since the mod time for target is older than the source file indicating
    // that there was an update to the source after the last sync took place.
    verifyFileContents(localFS, dest, updatedSourceBlock);
    // Verifying we skipped the 0 byte file and copied the updated source
    // file (since the modification time of the new source is older than the
    // target now).
    verifySkipAndCopyCounter(updatedSourceJobNewSrc, 1, 1);
  }

  /**
   * Method to check the skipped and copied counters of a distcp job.
   *
   * @param job               job to check.
   * @param skipExpectedValue expected skip counter value.
   * @param copyExpectedValue expected copy counter value.
   * @throws IOException throw in case of failures.
   */
  private void verifySkipAndCopyCounter(Job job,
      int skipExpectedValue, int copyExpectedValue) throws IOException {
    // get the skip and copy counters from the job.
    long skipActualValue = job.getCounters()
        .findCounter(CopyMapper.Counter.SKIP).getValue();
    long copyActualValue = job.getCounters()
        .findCounter(CopyMapper.Counter.COPY).getValue();
    // Verify if the actual values equals the expected ones.
    assertEquals("Mismatch in COPY counter value", copyExpectedValue,
        copyActualValue);
    assertEquals("Mismatch in SKIP counter value", skipExpectedValue,
        skipActualValue);
  }
}