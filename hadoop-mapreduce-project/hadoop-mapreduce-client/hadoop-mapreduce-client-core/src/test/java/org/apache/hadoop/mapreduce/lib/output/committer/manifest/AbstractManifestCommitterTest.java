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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SaveTaskManifestStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.functional.CloseableTaskPoolSubmitter;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.fs.contract.ContractTestUtils.readDataset;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConfig.createCloseableTaskSubmitter;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DEFAULT_WRITER_QUEUE_CAPACITY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ID_SOURCE_MAPREDUCE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_FACTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_DIAGNOSTICS_MANIFEST_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_SUMMARY_REPORT_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.getProjectBuildDir;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.validateSuccessFile;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants.NAME_FORMAT_JOB_ATTEMPT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createIOStatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createTaskManifest;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Tests which work with manifest committers.
 * This is a filesystem contract bound to the local filesystem;
 * subclasses may change the FS to test against other stores.
 * Some fields are set up in
 * in {@link #executeOneTaskAttempt(int, int, int)},
 * which is why fields are used.
 * when synchronized access is needed; synchronize on (this) rather
 * than individual fields
 */
public abstract class AbstractManifestCommitterTest
    extends AbstractFSContractTestBase {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractManifestCommitterTest.class);

  /**
   * Some Job and task IDs.
   */
  protected static final ManifestCommitterTestSupport.JobAndTaskIDsForTests
      TASK_IDS = new ManifestCommitterTestSupport.JobAndTaskIDsForTests(2, 2);

  public static final int JOB1 = 1;

  public static final int TASK0 = 0;

  public static final int TASK1 = 1;

  /**
   * Task attempt 0 index.
   */
  public static final int TA0 = 0;

  /**
   * Task attempt 1 index.
   */
  public static final int TA1 = 1;

  /**
   * Depth of dir tree to generate.
   */
  public static final int DEPTH = 3;

  /**
   * Width of dir tree at every level.
   */
  public static final int WIDTH = 2;

  /**
   * How many files to create in the leaf directories.
   */
  public static final int FILES_PER_DIRECTORY = 4;

  /**
   * Pool size.
   */
  public static final int POOL_SIZE = 32;

  /**
   * FileSystem statistics are collected across every test case.
   */
  protected static final IOStatisticsSnapshot FILESYSTEM_IOSTATS =
      snapshotIOStatistics();

  /**
   * Counter for creating files. Ensures that across all test suites,
   * duplicate filenames are never created. Helps assign blame.
   */
  private static final AtomicLong CREATE_FILE_COUNTER = new AtomicLong();

  protected static final byte[] NO_DATA = new byte[0];

  /**
   * The thread leak tracker.
   */
  private static final ThreadLeakTracker THREAD_LEAK_TRACKER = new ThreadLeakTracker();

  private static final int MAX_LEN = 64_000;

  /**
   * Submitter for tasks; may be null.
   */
  private CloseableTaskPoolSubmitter submitter;

  /**
   * Stage statistics. Created in test setup, and in
   * teardown updates {@link #FILESYSTEM_IOSTATS}.
   */
  private IOStatisticsStore stageStatistics;

  /**
   * Prefer to use these to interact with the FS to
   * ensure more implicit coverage.
   */
  private ManifestStoreOperations storeOperations;

  /**
   * Progress counter used in all stage configs.
   */
  private final ProgressCounter progressCounter = new ProgressCounter();

  /**
   * Directory for job summary reports.
   * This should be set up in test suites testing against real object stores.
   */
  private File reportDir;

  /**
   * List of task attempt IDs for those tests which create manifests.
   */
  private final List<String> taskAttemptIds = new ArrayList<>();

  /**
   * List of task IDs for those tests which create manifests.
   */
  private final List<String> taskIds = new ArrayList<>();

  /**
   * any job stage configuration created for operations.
   */
  private StageConfig jobStageConfig;

  /**
   * Destination dir of job.
   */
  private Path destDir;

  /**
   * When creating manifests, total data size.
   */
  private final AtomicLong totalDataSize = new AtomicLong();

  /**
   * Where to move manifests; must be in target FS.
   */
  private Path manifestDir;

  /**
   * Get the contract configuration.
   * @return the config used to create the FS.
   */
  protected Configuration getConfiguration() {
    return getContract().getConf();
  }

  /**
   * Store operations to interact with..
   * @return store operations.
   */
  protected ManifestStoreOperations getStoreOperations() {
    return storeOperations;
  }

  /**
   * Set store operations.
   * @param storeOperations new value
   */
  protected void setStoreOperations(final ManifestStoreOperations storeOperations) {
    this.storeOperations = storeOperations;
  }

  public List<String> getTaskAttemptIds() {
    return taskAttemptIds;
  }

  public List<String> getTaskIds() {
    return taskIds;
  }

  public long getTotalDataSize() {
    return totalDataSize.get();
  }

  public Path getManifestDir() {
    return manifestDir;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public AbstractManifestCommitterTest withManifestDir(Path value) {
    manifestDir = value;
    return this;
  }

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        getMethodName(),
        String.format(text, args));
  }

  /**
   * Local FS unless overridden.
   * @param conf configuration
   * @return the FS contract.
   */
  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new LocalFSContract(conf);
  }

  /** Enable the manifest committer options in the configuration. */
  @Override
  protected Configuration createConfiguration() {
    return enableManifestCommitter(super.createConfiguration());
  }

  @Override
  public void setup() throws Exception {

    // set the manifest committer to a localfs path for reports across
    // all threads.
    // do this before superclass setup so reportDir is non-null there
    // and can be used in creating the configuration.
    reportDir = new File(getProjectBuildDir(), "reports");
    reportDir.mkdirs();

    // superclass setup includes creating a filesystem instance
    // for the target store.
    super.setup();

    manifestDir = path("manifests");

    // destination directory defaults to method path in
    // target FS
    setDestDir(methodPath());

    // stage statistics
    setStageStatistics(createIOStatisticsStore().build());
    // thread pool for task submission.
    setSubmitter(createCloseableTaskSubmitter(POOL_SIZE, TASK_IDS.getJobId()));
    // store operations for the target filesystem.
    storeOperations = createManifestStoreOperations();
  }

  /**
   * Overrride point: create the store operations.
   * @return store operations for this suite.
   */
  protected ManifestStoreOperations createManifestStoreOperations() throws IOException {
    final FileSystem fs = getFileSystem();
    return ManifestCommitterSupport.createManifestStoreOperations(fs.getConf(), fs, getTestPath());
  }

  @Override
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");

    IOUtils.cleanupWithLogger(LOG, storeOperations, getSubmitter());
    storeOperations = null;
    super.teardown();
    FILESYSTEM_IOSTATS.aggregate(retrieveIOStatistics(getFileSystem()));
    FILESYSTEM_IOSTATS.aggregate(getStageStatistics());
  }

  /**
   * Add a long delay so that you don't get timeouts when working
   * with object stores or debugging.
   * @return a longer timeout than the base classes.
   */
  @Override
  protected int getTestTimeoutMillis() {
    return 600_000;
  }

  protected Path getTestPath() {
    return getContract().getTestPath();
  }

  /**
   * Get the task submitter.
   * @return submitter or null
   */
  protected CloseableTaskPoolSubmitter getSubmitter() {
    return submitter;
  }

  /**
   * Set the task submitter.
   * @param submitter new value.
   */
  protected void setSubmitter(CloseableTaskPoolSubmitter submitter) {
    this.submitter = submitter;
  }

  /**
   * Get the executor which the submitter also uses.
   * @return an executor.
   */
  protected ExecutorService getExecutorService() {
    return getSubmitter().getPool();
  }
  /**
   * @return IOStatistics for stage.
   */
  protected final IOStatisticsStore getStageStatistics() {
    return stageStatistics;
  }

  /**
   * Set the statistics.
   * @param stageStatistics statistics.
   */
  protected final void setStageStatistics(IOStatisticsStore stageStatistics) {
    this.stageStatistics = stageStatistics;
  }

  /**
   * Get the progress counter invoked during commit operations.
   * @return progress counter.
   */
  protected final ProgressCounter getProgressCounter() {
    return progressCounter;
  }

  /**
   * Get the report directory.
   * @return report directory.
   */
  public final File getReportDir() {
    return reportDir;
  }

  /**
   * Get the report directory as a URI.
   * @return report directory.
   */
  public final URI getReportDirUri() {
    return getReportDir().toURI();
  }

  /**
   * Get the (shared) thread leak tracker.
   * @return the thread leak tracker.
   */
  protected static ThreadLeakTracker getThreadLeakTracker() {
    return THREAD_LEAK_TRACKER;
  }

  /**
   * Make sure there's no thread leakage.
   */
  @AfterClass
  public static void threadLeakage() {
    THREAD_LEAK_TRACKER.assertNoThreadLeakage();
  }

  /**
   * Dump the filesystem statistics after the class.
   */
  @AfterClass
  public static void dumpFileSystemIOStatistics() {
    LOG.info("Aggregate FileSystem Statistics {}",
        ioStatisticsToPrettyString(FILESYSTEM_IOSTATS));
  }

  /**
   * Create a directory tree through an executor.
   * dirs created = width^depth;
   * file count = width^depth * files
   * If createDirs == true, then directories are created at the bottom,
   * not files.
   * @param base base dir
   * @param prefix prefix for filenames.
   * @param executor submitter.
   * @param depth depth of dirs
   * @param width width of dirs
   * @param files files to add in each base dir.
   * @param createDirs create directories rather than files?
   * @return the list of paths
   * @throws IOException failure.
   */
  public final List<Path> createFilesOrDirs(Path base,
      String prefix,
      ExecutorService executor,
      int depth,
      int width,
      int files,
      boolean createDirs) throws IOException {

    try (DurationInfo ignored = new DurationInfo(LOG, true,
        "Creating Files %s (%d, %d, %d) under %s",
        prefix, depth, width, files, base)) {

      assertPathExists("Task attempt dir", base);

      // create the files in the thread pool.
      List<Future<Path>> futures = createFilesOrDirs(
          new ArrayList<>(),
          base, prefix,
          executor,
          depth, width, files,
          createDirs);
      List<Path> result = new ArrayList<>();

      // now wait for the creations to finish.
      for (Future<Path> f : futures) {
        result.add(awaitFuture(f));
      }
      return result;
    }
  }

  /**
   * Counter incremented for each file created.
   */
  private final AtomicLong fileDataGenerator = new AtomicLong();

  /**
   * Create files or directories; done in a treewalk and building up
   * a list of futures to wait for. The list is
   * build up incrementally rather than through some merging of
   * lists created down the tree.
   * If createDirs == true, then directories are created at the bottom,
   * not files.
   *
   * @param futures list of futures to build up.
   * @param base base dir
   * @param prefix prefix for filenames.
   * @param executor submitter.
   * @param depth depth of dirs
   * @param width width of dirs
   * @param files files to add in each base dir.
   * @param createDirs create directories rather than files?
   * @return the list of futures
   */
  private List<Future<Path>> createFilesOrDirs(
      List<Future<Path>> futures,
      Path base,
      String prefix,
      ExecutorService executor,
      int depth,
      int width,
      int files,
      boolean createDirs) {

    if (depth > 0) {
      // still creating directories
      for (int i = 0; i < width; i++) {
        Path child = new Path(base,
            String.format("dir-%02d-%02d", depth, i));
        createFilesOrDirs(futures, child, prefix, executor, depth - 1, width, files, false);
      }
    } else {
      // time to create files
      for (int i = 0; i < files; i++) {
        Path file = new Path(base,
            String.format("%s-%04d", prefix,
                CREATE_FILE_COUNTER.incrementAndGet()));
        // buld the data. Not actually used in mkdir.
        long entry = fileDataGenerator.incrementAndGet() & 0xffff;
        byte[] data = new byte[2];
        data[0] = (byte) (entry & 0xff);
        data[1] = (byte) ((entry & 0xff00) >> 8);
        // the async operation.
        Future<Path> f = executor.submit(() -> {
          if (!createDirs) {
            // create files
            ContractTestUtils.createFile(getFileSystem(), file, true, data);
          } else {
            // create directories
            mkdirs(file);
          }
          return file;
        });
        futures.add(f);
      }
    }
    return futures;
  }

  /**
   * Create a list of paths under a dir.
   * @param base base dir
   * @param count count
   * @return the list
   */
  protected List<Path> subpaths(Path base, int count) {
    return IntStream.rangeClosed(1, count)
        .mapToObj(i -> new Path(base, String.format("entry-%02d", i)))
        .collect(Collectors.toList());
  }

  /**
   * Submit a mkdir call to the executor pool.
   * @param path path of dir to create.
   * @return future
   */
  protected CompletableFuture<Path> asyncMkdir(final Path path) {
    CompletableFuture<Path> f = new CompletableFuture<>();
    getExecutorService().submit(() -> {
      try {
        mkdirs(path);
        f.complete(path);
      } catch (IOException e) {
        f.completeExceptionally(e);
      }
    });
    return f;
  }

  /**
   * Given a list of paths, create the dirs async.
   * @param paths path list
   * @throws IOException failure
   */
  protected void asyncMkdirs(Collection<Path> paths) throws IOException {
    List<CompletableFuture<Path>> futures = new ArrayList<>();
    // initiate
    for (Path path: paths) {
      futures.add(asyncMkdir(path));
    }
    // await
    for (Future<Path> f : futures) {
      awaitFuture(f);
    }
  }

  /**
   * Submit an oepration to create a file to the executor pool.
   * @param path path of file to create.
   * @return future
   */
  protected CompletableFuture<Path> asyncPut(final Path path, byte[] data) {
    CompletableFuture<Path> f = new CompletableFuture<>();
    getExecutorService().submit(() -> {
      try {
        ContractTestUtils.createFile(getFileSystem(), path, true, data);
        f.complete(path);
      } catch (IOException e) {
        f.completeExceptionally(e);
      }
    });
    return f;
  }

  /**
   * Convert the manifest list to a map by task attempt ID.
   * @param list manifests
   * @return a map, indexed by task attempt ID.
   */
  protected Map<String, TaskManifest> toMap(List<TaskManifest> list) {
    return list.stream()
        .collect(Collectors.toMap(TaskManifest::getTaskAttemptID, x -> x));
  }

  /**
   * Verify the manifest files match the list of paths.
   * @param manifest manifest to audit
   * @param files list of files.
   */
  protected void verifyManifestFilesMatch(final TaskManifest manifest,
      final List<Path> files) {
    // get the list of source paths
    Set<Path> filesToRename = manifest.getFilesToCommit()
        .stream()
        .map(FileEntry::getSourcePath)
        .collect(Collectors.toSet());
    // which must match that of all the files created
    Assertions.assertThat(filesToRename)
        .containsExactlyInAnyOrderElementsOf(files);
  }

  /**
   * Verify that a task manifest has a given attempt ID.
   * @param manifest manifest, may be null.
   * @param attemptId expected attempt ID
   * @return the manifest, guaranteed to be non-null and of task attempt.
   */
  protected TaskManifest verifyManifestTaskAttemptID(
      final TaskManifest manifest,
      final String attemptId) {
    Assertions.assertThat(manifest)
        .describedAs("Manifest of task %s", attemptId)
        .isNotNull();
    Assertions.assertThat(manifest.getTaskAttemptID())
        .describedAs("Task Attempt ID of manifest %s", manifest)
        .isEqualTo(attemptId);
    return manifest;
  }

  /**
   * Assert that a path must exist; return the path.
   * @param message text for error message.
   * @param path path to validate.
   * @return the path
   * @throws IOException IO Failure
   */
  Path pathMustExist(final String message,
      final Path path) throws IOException {
    assertPathExists(message, path);
    return path;
  }

  /**
   * Assert that a path must exist; return the path.
   * It must also equal the expected value.
   * @param message text for error message.
   * @param expectedPath expected path.
   * @param actualPath path to validate.
   * @return the path
   * @throws IOException IO Failure
   */
  Path verifyPath(final String message,
      final Path expectedPath,
      final Path actualPath) throws IOException {
    Assertions.assertThat(actualPath)
        .describedAs(message)
        .isEqualTo(expectedPath);
    return pathMustExist(message, actualPath);
  }

  /**
   * Verify that the specified dir has the {@code _SUCCESS} marker
   * and that it can be loaded.
   * The contents will be logged and returned.
   * @param dir directory to scan
   * @param jobId job ID, only verified if non-empty
   * @return the loaded success data
   * @throws IOException IO Failure
   */
  protected ManifestSuccessData verifySuccessMarker(Path dir, String jobId)
      throws IOException {
    return validateSuccessFile(getFileSystem(), dir, 0, jobId);
  }

  /**
   * Read a UTF-8 file.
   * @param path path to read
   * @return string value
   * @throws IOException IO failure
   */
  protected String readFile(Path path) throws IOException {
    return ContractTestUtils.readUTF8(getFileSystem(), path, -1);
  }

  /**
   * Modify a (job) config to switch to the manifest committer;
   * output validation is also enabled.
   * @param conf config to patch.
   * @return the updated configuration.
   */
  protected Configuration enableManifestCommitter(final Configuration conf) {
    conf.set(COMMITTER_FACTORY_CLASS, MANIFEST_COMMITTER_FACTORY);
    // always create a job marker
    conf.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
    // and validate the output, for extra rigorousness
    conf.setBoolean(OPT_VALIDATE_OUTPUT, true);

    // set the manifest rename dir if non-null
    if (getManifestDir() != null) {
      conf.set(OPT_DIAGNOSTICS_MANIFEST_DIR,
          getManifestDir().toUri().toString());
    }

    // and bind the report dir
    conf.set(OPT_SUMMARY_REPORT_DIR, getReportDirUri().toString());
    return conf;
  }

  /**
   * Create the stage config for a job but don't finalize it.
   * Uses {@link #TASK_IDS} for job/task ID.
   * @param jobAttemptNumber job attempt number
   * @param outputPath path where the final output goes
   * @return the config
   */
  protected StageConfig createStageConfigForJob(
      final int jobAttemptNumber,
      final Path outputPath) {
    return createStageConfig(jobAttemptNumber, -1, 0, outputPath);
  }

  /**
   * Create the stage config for job or task but don't finalize it.
   * Uses {@link #TASK_IDS} for job/task ID.
   * @param jobAttemptNumber job attempt number
   * @param taskIndex task attempt index; -1 for job attempt only.
   * @param taskAttemptNumber task attempt number
   * @param outputPath path where the final output goes
   * @return the config
   */
  protected StageConfig createStageConfig(
      final int jobAttemptNumber,
      final int taskIndex,
      final int taskAttemptNumber,
      final Path outputPath) {
    final String jobId = TASK_IDS.getJobId();
    ManifestCommitterSupport.AttemptDirectories attemptDirs =
        new ManifestCommitterSupport.AttemptDirectories(outputPath,
            jobId, jobAttemptNumber);
    StageConfig config = new StageConfig();
    config
        .withConfiguration(getConfiguration())
        .withIOProcessors(getSubmitter())
        .withIOStatistics(getStageStatistics())
        .withJobId(jobId)
        .withJobIdSource(JOB_ID_SOURCE_MAPREDUCE)
        .withJobAttemptNumber(jobAttemptNumber)
        .withJobDirectories(attemptDirs)
        .withName(String.format(NAME_FORMAT_JOB_ATTEMPT, jobId))
        .withOperations(getStoreOperations())
        .withProgressable(getProgressCounter())
        .withSuccessMarkerFileLimit(100_000)
        .withWriterQueueCapacity(DEFAULT_WRITER_QUEUE_CAPACITY);

    // if there's a task attempt ID set, set up its details
    if (taskIndex >= 0) {
      String taskAttempt = TASK_IDS.getTaskAttempt(taskIndex,
          taskAttemptNumber);
      config
          .withTaskAttemptId(taskAttempt)
          .withTaskId(TASK_IDS.getTaskIdType(taskIndex).toString())
          .withTaskAttemptDir(
              attemptDirs.getTaskAttemptPath(taskAttempt));
    }
    return config;
  }

  /**
   * A job stage config.
   * @return stage config or null.
   */
  protected StageConfig getJobStageConfig() {
    return jobStageConfig;
  }

  protected void setJobStageConfig(StageConfig jobStageConfig) {
    this.jobStageConfig = jobStageConfig;
  }

  protected Path getDestDir() {
    return destDir;
  }

  protected void setDestDir(Path destDir) {
    this.destDir = destDir;
  }

  /**
   * Execute a set of tasks; task ID is a simple count.
   * task attempt is lowest 2 bits of task ID.
   * @param taskAttemptCount number of tasks.
   * @param filesPerTaskAttempt number of files to include in manifest.
   * @return the manifests.
   * @throws IOException IO failure.
   */
  protected List<TaskManifest> executeTaskAttempts(int taskAttemptCount,
      int filesPerTaskAttempt) throws IOException {

    try (DurationInfo di = new DurationInfo(LOG, true, "create manifests")) {

      // build a list of the task IDs.
      // it's really hard to create a list of Integers; the java8
      // IntStream etc doesn't quite fit as they do their best
      // keep things unboxed, trying to map(Integer::valueOf) doesn't help.
      List<Integer> taskIdList = new ArrayList<>(taskAttemptCount);
      for (int t = 0; t < taskAttemptCount; t++) {
        taskIdList.add(t);
      }

      /// execute the tasks
      List<TaskManifest> manifests = Collections.synchronizedList(
          new ArrayList<>());

      // then submit their creation/save to the pool.
      TaskPool.foreach(taskIdList)
          .executeWith(getSubmitter())
          .stopOnFailure()
          .run(i -> {
            manifests.add(
                executeOneTaskAttempt(i, i & 0x03, filesPerTaskAttempt));
          });
      return manifests;

    }
  }

  /**
   * Create at task ID and attempt (adding to taskIDs and taskAttemptIds)
   * setup the task, create a manifest with fake task entries
   * and save that manifest to the job attempt dir.
   * No actual files are created.
   * @param task task index
   * @param taskAttempt task attempt value
   * @param filesPerTaskAttempt number of files to include in manifest.
   * @return the manifest
   * @throws IOException failure
   */
  protected TaskManifest executeOneTaskAttempt(final int task,
      int taskAttempt, final int filesPerTaskAttempt) throws IOException {

    String tid = String.format("task_%03d", task);
    String taskAttemptId = String.format("%s_%02d",
        tid, taskAttempt);
    synchronized (this) {
      taskIds.add(tid);
      taskAttemptIds.add(taskAttemptId);
    }
    // for each task, a job config is created then patched with the task info
    StageConfig taskStageConfig = createTaskStageConfig(JOB1, tid, taskAttemptId);

    LOG.info("Generating manifest for {}", taskAttemptId);

    // task setup: create dest dir.
    // This helps generate a realistic
    // workload for the parallelized job cleanup.
    new SetupTaskStage(taskStageConfig).apply("task " + taskAttemptId);

    final TaskManifest manifest = createTaskManifest(taskStageConfig);

    Path taDir = taskStageConfig.getTaskAttemptDir();
    long size = task * 1000_0000L;

    // for each task, 10 dirs, one file per dir.
    for (int i = 0; i < filesPerTaskAttempt; i++) {
      Path in = new Path(taDir, "dir-" + i);
      Path out = new Path(getDestDir(), "dir-" + i);
      manifest.addDirectory(DirEntry.dirEntry(out, 0, 1));
      String name = taskStageConfig.getTaskAttemptId() + ".csv";
      Path src = new Path(in, name);
      Path dest = new Path(out, name);
      long fileSize = size + i * 1000L;
      manifest.addFileToCommit(
          new FileEntry(src, dest, fileSize, Long.toString(fileSize, 16)));
      totalDataSize.addAndGet(fileSize);
    }

    // save the manifest for this stage.
    new SaveTaskManifestStage(taskStageConfig).apply(manifest);
    return manifest;
  }

  public StageConfig createTaskStageConfig(final int jobId, final String tid,
      final String taskAttemptId) {
    Path jobAttemptTaskSubDir = getJobStageConfig().getJobAttemptTaskSubDir();
    StageConfig taskStageConfig = createStageConfigForJob(jobId, getDestDir())
        .withTaskId(tid)
        .withTaskAttemptId(taskAttemptId)
        .withTaskAttemptDir(new Path(jobAttemptTaskSubDir, taskAttemptId));
    return taskStageConfig;
  }

  /**
   * Verify that the job directories have been cleaned up.
   * @throws IOException IO failure
   */
  protected void verifyJobDirsCleanedUp() throws IOException {
    StageConfig stageConfig = getJobStageConfig();
    assertPathDoesNotExist("Job attempt dir", stageConfig.getJobAttemptDir());
    assertPathDoesNotExist("dest temp dir", stageConfig.getOutputTempSubDir());
  }

  /**
   * List a directory/directory tree and print files.
   * @param fileSystem FS
   * @param path path
   * @param recursive do a recursive listing?
   * @return the number of files found.
   * @throws IOException failure.
   */
  public static long lsR(FileSystem fileSystem, Path path, boolean recursive)
      throws Exception {
    if (path == null) {
      // surfaces when someone calls getParent() on something at the top
      // of the path
      LOG.info("Empty path");
      return 0;
    } else {
      LOG.info("Listing of {}", path);
      final long count = RemoteIterators.foreach(
          fileSystem.listFiles(path, recursive),
          (status) -> LOG.info("{}", status));
      LOG.info("Count of entries: {}", count);
      return count;
    }
  }

  /**
   * Assert that a cleanup stage coursehad a given outcome and
   * deleted the given number of directories.
   * @param result result to analyze
   * @param outcome expected outcome
   * @param expectedDirsDeleted #of directories deleted. -1 for no checks
   */
  protected void assertCleanupResult(
      CleanupJobStage.Result result,
      CleanupJobStage.Outcome outcome,
      int expectedDirsDeleted) {
    Assertions.assertThat(result.getOutcome())
        .describedAs("Outcome of cleanup() in %s", result)
        .isEqualTo(outcome);
    if (expectedDirsDeleted >= 0) {
      Assertions.assertThat(result.getDeleteCalls())
          .describedAs("Number of directories deleted in cleanup %s", result)
          .isEqualTo(expectedDirsDeleted);
    }
  }

  /**
   * Create and execute a cleanup stage.
   * @param enabled is the stage enabled?
   * @param deleteTaskAttemptDirsInParallel delete task attempt dirs in
   *        parallel?
   * @param suppressExceptions suppress exceptions?
   * @param outcome expected outcome.
   * @param expectedDirsDeleted #of directories deleted. -1 for no checks
   * @return the result
   * @throws IOException non-suppressed exception
   */
  protected CleanupJobStage.Result cleanup(
      final boolean enabled,
      final boolean deleteTaskAttemptDirsInParallel,
      final boolean suppressExceptions,
      final CleanupJobStage.Outcome outcome,
      final int expectedDirsDeleted) throws IOException {
    StageConfig stageConfig = getJobStageConfig();
    CleanupJobStage.Result result = new CleanupJobStage(stageConfig)
        .apply(new CleanupJobStage.Arguments(OP_STAGE_JOB_CLEANUP,
            enabled, deleteTaskAttemptDirsInParallel, suppressExceptions));
    assertCleanupResult(result, outcome, expectedDirsDeleted);
    return result;
  }

  /**
   * Read the UTF_8 text in the file.
   * @param path path to read
   * @return the string
   * @throws IOException failure
   */
  protected String readText(final Path path) throws IOException {

    final FileSystem fs = getFileSystem();
    final FileStatus st = fs.getFileStatus(path);
    Assertions.assertThat(st.getLen())
        .describedAs("length of file %s", st)
        .isLessThanOrEqualTo(MAX_LEN);

    return new String(
        readDataset(fs, path, (int) st.getLen()),
        StandardCharsets.UTF_8);
  }

  /**
   * Counter.
   */
  protected static final class ProgressCounter implements Progressable {

    private final AtomicLong counter = new AtomicLong();

    /**
     * Increment the counter.
     */
    @Override
    public void progress() {
      counter.incrementAndGet();
    }

    /**
     * Get the counter value.
     * @return the current value.
     */
    public long value() {
      return counter.get();
    }

    /**
     * Reset the counter.
     */
    public void reset() {
      counter.set(0);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ProgressCounter{");
      sb.append("counter=").append(counter.get());
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Get the progress counter of a stage.
   * @param stageConfig stage
   * @return its progress counter.
   */
  ProgressCounter progressOf(StageConfig stageConfig) {
    return (ProgressCounter) stageConfig.getProgressable();
  }
}
