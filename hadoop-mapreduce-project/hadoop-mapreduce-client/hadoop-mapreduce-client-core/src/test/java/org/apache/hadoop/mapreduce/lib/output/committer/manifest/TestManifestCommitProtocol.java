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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.RemoteIterators;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.contract.ContractTestUtils.listChildren;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_FACTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SPARK_WRITE_UUID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_BYTES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_FILES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_COMPLETED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_ABORT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createJobSummaryFilename;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.randomJobId;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.validateSuccessFile;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This is a contract test for the commit protocol on a target filesystem.
 * It is subclassed in the ABFS integration tests and elsewhere.
 * Derived from the S3A protocol suite, which was itself based off
 * the test suite {@code TestFileOutputCommitter}.
 *
 * Some of the methods trigger java warnings about unchecked casts;
 * it's impossible to remove them, so the checks are suppressed.
 */
@SuppressWarnings("unchecked")
public class TestManifestCommitProtocol
    extends AbstractManifestCommitterTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestManifestCommitProtocol.class);

  private static final String SUB_DIR = "SUB_DIR";

  /**
   * Part of the name of the output of task attempt 0.
   */
  protected static final String PART_00000 = "part-m-00000";

  private static final Text KEY_1 = new Text("key1");

  private static final Text KEY_2 = new Text("key2");

  private static final Text VAL_1 = new Text("val1");

  private static final Text VAL_2 = new Text("val2");

  /**
   * Snapshot of stats, which will be collected from
   * committers.
   */
  private static final IOStatisticsSnapshot IOSTATISTICS =
      IOStatisticsSupport.snapshotIOStatistics();

  /**
   * Job ID for jobs.
   */
  private final String jobId;

  /**
   * A random task attempt id for testing.
   */
  private final String attempt0;

  /**
   *  Attempt 0's task attempt ID.
   */
  private final TaskAttemptID taskAttempt0;

  /**
   * TA 1.
   */
  private final TaskAttemptID taskAttempt1;

  /**
   * Attempt 1 string value.
   */
  private final String attempt1;


  /** A job to abort in test case teardown. */
  private final List<JobData> abortInTeardown = new ArrayList<>(1);

  /**
   * Output directory.
   * This is the directory into which output goes;
   * all the job files go in _temporary underneath.
   */
  private Path outputDir;

  /**
   * Committer factory which calls back into
   * {@link #createCommitter(Path, TaskAttemptContext)}.
   */
  private final LocalCommitterFactory
      localCommitterFactory = new LocalCommitterFactory();

  /**
   * Clean up the output dir. No-op if
   * {@link #outputDir} is null.
   * @throws IOException failure to delete
   */
  private void cleanupOutputDir() throws IOException {
    if (outputDir != null) {
      getFileSystem().delete(outputDir, true);
    }
  }

  /**
   * Constructor.
   */
  public TestManifestCommitProtocol() {
    ManifestCommitterTestSupport.JobAndTaskIDsForTests taskIDs
        = new ManifestCommitterTestSupport.JobAndTaskIDsForTests(2, 2);
    jobId = taskIDs.getJobId();
    attempt0 = taskIDs.getTaskAttempt(0, 0);
    taskAttempt0 = taskIDs.getTaskAttemptIdType(0, 0);
    attempt1 = taskIDs.getTaskAttempt(0, 1);
    taskAttempt1 = taskIDs.getTaskAttemptIdType(0, 1);
  }

  /**
   * This must return the name of a suite which is unique to the test.
   * @return a string which must be unique and a valid path.
   */
  protected String suitename() {
    return "TestManifestCommitProtocolLocalFS";
  }

  /**
   * Get the log; can be overridden for test case log.
   * @return a log.
   */
  public Logger log() {
    return LOG;
  }

  /**
   * Overridden method returns the suitename as well as the method name,
   * so if more than one committer test is run in parallel, paths are
   * isolated.
   * @return a name for a method, unique across the suites and test cases.
   */
  @Override
  protected String getMethodName() {
    return suitename() + "-" + super.getMethodName();
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    outputDir = path(getMethodName());
    cleanupOutputDir();
  }

  @Override
  public void teardown() throws Exception {
    describe("teardown");
    Thread.currentThread().setName("teardown");
    for (JobData jobData : abortInTeardown) {
      // stop the job
      abortJobQuietly(jobData);
      // and then get its statistics
      IOSTATISTICS.aggregate(jobData.committer.getIOStatistics());
    }
    try {
      cleanupOutputDir();
    } catch (IOException e) {
      log().info("Exception during cleanup", e);
    }
    super.teardown();
  }

  @AfterClass
  public static void logAggregateIOStatistics() {
    LOG.info("Final IOStatistics {}",
        ioStatisticsToPrettyString(IOSTATISTICS));
  }

  /**
   * Add the specified job to the current list of jobs to abort in teardown.
   * @param jobData job data.
   */
  protected void abortInTeardown(JobData jobData) {
    abortInTeardown.add(jobData);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    bindCommitter(conf);
    return conf;
  }

  /***
   * Set job up to use the manifest committer.
   * @param conf configuration to set up
   */
  protected void bindCommitter(Configuration conf) {
    conf.set(COMMITTER_FACTORY_CLASS, MANIFEST_COMMITTER_FACTORY);
  }

  /**
   * Create a committer for a task.
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */
  protected ManifestCommitter createCommitter(
      TaskAttemptContext context) throws IOException {
    return createCommitter(getOutputDir(), context);
  }

  /**
   * Create a committer for a task and a given output path.
   * @param outputPath path
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */
  protected ManifestCommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    return new ManifestCommitter(outputPath, context);
  }

  protected Path getOutputDir() {
    return outputDir;
  }

  protected String getJobId() {
    return jobId;
  }

  protected String getAttempt0() {
    return attempt0;
  }

  protected TaskAttemptID getTaskAttempt0() {
    return taskAttempt0;
  }

  protected String getAttempt1() {
    return attempt1;
  }

  protected TaskAttemptID getTaskAttempt1() {
    return taskAttempt1;
  }

  /**
   * Functional interface for creating committers, designed to allow
   * different factories to be used to create different failure modes.
   */
  @FunctionalInterface
  public interface CommitterFactory {

    /**
     * Create a committer for a task.
     * @param context task context
     * @return new committer
     * @throws IOException failure
     */
    ManifestCommitter createCommitter(
        TaskAttemptContext context) throws IOException;
  }

  /**
   * The normal committer creation factory, uses the abstract methods
   * in the class.
   */
  protected class LocalCommitterFactory implements CommitterFactory {

    @Override
    public ManifestCommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return TestManifestCommitProtocol.this
          .createCommitter(context);
    }
  }

  /**
   * Assert that for a given output, the job context returns a manifest
   * committer factory. This is what FileOutputFormat does internally,
   * and is needed to make sure that the relevant settings are being passed
   * around.
   * @param context job/task context
   * @param output destination path.
   */
  protected void assertCommitterFactoryIsManifestCommitter(
      JobContext context, Path output) {

    final Configuration conf = context.getConfiguration();
    // check one: committer
    assertConfigurationUsesManifestCommitter(conf);
    final String factoryName = conf.get(COMMITTER_FACTORY_CLASS, "");
    final PathOutputCommitterFactory factory
        = PathOutputCommitterFactory.getCommitterFactory(
        output,
        conf);
    Assertions.assertThat(factory)
        .describedAs("Committer for output path %s"
                + " and factory name \"%s\"",
            output, factoryName)
        .isInstanceOf(ManifestCommitterFactory.class);
  }

  /**
   * This is to debug situations where the test committer factory
   * on tasks was binding to FileOutputCommitter even when
   * tests were overriding it.
   * @param conf configuration to probe.
   */
  private void assertConfigurationUsesManifestCommitter(
      Configuration conf) {
    final String factoryName = conf.get(COMMITTER_FACTORY_CLASS, null);
    Assertions.assertThat(factoryName)
        .describedAs("Value of %s", COMMITTER_FACTORY_CLASS)
        .isEqualTo(MANIFEST_COMMITTER_FACTORY);
  }

  /**
   * Write some text out.
   * @param context task
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   * @return the path written to
   */
  protected Path writeTextOutput(TaskAttemptContext context)
      throws IOException, InterruptedException {
    describe("write output");
    try (DurationInfo d = new DurationInfo(LOG,
        "Writing Text output for task %s", context.getTaskAttemptID())) {
      TextOutputForTests.LoggingLineRecordWriter<Writable, Object> writer
          = new TextOutputForTests<Writable, Object>().getRecordWriter(context);
      writeOutput(writer, context);
      return writer.getDest();
    }
  }

  /**
   * Write the standard output.
   * @param writer record writer
   * @param context task context
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeOutput(
      RecordWriter<Writable, Object> writer,
      TaskAttemptContext context) throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    try (ManifestCommitterTestSupport.CloseWriter<Writable, Object> cw =
             new ManifestCommitterTestSupport.CloseWriter<>(writer, context)) {
      writer.write(KEY_1, VAL_1);
      writer.write(null, nullWritable);
      writer.write(null, VAL_1);
      writer.write(nullWritable, VAL_2);
      writer.write(KEY_2, nullWritable);
      writer.write(KEY_1, null);
      writer.write(null, null);
      writer.write(KEY_2, VAL_2);
      writer.close(context);
    }
  }

  /**
   * Write the output of a map.
   * @param writer record writer
   * @param context task context
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeMapFileOutput(RecordWriter<WritableComparable<?>, Writable> writer,
      TaskAttemptContext context) throws IOException, InterruptedException {
    describe("\nWrite map output");
    try (DurationInfo d = new DurationInfo(LOG,
        "Writing Text output for task %s", context.getTaskAttemptID());
         ManifestCommitterTestSupport.CloseWriter<WritableComparable<?>, Writable> cw =
             new ManifestCommitterTestSupport.CloseWriter<>(writer, context)) {
      for (int i = 0; i < 10; ++i) {
        Text val = ((i & 1) == 1) ? VAL_1 : VAL_2;
        writer.write(new LongWritable(i), val);
      }
      LOG.debug("Closing writer {}", writer);
      writer.close(context);
    }
  }

  /**
   * Details on a job for use in {@code startJob} and elsewhere.
   */
  protected static final class JobData {

    private final Job job;

    private final JobContext jContext;

    private final TaskAttemptContext tContext;

    private final ManifestCommitter committer;

    private final Configuration conf;

    private Path writtenTextPath; // null if not written to

    public JobData(Job job,
        JobContext jContext,
        TaskAttemptContext tContext,
        ManifestCommitter committer) {
      this.job = job;
      this.jContext = jContext;
      this.tContext = tContext;
      this.committer = committer;
      conf = job.getConfiguration();
    }

    public String jobId() {
      return committer.getJobUniqueId();
    }
  }

  /**
   * Create a new job. Sets the task attempt ID,
   * and output dir; asks for a success marker.
   * @return the new job
   * @throws IOException failure
   */
  public Job newJob() throws IOException {
    return newJob(outputDir, getConfiguration(), attempt0);
  }

  /**
   * Create a new job. Sets the task attempt ID,
   * and output dir; asks for a success marker.
   * Committer factory is set to manifest factory, so is independent
   * of FS schema.
   * @param dir dest dir
   * @param configuration config to get the job from
   * @param taskAttemptId task attempt
   * @return the new job
   * @throws IOException failure
   */
  private Job newJob(Path dir, Configuration configuration,
      String taskAttemptId) throws IOException {
    Job job = Job.getInstance(configuration);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttemptId);
    enableManifestCommitter(conf);
    FileOutputFormat.setOutputPath(job, dir);
    return job;
  }

  /**
   * Start a job with a committer; optionally write the test data.
   * Always register the job to be aborted (quietly) in teardown.
   * This is, from an "OO-purity perspective" the wrong kind of method to
   * do: it's setting things up, mixing functionality, registering for teardown.
   * Its aim is simple though: a common body of code for starting work
   * in test cases.
   * @param writeText should the text be written?
   * @return the job data 4-tuple
   * @throws IOException IO problems
   * @throws InterruptedException interruption during write
   */
  protected JobData startJob(boolean writeText)
      throws IOException, InterruptedException {
    return startJob(localCommitterFactory, writeText);
  }

  /**
   * Start a job with a committer; optionally write the test data.
   * Always register the job to be aborted (quietly) in teardown.
   * This is, from an "OO-purity perspective" the wrong kind of method to
   * do: it's setting things up, mixing functionality, registering for teardown.
   * Its aim is simple though: a common body of code for starting work
   * in test cases.
   * @param factory the committer factory to use
   * @param writeText should the text be written?
   * @return the job data 4-tuple
   * @throws IOException IO problems
   * @throws InterruptedException interruption during write
   */
  protected JobData startJob(CommitterFactory factory, boolean writeText)
      throws IOException, InterruptedException {
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    assertConfigurationUsesManifestCommitter(conf);
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, taskAttempt0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    ManifestCommitter committer = factory.createCommitter(tContext);

    // setup
    JobData jobData = new JobData(job, jContext, tContext, committer);
    setupJob(jobData);
    abortInTeardown(jobData);

    if (writeText) {
      // write output
      jobData.writtenTextPath = writeTextOutput(tContext);
    }
    return jobData;
  }

  /**
   * Set up the job and task.
   * @param jobData job data
   * @throws IOException problems
   */
  protected void setupJob(JobData jobData) throws IOException {
    ManifestCommitter committer = jobData.committer;
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    describe("\nsetup job");
    try (DurationInfo d = new DurationInfo(LOG,
        "setup job %s", jContext.getJobID())) {
      committer.setupJob(jContext);
    }
    setupCommitter(committer, tContext);
    describe("setup complete");
  }

  private void setupCommitter(
      final ManifestCommitter committer,
      final TaskAttemptContext tContext) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "setup task %s", tContext.getTaskAttemptID())) {
      committer.setupTask(tContext);
    }
  }

  /**
   * Abort a job quietly.
   * @param jobData job info
   */
  protected void abortJobQuietly(JobData jobData) {
    abortJobQuietly(jobData.committer, jobData.jContext, jobData.tContext);
  }

  /**
   * Abort a job quietly: first task, then job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   */
  protected void abortJobQuietly(ManifestCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) {
    describe("\naborting task");
    try {
      committer.abortTask(tContext);
    } catch (Exception e) {
      log().warn("Exception aborting task:", e);
    }
    describe("\naborting job");
    try {
      committer.abortJob(jContext, JobStatus.State.KILLED);
    } catch (Exception e) {
      log().warn("Exception aborting job", e);
    }
  }

  /**
   * Commit the task and then the job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   * @throws IOException problems
   */
  protected void commitTaskAndJob(ManifestCommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "committing Job %s", jContext.getJobID())) {
      describe("\ncommitting task");
      committer.commitTask(tContext);
      describe("\ncommitting job");
      committer.commitJob(jContext);
      describe("commit complete\n");
    }
  }

  /**
   * Execute work as part of a test, after creating the job.
   * After the execution, {@link #abortJobQuietly(JobData)} is
   * called for abort/cleanup.
   * @param name name of work (for logging)
   * @param action action to execute
   * @throws Exception failure
   */
  protected void executeWork(String name, ActionToTest action)
      throws Exception {
    executeWork(name, startJob(false), action);
  }

  /**
   * Execute work as part of a test, against the created job.
   * After the execution, {@link #abortJobQuietly(JobData)} is
   * called for abort/cleanup.
   * @param name name of work (for logging)
   * @param jobData job info
   * @param action action to execute
   * @throws Exception failure
   */
  public void executeWork(String name,
      JobData jobData,
      ActionToTest action) throws Exception {
    try (DurationInfo d = new DurationInfo(LOG, "Executing %s", name)) {
      action.exec(jobData.job,
          jobData.jContext,
          jobData.tContext,
          jobData.committer);
    } finally {
      abortJobQuietly(jobData);
    }
  }

  /**
   * Load a manifest from the test FS.
   * @param path path
   * @return the manifest
   * @throws IOException failure to load
   */
  TaskManifest loadManifest(Path path) throws IOException {
    return TaskManifest.load(getFileSystem(), path);
  }

  /**
   * Verify that recovery doesn't work for these committers.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testRecoveryAndCleanup() throws Exception {
    describe("Test (unsupported) task recovery.");
    JobData jobData = startJob(true);
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;

    Assertions.assertThat(committer.getWorkPath())
        .as("null workPath in committer " + committer)
        .isNotNull();
    Assertions.assertThat(committer.getOutputPath())
        .as("null outputPath in committer " + committer)
        .isNotNull();

    // Commit the task.
    commitTask(committer, tContext);

    // load and log the manifest
    final TaskManifest manifest = loadManifest(
        committer.getTaskManifestPath(tContext));
    LOG.info("Manifest {}", manifest);

    Configuration conf2 = jobData.job.getConfiguration();
    conf2.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
    JobContext jContext2 = new JobContextImpl(conf2, taskAttempt0.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2,
        taskAttempt0);
    ManifestCommitter committer2 = createCommitter(tContext2);
    committer2.setupJob(tContext2);

    Assertions.assertThat(committer2.isRecoverySupported())
        .as("recoverySupported in " + committer2)
        .isFalse();
    intercept(IOException.class, "recover",
        () -> committer2.recoverTask(tContext2));

    // at this point, task attempt 0 has failed to recover
    // it should be abortable though. This will be a no-op as it already
    // committed
    describe("aborting task attempt 2; expect nothing to clean up");
    committer2.abortTask(tContext2);
    describe("Aborting job 2; expect pending commits to be aborted");
    committer2.abortJob(jContext2, JobStatus.State.KILLED);
  }

  /**
   * Assert that the task attempt FS Doesn't have a task attempt
   * directory.
   * @param committer committer
   * @param context task context
   * @throws IOException IO failure.
   */
  protected void assertTaskAttemptPathDoesNotExist(
      ManifestCommitter committer, TaskAttemptContext context)
      throws IOException {
    Path attemptPath = committer.getTaskAttemptPath(context);
    ContractTestUtils.assertPathDoesNotExist(
        attemptPath.getFileSystem(context.getConfiguration()),
        "task attempt dir",
        attemptPath);
  }

  protected void assertJobAttemptPathDoesNotExist(
      ManifestCommitter committer, JobContext context)
      throws IOException {
    Path attemptPath = committer.getJobAttemptPath(context);
    ContractTestUtils.assertPathDoesNotExist(
        attemptPath.getFileSystem(context.getConfiguration()),
        "job attempt dir",
        attemptPath);
  }

  /**
   * Verify the output of the directory.
   * That includes the {@code part-m-00000-*}
   * file existence and contents, as well as optionally, the success marker.
   * @param dir directory to scan.
   * @param expectSuccessMarker check the success marker?
   * @param expectedJobId job ID, verified if non-empty and success data loaded
   * @throws Exception failure.
   * @return the success data
   */
  private ManifestSuccessData validateContent(Path dir,
      boolean expectSuccessMarker,
      String expectedJobId) throws Exception {
    lsR(getFileSystem(), dir, true);
    ManifestSuccessData successData;
    if (expectSuccessMarker) {
      successData = verifySuccessMarker(dir, expectedJobId);
    } else {
      successData = null;
    }
    Path expectedFile = getPart0000(dir);
    log().debug("Validating content in {}", expectedFile);
    StringBuilder expectedOutput = new StringBuilder();
    expectedOutput.append(KEY_1).append('\t').append(VAL_1).append("\n");
    expectedOutput.append(VAL_1).append("\n");
    expectedOutput.append(VAL_2).append("\n");
    expectedOutput.append(KEY_2).append("\n");
    expectedOutput.append(KEY_1).append("\n");
    expectedOutput.append(KEY_2).append('\t').append(VAL_2).append("\n");
    String output = readFile(expectedFile);
    Assertions.assertThat(output)
        .describedAs("Content of %s", expectedFile)
        .isEqualTo(expectedOutput.toString());
    return successData;
  }

  /**
   * Identify any path under the directory which begins with the
   * {@code "part-m-00000"} sequence. There's some compensation for
   * eventual consistency here.
   * @param dir directory to scan
   * @return the full path
   * @throws FileNotFoundException the path is missing.
   * @throws Exception failure.
   */
  protected Path getPart0000(final Path dir) throws Exception {
    final FileSystem fs = dir.getFileSystem(getConfiguration());
    FileStatus[] statuses = fs.listStatus(dir,
        path -> path.getName().startsWith(PART_00000));
    if (statuses.length != 1) {
      // fail, with a listing of the parent dir
      ContractTestUtils.assertPathExists(fs, "Output file",
          new Path(dir, PART_00000));
    }
    return statuses[0].getPath();
  }

  /**
   * Look for the partFile subdir of the output dir
   * and the ma and data entries.
   * @param fs filesystem
   * @param dir output dir
   * @throws Exception failure.
   */
  private void validateMapFileOutputContent(
      FileSystem fs, Path dir) throws Exception {
    // map output is a directory with index and data files
    assertPathExists("Map output", dir);
    Path expectedMapDir = getPart0000(dir);
    assertPathExists("Map output", expectedMapDir);
    assertIsDirectory(expectedMapDir);
    FileStatus[] files = fs.listStatus(expectedMapDir);
    Assertions.assertThat(files)
        .as("No files found in " + expectedMapDir)
        .isNotEmpty();
    assertPathExists("index file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.INDEX_FILE_NAME));
    assertPathExists("data file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.DATA_FILE_NAME));
  }

  /**
   * Full test of the expected lifecycle: start job, task, write, commit task,
   * commit job.
   * @throws Exception on a failure
   */
  @Test
  public void testCommitLifecycle() throws Exception {
    describe("Full test of the expected lifecycle:\n" +
        " start job, task, write, commit task, commit job.\n" +
        "Verify:\n" +
        "* no files are visible after task commit\n" +
        "* the expected file is visible after job commit\n");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;
    assertCommitterFactoryIsManifestCommitter(tContext,
        tContext.getWorkingDirectory());
    validateTaskAttemptWorkingDirectory(committer, tContext);

    // write output
    describe("1. Writing output");
    final Path textOutputPath = writeTextOutput(tContext);
    describe("Output written to %s", textOutputPath);

    describe("2. Committing task");
    Assertions.assertThat(committer.needsTaskCommit(tContext))
        .as("No files to commit were found by " + committer)
        .isTrue();
    commitTask(committer, tContext);
    final TaskManifest taskManifest = requireNonNull(
        committer.getTaskAttemptCommittedManifest(), "committerTaskManifest");
    final String manifestJSON = taskManifest.toJson();
    LOG.info("Task manifest {}", manifestJSON);
    int filesCreated = 1;
    Assertions.assertThat(taskManifest.getFilesToCommit())
        .describedAs("Files to commit in task manifest %s", manifestJSON)
        .hasSize(filesCreated);
    Assertions.assertThat(taskManifest.getDestDirectories())
        .describedAs("Directories to create in task manifest %s",
            manifestJSON)
        .isEmpty();

    // this is only task commit; there MUST be no part- files in the dest dir
    try {
      RemoteIterators.foreach(getFileSystem().listFiles(outputDir, false),
          (status) ->
              Assertions.assertThat(status.getPath().toString())
                  .as("task committed file to dest :" + status)
                  .contains("part"));
    } catch (FileNotFoundException ignored) {
      log().info("Outdir {} is not created by task commit phase ",
          outputDir);
    }

    describe("3. Committing job");

    commitJob(committer, jContext);

    // validate output
    describe("4. Validating content");
    String jobUniqueId = jobData.jobId();
    ManifestSuccessData successData = validateContent(outputDir,
        true,
        jobUniqueId);
    // look in the SUMMARY
    Assertions.assertThat(successData.getDiagnostics())
        .describedAs("Stage entry in SUCCESS")
        .containsEntry(STAGE, OP_STAGE_JOB_COMMIT);
    IOStatisticsSnapshot jobStats = successData.getIOStatistics();
    // manifest
    verifyStatisticCounterValue(jobStats,
        OP_LOAD_MANIFEST, 1);
    FileStatus st = getFileSystem().getFileStatus(getPart0000(outputDir));
    verifyStatisticCounterValue(jobStats,
        COMMITTER_FILES_COMMITTED_COUNT, filesCreated);
    verifyStatisticCounterValue(jobStats,
        COMMITTER_BYTES_COMMITTED_COUNT, st.getLen());

    // now load and examine the job report.
    // this MUST contain all the stats of the summary, plus timings on
    // job commit itself

    ManifestSuccessData report = loadReport(jobUniqueId, true);
    Map<String, String> diag = report.getDiagnostics();
    Assertions.assertThat(diag)
        .describedAs("Stage entry in report")
        .containsEntry(STAGE, OP_STAGE_JOB_COMMIT);
    IOStatisticsSnapshot reportStats = report.getIOStatistics();
    verifyStatisticCounterValue(reportStats,
        OP_LOAD_MANIFEST, 1);
    verifyStatisticCounterValue(reportStats,
        OP_STAGE_JOB_COMMIT, 1);
    verifyStatisticCounterValue(reportStats,
        COMMITTER_FILES_COMMITTED_COUNT, filesCreated);
    verifyStatisticCounterValue(reportStats,
        COMMITTER_BYTES_COMMITTED_COUNT, st.getLen());

  }

  /**
   * Load a summary from the report dir.
   * @param jobUniqueId job ID
   * @param expectSuccess is the job expected to have succeeded.
   * @throws IOException failure to load
   * @return the report
   */
  private ManifestSuccessData loadReport(String jobUniqueId,
      boolean expectSuccess) throws IOException {
    File file = new File(getReportDir(),
        createJobSummaryFilename(jobUniqueId));
    ContractTestUtils.assertIsFile(FileSystem.getLocal(getConfiguration()),
        new Path(file.toURI()));
    ManifestSuccessData report = ManifestSuccessData.serializer().load(file);
    LOG.info("Report for job {}:\n{}", jobUniqueId, report.toJson());
    Assertions.assertThat(report.getSuccess())
        .describedAs("success flag in report")
        .isEqualTo(expectSuccess);
    return report;
  }

  /**
   * Repeated commit call after job commit.
   */
  @Test
  public void testCommitterWithDuplicatedCommit() throws Exception {
    describe("Call a task then job commit twice;" +
        "expect the second task commit to fail.");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;

    // do commit
    describe("committing task");
    committer.commitTask(tContext);

    // repeated commit while TA dir exists fine/idempotent
    committer.commitTask(tContext);

    describe("committing job");
    committer.commitJob(jContext);
    describe("commit complete\n");

    describe("cleanup");
    committer.cleanupJob(jContext);
    // validate output
    validateContent(outputDir, shouldExpectSuccessMarker(),
        committer.getJobUniqueId());

    // commit task to fail on retry as task attempt dir doesn't exist
    describe("Attempting commit of the same task after job commit -expecting failure");
    expectFNFEonTaskCommit(committer, tContext);
  }

  /**
   * HADOOP-17258. If a second task attempt is committed, it
   * must succeed, and the output of the first TA, even if already
   * committed, MUST NOT be visible in the final output.
   * <p></p>
   * What's important is not just that only one TA must succeed,
   * but it must be the last one executed.
   */
  @Test
  public void testTwoTaskAttemptsCommit() throws Exception {
    describe("Commit two task attempts;" +
        " expect the second attempt to succeed.");
    JobData jobData = startJob(false);
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;
    // do commit
    describe("\ncommitting task");
    // write output for TA 1,
    Path outputTA1 = writeTextOutput(tContext);

    // speculatively execute committer 2.

    // jobconf with a different base to its parts.
    Configuration conf2 = jobData.conf;
    conf2.set("mapreduce.output.basename", "attempt2");
    String attempt2 = "attempt_" + jobId + "_m_000000_1";
    TaskAttemptID ta2 = TaskAttemptID.forName(attempt2);
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(
        conf2, ta2);

    ManifestCommitter committer2 = localCommitterFactory
        .createCommitter(tContext2);
    setupCommitter(committer2, tContext2);

    // verify working dirs are different
    Assertions.assertThat(committer.getWorkPath())
        .describedAs("Working dir of %s", committer)
        .isNotEqualTo(committer2.getWorkPath());

    // write output for TA 2,
    Path outputTA2 = writeTextOutput(tContext2);

    // verify the names are different.
    String name1 = outputTA1.getName();
    String name2 = outputTA2.getName();
    Assertions.assertThat(name1)
        .describedAs("name of task attempt output %s", outputTA1)
        .isNotEqualTo(name2);

    // commit task 1
    committer.commitTask(tContext);

    // then pretend that task1 didn't respond, so
    // commit task 2
    committer2.commitTask(tContext2);

    // and the job
    committer2.commitJob(tContext);

    // validate output
    FileSystem fs = getFileSystem();
    ManifestSuccessData successData = validateSuccessFile(fs, outputDir,
        1,
        "");
    Assertions.assertThat(successData.getFilenames())
        .describedAs("Files committed")
        .hasSize(1);

    assertPathExists("attempt2 output", new Path(outputDir, name2));
    assertPathDoesNotExist("attempt1 output", new Path(outputDir, name1));

  }

  protected boolean shouldExpectSuccessMarker() {
    return true;
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  /*@Test
  public void testCommitterWithFailure() throws Exception {
    describe("Fail the first job commit then retry");
    JobData jobData = startJob(new FailingCommitterFactory(), true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);

    // now fail job
    expectSimulatedFailureOnJobCommit(jContext, committer);

    commitJob(committer, jContext);

    // but the data got there, due to the order of operations.
    validateContent(outDir, shouldExpectSuccessMarker(),
        committer.getUUID());
    expectJobCommitToFail(jContext, committer);
  }
*/

  /**
   * Override point: the failure expected on the attempt to commit a failed
   * job.
   * @param jContext job context
   * @param committer committer
   * @throws Exception any unexpected failure.
   */
  protected void expectJobCommitToFail(JobContext jContext,
      ManifestCommitter committer) throws Exception {
    // next attempt will fail as there is no longer a directory to commit
    expectJobCommitFailure(jContext, committer,
        FileNotFoundException.class);
  }

  /**
   * Expect a job commit operation to fail with a specific exception.
   * @param jContext job context
   * @param committer committer
   * @param clazz class of exception
   * @return the caught exception
   * @throws Exception any unexpected failure.
   */
  protected static <E extends IOException> E expectJobCommitFailure(
      JobContext jContext,
      ManifestCommitter committer,
      Class<E> clazz)
      throws Exception {

    return intercept(clazz,
        () -> {
          committer.commitJob(jContext);
          return committer.toString();
        });
  }

  protected static void expectFNFEonTaskCommit(
      ManifestCommitter committer,
      TaskAttemptContext tContext) throws Exception {
    intercept(FileNotFoundException.class,
        () -> {
          committer.commitTask(tContext);
          return committer.toString();
        });
  }

  /**
   * Commit a task with no output.
   * Dest dir should exist.
   */
  @Test
  public void testCommitterWithNoOutputs() throws Exception {
    describe("Have a task and job with no outputs: expect success");
    JobData jobData = startJob(localCommitterFactory, false);
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);
    Path attemptPath = committer.getTaskAttemptPath(tContext);
    ContractTestUtils.assertPathExists(
        attemptPath.getFileSystem(tContext.getConfiguration()),
        "task attempt dir",
        attemptPath);
  }


  @Test
  public void testMapFileOutputCommitter() throws Exception {
    describe("Test that the committer generates map output into a directory\n" +
        "starting with the prefix part-");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;
    Configuration conf = jobData.conf;

    // write output
    writeMapFileOutput(new MapFileOutputFormat()
            .getRecordWriter(tContext), tContext);

    // do commit
    commitTaskAndJob(committer, jContext, tContext);
    FileSystem fs = getFileSystem();

    lsR(fs, outputDir, true);
    String ls = ls(outputDir);
    describe("\nvalidating");

    // validate output
    verifySuccessMarker(outputDir, committer.getJobUniqueId());

    describe("validate output of %s", outputDir);
    validateMapFileOutputContent(fs, outputDir);

    // Ensure getReaders call works and also ignores
    // hidden filenames (_ or . prefixes)
    describe("listing");
    FileStatus[] filtered = fs.listStatus(outputDir, HIDDEN_FILE_FILTER);
    Assertions.assertThat(filtered)
        .describedAs("listed children under %s", ls)
        .hasSize(1);
    FileStatus fileStatus = filtered[0];
    Assertions.assertThat(fileStatus.getPath().getName())
        .as("Not the part file: " + fileStatus)
        .startsWith(PART_00000);

    describe("getReaders()");
    Assertions.assertThat(getReaders(fs, outputDir, conf))
        .describedAs("getReaders() MapFile.Reader entries with shared FS %s %s", outputDir, ls)
        .hasSize(1);

    describe("getReaders(new FS)");
    FileSystem fs2 = FileSystem.get(outputDir.toUri(), conf);
    Assertions.assertThat(getReaders(fs2, outputDir, conf))
        .describedAs("getReaders(new FS) %s %s", outputDir, ls)
        .hasSize(1);

    describe("MapFileOutputFormat.getReaders");
    Assertions.assertThat(MapFileOutputFormat.getReaders(outputDir, conf))
        .describedAs("MapFileOutputFormat.getReaders(%s) %s", outputDir, ls)
        .hasSize(1);

  }

  /** Open the output generated by this format. */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private static MapFile.Reader[] getReaders(FileSystem fs,
      Path dir,
      Configuration conf) throws IOException {
    Path[] names = FileUtil.stat2Paths(fs.listStatus(dir, HIDDEN_FILE_FILTER));

    // sort names, so that hash partitioning works
    Arrays.sort(names);

    MapFile.Reader[] parts = new MapFile.Reader[names.length];
    for (int i = 0; i < names.length; i++) {
      parts[i] = new MapFile.Reader(names[i], conf);
    }
    return parts;
  }

  public static final PathFilter HIDDEN_FILE_FILTER = (path) ->
      !path.getName().startsWith("_") && !path.getName().startsWith(".");

  /**
   * A functional interface which an action to test must implement.
   */
  @FunctionalInterface
  public interface ActionToTest {

    void exec(Job job, JobContext jContext, TaskAttemptContext tContext,
        ManifestCommitter committer) throws Exception;
  }

  @Test
  public void testAbortTaskNoWorkDone() throws Exception {
    executeWork("abort task no work",
        (job, jContext, tContext, committer) ->
            committer.abortTask(tContext));
  }

  @Test
  public void testAbortJobNoWorkDone() throws Exception {
    executeWork("abort task no work",
        (job, jContext, tContext, committer) ->
            committer.abortJob(jContext, JobStatus.State.RUNNING));
  }

  @Test
  public void testCommitJobButNotTask() throws Exception {
    executeWork("commit a job while a task's work is pending, " +
            "expect task writes to be cancelled.",
        (job, jContext, tContext, committer) -> {
          // step 1: write the text
          writeTextOutput(tContext);
          // step 2: commit the job
          createCommitter(tContext).commitJob(tContext);
          // verify that no output can be observed
          assertPart0000DoesNotExist(outputDir);
        }
    );
  }

  @Test
  public void testAbortTaskThenJob() throws Exception {
    JobData jobData = startJob(true);
    ManifestCommitter committer = jobData.committer;

    // do abort
    committer.abortTask(jobData.tContext);

    intercept(FileNotFoundException.class, "",
        () -> getPart0000(committer.getWorkPath()));

    committer.abortJob(jobData.jContext, JobStatus.State.FAILED);
    assertJobAbortCleanedUp(jobData);

  }

  /**
   * Extension point: assert that the job was all cleaned up after an abort.
   * Base assertions
   * <ul>
   *   <li>Output dir is absent or, if present, empty</li>
   * </ul>
   * @param jobData job data
   * @throws Exception failure
   */
  public void assertJobAbortCleanedUp(JobData jobData) throws Exception {
    FileSystem fs = getFileSystem();
    try {
      FileStatus[] children = listChildren(fs, outputDir);
      if (children.length != 0) {
        lsR(fs, outputDir, true);
      }
      Assertions.assertThat(children)
          .as("Output directory not empty " + ls(outputDir))
          .containsExactly(new FileStatus[0]);
    } catch (FileNotFoundException e) {
      // this is a valid state; it means the dest dir doesn't exist yet.
    }

  }

  @Test
  public void testFailAbort() throws Exception {
    describe("Abort the task, then job (failed), abort the job again");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    ManifestCommitter committer = jobData.committer;

    // do abort
    committer.abortTask(tContext);

    committer.getJobAttemptPath(jContext);
    committer.getTaskAttemptPath(tContext);
    assertPart0000DoesNotExist(outputDir);
    assertSuccessMarkerDoesNotExist(outputDir);
    describe("Aborting job into %s", outputDir);

    committer.abortJob(jContext, JobStatus.State.FAILED);

    assertTaskAttemptPathDoesNotExist(committer, tContext);
    assertJobAttemptPathDoesNotExist(committer, jContext);

    // verify a failure report
    ManifestSuccessData report = loadReport(jobData.jobId(), false);
    Map<String, String> diag = report.getDiagnostics();
    Assertions.assertThat(diag)
        .describedAs("Stage entry in report")
        .containsEntry(STAGE, OP_STAGE_JOB_ABORT);
    IOStatisticsSnapshot reportStats = report.getIOStatistics();
    verifyStatisticCounterValue(reportStats,
        OP_STAGE_JOB_ABORT, 1);

    // try again; expect abort to be idempotent.
    committer.abortJob(jContext, JobStatus.State.FAILED);

  }

  /**
   * Assert that the given dir does not have the {@code _SUCCESS} marker.
   * @param dir dir to scan
   * @throws IOException IO Failure
   */
  protected void assertSuccessMarkerDoesNotExist(Path dir) throws IOException {
    assertPathDoesNotExist("Success marker",
        new Path(dir, SUCCESS_MARKER));
  }

  public void assertPart0000DoesNotExist(Path dir) throws Exception {
    intercept(FileNotFoundException.class,
        () -> getPart0000(dir));
    assertPathDoesNotExist("expected output file", new Path(dir, PART_00000));
  }

  @Test
  public void testAbortJobNotTask() throws Exception {
    executeWork("abort task no work",
        (job, jContext, tContext, committer) -> {
          // write output
          writeTextOutput(tContext);
          committer.abortJob(jContext, JobStatus.State.RUNNING);
          assertTaskAttemptPathDoesNotExist(
              committer, tContext);
          assertJobAttemptPathDoesNotExist(
              committer, jContext);
        });
  }

  /**
   * This looks at what happens with concurrent commits.
   * However, the failure condition it looks for (subdir under subdir)
   * is the kind of failure you see on a rename-based commit.
   *
   * What it will not detect is the fact that both tasks will each commit
   * to the destination directory. That is: whichever commits last wins.
   *
   * There's no way to stop this. Instead it is a requirement that the task
   * commit operation is only executed when the committer is happy to
   * commit only those tasks which it knows have succeeded, and abort those
   * which have not.
   * @throws Exception failure
   */
  @Test
  public void testConcurrentCommitTaskWithSubDir() throws Exception {
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outputDir);
    final Configuration conf = job.getConfiguration();

    final JobContext jContext =
        new JobContextImpl(conf, taskAttempt0.getJobID());
    ManifestCommitter amCommitter = createCommitter(
        new TaskAttemptContextImpl(conf, taskAttempt0));
    amCommitter.setupJob(jContext);

    final TaskAttemptContext[] taCtx = new TaskAttemptContextImpl[2];
    taCtx[0] = new TaskAttemptContextImpl(conf, taskAttempt0);
    taCtx[1] = new TaskAttemptContextImpl(conf, taskAttempt1);

    // IDE/checkstyle complain here about type casting but they
    // are confused.
    final TextOutputFormat<Writable, Object>[] tof =
        new TextOutputForTests[2];

    for (int i = 0; i < tof.length; i++) {
      tof[i] = new TextOutputForTests<Writable, Object>() {
        @Override
        public Path getDefaultWorkFile(
            TaskAttemptContext context,
            String extension) throws IOException {
          final ManifestCommitter foc = (ManifestCommitter)
              getOutputCommitter(context);
          return new Path(new Path(foc.getWorkPath(), SUB_DIR),
              getUniqueFile(context, getOutputName(context), extension));
        }
      };
    }

    final ExecutorService executor = HadoopExecutors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < taCtx.length; i++) {
        final int taskIdx = i;
        executor.submit(() -> {
          final OutputCommitter outputCommitter =
              tof[taskIdx].getOutputCommitter(taCtx[taskIdx]);
          outputCommitter.setupTask(taCtx[taskIdx]);
          writeOutput(tof[taskIdx].getRecordWriter(taCtx[taskIdx]), taCtx[taskIdx]);
          describe("Committing Task %d", taskIdx);
          outputCommitter.commitTask(taCtx[taskIdx]);
          return null;
        });
      }
    } finally {
      executor.shutdown();
      while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        log().info("Awaiting thread termination!");
      }
    }

    // if we commit here then all tasks will be committed, so there will
    // be contention for that final directory: both parts will go in.

    describe("\nCommitting Job");
    amCommitter.commitJob(jContext);
    assertPathExists("base output directory", outputDir);
    assertPart0000DoesNotExist(outputDir);
    Path outSubDir = new Path(outputDir, SUB_DIR);
    assertPathDoesNotExist("Must not end up with sub_dir/sub_dir",
        new Path(outSubDir, SUB_DIR));

    // validate output
    // There's no success marker in the subdirectory
    validateContent(outSubDir, false, "");
  }

  @Test
  public void testUnsupportedSchema() throws Throwable {
    intercept(PathIOException.class, () ->
        new ManifestCommitterFactory()
            .createOutputCommitter(new Path("s3a://unsupported/"), null));
  }

  /**
   * Factory for failing committers.
   */


/*
  protected ManifestCommitter createFailingCommitter(
  final TaskAttemptContext tContext)
      throws IOException {
    //     TODO
    return null;
  }

  public class FailingCommitterFactory implements CommitterFactory {

    @Override
    public ManifestCommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return createFailingCommitter(context);
    }
  }*/
  @Test
  public void testOutputFormatIntegration() throws Throwable {
    Configuration conf = getConfiguration();
    Job job = newJob();
    assertCommitterFactoryIsManifestCommitter(job, outputDir);
    job.setOutputFormatClass(TextOutputForTests.class);
    conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, taskAttempt0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    TextOutputForTests<IntWritable, IntWritable> outputFormat =
        (TextOutputForTests<IntWritable, IntWritable>)
            ReflectionUtils.newInstance(tContext.getOutputFormatClass(), conf);
    ManifestCommitter committer = (ManifestCommitter)
        outputFormat.getOutputCommitter(tContext);

    // check path capabilities directly
    Assertions.assertThat(committer.hasCapability(
            ManifestCommitterConstants.CAPABILITY_DYNAMIC_PARTITIONING))
        .describedAs("dynamic partitioning capability in committer %s",
            committer)
        .isTrue();
    // and through a binding committer -passthrough is critical
    // for the spark binding.
    BindingPathOutputCommitter bindingCommitter =
        new BindingPathOutputCommitter(outputDir, tContext);
    Assertions.assertThat(bindingCommitter.hasCapability(
            ManifestCommitterConstants.CAPABILITY_DYNAMIC_PARTITIONING))
        .describedAs("dynamic partitioning capability in committer %s",
            bindingCommitter)
        .isTrue();


    // setup
    JobData jobData = new JobData(job, jContext, tContext, committer);
    setupJob(jobData);
    abortInTeardown(jobData);
    TextOutputForTests.LoggingLineRecordWriter<IntWritable, IntWritable> recordWriter
        = outputFormat.getRecordWriter(tContext);
    IntWritable iw = new IntWritable(1);
    recordWriter.write(iw, iw);
    long expectedLength = 4;
    Path dest = recordWriter.getDest();
    validateTaskAttemptPathDuringWrite(dest, expectedLength);
    recordWriter.close(tContext);
    // at this point
    validateTaskAttemptPathAfterWrite(dest, expectedLength);
    Assertions.assertThat(committer.needsTaskCommit(tContext))
        .as("Committer does not have data to commit " + committer)
        .isTrue();
    commitTask(committer, tContext);
    // at this point the committer tasks stats should be current.
    IOStatisticsSnapshot snapshot = new IOStatisticsSnapshot(
        committer.getIOStatistics());
    String commitsCompleted = COMMITTER_TASKS_COMPLETED_COUNT;
    LOG.info("after task commit {}", ioStatisticsToPrettyString(snapshot));
    verifyStatisticCounterValue(snapshot,
        commitsCompleted, 1);
    final TaskManifest manifest = loadManifest(
        committer.getTaskManifestPath(tContext));
    LOG.info("Manifest {}", manifest.toJson());

    commitJob(committer, jContext);
    LOG.info("committer iostatistics {}",
        ioStatisticsSourceToString(committer));

    // validate output
    ManifestSuccessData successData = verifySuccessMarker(outputDir,
        committer.getJobUniqueId());

    // the task commit count should get through the job commit
    IOStatisticsSnapshot successStats = successData.getIOStatistics();
    LOG.info("loaded statistics {}", successStats);
    verifyStatisticCounterValue(successStats,
        commitsCompleted, 1);
  }

  /**
   * Create a committer through reflection then use it to abort
   * a task. This mimics the action of an AM when a container fails and
   * the AM wants to abort the task attempt.
   */
  @Test
  public void testAMWorkflow() throws Throwable {
    describe("Create a committer with a null output path & use as an AM");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;

    TaskAttemptContext newAttempt = new TaskAttemptContextImpl(
        jContext.getConfiguration(),
        taskAttempt0);
    Configuration conf = jContext.getConfiguration();

    // bind
    TextOutputForTests.bind(conf);

    OutputFormat<?, ?> outputFormat
        = ReflectionUtils.newInstance(newAttempt.getOutputFormatClass(), conf);
    Path outputPath = FileOutputFormat.getOutputPath(newAttempt);
    Assertions.assertThat(outputPath)
        .as("null output path in new task attempt")
        .isNotNull();

    ManifestCommitter committer2 = (ManifestCommitter)
        outputFormat.getOutputCommitter(newAttempt);
    committer2.abortTask(tContext);

  }

  /**
   * Make sure that two jobs in parallel directory trees coexist.
   * Note: the two jobs are not trying to write to the same
   * output directory.
   * That should be possible, but cleanup must be disabled.
   */
  @Test
  public void testParallelJobsToAdjacentPaths() throws Throwable {

    describe("Run two jobs in parallel, assert they both complete");
    JobData jobData = startJob(true);
    Job job1 = jobData.job;
    ManifestCommitter committer1 = jobData.committer;
    JobContext jContext1 = jobData.jContext;
    TaskAttemptContext tContext1 = jobData.tContext;

    // now build up a second job
    String jobId2 = randomJobId();
    String attempt20 = "attempt_" + jobId2 + "_m_000000_0";
    TaskAttemptID taskAttempt20 = TaskAttemptID.forName(attempt20);
    String attempt21 = "attempt_" + jobId2 + "_m_000001_0";
    TaskAttemptID taskAttempt21 = TaskAttemptID.forName(attempt21);

    Path job1Dest = outputDir;
    Path job2Dest = new Path(getOutputDir().getParent(),
        getMethodName() + "job2Dest");
    // little safety check
    Assertions.assertThat(job2Dest)
        .describedAs("Job destinations")
        .isNotEqualTo(job1Dest);

    // create the second job
    Job job2 = newJob(job2Dest,
        unsetUUIDOptions(new JobConf(getConfiguration())),
        attempt20);
    Configuration conf2 = job2.getConfiguration();
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    ManifestCommitter committer2 = null;
    try {
      JobContext jContext2 = new JobContextImpl(conf2,
          taskAttempt20.getJobID());
      TaskAttemptContext tContext2 =
          new TaskAttemptContextImpl(conf2, taskAttempt20);
      committer2 = createCommitter(job2Dest, tContext2);
      JobData jobData2 = new JobData(job2, jContext2, tContext2, committer2);
      setupJob(jobData2);
      abortInTeardown(jobData2);
      // make sure the directories are different
      Assertions.assertThat(committer1.getOutputPath())
          .describedAs("Committer output path of %s and %s", committer1, committer2)
          .isNotEqualTo(committer2.getOutputPath());
      // and job IDs
      Assertions.assertThat(committer1.getJobUniqueId())
          .describedAs("JobUnique IDs of %s and %s", committer1, committer2)
          .isNotEqualTo(committer2.getJobUniqueId());

      // job2 setup, write some data there
      writeTextOutput(tContext2);

      // at this point, job1 and job2 both have uncommitted tasks

      // commit tasks in order task 2, task 1.
      commitTask(committer2, tContext2);
      commitTask(committer1, tContext1);

      // commit jobs in order job 1, job 2
      commitJob(committer1, jContext1);

      getPart0000(job1Dest);

      commitJob(committer2, jContext2);
      getPart0000(job2Dest);

    } finally {
      // clean things up in test failures.
      FileSystem fs = getFileSystem();
      if (committer1 != null) {
        fs.delete(committer1.getOutputPath(), true);
      }
      if (committer2 != null) {
        fs.delete(committer2.getOutputPath(), true);
      }
    }

  }

  /**
   * Strip staging/spark UUID options.
   * @param conf config
   * @return the patched config
   */
  protected Configuration unsetUUIDOptions(final Configuration conf) {
    conf.unset(SPARK_WRITE_UUID);
    return conf;
  }

  /**
   * Assert that a committer's job attempt path exists.
   * For the staging committers, this is in the cluster FS.
   * @param committer committer
   * @param jobContext job context
   * @throws IOException failure
   */
  protected void assertJobAttemptPathExists(
      final ManifestCommitter committer,
      final JobContext jobContext) throws IOException {
    Path attemptPath = committer.getJobAttemptPath(jobContext);
    ContractTestUtils.assertIsDirectory(
        attemptPath.getFileSystem(committer.getConf()),
        attemptPath);
  }

  /**
   * Validate the path of a file being written to during the write
   * itself.
   * @param p path
   * @param expectedLength
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptPathDuringWrite(Path p,
      final long expectedLength) throws IOException {

  }

  /**
   * Validate the path of a file being written to after the write
   * operation has completed.
   * @param p path
   * @param expectedLength
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptPathAfterWrite(Path p,
      final long expectedLength) throws IOException {

  }

  /**
   * Perform any actions needed to validate the working directory of
   * a committer.
   * For example: filesystem, path attributes
   * @param committer committer instance
   * @param context task attempt context
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptWorkingDirectory(
      ManifestCommitter committer,
      TaskAttemptContext context) throws IOException {
  }

  /**
   * Commit a task then validate the state of the committer afterwards.
   * @param committer committer
   * @param tContext task context
   * @throws IOException IO failure
   */
  protected void commitTask(final ManifestCommitter committer,
      final TaskAttemptContext tContext) throws IOException {
    committer.commitTask(tContext);
  }

  /**
   * Commit a job then validate the state of the committer afterwards.
   * @param committer committer
   * @param jContext job context
   * @throws IOException IO failure
   */
  protected void commitJob(final ManifestCommitter committer,
      final JobContext jContext) throws IOException {
    committer.commitJob(jContext);

  }

}
