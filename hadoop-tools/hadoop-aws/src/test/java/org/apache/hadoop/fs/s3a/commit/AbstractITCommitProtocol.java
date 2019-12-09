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

package org.apache.hadoop.fs.s3a.commit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test the job/task commit actions of an S3A Committer, including trying to
 * simulate some failure and retry conditions.
 * Derived from
 * {@code org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter}.
 *
 * This is a complex test suite as it tries to explore the full lifecycle
 * of committers, and is designed for subclassing.
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown", "unused"})
public abstract class AbstractITCommitProtocol extends AbstractCommitITest {
  private Path outDir;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractITCommitProtocol.class);

  private static final String SUB_DIR = "SUB_DIR";

  protected static final String PART_00000 = "part-m-00000";

  /**
   * Counter to guarantee that even in parallel test runs, no job has the same
   * ID.
   */

  private String jobId;

  // A random task attempt id for testing.
  private String attempt0;
  private TaskAttemptID taskAttempt0;

  private String attempt1;
  private TaskAttemptID taskAttempt1;

  private static final Text KEY_1 = new Text("key1");
  private static final Text KEY_2 = new Text("key2");
  private static final Text VAL_1 = new Text("val1");
  private static final Text VAL_2 = new Text("val2");

  /** A job to abort in test case teardown. */
  private List<JobData> abortInTeardown = new ArrayList<>(1);

  private final StandardCommitterFactory
      standardCommitterFactory = new StandardCommitterFactory();

  private void cleanupDestDir() throws IOException {
    rmdir(this.outDir, getConfiguration());
  }

  /**
   * This must return the name of a suite which is unique to the non-abstract
   * test.
   * @return a string which must be unique and a valid path.
   */
  protected abstract String suitename();

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
    jobId = randomJobId();
    attempt0 = "attempt_" + jobId + "_m_000000_0";
    taskAttempt0 = TaskAttemptID.forName(attempt0);
    attempt1 = "attempt_" + jobId + "_m_000001_0";
    taskAttempt1 = TaskAttemptID.forName(attempt1);

    outDir = path(getMethodName());
    S3AFileSystem fileSystem = getFileSystem();
    bindFileSystem(fileSystem, outDir, fileSystem.getConf());
    abortMultipartUploadsUnderPath(outDir);
    cleanupDestDir();
  }

  @Override
  public void teardown() throws Exception {
    describe("teardown");
    abortInTeardown.forEach(this::abortJobQuietly);
    if (outDir != null) {
      try {
        abortMultipartUploadsUnderPath(outDir);
        cleanupDestDir();
      } catch (IOException e) {
        log().info("Exception during cleanup", e);
      }
    }
    S3AFileSystem fileSystem = getFileSystem();
    if (fileSystem != null) {
      log().info("Statistics for {}:\n{}", fileSystem.getUri(),
          fileSystem.getInstrumentation().dump("  ", " =  ", "\n", true));
    }

    super.teardown();
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
    disableFilesystemCaching(conf);
    bindCommitter(conf);
    return conf;
  }

  /**
   * Bind a path to the FS in the cache.
   * @param fs filesystem
   * @param path s3 path
   * @param conf configuration
   * @throws IOException any problem
   */
  private void bindFileSystem(FileSystem fs, Path path, Configuration conf)
      throws IOException {
    FileSystemTestHelper.addFileSystemForTesting(path.toUri(), conf, fs);
  }

  /***
   * Bind to the committer from the methods of
   * {@link #getCommitterFactoryName()} and {@link #getCommitterName()}.
   * @param conf configuration to set up
   */
  protected void bindCommitter(Configuration conf) {
    super.bindCommitter(conf, getCommitterFactoryName(), getCommitterName());
  }

  /**
   * Create a committer for a task.
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */
  protected AbstractS3ACommitter createCommitter(
      TaskAttemptContext context) throws IOException {
    return createCommitter(getOutDir(), context);
  }

  /**
   * Create a committer for a task and a given output path.
   * @param outputPath path
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */
  protected abstract AbstractS3ACommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException;


  protected String getCommitterFactoryName() {
    return CommitConstants.S3A_COMMITTER_FACTORY;
  }

  protected abstract String getCommitterName();

  protected Path getOutDir() {
    return outDir;
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
    AbstractS3ACommitter createCommitter(
        TaskAttemptContext context) throws IOException;
  }

  /**
   * The normal committer creation factory, uses the abstract methods
   * in the class.
   */
  public class StandardCommitterFactory implements CommitterFactory {
    @Override
    public AbstractS3ACommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return AbstractITCommitProtocol.this.createCommitter(context);
    }
  }

  /**
   * Write some text out.
   * @param context task
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  protected void writeTextOutput(TaskAttemptContext context)
      throws IOException, InterruptedException {
    describe("write output");
    try (DurationInfo d = new DurationInfo(LOG,
        "Writing Text output for task %s", context.getTaskAttemptID())) {
      writeOutput(new LoggingTextOutputFormat().getRecordWriter(context),
          context);
    }
  }

  /**
   * Write the standard output.
   * @param writer record writer
   * @param context task context
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeOutput(RecordWriter writer,
      TaskAttemptContext context) throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    try(CloseWriter cw = new CloseWriter(writer, context)) {
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
  private void writeMapFileOutput(RecordWriter writer,
      TaskAttemptContext context) throws IOException, InterruptedException {
    describe("\nWrite map output");
    try (DurationInfo d = new DurationInfo(LOG,
        "Writing Text output for task %s", context.getTaskAttemptID());
         CloseWriter cw = new CloseWriter(writer, context)) {
      for (int i = 0; i < 10; ++i) {
        Text val = ((i & 1) == 1) ? VAL_1 : VAL_2;
        writer.write(new LongWritable(i), val);
      }
      writer.close(context);
    }
  }

  /**
   * Details on a job for use in {@code startJob} and elsewhere.
   */
  public static class JobData {
    private final Job job;
    private final JobContext jContext;
    private final TaskAttemptContext tContext;
    private final AbstractS3ACommitter committer;
    private final Configuration conf;

    public JobData(Job job,
        JobContext jContext,
        TaskAttemptContext tContext,
        AbstractS3ACommitter committer) {
      this.job = job;
      this.jContext = jContext;
      this.tContext = tContext;
      this.committer = committer;
      conf = job.getConfiguration();
    }
  }

  /**
   * Create a new job. Sets the task attempt ID,
   * and output dir; asks for a success marker.
   * @return the new job
   * @throws IOException failure
   */
  public Job newJob() throws IOException {
    return newJob(outDir, getConfiguration(), attempt0);
  }

  /**
   * Create a new job. Sets the task attempt ID,
   * and output dir; asks for a success marker.
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
    conf.setBoolean(CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
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
    return startJob(standardCommitterFactory, writeText);
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
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, taskAttempt0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    AbstractS3ACommitter committer = factory.createCommitter(tContext);

    // setup
    JobData jobData = new JobData(job, jContext, tContext, committer);
    setup(jobData);
    abortInTeardown(jobData);

    if (writeText) {
      // write output
      writeTextOutput(tContext);
    }
    return jobData;
  }

  /**
   * Set up the job and task.
   * @param jobData job data
   * @throws IOException problems
   */
  protected void setup(JobData jobData) throws IOException {
    AbstractS3ACommitter committer = jobData.committer;
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    describe("\nsetup job");
    try (DurationInfo d = new DurationInfo(LOG,
        "setup job %s", jContext.getJobID())) {
      committer.setupJob(jContext);
    }
    try (DurationInfo d = new DurationInfo(LOG,
        "setup task %s", tContext.getTaskAttemptID())) {
      committer.setupTask(tContext);
    }
    describe("setup complete\n");
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
  protected void abortJobQuietly(AbstractS3ACommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) {
    describe("\naborting task");
    try {
      committer.abortTask(tContext);
    } catch (IOException e) {
      log().warn("Exception aborting task:", e);
    }
    describe("\naborting job");
    try {
      committer.abortJob(jContext, JobStatus.State.KILLED);
    } catch (IOException e) {
      log().warn("Exception aborting job", e);
    }
  }

  /**
   * Commit up the task and then the job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   * @throws IOException problems
   */
  protected void commit(AbstractS3ACommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "committing work", jContext.getJobID())) {
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
   * Verify that recovery doesn't work for these committers.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testRecoveryAndCleanup() throws Exception {
    describe("Test (unsupported) task recovery.");
    JobData jobData = startJob(true);
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    assertNotNull("null workPath in committer " + committer,
        committer.getWorkPath());
    assertNotNull("null outputPath in committer " + committer,
        committer.getOutputPath());

    // Commit the task. This will promote data and metadata to where
    // job commits will pick it up on commit or abort.
    committer.commitTask(tContext);
    assertTaskAttemptPathDoesNotExist(committer, tContext);

    Configuration conf2 = jobData.job.getConfiguration();
    conf2.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
    JobContext jContext2 = new JobContextImpl(conf2, taskAttempt0.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2,
        taskAttempt0);
    AbstractS3ACommitter committer2 = createCommitter(tContext2);
    committer2.setupJob(tContext2);

    assertFalse("recoverySupported in " + committer2,
        committer2.isRecoverySupported());
    intercept(PathCommitException.class, "recover",
        () -> committer2.recoverTask(tContext2));

    // at this point, task attempt 0 has failed to recover
    // it should be abortable though. This will be a no-op as it already
    // committed
    describe("aborting task attempt 2; expect nothing to clean up");
    committer2.abortTask(tContext2);
    describe("Aborting job 2; expect pending commits to be aborted");
    committer2.abortJob(jContext2, JobStatus.State.KILLED);
    // now, state of system may still have pending data
    assertNoMultipartUploadsPending(outDir);
  }

  protected void assertTaskAttemptPathDoesNotExist(
      AbstractS3ACommitter committer, TaskAttemptContext context)
      throws IOException {
    Path attemptPath = committer.getTaskAttemptPath(context);
    ContractTestUtils.assertPathDoesNotExist(
        attemptPath.getFileSystem(context.getConfiguration()),
        "task attempt dir",
        attemptPath);
  }

  protected void assertJobAttemptPathDoesNotExist(
      AbstractS3ACommitter committer, JobContext context)
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
   * @throws Exception failure.
   */
  private void validateContent(Path dir, boolean expectSuccessMarker)
      throws Exception {
    if (expectSuccessMarker) {
      verifySuccessMarker(dir);
    }
    Path expectedFile = getPart0000(dir);
    log().debug("Validating content in {}", expectedFile);
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(KEY_1).append('\t').append(VAL_1).append("\n");
    expectedOutput.append(VAL_1).append("\n");
    expectedOutput.append(VAL_2).append("\n");
    expectedOutput.append(KEY_2).append("\n");
    expectedOutput.append(KEY_1).append("\n");
    expectedOutput.append(KEY_2).append('\t').append(VAL_2).append("\n");
    String output = readFile(expectedFile);
    assertEquals("Content of " + expectedFile,
        expectedOutput.toString(), output);
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
    return eventually(CONSISTENCY_WAIT, CONSISTENCY_PROBE_INTERVAL,
        () -> getPart0000Immediately(fs, dir));
  }

  /**
   * Identify any path under the directory which begins with the
   * {@code "part-m-00000"} sequence. There's some compensation for
   * eventual consistency here.
   * @param fs FS to probe
   * @param dir directory to scan
   * @return the full path
   * @throws FileNotFoundException the path is missing.
   * @throws IOException failure.
   */
  private Path getPart0000Immediately(FileSystem fs, Path dir)
      throws IOException {
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
   * Look for the partFile subdir of the output dir.
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
    assertTrue("No files found in " + expectedMapDir, files.length > 0);
    assertPathExists("index file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.INDEX_FILE_NAME));
    assertPathExists("data file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.DATA_FILE_NAME));
  }

  /**
   * Dump all MPUs in the filesystem.
   * @throws IOException IO failure
   */
  protected void dumpMultipartUploads() throws IOException {
    countMultipartUploads("");
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
        "* the expected file is visible after job commit\n" +
        "* no outstanding MPUs after job commit");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;
    validateTaskAttemptWorkingDirectory(committer, tContext);

    // write output
    describe("1. Writing output");
    writeTextOutput(tContext);

    dumpMultipartUploads();
    describe("2. Committing task");
    assertTrue("No files to commit were found by " + committer,
        committer.needsTaskCommit(tContext));
    committer.commitTask(tContext);

    // this is only task commit; there MUST be no part- files in the dest dir
    waitForConsistency();
    try {
      applyLocatedFiles(getFileSystem().listFiles(outDir, false),
          (status) ->
              assertFalse("task committed file to dest :" + status,
                  status.getPath().toString().contains("part")));
    } catch (FileNotFoundException ignored) {
      log().info("Outdir {} is not created by task commit phase ",
          outDir);
    }

    describe("3. Committing job");
    assertMultipartUploadsPending(outDir);
    committer.commitJob(jContext);

    // validate output
    describe("4. Validating content");
    validateContent(outDir, shouldExpectSuccessMarker());
    assertNoMultipartUploadsPending(outDir);
  }

  @Test
  public void testCommitterWithDuplicatedCommit() throws Exception {
    describe("Call a task then job commit twice;" +
        "expect the second task commit to fail.");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do commit
    commit(committer, jContext, tContext);

    // validate output
    validateContent(outDir, shouldExpectSuccessMarker());

    assertNoMultipartUploadsPending(outDir);

    // commit task to fail on retry
    expectFNFEonTaskCommit(committer, tContext);
  }

  protected boolean shouldExpectSuccessMarker() {
    return true;
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  @Test
  public void testCommitterWithFailure() throws Exception {
    describe("Fail the first job commit then retry");
    JobData jobData = startJob(new FailingCommitterFactory(), true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);

    // now fail job
    expectSimulatedFailureOnJobCommit(jContext, committer);

    committer.commitJob(jContext);

    // but the data got there, due to the order of operations.
    validateContent(outDir, shouldExpectSuccessMarker());
    expectJobCommitToFail(jContext, committer);
  }

  /**
   * Override point: the failure expected on the attempt to commit a failed
   * job.
   * @param jContext job context
   * @param committer committer
   * @throws Exception any unexpected failure.
   */
  protected void expectJobCommitToFail(JobContext jContext,
      AbstractS3ACommitter committer) throws Exception {
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
      AbstractS3ACommitter committer,
      Class<E> clazz)
      throws Exception {

    return intercept(clazz,
        () -> {
          committer.commitJob(jContext);
          return committer.toString();
        });
  }

  protected static void expectFNFEonTaskCommit(
      AbstractS3ACommitter committer,
      TaskAttemptContext tContext) throws Exception {
    intercept(FileNotFoundException.class,
        () -> {
          committer.commitTask(tContext);
          return committer.toString();
        });
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  @Test
  public void testCommitterWithNoOutputs() throws Exception {
    describe("Have a task and job with no outputs: expect success");
    JobData jobData = startJob(new FailingCommitterFactory(), false);
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);
    assertTaskAttemptPathDoesNotExist(committer, tContext);
  }

  protected static void expectSimulatedFailureOnJobCommit(JobContext jContext,
      AbstractS3ACommitter committer) throws Exception {
    ((CommitterFaultInjection) committer).setFaults(
        CommitterFaultInjection.Faults.commitJob);
    expectJobCommitFailure(jContext, committer,
        CommitterFaultInjectionImpl.Failure.class);
  }

  @Test
  public void testMapFileOutputCommitter() throws Exception {
    describe("Test that the committer generates map output into a directory\n" +
        "starting with the prefix part-");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;
    Configuration conf = jobData.conf;

    // write output
    writeMapFileOutput(new MapFileOutputFormat().getRecordWriter(tContext),
        tContext);

    // do commit
    commit(committer, jContext, tContext);
    S3AFileSystem fs = getFileSystem();
    waitForConsistency();
    lsR(fs, outDir, true);
    String ls = ls(outDir);
    describe("\nvalidating");

    // validate output
    verifySuccessMarker(outDir);

    describe("validate output of %s", outDir);
    validateMapFileOutputContent(fs, outDir);

    // Ensure getReaders call works and also ignores
    // hidden filenames (_ or . prefixes)
    describe("listing");
    FileStatus[] filtered = fs.listStatus(outDir, HIDDEN_FILE_FILTER);
    assertEquals("listed children under " + ls,
        1, filtered.length);
    FileStatus fileStatus = filtered[0];
    assertTrue("Not the part file: " + fileStatus,
        fileStatus.getPath().getName().startsWith(PART_00000));

    describe("getReaders()");
    assertEquals("Number of MapFile.Reader entries with shared FS "
            + outDir + " : " + ls,
        1, getReaders(fs, outDir, conf).length);

    describe("getReaders(new FS)");
    FileSystem fs2 = FileSystem.get(outDir.toUri(), conf);
    assertEquals("Number of MapFile.Reader entries with shared FS2 "
            + outDir + " : " + ls,
        1, getReaders(fs2, outDir, conf).length);

    describe("MapFileOutputFormat.getReaders");
    assertEquals("Number of MapFile.Reader entries with new FS in "
            + outDir + " : " + ls,
        1, MapFileOutputFormat.getReaders(outDir, conf).length);
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

  /**
   * A functional interface which an action to test must implement.
   */
  @FunctionalInterface
  public interface ActionToTest {
    void exec(Job job, JobContext jContext, TaskAttemptContext tContext,
        AbstractS3ACommitter committer) throws Exception;
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
          assertPart0000DoesNotExist(outDir);
          // that includes, no pending MPUs; commitJob is expected to
          // cancel any.
          assertNoMultipartUploadsPending(outDir);
        }
    );
  }

  @Test
  public void testAbortTaskThenJob() throws Exception {
    JobData jobData = startJob(true);
    AbstractS3ACommitter committer = jobData.committer;

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
   *   <li>No pending MPUs to/under the output dir</li>
   * </ul>
   * @param jobData job data
   * @throws Exception failure
   */
  public void assertJobAbortCleanedUp(JobData jobData) throws Exception {
    // special handling of magic directory; harmless in staging
    S3AFileSystem fs = getFileSystem();
    try {
      FileStatus[] children = listChildren(fs, outDir);
      if (children.length != 0) {
        lsR(fs, outDir, true);
      }
      assertArrayEquals("Output directory not empty " + ls(outDir),
          new FileStatus[0], children);
    } catch (FileNotFoundException e) {
      // this is a valid failure mode; it means the dest dir doesn't exist yet.
    }
    assertNoMultipartUploadsPending(outDir);
  }

  @Test
  public void testFailAbort() throws Exception {
    describe("Abort the task, then job (failed), abort the job again");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do abort
    committer.abortTask(tContext);

    committer.getJobAttemptPath(jContext);
    committer.getTaskAttemptPath(tContext);
    assertPart0000DoesNotExist(outDir);
    assertSuccessMarkerDoesNotExist(outDir);
    describe("Aborting job into %s", outDir);

    committer.abortJob(jContext, JobStatus.State.FAILED);

    assertTaskAttemptPathDoesNotExist(committer, tContext);
    assertJobAttemptPathDoesNotExist(committer, jContext);

    // try again; expect abort to be idempotent.
    committer.abortJob(jContext, JobStatus.State.FAILED);
    assertNoMultipartUploadsPending(outDir);
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
          assertNoMultipartUploadsPending(outDir);
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
    FileOutputFormat.setOutputPath(job, outDir);
    final Configuration conf = job.getConfiguration();

    final JobContext jContext =
        new JobContextImpl(conf, taskAttempt0.getJobID());
    AbstractS3ACommitter amCommitter = createCommitter(
        new TaskAttemptContextImpl(conf, taskAttempt0));
    amCommitter.setupJob(jContext);

    final TaskAttemptContext[] taCtx = new TaskAttemptContextImpl[2];
    taCtx[0] = new TaskAttemptContextImpl(conf, taskAttempt0);
    taCtx[1] = new TaskAttemptContextImpl(conf, taskAttempt1);

    final TextOutputFormat[] tof = new LoggingTextOutputFormat[2];
    for (int i = 0; i < tof.length; i++) {
      tof[i] = new LoggingTextOutputFormat() {
        @Override
        public Path getDefaultWorkFile(
            TaskAttemptContext context,
            String extension) throws IOException {
          final AbstractS3ACommitter foc = (AbstractS3ACommitter)
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
          final RecordWriter rw =
              tof[taskIdx].getRecordWriter(taCtx[taskIdx]);
          writeOutput(rw, taCtx[taskIdx]);
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
    assertPathExists("base output directory", outDir);
    assertPart0000DoesNotExist(outDir);
    Path outSubDir = new Path(outDir, SUB_DIR);
    assertPathDoesNotExist("Must not end up with sub_dir/sub_dir",
        new Path(outSubDir, SUB_DIR));

    // validate output
    // There's no success marker in the subdirectory
    validateContent(outSubDir, false);
  }

  /**
   * Create a committer which fails; the class
   * {@link CommitterFaultInjectionImpl} implements the logic.
   * @param tContext task context
   * @return committer instance
   * @throws IOException failure to instantiate
   */
  protected abstract AbstractS3ACommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException;

  /**
   * Factory for failing committers.
   */
  public class FailingCommitterFactory implements CommitterFactory {
    @Override
    public AbstractS3ACommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return createFailingCommitter(context);
    }
  }

  @Test
  public void testOutputFormatIntegration() throws Throwable {
    Configuration conf = getConfiguration();
    Job job = newJob();
    job.setOutputFormatClass(LoggingTextOutputFormat.class);
    conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, taskAttempt0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    LoggingTextOutputFormat outputFormat = (LoggingTextOutputFormat)
        ReflectionUtils.newInstance(tContext.getOutputFormatClass(), conf);
    AbstractS3ACommitter committer = (AbstractS3ACommitter)
        outputFormat.getOutputCommitter(tContext);

    // setup
    JobData jobData = new JobData(job, jContext, tContext, committer);
    setup(jobData);
    abortInTeardown(jobData);
    LoggingTextOutputFormat.LoggingLineRecordWriter recordWriter
        = outputFormat.getRecordWriter(tContext);
    IntWritable iw = new IntWritable(1);
    recordWriter.write(iw, iw);
    Path dest = recordWriter.getDest();
    validateTaskAttemptPathDuringWrite(dest);
    recordWriter.close(tContext);
    // at this point
    validateTaskAttemptPathAfterWrite(dest);
    assertTrue("Committer does not have data to commit " + committer,
        committer.needsTaskCommit(tContext));
    committer.commitTask(tContext);
    committer.commitJob(jContext);
    // validate output
    verifySuccessMarker(outDir);
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

    TaskAttemptContext newAttempt = taskAttemptForJob(
        MRBuilderUtils.newJobId(1, 1, 1), jContext);
    Configuration conf = jContext.getConfiguration();

    // bind
    LoggingTextOutputFormat.bind(conf);

    OutputFormat<?, ?> outputFormat
        = ReflectionUtils.newInstance(newAttempt
        .getOutputFormatClass(), conf);
    Path outputPath = FileOutputFormat.getOutputPath(newAttempt);
    assertNotNull("null output path in new task attempt", outputPath);

    AbstractS3ACommitter committer2 = (AbstractS3ACommitter)
        outputFormat.getOutputCommitter(newAttempt);
    committer2.abortTask(tContext);
    assertNoMultipartUploadsPending(getOutDir());
  }


  @Test
  public void testParallelJobsToAdjacentPaths() throws Throwable {

    describe("Run two jobs in parallel, assert they both complete");
    JobData jobData = startJob(true);
    Job job1 = jobData.job;
    AbstractS3ACommitter committer1 = jobData.committer;
    JobContext jContext1 = jobData.jContext;
    TaskAttemptContext tContext1 = jobData.tContext;

    // now build up a second job
    String jobId2 = randomJobId();
    String attempt20 = "attempt_" + jobId2 + "_m_000000_0";
    TaskAttemptID taskAttempt20 = TaskAttemptID.forName(attempt20);
    String attempt21 = "attempt_" + jobId2 + "_m_000001_0";
    TaskAttemptID taskAttempt21 = TaskAttemptID.forName(attempt21);

    Path job1Dest = outDir;
    Path job2Dest = new Path(getOutDir().getParent(),
        getMethodName() + "job2Dest");
    // little safety check
    assertNotEquals(job1Dest, job2Dest);

    // create the second job
    Job job2 = newJob(job2Dest, new JobConf(getConfiguration()), attempt20);
    Configuration conf2 = job2.getConfiguration();
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    try {
      JobContext jContext2 = new JobContextImpl(conf2,
          taskAttempt20.getJobID());
      TaskAttemptContext tContext2 =
          new TaskAttemptContextImpl(conf2, taskAttempt20);
      AbstractS3ACommitter committer2 = createCommitter(job2Dest, tContext2);
      JobData jobData2 = new JobData(job2, jContext2, tContext2, committer2);
      setup(jobData2);
      abortInTeardown(jobData2);
      // make sure the directories are different
      assertEquals(job2Dest, committer2.getOutputPath());

      // job2 setup, write some data there
      writeTextOutput(tContext2);

      // at this point, job1 and job2 both have uncommitted tasks

      // commit tasks in order task 2, task 1.
      committer2.commitTask(tContext2);
      committer1.commitTask(tContext1);

      assertMultipartUploadsPending(job1Dest);
      assertMultipartUploadsPending(job2Dest);

      // commit jobs in order job 1, job 2
      committer1.commitJob(jContext1);
      assertNoMultipartUploadsPending(job1Dest);
      getPart0000(job1Dest);
      assertMultipartUploadsPending(job2Dest);

      committer2.commitJob(jContext2);
      getPart0000(job2Dest);
      assertNoMultipartUploadsPending(job2Dest);
    } finally {
      // uncommitted files to this path need to be deleted in tests which fail
      abortMultipartUploadsUnderPath(job2Dest);
    }

  }

  @Test
  public void testS3ACommitterFactoryBinding() throws Throwable {
    describe("Verify that the committer factory returns this "
        + "committer when configured to do so");
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    String name = getCommitterName();
    S3ACommitterFactory factory = new S3ACommitterFactory();
    assertEquals("Wrong committer from factory",
        createCommitter(outDir, tContext).getClass(),
        factory.createOutputCommitter(outDir, tContext).getClass());
  }

  /**
   * Validate the path of a file being written to during the write
   * itself.
   * @param p path
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptPathDuringWrite(Path p) throws IOException {

  }

  /**
   * Validate the path of a file being written to after the write
   * operation has completed.
   * @param p path
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptPathAfterWrite(Path p) throws IOException {

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
      AbstractS3ACommitter committer,
      TaskAttemptContext context) throws IOException {
  }

}
