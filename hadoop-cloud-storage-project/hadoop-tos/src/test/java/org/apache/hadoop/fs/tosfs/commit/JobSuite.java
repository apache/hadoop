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

package org.apache.hadoop.fs.tosfs.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class JobSuite extends BaseJobSuite {
  private static final CommitterFactory FACTORY = new CommitterFactory();
  private final JobContext jobContext;
  private final TaskAttemptContext taskAttemptContext;
  private final Committer committer;

  private JobSuite(FileSystem fs, Configuration conf, TaskAttemptID taskAttemptId, int appAttemptId,
      Path outputPath) throws IOException {
    setFs(fs);
    // Initialize the job instance.
    setJob(Job.getInstance(conf));
    job().setJobID(JobID.forName(CommitUtils.buildJobId(conf, taskAttemptId.getJobID())));
    this.jobContext = createJobContext(job().getConfiguration(), taskAttemptId);
    setJobId(CommitUtils.buildJobId(jobContext));
    this.taskAttemptContext =
        createTaskAttemptContext(job().getConfiguration(), taskAttemptId, appAttemptId);

    // Set job output directory.
    FileOutputFormat.setOutputPath(job(), outputPath);
    setOutputPath(outputPath);
    setObjectStorage(ObjectStorageFactory.create(outputPath.toUri().getScheme(),
        outputPath.toUri().getAuthority(), conf));

    // Initialize committer.
    this.committer = (Committer) FACTORY.createOutputCommitter(outputPath, taskAttemptContext);
  }

  public static JobSuite create(Configuration conf, TaskAttemptID taskAttemptId, Path outDir)
      throws IOException {
    FileSystem fs = outDir.getFileSystem(conf);
    return new JobSuite(fs, conf, taskAttemptId, DEFAULT_APP_ATTEMPT_ID, outDir);
  }

  public static TaskAttemptID createTaskAttemptId(String trimmedJobId, int attemptId) {
    String attempt = String.format("attempt_%s_m_000000_%d", trimmedJobId, attemptId);
    return TaskAttemptID.forName(attempt);
  }

  public static TaskAttemptID createTaskAttemptId(String trimmedJobId, int taskId, int attemptId) {
    String[] parts = trimmedJobId.split("_");
    return new TaskAttemptID(parts[0], Integer.parseInt(parts[1]), TaskType.MAP, taskId, attemptId);
  }

  public static JobContext createJobContext(Configuration jobConf, TaskAttemptID taskAttemptId) {
    return new JobContextImpl(jobConf, taskAttemptId.getJobID());
  }

  public static TaskAttemptContext createTaskAttemptContext(
      Configuration jobConf, TaskAttemptID taskAttemptId, int appAttemptId) throws IOException {
    // Set the key values for job configuration.
    jobConf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttemptId.toString());
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttemptId);
    jobConf.set(PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS,
        CommitterFactory.class.getName());
    return new TaskAttemptContextImpl(jobConf, taskAttemptId);
  }

  public void setupJob() throws IOException {
    committer.setupJob(jobContext);
  }

  public void setupTask() throws IOException {
    committer.setupTask(taskAttemptContext);
  }

  // This method simulates the scenario that the job may set up task with a different
  // taskAttemptContext, e.g., for a spark job.
  public void setupTask(TaskAttemptContext taskAttemptCxt) throws IOException {
    committer.setupTask(taskAttemptCxt);
  }

  public void writeOutput() throws Exception {
    writeOutput(taskAttemptContext);
  }

  // This method simulates the scenario that the job may set up task with a different
  // taskAttemptContext, e.g., for a spark job.
  public void writeOutput(TaskAttemptContext taskAttemptCxt) throws Exception {
    RecordWriter<Object, Object> writer = new TextOutputFormat<>().getRecordWriter(taskAttemptCxt);
    NullWritable nullKey = NullWritable.get();
    NullWritable nullVal = NullWritable.get();
    Object[] keys = new Object[]{KEY_1, nullKey, null, nullKey, null, KEY_2};
    Object[] vals = new Object[]{VAL_1, nullVal, null, null, nullVal, VAL_2};
    try {
      assertEquals(keys.length, vals.length);
      for (int i = 0; i < keys.length; i++) {
        writer.write(keys[i], vals[i]);
      }
    } finally {
      writer.close(taskAttemptCxt);
    }
  }

  public boolean needsTaskCommit() {
    return committer.needsTaskCommit(taskAttemptContext);
  }

  public void commitTask() throws IOException {
    committer.commitTask(taskAttemptContext);
  }

  // This method simulates the scenario that the job may set up task with a different
  // taskAttemptContext, e.g., for a spark job.
  public void commitTask(TaskAttemptContext taskAttemptCxt) throws IOException {
    committer.commitTask(taskAttemptCxt);
  }

  public void abortTask() throws IOException {
    committer.abortTask(taskAttemptContext);
  }

  public void commitJob() throws IOException {
    committer.commitJob(jobContext);
  }

  @Override
  public Path magicPartPath() {
    return new Path(committer.getWorkPath(),
        FileOutputFormat.getUniqueFile(taskAttemptContext, "part", ""));
  }

  @Override
  public Path magicPendingSetPath() {
    return CommitUtils.magicTaskPendingSetPath(taskAttemptContext, outputPath());
  }

  public TaskAttemptContext taskAttemptContext() {
    return taskAttemptContext;
  }

  public Committer committer() {
    return committer;
  }

  @Override
  public void assertNoTaskAttemptPath() throws IOException {
    Path path = CommitUtils.magicTaskAttemptBasePath(taskAttemptContext, outputPath());
    assertFalse(fs().exists(path), "Task attempt path should be not existing");
    String pathToKey = ObjectUtils.pathToKey(path);
    assertNull(storage().head(pathToKey), "Should have no task attempt path key");
  }

  @Override
  protected boolean skipTests() {
    return storage().bucket().isDirectory();
  }

  @Override
  public void assertSuccessMarker() throws IOException {
    Path succPath = CommitUtils.successMarker(outputPath());
    assertTrue(fs().exists(succPath), String.format("%s should be exists", succPath));
    SuccessData successData = SuccessData.deserialize(CommitUtils.load(fs(), succPath));
    assertEquals(SuccessData.class.getName(), successData.name());
    assertTrue(successData.success());
    assertEquals(NetUtils.getHostname(), successData.hostname());
    assertEquals(CommitUtils.COMMITTER_NAME, successData.committer());
    assertEquals(
        String.format("Task committer %s", taskAttemptContext.getTaskAttemptID()),
        successData.description());
    assertEquals(job().getJobID().toString(), successData.jobId());
    assertEquals(1, successData.filenames().size());
    assertEquals(destPartKey(), successData.filenames().get(0));
  }

  @Override
  public void assertSummaryReport(Path reportDir) throws IOException {
    Path reportPath = CommitUtils.summaryReport(reportDir, job().getJobID().toString());
    assertTrue(fs().exists(reportPath), String.format("%s should be exists", reportPath));
    SuccessData reportData = SuccessData.deserialize(CommitUtils.load(fs(), reportPath));
    assertEquals(SuccessData.class.getName(), reportData.name());
    assertTrue(reportData.success());
    assertEquals(NetUtils.getHostname(), reportData.hostname());
    assertEquals(CommitUtils.COMMITTER_NAME, reportData.committer());
    assertEquals(
        String.format("Task committer %s", taskAttemptContext.getTaskAttemptID()),
        reportData.description());
    assertEquals(job().getJobID().toString(), reportData.jobId());
    assertEquals(1, reportData.filenames().size());
    assertEquals(destPartKey(), reportData.filenames().get(0));
    assertEquals("clean", reportData.diagnostics().get("stage"));
  }
}
