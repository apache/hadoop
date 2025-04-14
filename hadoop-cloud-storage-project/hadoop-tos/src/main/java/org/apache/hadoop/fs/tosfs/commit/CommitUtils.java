/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.tosfs.commit.mapred.Committer;
import org.apache.hadoop.fs.tosfs.util.Serializer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public final class CommitUtils {
  private CommitUtils() {
  }

  public static final String COMMITTER_NAME = Committer.class.getName();

  /**
   * Support scheme for tos committer.
   */
  public static final String FS_STORAGE_OBJECT_SCHEME = "fs.object-storage.scheme";
  public static final String DEFAULT_FS_STORAGE_OBJECT_SCHEME = "tos,oss,s3,s3a,s3n,obs,filestore";

  /**
   * Path for "magic" writes: path and {@link #PENDING_SUFFIX} files: {@value}.
   */
  public static final String MAGIC = "__magic";

  /**
   * Marker of the start of a directory tree for calculating the final path names: {@value}.
   */
  public static final String BASE = "__base";

  /**
   * Suffix applied to pending commit metadata: {@value}.
   */
  public static final String PENDING_SUFFIX = ".pending";

  /**
   * Suffix applied to multiple pending commit metadata: {@value}.
   */
  public static final String PENDINGSET_SUFFIX = ".pendingset";

  /**
   * Marker file to create on success: {@value}.
   */
  public static final String SUCCESS = "_SUCCESS";

  /**
   * Format string used to build a summary file from a Job ID.
   */
  public static final String SUMMARY_FILENAME_FORMAT = "summary-%s.json";

  /**
   * Extra Data key for task attempt in pendingset files.
   */
  public static final String TASK_ATTEMPT_ID = "task.attempt.id";

  /**
   * The UUID for jobs: {@value}.
   * This was historically created in Spark 1.x's SQL queries, see SPARK-33230.
   */
  public static final String SPARK_WRITE_UUID = "spark.sql.sources.writeJobUUID";

  /**
   * Get the magic location for the output path.
   * Format: ${out}/__magic
   *
   * @param out the base output directory.
   * @return the location of magic job attempts.
   */
  public static Path magicPath(Path out) {
    return new Path(out, MAGIC);
  }

  /**
   * Compute the "magic" path for a job. <br>
   * Format: ${jobOutput}/__magic/${jobId}
   *
   * @param jobId     unique Job ID.
   * @param jobOutput the final output directory.
   * @return the path to store job attempt data.
   */
  public static Path magicJobPath(String jobId, Path jobOutput) {
    return new Path(magicPath(jobOutput), jobId);
  }

  /**
   * Get the Application Attempt ID for this job.
   *
   * @param context the context to look in
   * @return the Application Attempt ID for a given job, or 0
   */
  public static int appAttemptId(JobContext context) {
    return context.getConfiguration().getInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Compute the "magic" path for a job attempt. <br>
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}
   *
   * @param jobId        unique Job ID.
   * @param appAttemptId the ID of the application attempt for this job.
   * @param jobOutput    the final output directory.
   * @return the path to store job attempt data.
   */
  public static Path magicJobAttemptPath(String jobId, int appAttemptId, Path jobOutput) {
    return new Path(magicPath(jobOutput), formatAppAttemptDir(jobId, appAttemptId));
  }

  /**
   * Compute the "magic" path for a job attempt. <br>
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}
   *
   * @param context      the context of the job.
   * @param jobOutput    the final output directory.
   * @return the path to store job attempt data.
   */
  public static Path magicJobAttemptPath(JobContext context, Path jobOutput) {
    String jobId = buildJobId(context);
    return magicJobAttemptPath(jobId, appAttemptId(context), jobOutput);
  }

  private static String formatAppAttemptDir(String jobId, int appAttemptId) {
    return String.format("%s/%02d", jobId, appAttemptId);
  }

  /**
   * Compute the path where the output of magic task attempts are stored. <br>
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}/tasks
   *
   * @param jobId        unique Job ID.
   * @param jobOutput    The output path to commit work into.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path where the output of magic task attempts are stored.
   */
  public static Path magicTaskAttemptsPath(String jobId, Path jobOutput, int appAttemptId) {
    return new Path(magicJobAttemptPath(jobId, appAttemptId, jobOutput), "tasks");
  }

  /**
   * Compute the path where the output of a task attempt is stored until that task is committed.
   * This path is marked as a base path for relocations, so subdirectory information is preserved.
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}/tasks/${taskAttemptId}/__base
   *
   * @param context   the context of the task attempt.
   * @param jobId     unique Job ID.
   * @param jobOutput The output path to commit work into.
   * @return the path where a task attempt should be stored.
   */
  public static Path magicTaskAttemptBasePath(TaskAttemptContext context, String jobId,
      Path jobOutput) {
    return new Path(magicTaskAttemptPath(context, jobId, jobOutput), BASE);
  }

  /**
   * Compute the path where the output of a task attempt is stored until that task is committed.
   * This path is marked as a base path for relocations, so subdirectory information is preserved.
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}/tasks/${taskAttemptId}/__base
   *
   * @param context   the context of the task attempt.
   * @param jobOutput The output path to commit work into.
   * @return the path where a task attempt should be stored.
   */
  public static Path magicTaskAttemptBasePath(TaskAttemptContext context, Path jobOutput) {
    String jobId = buildJobId(context);
    return magicTaskAttemptBasePath(context, jobId, jobOutput);
  }

  /**
   * Get the magic task attempt path, without any annotations to mark relative references.
   * If there is an app attempt property in the context configuration, that is included.
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}/tasks/${taskAttemptId}
   *
   * @param context   the context of the task attempt.
   * @param jobId     unique Job ID.
   * @param jobOutput The output path to commit work into.
   * @return the path under which all attempts go.
   */
  public static Path magicTaskAttemptPath(TaskAttemptContext context, String jobId,
      Path jobOutput) {
    return new Path(magicTaskAttemptsPath(jobId, jobOutput, appAttemptId(context)),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Get the magic task attempt path, without any annotations to mark relative references.
   * If there is an app attempt property in the context configuration, that is included.
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}/tasks/${taskAttemptId}
   *
   * @param context   the context of the task attempt.
   * @param jobOutput The output path to commit work into.
   * @return the path under which all attempts go.
   */
  public static Path magicTaskAttemptPath(TaskAttemptContext context, Path jobOutput) {
    String jobId = buildJobId(context);
    return magicTaskAttemptPath(context, jobId, jobOutput);
  }

  /**
   * Get the magic task pendingset path.
   * Format: ${jobOutput}/__magic/${jobId}/${appAttemptId}/${taskId}.pendingset
   *
   * @param context   the context of the task attempt.
   * @param jobOutput The output path to commit work into.
   * @return the magic pending set path.
   */
  public static Path magicTaskPendingSetPath(TaskAttemptContext context, Path jobOutput) {
    String taskId = String.valueOf(context.getTaskAttemptID().getTaskID());
    return new Path(magicJobAttemptPath(context, jobOutput),
        String.format("%s%s", taskId, PENDINGSET_SUFFIX));
  }

  public static String buildJobId(Configuration conf, JobID jobId) {
    String jobUUID = conf.getTrimmed(SPARK_WRITE_UUID, "");
    if (!jobUUID.isEmpty()) {
      if (jobUUID.startsWith(JobID.JOB)) {
        return jobUUID;
      } else {
        return String.format("%s_%s", JobID.JOB, jobUUID);
      }
    }

    // if no other option was supplied, return the job ID.
    // This is exactly what MR jobs expect, but is not what
    // Spark jobs can do as there is a risk of jobID collision.
    return jobId != null ? jobId.toString() : "NULL_JOB_ID";
  }

  public static String buildJobId(JobContext context) {
    return buildJobId(context.getConfiguration(), context.getJobID());
  }

  /**
   * Get a job name; returns meaningful text if there is no name.
   *
   * @param context job context
   * @return a string for logs
   */
  public static String jobName(JobContext context) {
    String name = context.getJobName();
    return (name != null && !name.isEmpty()) ? name : "(anonymous)";
  }

  /**
   * Format: ${output}/_SUCCESS.
   *
   * @param output the output path.
   * @return the success marker file path.
   */
  public static Path successMarker(Path output) {
    return new Path(output, SUCCESS);
  }

  /**
   * Format: ${reportDir}/summary-xxxxx.json.
   *
   * @param reportDir the report directory.
   * @param jobId     the job id.
   * @return the summary report file path.
   */
  public static Path summaryReport(Path reportDir, String jobId) {
    return new Path(reportDir, String.format(SUMMARY_FILENAME_FORMAT, jobId));
  }

  public static void save(FileSystem fs, Path path, byte[] data) throws IOException {
    // By default, fs.create(path) will create parent folder recursively, and overwrite
    // it if it's already exist.
    try (FSDataOutputStream out = fs.create(path)) {
      IOUtils.copy(new ByteArrayInputStream(data), out);
    }
  }

  public static void save(FileSystem fs, Path path, Serializer instance) throws IOException {
    save(fs, path, instance.serialize());
  }

  public static byte[] load(FileSystem fs, Path path) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (FSDataInputStream in = fs.open(path)) {
      IOUtils.copy(in, out);
    }
    return out.toByteArray();
  }

  public static List<FileStatus> listPendingFiles(FileSystem fs, Path dir) throws IOException {
    List<FileStatus> pendingFiles = Lists.newArrayList();
    CommitUtils.listFiles(fs, dir, true, f -> {
      if (f.getPath().toString().endsWith(CommitUtils.PENDING_SUFFIX)) {
        pendingFiles.add(f);
      }
    });
    return pendingFiles;
  }

  public static void listFiles(FileSystem fs, Path dir, boolean recursive, FileVisitor visitor)
      throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(dir, recursive);
    while (iter.hasNext()) {
      FileStatus f = iter.next();
      visitor.visit(f);
    }
  }

  public interface FileVisitor {
    void visit(FileStatus f);
  }

  public static boolean supportObjectStorageCommit(Configuration conf, Path outputPath) {
    return supportSchemes(conf).contains(outputPath.toUri().getScheme());
  }

  private static List<String> supportSchemes(Configuration conf) {
    String schemes = conf.get(FS_STORAGE_OBJECT_SCHEME, DEFAULT_FS_STORAGE_OBJECT_SCHEME);
    Preconditions.checkNotNull(schemes, "%s cannot be null", FS_STORAGE_OBJECT_SCHEME);
    return Arrays.asList(schemes.split(","));
  }

  private static Set<String> errorStage = new HashSet<>();
  private static boolean testMode = false;

  public static void injectError(String stage) {
    errorStage.add(stage);
    testMode = true;
  }

  public static void removeError(String stage) {
    errorStage.remove(stage);
  }

  public static <T extends Exception> void triggerError(Supplier<T> error, String stage) throws T {
    if (testMode && errorStage.contains(stage)) {
      throw error.get();
    }
  }
}
