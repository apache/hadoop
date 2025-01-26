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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.commit.ops.PendingOps;
import org.apache.hadoop.fs.tosfs.commit.ops.PendingOpsFactory;
import org.apache.hadoop.fs.tosfs.common.Tasks;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.object.MultipartUpload;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class Committer extends PathOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(Committer.class);

  public static final String COMMITTER_THREADS = "fs.job.committer.threads";
  public static final String COMMITTER_SUMMARY_REPORT_DIR =
      "fs.job.committer.summary.report.directory";
  public static final int DEFAULT_COMMITTER_THREADS = Runtime.getRuntime().availableProcessors();
  public static final String THREADS_PREFIX = "job-committer-thread-pool";

  private final String jobId;
  private final Path outputPath;
  // This is the directory for all intermediate work, where the output format will write data.
  // This may not be on the final file system
  private Path workPath;
  private final String role;
  private final Configuration conf;
  private final FileSystem destFs;
  private final ObjectStorage storage;
  private final PendingOps ops;

  public Committer(Path outputPath, TaskAttemptContext context) throws IOException {
    this(outputPath, context, String.format("Task committer %s", context.getTaskAttemptID()));
    this.workPath = CommitUtils.magicTaskAttemptBasePath(context, outputPath);
    LOG.info("Task attempt {} has work path {}", context.getTaskAttemptID(), getWorkPath());
  }

  public Committer(Path outputPath, JobContext context) throws IOException {
    this(outputPath, context, String.format("Job committer %s", context.getJobID()));
  }

  private Committer(Path outputPath, JobContext context, String role) throws IOException {
    super(outputPath, context);
    this.jobId = CommitUtils.buildJobId(context);
    this.outputPath = outputPath;
    this.role = role;
    this.conf = context.getConfiguration();
    this.destFs = outputPath.getFileSystem(conf);
    LOG.info("{} instantiated for job '{}' ID {} with destination {}",
        role,
        CommitUtils.jobName(context),
        jobId, outputPath);
    // Initialize the object storage.
    this.storage = ObjectStorageFactory.create(outputPath.toUri().getScheme(),
        outputPath.toUri().getAuthority(), conf);
    this.ops = PendingOpsFactory.create(destFs, storage);
  }

  @Override
  public Path getOutputPath() {
    return outputPath;
  }

  @Override
  public Path getWorkPath() {
    return workPath;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    checkJobId(context);
    LOG.info("Setup Job {}", jobId);
    Path jobOutput = getOutputPath();

    // delete the success marker if exists.
    destFs.delete(CommitUtils.successMarker(jobOutput), false);

    // create the destination directory.
    destFs.mkdirs(jobOutput);

    logUncompletedMPUIfPresent(jobOutput);

    // Reset the job path, and create the job path with job attempt sub path.
    Path jobPath = CommitUtils.magicJobPath(jobId, outputPath);
    Path jobAttemptPath = CommitUtils.magicJobAttemptPath(context, outputPath);
    destFs.delete(jobPath, true);
    destFs.mkdirs(jobAttemptPath);
  }

  private void logUncompletedMPUIfPresent(Path jobOutput) {
    // do a scan and add warn log message for active uploads.
    int nums = 0;
    for (MultipartUpload upload : storage.listUploads(ObjectUtils.pathToKey(jobOutput, true))) {
      if (nums++ > 10) {
        LOG.warn("There are more than 10 uncompleted multipart uploads under path {}.", jobOutput);
        break;
      }
      LOG.warn("Uncompleted multipart upload {} is under path {}, either jobs are running"
          + " concurrently or failed jobs are not being cleaned up.", upload, jobOutput);
    }
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    checkJobId(context);
    LOG.info("{}: committing job {}", role, jobId);
    String stage = null;
    Exception failure = null;
    SuccessData successData = null;

    ExecutorService threadPool = ThreadPools.newWorkerPool(THREADS_PREFIX, commitThreads());
    List<FileStatus> pendingSets = Lists.newArrayList();
    try {
      // Step.1 List active pending commits.
      stage = "preparing";
      CommitUtils.listFiles(destFs, CommitUtils.magicJobAttemptPath(context, outputPath), true,
          f -> {
        if (f.getPath().toString().endsWith(CommitUtils.PENDINGSET_SUFFIX)) {
          pendingSets.add(f);
        }
      });

      // Step.2 Load and commit those active pending commits.
      stage = "commit";
      CommitContext commitCtxt = new CommitContext(pendingSets);
      loadAndCommitPendingSets(threadPool, commitCtxt);

      // Step.3 Save the success marker.
      stage = "marker";
      successData = createSuccessData(commitCtxt.destKeys());
      CommitUtils.triggerError(() -> new IOException("Mock error of success marker."), stage);
      CommitUtils.save(destFs, CommitUtils.successMarker(outputPath), successData);

      // Step.4 Abort those orphan multipart uploads and cleanup the staging dir.
      stage = "clean";
      cleanup(threadPool, true);
    } catch (Exception e) {
      failure = e;
      LOG.warn("Commit failure for job {} stage {}", CommitUtils.buildJobId(context), stage, e);

      // Revert all pending sets when marker step fails.
      if (stage.equals("marker")) {
        CommonUtils.runQuietly(
            () -> loadAndRevertPendingSets(threadPool, new CommitContext(pendingSets)));
      }
      CommonUtils.runQuietly(() -> cleanup(threadPool, true));
      throw e;
    } finally {
      saveSummaryReportQuietly(stage, context, successData, failure);
      CommonUtils.runQuietly(threadPool::shutdown);

      cleanupResources();
    }
  }

  private SuccessData createSuccessData(Iterable<String> filenames) {
    SuccessData data = SuccessData.builder()
        .setName(SuccessData.class.getName())
        .setCommitter(CommitUtils.COMMITTER_NAME)
        .setTimestamp(System.currentTimeMillis())
        .setHostname(NetUtils.getHostname())
        .setDescription(role)
        .setJobId(jobId)
        .addFileNames(filenames)
        .build();

    data.addDiagnosticInfo(COMMITTER_THREADS, Integer.toString(commitThreads()));
    return data;
  }

  private void saveSummaryReportQuietly(String activeStage, JobContext context, SuccessData report,
      Throwable thrown) {
    Configuration jobConf = context.getConfiguration();
    String reportDir = jobConf.get(COMMITTER_SUMMARY_REPORT_DIR, "");
    if (reportDir.isEmpty()) {
      LOG.debug("Summary directory conf: {} is not set", COMMITTER_SUMMARY_REPORT_DIR);
      return;
    }

    Path path = CommitUtils.summaryReport(new Path(reportDir), jobId);
    LOG.debug("Summary report path is {}", path);

    try {
      if (report == null) {
        report = createSuccessData(null);
      }
      if (thrown != null) {
        report.recordJobFailure(thrown);
      }
      report.addDiagnosticInfo("stage", activeStage);

      CommitUtils.save(path.getFileSystem(jobConf), path, report);
      LOG.info("Job summary saved to {}", path);
    } catch (Exception e) {
      LOG.warn("Failed to save summary to {}", path, e);
    }
  }

  private void loadAndCommitPendingSets(ExecutorService outerPool, CommitContext commitContext) {
    ExecutorService innerPool =
        ThreadPools.newWorkerPool("commit-pending-files-pool", commitThreads());
    try {
      Tasks.foreach(commitContext.pendingSets())
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(outerPool)
          .abortWith(pendingSet -> loadAndAbort(innerPool, pendingSet))
          .revertWith(pendingSet -> loadAndRevert(innerPool, pendingSet))
          .run(pendingSet -> loadAndCommit(commitContext, innerPool, pendingSet));
    } finally {
      CommonUtils.runQuietly(innerPool::shutdown);
    }
  }

  private void loadAndRevertPendingSets(ExecutorService outerPool, CommitContext commitContext) {
    Tasks.foreach(commitContext.pendingSets())
        .throwFailureWhenFinished()
        .executeWith(outerPool)
        .run(pendingSet -> loadAndRevert(outerPool, pendingSet));
  }

  /**
   * Load {@link PendingSet} from file and abort those {@link Pending} commits.
   */
  private void loadAndAbort(ExecutorService pool, FileStatus pendingSetFile) {
    PendingSet pendingSet = PendingSet.deserialize(destFs, pendingSetFile);
    Tasks.foreach(pendingSet.commits())
        .suppressFailureWhenFinished()
        .executeWith(pool)
        .run(ops::abort);
  }

  /**
   * Load {@link PendingSet} from file and revert those {@link Pending} commits.
   */
  private void loadAndRevert(ExecutorService pool, FileStatus pendingSetFile) {
    PendingSet pendingSet = PendingSet.deserialize(destFs, pendingSetFile);
    Tasks.foreach(pendingSet.commits())
        .suppressFailureWhenFinished()
        .executeWith(pool)
        .run(ops::revert);
  }

  /**
   * Load {@link PendingSet} from file and commit those {@link Pending} commits.
   */
  private void loadAndCommit(CommitContext commitCtxt, ExecutorService pool,
      FileStatus pendingSetFile) {
    PendingSet pendingSet = PendingSet.deserialize(destFs, pendingSetFile);
    // Verify that whether the job id is matched.
    String jobId = pendingSet.jobId();
    if (!StringUtils.isNoneEmpty(jobId) && !Objects.equals(jobId, jobId())) {
      throw new IllegalStateException(
          String.format("Mismatch in Job ID (%s) and commit job ID (%s)", jobId(), jobId));
    }

    Tasks.foreach(pendingSet.commits())
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(pool)
        .onFailure((pending, exception) -> ops.abort(pending))
        .abortWith(ops::abort)
        .revertWith(ops::revert)
        .run(pending -> {
          ops.commit(pending);
          commitCtxt.addDestKey(pending.destKey());
        });
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state) {
    checkJobId(context);
    LOG.info("{}: aborting job {} in state {}", role, jobId, state);
    ExecutorService service = ThreadPools.newWorkerPool(THREADS_PREFIX, commitThreads());
    try {
      cleanup(service, false);
    } finally {
      service.shutdown();

      cleanupResources();
    }
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    checkJobId(context);
    LOG.info("Setup Task {}", context.getTaskAttemptID());
    Path taskAttemptBasePath = CommitUtils.magicTaskAttemptBasePath(context, outputPath);
    // Delete the task attempt path if somehow it was there.
    destFs.delete(taskAttemptBasePath, true);
    // Make an empty directory.
    destFs.mkdirs(taskAttemptBasePath);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
    return true;
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    checkJobId(context);
    LOG.info("Commit task {}", context);
    ExecutorService pool = ThreadPools.newWorkerPool(THREADS_PREFIX, commitThreads());
    try {
      PendingSet commits = innerCommitTask(pool, context);
      LOG.info("Task {} committed {} files", context.getTaskAttemptID(), commits.size());
    } catch (IOException e) {
      LOG.error("Failed to commit task {}", context.getTaskAttemptID(), e);
      throw e;
    } finally {
      // Shutdown the thread pool quietly.
      CommonUtils.runQuietly(pool::shutdown);

      // Delete the task attempt path quietly.
      Path taskAttemptPath = CommitUtils.magicTaskAttemptPath(context, outputPath);
      LOG.info("Delete task attempt path {}", taskAttemptPath);
      CommonUtils.runQuietly(() -> destFs.delete(taskAttemptPath, true));
    }
  }

  private PendingSet innerCommitTask(ExecutorService pool, TaskAttemptContext context)
      throws IOException {
    Path taskAttemptBasePath = CommitUtils.magicTaskAttemptBasePath(context, outputPath);
    PendingSet pendingSet = new PendingSet(jobId);
    try {
      // Load the pending files and fill them into the pending set.
      List<FileStatus> pendingFiles = CommitUtils.listPendingFiles(destFs, taskAttemptBasePath);
      // Use the thread-safe collection to collect the pending list.
      List<Pending> pendings = Collections.synchronizedList(Lists.newArrayList());
      Tasks.foreach(pendingFiles)
          .throwFailureWhenFinished()
          .executeWith(pool)
          .run(f -> {
            try {
              byte[] data = CommitUtils.load(destFs, f.getPath());
              pendings.add(Pending.deserialize(data));
            } catch (IOException e) {
              LOG.warn("Failed to load .pending file {}", f.getPath(), e);
              throw new UncheckedIOException(e);
            }
          });
      pendingSet.addAll(pendings);

      // Add the extra task attempt id property.
      String taskId = String.valueOf(context.getTaskAttemptID());
      pendingSet.addExtraData(CommitUtils.TASK_ATTEMPT_ID, taskId);

      // Save the pending set to file system.
      Path taskOutput = CommitUtils.magicTaskPendingSetPath(context, outputPath);
      LOG.info("Saving work of {} to {}", taskId, taskOutput);
      CommitUtils.save(destFs, taskOutput, pendingSet.serialize());

    } catch (Exception e) {
      LOG.error("Encounter error when loading pending set from {}", taskAttemptBasePath, e);
      if (!pendingSet.commits().isEmpty()) {
        Tasks.foreach(pendingSet.commits())
            .executeWith(pool)
            .suppressFailureWhenFinished()
            .run(ops::abort);
      }
      throw e;
    }

    return pendingSet;
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    checkJobId(context);
    Path taskAttemptBasePath = CommitUtils.magicTaskAttemptBasePath(context, outputPath);
    try {
      // Load the pending files from the underlying filesystem.
      List<FileStatus> pendingFiles = CommitUtils.listPendingFiles(destFs, taskAttemptBasePath);
      Tasks.foreach(pendingFiles)
          .throwFailureWhenFinished()
          .run(f -> {
            try {
              byte[] serializedData = CommitUtils.load(destFs, f.getPath());
              ops.abort(Pending.deserialize(serializedData));
            } catch (FileNotFoundException e) {
              LOG.debug("Listed file already deleted: {}", f);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            } finally {
              final FileStatus pendingFile = f;
              CommonUtils.runQuietly(() -> destFs.delete(pendingFile.getPath(), false));
            }
          });
    } finally {
      CommonUtils.runQuietly(() -> destFs.delete(taskAttemptBasePath, true));
    }
  }

  @Override
  public void recoverTask(TaskAttemptContext context) {
    checkJobId(context);
    String taskId = context.getTaskAttemptID().toString();
    throw new UnsupportedOperationException(
        String.format("Unable to recover task %s, output: %s", taskId, outputPath));
  }

  private int commitThreads() {
    return conf.getInt(COMMITTER_THREADS, DEFAULT_COMMITTER_THREADS);
  }

  private void cleanup(ExecutorService pool, boolean suppress) {
    LOG.info("Cleanup the job by abort the multipart uploads and clean staging dir, suppress {}",
        suppress);
    try {
      Path jobOutput = getOutputPath();
      Iterable<MultipartUpload> pending = storage.listUploads(
          ObjectUtils.pathToKey(CommitUtils.magicJobPath(jobId, jobOutput), true));
      Tasks.foreach(pending)
          .executeWith(pool)
          .suppressFailureWhenFinished()
          .run(u -> storage.abortMultipartUpload(u.key(), u.uploadId()));
    } catch (Exception e) {
      if (suppress) {
        LOG.error("The following exception has been suppressed when cleanup job", e);
      } else {
        throw e;
      }
    } finally {
      CommonUtils.runQuietly(this::cleanupStagingDir);
    }
  }

  private void cleanupStagingDir() throws IOException {
    // Note: different jobs share the same __magic folder, like,
    // tos://bucket/path/to/table/__magic/job-A/..., and
    // tos://bucket/path/to/table/__magic/job-B/...
    // Job should only delete its own job folder to avoid the failure of other jobs,
    // and, folder __magic should be deleted by the last job.
    // This design does not assure the security of two jobs that one job founds there
    // isn't another job be running, however, when it is deleting __magic but another
    // job will visit it at the same time. We think the probability is low and we don't
    // deal with it.
    destFs.delete(CommitUtils.magicJobPath(jobId, outputPath), true);
    Path magicPath = CommitUtils.magicPath(outputPath);
    if (destFs.listStatus(magicPath).length == 0) {
      destFs.delete(magicPath, true);
    }
  }

  public String jobId() {
    return jobId;
  }

  private void checkJobId(JobContext context) {
    String jobIdInContext = CommitUtils.buildJobId(context);
    Preconditions.checkArgument(Objects.equals(jobId, jobIdInContext), String.format(
        "JobId set in the context: %s is not consistent with the initial jobId of the committer:"
            + " %s, please check you settings in your taskAttemptContext.",
        jobIdInContext, jobId));
  }

  private void cleanupResources() {
    CommonUtils.runQuietly(storage::close);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("role", role)
        .add("jobId", jobId)
        .add("outputPath", outputPath)
        .toString();
  }
}
