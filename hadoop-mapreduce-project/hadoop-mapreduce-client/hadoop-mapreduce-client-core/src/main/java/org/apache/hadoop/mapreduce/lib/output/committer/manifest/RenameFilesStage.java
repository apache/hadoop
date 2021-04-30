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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER_FILE_LIMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_RENAME_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createManifestOutcome;
import static org.apache.hadoop.thirdparty.com.google.common.collect.Iterables.concat;

/**
 * This stage renames all the files.
 * It retuns a manifest success data file summarizing the
 * output, but does not add iostatistics to it.
 */
public class RenameFilesStage extends
    AbstractJobCommitStage<List<TaskManifest>, ManifestSuccessData> {

  private static final Logger LOG = LoggerFactory.getLogger(
      RenameFilesStage.class);

  /**
   * List of all files committed.
   */
  private List<FileOrDirEntry> filesCommitted = new ArrayList<>();

  /**
   * Total file size.
   */
  private long totalFileSize = 0;

  public RenameFilesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_RENAME_FILES, true);
  }

  /**
   * Get the list of files committed.
   * Access is not synchronized.
   * @return direct access to the list of files.
   */
  public synchronized  List<FileOrDirEntry> getFilesCommitted() {
    return filesCommitted;
  }

  /**
   * Get the total file size of the committed task.
   * @return a number greater than or equal to zero.
   */
  public synchronized long getTotalFileSize() {
    return totalFileSize;
  }

  /**
   * Rename files in job commit.
   * @param taskManifests a list of task manifests containing files.
   * @return the job report.
   * @throws IOException failure
   */
  @Override
  protected ManifestSuccessData executeStage(
      final List<TaskManifest> taskManifests)
      throws IOException {

    // set the list of files to be as big as the number of tasks.
    filesCommitted = new ArrayList<>(taskManifests.size());

    final ManifestSuccessData success = createManifestOutcome(getStageConfig(),
        OP_STAGE_JOB_COMMIT);

    LOG.info("Executing Manifest Job Commit with manifests in {}",
        getJobAttemptDir());

    LOG.info("Committing the output of successful tasks");
    // first step is to aggregate the output of all manifests into a single
    // list of files to commit.
    // Which Guava can do in a zero-copy concatenated iterator

    final Iterable<FileOrDirEntry> filesToCommit = concat(taskManifests.stream()
        .map(TaskManifest::getFilesToCommit)
        .collect(Collectors.toList()));

    TaskPool.foreach(filesToCommit)
        .executeWith(getIOProcessors())
        .stopOnFailure()
        .run(this::commitOneFile);

    // synchronized block to keep spotbugs happy.
    List<FileOrDirEntry> committed = getFilesCommitted();
    LOG.info("Files committed: {}. Total size {}",
        committed.size(), getTotalFileSize());

    // Add a subset of the destination files to the success file;
    // enough for simple testing
    success.getFilenames().addAll(
        committed
            .subList(0, Math.min(committed.size(), SUCCESS_MARKER_FILE_LIMIT))
            .stream().map(FileOrDirEntry::getDest)
            .collect(Collectors.toList()));

    success.setSuccess(true);

    return success;
  }

  /**
   * Commit one file by rename, then, if that doesn't fail,
   * add to the files committed list.
   * @param entry entry to commit.
   * @throws IOException faiure.
   */
  private void commitOneFile(FileOrDirEntry entry) throws IOException {
    updateAuditContext(OP_STAGE_JOB_RENAME_FILES);

    // report progress back
    progress();
    // do the rename
    rename(entry.getSourcePath(), entry.getDestPath());

    // update the list and IOStats
    synchronized (this) {
      filesCommitted.add(entry);
      totalFileSize += entry.getSize();
    }

  }

}
