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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import java.io.IOException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_TASK_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SAVE_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestTempPathForTaskAttempt;

/**
 * Save a task manifest to the job attempt dir, using the task
 * ID for the name of the final file.
 * For atomic writes, the manifest is saved
 * by writing to a temp file and then renaming it.
 * Uses both the task ID and task attempt ID to determine the temp filename;
 * Before the rename of (temp, final-path), any file at the final path
 * is deleted.
 * <p>
 * This is so that when this stage is invoked in a task commit, its output
 * overwrites any of the first commit.
 * When it succeeds, therefore, unless there is any subsequent commit of
 * another task, the task manifest at the final path is from this
 * operation.
 * <p>
 * If the save and rename fails, there are a limited number of retries, with no sleep
 * interval.
 * This is to briefly try recover from any transient rename() failure, including a
 * race condition with any other task commit.
 * <ol>
 *   <li>If the previous task commit has already succeeded, this rename will overwrite it.
 *        Both task attempts will report success.</li>
 *   <li>If after, writing, another task attempt overwrites it, again, both
 *        task attempts will report success.</li>
 *   <li>If another task commits between the delete() and rename() operations, the retry will
 *        attempt to recover by repeating the manifest write, and then report success.</li>
 * </ol>
 * This means that multiple task attempts may report success, but only one will have it actual
 * manifest saved.
 * The mapreduce and spark committers only schedule a second task commit attempt if the first
 * task attempt's commit operation fails <i>or fails to report success in the allocated time</i>.
 * The overwrite with retry loop is an attempt to ensure that the second attempt will report
 * success, if a partitioned cluster means that the original TA commit is still in progress.
 * <p>
 * Returns (the path where the manifest was saved, the manifest).
 */
public class SaveTaskManifestStage extends
    AbstractJobOrTaskStage<Supplier<TaskManifest>, Pair<Path, TaskManifest>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SaveTaskManifestStage.class);

  public SaveTaskManifestStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SAVE_MANIFEST, false);
  }

  /**
   * Generate and save a manifest to a temp file and rename to the final
   * manifest destination.
   * The manifest is generated on each retried attempt.
   * @param manifestSource supplier the manifest/success file
   *
   * @return the path to the final entry
   * @throws IOException IO failure.
   */
  @Override
  protected Pair<Path, TaskManifest> executeStage(Supplier<TaskManifest> manifestSource)
      throws IOException {

    final Path manifestDir = getTaskManifestDir();
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(manifestDir,
        getRequiredTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        getRequiredTaskAttemptId());
    LOG.info("{}: Saving manifest file to {}", getName(), manifestFile);
    final TaskManifest manifest =
        saveManifest(manifestSource, manifestTempFile, manifestFile, OP_SAVE_TASK_MANIFEST);
    return Pair.of(manifestFile, manifest);
  }

}
