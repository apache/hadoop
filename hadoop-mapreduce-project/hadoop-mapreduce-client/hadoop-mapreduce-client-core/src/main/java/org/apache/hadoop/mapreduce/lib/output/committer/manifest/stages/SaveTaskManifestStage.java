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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

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
 * This is so that when this stage is invoked in a task commit, its output
 * overwrites any of the first commit.
 * When it succeeds, therefore, unless there is any subsequent commit of
 * another task, the task manifest at the final path is from this
 * operation.
 *
 * Returns the path where the manifest was saved.
 */
public class SaveTaskManifestStage extends
    AbstractJobOrTaskStage<TaskManifest, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SaveTaskManifestStage.class);

  public SaveTaskManifestStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SAVE_MANIFEST, false);
  }

  /**
   * Save the manifest to a temp file and rename to the final
   * manifest destination.
   * @param manifest manifest
   * @return the path to the final entry
   * @throws IOException IO failure.
   */
  @Override
  protected Path executeStage(final TaskManifest manifest)
      throws IOException {

    final Path manifestDir = getTaskManifestDir();
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(manifestDir,
        getRequiredTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        getRequiredTaskAttemptId());
    LOG.info("{}: Saving manifest file to {}", getName(), manifestFile);
    save(manifest, manifestTempFile, manifestFile);
    return manifestFile;
  }

}
