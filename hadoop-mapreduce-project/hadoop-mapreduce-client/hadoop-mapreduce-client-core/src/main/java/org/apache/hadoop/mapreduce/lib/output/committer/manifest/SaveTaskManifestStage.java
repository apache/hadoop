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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SAVE_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.manifestTempPathForTaskAttempt;

/**
 * Save A task manifest via a temp file.
 * Uses the task ID and task attempt ID to determine the filename;
 * returns the final path.
 */
public class SaveTaskManifestStage extends
    AbstractJobCommitStage<TaskManifest, Path> {

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

    final Path jobAttemptDir = directoryMustExist("Job attempt dir",
        getJobAttemptDir());
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(jobAttemptDir,
        getRequiredTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(jobAttemptDir,
        getRequiredTaskAttemptId());
    LOG.info("Saving manifest file to {}", manifestFile);
    save(manifest, manifestTempFile, manifestFile);
    return manifestFile;
  }

}
