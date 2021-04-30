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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_COMMIT;

/**
 * Commit a task attempt.
 * Scan the task attempt directories through
 * {@link ScanTaskAttemptDirectoryStage}
 * and then save to the task manifest path at
 * {@link SaveTaskManifestStage}.
 */

public class CommitTaskStage extends
    AbstractJobCommitStage<Void, Pair<Path, TaskManifest>> {

  public CommitTaskStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_COMMIT, false);
  }

  /**
   * Scan the task attempt dir then save the manifest.
   * A snapshot of the IOStats will be included in the manifest;
   * this includes the scan time.
   * @param arguments arguments to the function.
   * @return the path the manifest was saved to, and the manifest.
   * @throws IOException IO failure.
   */
  @Override
  protected Pair<Path, TaskManifest> executeStage(final Void arguments)
      throws IOException {

    TaskManifest manifest = new ScanTaskAttemptDirectoryStage(
        getStageConfig())
        .apply(arguments);
    // Now save with rename
    Path manifestPath = new SaveTaskManifestStage(getStageConfig())
        .apply(manifest);
    return Pair.of(manifestPath, manifest);
  }

}
