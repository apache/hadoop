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

import org.apache.hadoop.fs.Path;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SETUP;

/**
 * Stage to set up task.
 * This creates the task attempt directory, after verifying
 * that the job attempt dir exists (i.e. this is invoked
 * after the job is started and before any cleanup.
 * Argument passed in is task name:only for logging.
 */
public class SetupTaskStage extends
    AbstractJobOrTaskStage<String, Path> {

  public SetupTaskStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SETUP, false);
  }

  /**
   * Set up a task.
   * @param name task name (for logging)
   * @return task attempt directory
   * @throws IOException IO failure.
   */
  @Override
  protected Path executeStage(final String name) throws IOException {
    return createNewDirectory("Task setup " + name,
        requireNonNull(getTaskAttemptDir(), "No task attempt directory"));
  }

}
