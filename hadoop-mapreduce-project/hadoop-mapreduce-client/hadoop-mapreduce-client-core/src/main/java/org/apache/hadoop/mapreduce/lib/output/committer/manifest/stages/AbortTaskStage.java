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

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_ABORT_TASK;

/**
 * Abort a task.
 *
 * This is done by deleting the task directory.
 * Exceptions may/may not be suppressed.
 */
public class AbortTaskStage extends
    AbstractJobOrTaskStage<Boolean, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbortTaskStage.class);

  public AbortTaskStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_ABORT_TASK, false);
  }

  /**
   * Delete the task attempt directory.
   * @param suppressExceptions should exceptions be ignored?
   * @return the directory
   * @throws IOException failure when exceptions were not suppressed
   */
  @Override
  protected Path executeStage(final Boolean suppressExceptions)
      throws IOException {
    final Path dir = getTaskAttemptDir();
    if (dir != null) {
      LOG.info("{}: Deleting task attempt directory {}", getName(), dir);
      deleteDir(dir, suppressExceptions);
    }
    return dir;
  }

}
