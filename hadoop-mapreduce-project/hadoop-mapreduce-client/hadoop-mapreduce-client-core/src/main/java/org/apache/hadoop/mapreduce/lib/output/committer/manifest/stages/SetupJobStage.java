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

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_SETUP;

/**
 * Stage to set up a job by creating the job attempt directory.
 * The job attempt directory must not exist before the call.
 */
public class SetupJobStage extends
    AbstractJobOrTaskStage<Boolean, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SetupJobStage.class);

  public SetupJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_SETUP, false);
  }

  /**
   * Execute the job setup stage.
   * @param deleteMarker: should any success marker be deleted.
   * @return the job attempted directory.
   * @throws IOException failure.
   */
  @Override
  protected Path executeStage(final Boolean deleteMarker) throws IOException {
    final Path path = getJobAttemptDir();
    LOG.info("{}: Creating Job Attempt directory {}", getName(), path);
    createNewDirectory("Job setup", path);
    createNewDirectory("Creating task manifest dir", getTaskManifestDir());
    // delete any success marker if so instructed.
    if (deleteMarker) {
      delete(getStageConfig().getJobSuccessMarkerPath(), false);
    }
    return path;
  }

}
