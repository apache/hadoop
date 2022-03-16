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
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.TMP_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_SAVE_SUCCESS;

/**
 * Save the _SUCCESS file to the destination directory
 * via a temp file in the job attempt dir.
 * Returns the path of the file
 */
public class SaveSuccessFileStage extends
    AbstractJobOrTaskStage<ManifestSuccessData, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SaveSuccessFileStage.class);

  public SaveSuccessFileStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_SAVE_SUCCESS, false);
  }

  /**
   * Stage name is always job commit.
   * @param arguments args to the invocation.
   * @return stage name
   */
  @Override
  protected String getStageName(ManifestSuccessData arguments) {
    // set it to the job commit stage, always.
    return OP_STAGE_JOB_COMMIT;
  }

  /**
   * Execute.
   * @param successData success data to save
   * @return path saved to.
   * @throws IOException failure
   */
  @Override
  protected Path executeStage(final ManifestSuccessData successData)
      throws IOException {
    // Save the marker
    Path successFile = getStageConfig().getJobSuccessMarkerPath();
    Path successTempFile = new Path(getJobAttemptDir(), SUCCESS_MARKER + TMP_SUFFIX);
    LOG.debug("{}: Saving _SUCCESS file to {} via {}", successFile,
        getName(),
        successTempFile);
    save(successData, successTempFile, successFile);
    return successFile;
  }

}
