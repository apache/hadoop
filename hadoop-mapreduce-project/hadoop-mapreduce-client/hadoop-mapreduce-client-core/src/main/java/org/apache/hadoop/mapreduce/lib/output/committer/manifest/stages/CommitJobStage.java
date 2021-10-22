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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_BYTES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_FILES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage.DISABLED;

/**
 * Commit the Job.
 * Arguments (save manifest, validate output)
 * Inputs: saveMarker: boolean, validateOutput: boolean
 * Outputs: SuccessData
 */
public class CommitJobStage extends
    AbstractJobCommitStage<
        CommitJobStage.Arguments,
        CommitJobStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CommitJobStage.class);

  public CommitJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_COMMIT, true);
  }

  @Override
  protected CommitJobStage.Result executeStage(
      final CommitJobStage.Arguments arguments) throws IOException {

    boolean createMarker = arguments.createMarker;

    // load the manifests
    final StageConfig stageConfig = getStageConfig();
    LoadManifestsStage.Result result
        = new LoadManifestsStage(stageConfig).apply(true);
    List<TaskManifest> manifests = result.getManifests();
    LoadManifestsStage.SummaryInfo summary = result.getSummary();

    LOG.debug("Job Summary {}", summary);
    LOG.info("Committing job with file count: {}; total size {} bytes",
        summary.getFileCount(),
        byteCountToDisplaySize(summary.getTotalFileSize()));

    // add in the manifest statistics to our local IOStatistics for
    // reporting.
    IOStatisticsStore iostats = getIOStatistics();
    iostats.aggregate(summary.getIOStatistics());

    // prepare destination directories.
    new CreateOutputDirectoriesStage(stageConfig)
        .apply(manifests);

    // commit all the tasks.
    // The success data includes a snapshot of the IO Statistics
    // and hence all aggregate stats from the tasks.
    ManifestSuccessData successData;
    successData = new RenameFilesStage(stageConfig).apply(manifests);
    LOG.debug("{}: _SUCCESS file summary {}", getName(), successData.toJson());
    // update the counter of bytes committed and files.
    // use setCounter so as to ignore any values accumlated when
    // aggregating tasks.
    iostats.setCounter(
        COMMITTER_FILES_COMMITTED_COUNT,
        summary.getFileCount());
    iostats.setCounter(
        COMMITTER_BYTES_COMMITTED_COUNT,
        summary.getTotalFileSize());
    successData.snapshotIOStatistics(iostats);


    // rename manifests. Only warn on failure here.
    final String manifestRenameDir = arguments.getManifestRenameDir();
    if (isNotBlank(manifestRenameDir)) {
      Path manifestRenamePath = new Path(
          new Path(manifestRenameDir),
          getJobId());
      LOG.info("Renaming manifests to {}", manifestRenamePath);
      try {
        renameDir(getTaskAttemptDir(), manifestRenamePath);

        // save this path in the summary diagnostics
        successData.getDiagnostics().put(MANIFESTS, manifestRenamePath.toUri().toString());
      } catch (IOException | IllegalArgumentException e) {
        // rename failure, including path for wrong filesystem
        LOG.warn("Failed to rename manifests to {}", manifestRenamePath, e);
      }
    }

    // save the _SUCCESS if the option is enabled.
    if (createMarker) {
      // save a snapshot of the IO Statistics

      Path succesPath = new SaveSuccessFileStage(stageConfig)
          .apply(successData);
      LOG.debug("{}: Saving _SUCCESS file to {}", getName(), succesPath);
    }

    // optional cleanup
    new CleanupJobStage(stageConfig).apply(arguments.getCleanupArguments());

    // and then, after everything else: optionally validate.
    if (arguments.validateOutput) {
      // cache and restore the active stage field
      LOG.info("{}: Validating output.", getName());
      new ValidateRenamedFilesStage(stageConfig)
          .apply(result.getManifests());
    }

    // restore the active stage so that when the report is saved
    // it is declared as job commit, not cleanup or validate.
    stageConfig.enterStage(getStageName(arguments));

    // the result
    return new CommitJobStage.Result(successData, manifests);
  }

  /**
   * Arguments for job commit.
   */
  public static final class Arguments {
    private final boolean createMarker;
    private final boolean validateOutput;
    private final String manifestRenameDir;
    private final CleanupJobStage.Arguments cleanupArguments;

    public Arguments(boolean createMarker,
        boolean validateOutput,
        String manifestRenameDir,
        CleanupJobStage.Arguments cleanupArguments) {
      this.createMarker = createMarker;
      this.validateOutput = validateOutput;
      this.manifestRenameDir = manifestRenameDir;
      this.cleanupArguments = cleanupArguments;
    }

    public Arguments(boolean createMarker, boolean validateOutput) {
      this(createMarker, validateOutput, null, DISABLED);
    }

    public boolean isCreateMarker() {
      return createMarker;
    }

    public boolean isValidateOutput() {
      return validateOutput;
    }

    public String getManifestRenameDir() {
      return manifestRenameDir;
    }

    public CleanupJobStage.Arguments getCleanupArguments() {
      return cleanupArguments;
    }
  }

  /**
   * Result of the stage.
   */
  public static final class Result {
    private final ManifestSuccessData jobSuccessData;
    private final List<TaskManifest> manifests;

    public Result(ManifestSuccessData jobSuccessData,
        List<TaskManifest> manifests) {
      this.jobSuccessData = jobSuccessData;
      this.manifests = manifests;
    }

    public ManifestSuccessData getJobSuccessData() {
      return jobSuccessData;
    }

    public List<TaskManifest> getManifests() {
      return manifests;
    }
  }
}
