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

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_BYTES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_FILES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_LOAD_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_RENAME_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.addHeapInformation;

/**
 * Commit the Job.
 * Arguments (save manifest, validate output)
 * Inputs: saveMarker: boolean, validateOutput: boolean
 * Outputs: SuccessData
 */
public class CommitJobStage extends
    AbstractJobOrTaskStage<
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

    LOG.info("{}: Committing job \"{}\". resilient commit supported = {}",
        getName(),
        getJobId(),
        storeSupportsResilientCommit());

    // once the manifest has been loaded, a temp file needs to be
    // deleted; so track the value.
    LoadedManifestData loadedManifestData = null;

    try {
      boolean createMarker = arguments.isCreateMarker();
      IOStatisticsSnapshot heapInfo = new IOStatisticsSnapshot();
      addHeapInformation(heapInfo, "setup");
      // load the manifests
      final StageConfig stageConfig = getStageConfig();
      LoadManifestsStage.Result result = new LoadManifestsStage(stageConfig).apply(
          new LoadManifestsStage.Arguments(
              File.createTempFile("manifest", ".list"),
              /* do not cache manifests */
              stageConfig.getWriterQueueCapacity()));
      LoadManifestsStage.SummaryInfo loadedManifestSummary = result.getSummary();
      loadedManifestData = result.getLoadedManifestData();

      LOG.debug("{}: Job Summary {}", getName(), loadedManifestSummary);
      LOG.info("{}: Committing job with file count: {}; total size {} bytes",
          getName(),
          loadedManifestSummary.getFileCount(),
          String.format("%,d", loadedManifestSummary.getTotalFileSize()));
      addHeapInformation(heapInfo, OP_STAGE_JOB_LOAD_MANIFESTS);

      // add in the manifest statistics to our local IOStatistics for
      // reporting.
      IOStatisticsStore iostats = getIOStatistics();
      iostats.aggregate(loadedManifestSummary.getIOStatistics());

      // prepare destination directories.
      final CreateOutputDirectoriesStage.Result dirStageResults =
          new CreateOutputDirectoriesStage(stageConfig)
              .apply(loadedManifestData.getDirectories());
      addHeapInformation(heapInfo, OP_STAGE_JOB_CREATE_TARGET_DIRS);

      // commit all the tasks.
      // The success data includes a snapshot of the IO Statistics
      // and hence all aggregate stats from the tasks.
      ManifestSuccessData successData;
      successData = new RenameFilesStage(stageConfig).apply(
          Triple.of(loadedManifestData,
              dirStageResults.getCreatedDirectories(),
              stageConfig.getSuccessMarkerFileLimit()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: _SUCCESS file summary {}", getName(), successData.toJson());
      }
      addHeapInformation(heapInfo, OP_STAGE_JOB_RENAME_FILES);

      // update the counter of bytes committed and files.
      // use setCounter so as to ignore any values accumulated when
      // aggregating tasks.
      iostats.setCounter(
          COMMITTER_FILES_COMMITTED_COUNT,
          loadedManifestSummary.getFileCount());
      iostats.setCounter(
          COMMITTER_BYTES_COMMITTED_COUNT,
          loadedManifestSummary.getTotalFileSize());
      successData.snapshotIOStatistics(iostats);
      successData.getIOStatistics().aggregate(heapInfo);

      // rename manifests. Only warn on failure here.
      final String manifestRenameDir = arguments.getManifestRenameDir();
      if (isNotBlank(manifestRenameDir)) {
        Path manifestRenamePath = new Path(
            new Path(manifestRenameDir),
            getJobId());
        LOG.info("{}: Renaming manifests to {}", getName(), manifestRenamePath);
        try {
          renameDir(getTaskManifestDir(), manifestRenamePath);

          // save this path in the summary diagnostics
          successData.getDiagnostics().put(MANIFESTS, manifestRenamePath.toUri().toString());
        } catch (IOException | IllegalArgumentException e) {
          // rename failure, including path for wrong filesystem
          LOG.warn("{}: Failed to rename manifests to {}", getName(), manifestRenamePath, e);
        }
      }

      // save the _SUCCESS if the option is enabled.
      Path successPath = null;
      if (createMarker) {
        // save a snapshot of the IO Statistics

        successPath = new SaveSuccessFileStage(stageConfig)
            .apply(successData);
        LOG.debug("{}: Saving _SUCCESS file to {}", getName(), successPath);
      }

      // optional cleanup
      new CleanupJobStage(stageConfig).apply(arguments.getCleanupArguments());

      // and then, after everything else: optionally validate.
      if (arguments.isValidateOutput()) {
        // cache and restore the active stage field
        LOG.info("{}: Validating output.", getName());
        new ValidateRenamedFilesStage(stageConfig)
            .apply(loadedManifestData.getEntrySequenceData());
      }

      // restore the active stage so that when the report is saved
      // it is declared as job commit, not cleanup or validate.
      stageConfig.enterStage(getStageName(arguments));

      // the result
      return new Result(successPath, successData);
    } finally {
      // cleanup; return code is ignored.
      if (loadedManifestData != null) {
        loadedManifestData.deleteEntrySequenceFile();
      }

    }
  }

  /**
   * Arguments for job commit.
   */
  public static final class Arguments {

    /** create the _SUCCESS marker? */
    private final boolean createMarker;

    /** perform validation checks on the files? */
    private final boolean validateOutput;

    /** optional directory to rename the task manifests to. */
    private final String manifestRenameDir;

    /** cleanup arguments.. */
    private final CleanupJobStage.Arguments cleanupArguments;

    /**
     *
     * @param createMarker create the _SUCCESS marker?
     * @param validateOutput perform validation checks on the files?
     * @param manifestRenameDir optional directory to rename the task manifests to
     * @param cleanupArguments cleanup arguments.
     */
    public Arguments(
        boolean createMarker,
        boolean validateOutput,
        @Nullable String manifestRenameDir,
        CleanupJobStage.Arguments cleanupArguments) {

      this.createMarker = createMarker;
      this.validateOutput = validateOutput;
      this.manifestRenameDir = manifestRenameDir;
      this.cleanupArguments = requireNonNull(cleanupArguments);
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
    /**
     * Manifest success data.
     */
    private final ManifestSuccessData jobSuccessData;

    /**
     * Success file path. null if not saved.
     */
    private final Path successPath;

    public Result(final Path successPath,
        ManifestSuccessData jobSuccessData) {
      this.successPath = successPath;
      this.jobSuccessData = jobSuccessData;
    }

    public ManifestSuccessData getJobSuccessData() {
      return jobSuccessData;
    }

    public Path getSuccessPath() {
      return successPath;
    }
  }
}
