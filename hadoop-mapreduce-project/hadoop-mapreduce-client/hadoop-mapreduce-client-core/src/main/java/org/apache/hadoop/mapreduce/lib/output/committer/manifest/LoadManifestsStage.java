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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.functional.RemoteIterators;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DIRECTORY_SCAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_ALL_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_LOAD_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.maybeAddIOStatistics;

/**
 * Stage to load the manifests.
 * Manifests are loaded in parallel.
 * The IOStatistics snapshot passed in is built up with the statistics
 * and the statistics stripped from the manifest if prune == true.
 * This keeps the memory footprint of each manifest down.
 */
public class LoadManifestsStage extends
    AbstractJobCommitStage<Boolean,
        Pair<LoadManifestsStage.SummaryInfo, List<TaskManifest>>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      LoadManifestsStage.class);

  /**
   * Summary of manifest loading.
   */
  private final SummaryInfo summaryInfo = new SummaryInfo();

  /**
   * Should manifests be pruned of IOStatistics?
   */
  private boolean pruneManifests;

  /**
   * List of loaded manifests.
   */
  private final List<TaskManifest> manifests = new ArrayList<>();

  LoadManifestsStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_LOAD_MANIFESTS, true);
  }

  /**
   * Load the manifests.
   * @param prune should manifests be pruned of IOStatistics?
   * @return the summary and a list of manifests.
   * @throws IOException IO failure.
   */
  @Override
  protected Pair<SummaryInfo, List<TaskManifest>> executeStage(
      final Boolean prune) throws IOException {

    LOG.info("Executing Manifest Job Commit with manifests in {}",
        getJobAttemptDir());
    pruneManifests = prune;
    // build a list of all manifests in the JA directory.
    List<FileStatus> manifestFiles = new ArrayList<>();
    trackDurationOfInvocation(getIOStatistics(), OP_DIRECTORY_SCAN, () -> {
      msync(getJobAttemptDir());
      final RemoteIterator<FileStatus> source = listManifestsInJobAttemptDir();
      RemoteIterators.foreach(source, manifestFiles::add);
      // if the iterator provided statistics, collect them.
      maybeAddIOStatistics(getIOStatistics(), source);
    });

    final List<TaskManifest> manifestList = loadAllManifests(manifestFiles);
    LOG.info("Summary of {} manifests loaded in {}: {}",
        manifestFiles.size(),
        getJobAttemptDir(),
        summaryInfo);

    return Pair.of(summaryInfo, manifestList);
  }

  /**
   * Load all the manifests.
   * @param manifestFiles list of manifest files.
   * @return the loaded manifests.
   * @throws IOException IO Failure.
   */
  private List<TaskManifest> loadAllManifests(
      final List<FileStatus> manifestFiles) throws IOException {

    trackDurationOfInvocation(getIOStatistics(), OP_LOAD_ALL_MANIFESTS, () -> {
      TaskPool.foreach(manifestFiles)
          .executeWith(getIOProcessors())
          .stopOnFailure()
          .run(this::processOneManifest);
    });
    return manifests;
  }

  /**
   * Method invoked to process one manifest.
   * @param status file to process.
   * @throws IOException failure to load/parse
   */
  private void processOneManifest(FileStatus status)
      throws IOException {
    updateAuditContext(OP_LOAD_ALL_MANIFESTS);

    TaskManifest m = fetchTaskManifest(status);
    progress();

    // update the manifest list in a synchronized block.

    synchronized (manifests) {
      manifests.add(m);
      // and the summary info in the same block, to
      // eliminate the need to acquire a second lock.
      summaryInfo.add(m);
    }
    if (pruneManifests) {
      m.setIOStatistics(null);
      m.getExtraData().clear();
    }
  }

  /**
   * Precommit preparation of a single manifest file.
   * To reduce the memory foot print, the IOStatistics and
   * extra data of each manifest is cleared.
   * @param status status of file.
   * @return number of files.
   * @throws IOException IO Failure.
   */
  private TaskManifest fetchTaskManifest(FileStatus status)
      throws IOException {
    if (status.getLen() == 0 || !status.isFile()) {
      throw new PathIOException(status.getPath().toString(),
          "Not a valid manifest file; file status = " + status);
    }
    // load the manifest, which includes validation.
    final TaskManifest manifest = loadManifest(status);
    final String id = manifest.getTaskAttemptID();
    final int filecount = manifest.getFilesToCommit().size();
    final long size = manifest.getTotalFileSize();
    LOG.info("Task Attempt {} file {}: File count: {}; data size={}",
        id, status.getPath(), filecount, size);
    return manifest;
  }

  /**
   * Summary information.
   */
  public static final class SummaryInfo implements IOStatisticsSource {

    /**
     * Aggregate IOStatistics.
     */
    private IOStatisticsSnapshot iostatistics = snapshotIOStatistics();

    /**
     * How many manifests were loaded.
     */
    private long manifestCount;

    /**
     * Total number of files to rename.
     */
    private long fileCount;

    /**
     * Total number of directories which may need
     * to be created.
     * As there is no dedup, this is likely to be
     * a (major) overestimate.
     */
    private long directoryCount;

    /**
     * Total amount of data to be committed.
     */
    private long totalFileSize;

    /**
     * Get the IOStatistics.
     * @return aggregate IOStatistics
     */
    @Override
    public IOStatisticsSnapshot getIOStatistics() {
      return iostatistics;
    }

    long getFileCount() {
      return fileCount;
    }

    long getDirectoryCount() {
      return directoryCount;
    }

    long getTotalFileSize() {
      return totalFileSize;
    }

    long getManifestCount() {
      return manifestCount;
    }

    /**
     * Add all statistics.
     * @param manifest manifest to add.
     */
    void add(TaskManifest manifest) {
      manifestCount++;
      iostatistics.aggregate(manifest.getIOStatistics());
      fileCount += manifest.getFilesToCommit().size();
      directoryCount += manifest.getDirectoriesToCreate().size();
      totalFileSize += manifest.getTotalFileSize();
    }

    /**
     * To String includes all summary info except statistics.
     * @return string value
     */
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "SummaryInfo{");
      sb.append("manifestCount=").append(manifestCount);
      sb.append(", fileCount=").append(fileCount);
      sb.append(", directoryCount=").append(directoryCount);
      sb.append(", totalFileSize=").append(
          byteCountToDisplaySize(totalFileSize));
      sb.append('}');
      return sb.toString();
    }
  }
}
