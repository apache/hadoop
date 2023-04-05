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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_FILE_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_MANIFEST_FILE_SIZE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_ALL_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_LOAD_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.maybeAddIOStatistics;
import static org.apache.hadoop.util.functional.RemoteIterators.haltableRemoteIterator;

/**
 * Stage to load all the task manifests in the job attempt directory.
 * Invoked in Job Commit.
 * Manifests are loaded in parallel.
 * The IOStatistics snapshot passed in is built up with the statistics
 * and the statistics stripped from the manifest if prune == true.
 * This keeps the memory footprint of each manifest down.
 */
public class LoadManifestsStage extends
    AbstractJobOrTaskStage<
        LoadManifestsStage.Arguments,
        LoadManifestsStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      LoadManifestsStage.class);

  /**
   * Summary of manifest loading.
   */
  private final SummaryInfo summaryInfo = new SummaryInfo();




  /**
   * List of loaded manifests.
   */
  private final List<TaskManifest> manifests = new ArrayList<>();

  /**
   * Map of directories from manifests, coalesced to reduce duplication.
   */
  private final Map<String, DirEntry> directories = new ConcurrentHashMap<>();

  /**
   * Writer of entries.
   */
  private EntryFileIO.EntryWriter entryWriter;

  /**
   * Should the manifests be cached and returned?
   * only for testing.
   */
  private boolean cacheManifests;

  public LoadManifestsStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_LOAD_MANIFESTS, true);
  }

  /**
   * Load the manifests.
   * @param arguments stage arguments
   * @return the summary and a list of manifests.
   * @throws IOException IO failure.
   */
  @Override
  protected LoadManifestsStage.Result executeStage(
      final LoadManifestsStage.Arguments arguments) throws IOException {

    EntryFileIO entryFileIO = new EntryFileIO(getStageConfig().getConf());

    final Path manifestDir = getTaskManifestDir();
    LOG.info("{}: Executing Manifest Job Commit with manifests in {}",
        getName(),
        manifestDir);
    cacheManifests = arguments.cacheManifests;

    final Path entrySequenceFile = arguments.getEntrySequenceFile();

    // the entry writer for queuing data.
    entryWriter = entryFileIO.launchEntryWriter(
            entryFileIO.createWriter(entrySequenceFile),
            arguments.queueCapacity);
    try {

      // sync fs before the listj
      msync(manifestDir);

      // build a list of all task manifests successfully committed,
      // which will break out if the writing is stopped (due to any failure)
      final RemoteIterator<FileStatus> manifestFiles =
          haltableRemoteIterator(listManifests(), () -> entryWriter.isActive());

      final List<TaskManifest> manifestList = loadAllManifests(manifestFiles);
      LOG.info("{}: Summary of {} manifests loaded in {}: {}",
          getName(),
          manifestList.size(),
          manifestDir,
          summaryInfo);

      // close cleanly
      entryWriter.close();

      // if anything failed, raise it.
      entryWriter.maybeRaiseWriteException();

      // collect any stats
      maybeAddIOStatistics(getIOStatistics(), manifestFiles);
    } finally {
      entryWriter.close();
    }
    final LoadedManifestData loadedManifestData = new LoadedManifestData(
        new ArrayList<>(directories.values()),  // new array to free up the map
        entrySequenceFile,
        entryWriter.getCount());

    return new LoadManifestsStage.Result(summaryInfo, loadedManifestData, null);
  }

  /**
   * Load all the manifests.
   * @param manifestFiles list of manifest files.
   * @return the loaded manifests.
   * @throws IOException IO Failure.
   */
  private List<TaskManifest> loadAllManifests(
      final RemoteIterator<FileStatus> manifestFiles) throws IOException {

    trackDurationOfInvocation(getIOStatistics(), OP_LOAD_ALL_MANIFESTS, () ->
        TaskPool.foreach(manifestFiles)
            .executeWith(getIOProcessors())
            .stopOnFailure()
            .run(this::processOneManifest));
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

    TaskManifest manifest = fetchTaskManifest(status);
    progress();

    // update the directories
    final int created = coalesceDirectories(manifest);
    LOG.debug("{}: task {} added {} directories",
        getName(), manifest.getTaskID(), created);

    // queue those files.
    final boolean enqueued = entryWriter.enqueue(manifest.getFilesToCommit());
    if (!enqueued) {
      LOG.warn("{}: Failed to write manifest for task {}",
          getName(),
          manifest.getTaskID());
    }

    // add to the summary.
    summaryInfo.add(manifest);

    // if manifests are cached, clear extra data
    // and then save.
    if (cacheManifests) {
      manifest.setIOStatistics(null);
      manifest.getExtraData().clear();
      // update the manifest list in a synchronized block.
      synchronized (manifests) {
        manifests.add(manifest);
      }
    }
  }

  /**
   * Coalesce all directories and clear the entry in the manifest.
   * There's only ever one writer at a time, which it is hoped reduces
   * contention. before the lock is acquired: if there are no new directories,
   * the write lock is never needed.
   * @param manifest manifest to process
   * @return the number of directories created;
   */
  @VisibleForTesting
  int coalesceDirectories(final TaskManifest manifest) {

    // build a list of dirs to create.
    // this scans the map
    final List<DirEntry> toCreate = manifest.getDestDirectories().stream()
        .filter(e -> !directories.containsKey(e))
        .collect(Collectors.toList());
    if (!toCreate.isEmpty()) {
      // need to add more directories;
      // still a possibility that they may be created between the
      // filtering and this thread having the write lock.

      synchronized (directories) {
        toCreate.forEach(entry -> {
          directories.putIfAbsent(entry.getDir(), entry);
        });
      }
    }
    return toCreate.size();

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
    LOG.info("{}: Task Attempt {} file {}: File count: {}; data size={}",
        getName(), id, status.getPath(), filecount, size);

    // record file size for tracking of memory consumption, work etc.
    final IOStatisticsStore iostats = getIOStatistics();
    iostats.addSample(COMMITTER_TASK_MANIFEST_FILE_SIZE, status.getLen());
    iostats.addSample(COMMITTER_TASK_FILE_COUNT_MEAN, filecount);
    iostats.addSample(COMMITTER_TASK_DIRECTORY_COUNT_MEAN,
        manifest.getDestDirectories().size());
    return manifest;
  }

  /**
   * Stage arguments.
   */
  public static final class Arguments {
    /**
     * File where the listing has been saved.
     */
    private final File entrySequenceFile;

    /**
     * build a list of manifests and return them?
     */
    private final boolean cacheManifests;

    /**
     * Capacity for queue between manifest loader and the writers.
     */
    private final int queueCapacity;

    public Arguments(final File entrySequenceFile,
        final boolean cacheManifests,
        final int queueCapacity) {
      this.entrySequenceFile = entrySequenceFile;
      this.cacheManifests = cacheManifests;
      this.queueCapacity = queueCapacity;
    }

    private Path getEntrySequenceFile() {
      return new Path(entrySequenceFile.toURI());

    }
  }

  /**
   * Result of the stage.
   */
  public static final class Result {
    private final SummaryInfo summary;

    /**
     * manifest list, non-null only if cacheManifests is true.
     */
    private final List<TaskManifest> manifests;

    /**
     * Output of this stage to pass on to the subsequence stages.
     */
    private final LoadedManifestData loadedManifestData;

    public Result(SummaryInfo summary,
        final LoadedManifestData loadedManifestData,
        final List<TaskManifest> manifests) {
      this.summary = summary;
      this.manifests = manifests;
      this.loadedManifestData = loadedManifestData;
    }

    public SummaryInfo getSummary() {
      return summary;
    }

    public List<TaskManifest> getManifests() {
      return manifests;
    }

    public LoadedManifestData getLoadedManifestData() {
      return loadedManifestData;
    }
  }

  /**
   * Summary information.
   */
  public static final class SummaryInfo implements IOStatisticsSource {

    /**
     * Aggregate IOStatistics.
     */
    private final IOStatisticsSnapshot iostatistics = snapshotIOStatistics();

    /**
     * Task IDs.
     */
    private final List<String> taskIDs = new ArrayList<>();

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

    public long getFileCount() {
      return fileCount;
    }

    public long getDirectoryCount() {
      return directoryCount;
    }

    public long getTotalFileSize() {
      return totalFileSize;
    }

    public long getManifestCount() {
      return manifestCount;
    }

    public List<String> getTaskIDs() {
      return taskIDs;
    }

    /**
     * Add all statistics; synchronized.
     * @param manifest manifest to add.
     */
    public synchronized void add(TaskManifest manifest) {
      manifestCount++;
      iostatistics.aggregate(manifest.getIOStatistics());
      fileCount += manifest.getFilesToCommit().size();
      directoryCount += manifest.getDestDirectories().size();
      totalFileSize += manifest.getTotalFileSize();
      taskIDs.add(manifest.getTaskID());
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
