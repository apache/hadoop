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
import java.util.concurrent.atomic.AtomicLong;
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
   * Map of directories from manifests, coalesced to reduce duplication.
   */
  private final Map<String, DirEntry> directories = new ConcurrentHashMap<>();

  /**
   * Writer of entries.
   */
  private EntryFileIO.EntryWriter entryWriter;

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

    final Path entrySequenceData = arguments.getEntrySequenceData();

    // the entry writer for queuing data.
    entryWriter = entryFileIO.launchEntryWriter(
            entryFileIO.createWriter(entrySequenceData),
            arguments.queueCapacity);

    try {

      // sync fs before the list
      msync(manifestDir);

      // build a list of all task manifests successfully committed,
      // which will break out if the writing is stopped (due to any failure)
      final RemoteIterator<FileStatus> manifestFiles =
          haltableRemoteIterator(listManifests(),
              () -> entryWriter.isActive());

      processAllManifests(manifestFiles);
      maybeAddIOStatistics(getIOStatistics(), manifestFiles);

      LOG.info("{}: Summary of {} manifests loaded in {}: {}",
          getName(),
          summaryInfo.manifestCount,
          manifestDir,
          summaryInfo);

      // close cleanly
      entryWriter.close();

      // if anything failed, raise it.
      entryWriter.maybeRaiseWriteException();

      // collect any stats
    } catch (EntryWriteException e) {
      // something went wrong while writing.
      // raise anything on the write thread,
      entryWriter.maybeRaiseWriteException();

      // falling back to that from the worker thread
      throw e;
    } finally {
      // close which is a no-op if the clean close was invoked;
      // it is not a no-op if something went wrong with reading/parsing/processing
      // the manifests.
      entryWriter.close();
    }

    final LoadedManifestData loadedManifestData = new LoadedManifestData(
        new ArrayList<>(directories.values()),  // new array to free up the map
        entrySequenceData,
        entryWriter.getCount());

    return new LoadManifestsStage.Result(summaryInfo, loadedManifestData);
  }

  /**
   * Load and process all the manifests.
   * @param manifestFiles list of manifest files.
   * @throws IOException failure to load/parse/queue
   */
  private void processAllManifests(
      final RemoteIterator<FileStatus> manifestFiles) throws IOException {

    trackDurationOfInvocation(getIOStatistics(), OP_LOAD_ALL_MANIFESTS, () ->
        TaskPool.foreach(manifestFiles)
            .executeWith(getIOProcessors())
            .stopOnFailure()
            .run(this::processOneManifest));
  }

  /**
   * Method invoked to process one manifest.
   * @param status file to process.
   * @throws IOException failure to load/parse/queue
   */
  private void processOneManifest(FileStatus status)
      throws IOException {
    updateAuditContext(OP_LOAD_ALL_MANIFESTS);

    TaskManifest manifest = fetchTaskManifest(status);
    progress();

    // update the directories
    final int created = coalesceDirectories(manifest);
    final String attemptID = manifest.getTaskAttemptID();
    LOG.debug("{}: task attempt {} added {} directories",
        getName(), attemptID, created);

    // add to the summary.
    summaryInfo.add(manifest);

    // clear the manifest extra data so if
    // blocked waiting for queue capacity,
    // memory use is reduced.
    manifest.setIOStatistics(null);
    manifest.getExtraData().clear();

    // queue those files.
    final boolean enqueued = entryWriter.enqueue(manifest.getFilesToCommit());
    if (!enqueued) {
      LOG.warn("{}: Failed to write manifest for task {}",
          getName(), attemptID);
      throw new EntryWriteException(attemptID);
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
     * Capacity for queue between manifest loader and the writers.
     */
    private final int queueCapacity;

    /**
     * Arguments.
     * @param entrySequenceFile path to local file to create for storing entries
     * @param queueCapacity capacity of the queue
     */
    public Arguments(
        final File entrySequenceFile,
        final int queueCapacity) {
      this.entrySequenceFile = entrySequenceFile;
      this.queueCapacity = queueCapacity;
    }

    private Path getEntrySequenceData() {
      return new Path(entrySequenceFile.toURI());

    }
  }

  /**
   * Result of the stage.
   */
  public static final class Result {
    private final SummaryInfo summary;

    /**
     * Output of this stage to pass on to the subsequence stages.
     */
    private final LoadedManifestData loadedManifestData;

    /**
     * Result.
     * @param summary summary of jobs
     * @param loadedManifestData all loaded manifest data
     */
    public Result(
        final SummaryInfo summary,
        final LoadedManifestData loadedManifestData) {
      this.summary = summary;
      this.loadedManifestData = loadedManifestData;
    }

    public SummaryInfo getSummary() {
      return summary;
    }

    public LoadedManifestData getLoadedManifestData() {
      return loadedManifestData;
    }
  }

  /**
   * IOE to raise on queueing failure.
   */
  public static final class EntryWriteException extends IOException {

    private EntryWriteException(String taskId) {
      super("Failed to write manifest data for task "
          + taskId + "to local file");
    }
  }
  /**
   * Summary information.
   * Implementation note: atomic counters are used here to keep spotbugs quiet,
   * not because of any concurrency risks.
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
     * Task IDs.
     */
    private final List<String> taskAttemptIDs = new ArrayList<>();

    /**
     * How many manifests were loaded.
     */
    private AtomicLong manifestCount = new AtomicLong();

    /**
     * Total number of files to rename.
     */
    private AtomicLong fileCount = new AtomicLong();

    /**
     * Total number of directories which may need
     * to be created.
     * As there is no dedup, this is likely to be
     * a (major) overestimate.
     */
    private AtomicLong directoryCount = new AtomicLong();

    /**
     * Total amount of data to be committed.
     */
    private AtomicLong totalFileSize = new AtomicLong();

    /**
     * Get the IOStatistics.
     * @return aggregate IOStatistics
     */
    @Override
    public IOStatisticsSnapshot getIOStatistics() {
      return iostatistics;
    }

    public long getFileCount() {
      return fileCount.get();
    }

    public long getDirectoryCount() {
      return directoryCount.get();
    }

    public long getTotalFileSize() {
      return totalFileSize.get();
    }

    public long getManifestCount() {
      return manifestCount.get();
    }

    public List<String> getTaskIDs() {
      return taskIDs;
    }

    public List<String> getTaskAttemptIDs() {
      return taskAttemptIDs;
    }

    /**
     * Add all statistics; synchronized.
     * @param manifest manifest to add.
     */
    public synchronized void add(TaskManifest manifest) {
      manifestCount.incrementAndGet();
      iostatistics.aggregate(manifest.getIOStatistics());
      fileCount.addAndGet(manifest.getFilesToCommit().size());
      directoryCount.addAndGet(manifest.getDestDirectories().size());
      totalFileSize.addAndGet(manifest.getTotalFileSize());
      taskIDs.add(manifest.getTaskID());
      taskAttemptIDs.add(manifest.getTaskAttemptID());
    }

    /**
     * To String includes all summary info except statistics.
     * @return string value
     */
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "SummaryInfo{");
      sb.append("manifestCount=").append(getManifestCount());
      sb.append(", fileCount=").append(getFileCount());
      sb.append(", directoryCount=").append(getDirectoryCount());
      sb.append(", totalFileSize=").append(
          byteCountToDisplaySize(getTotalFileSize()));
      sb.append('}');
      return sb.toString();
    }
  }
}
