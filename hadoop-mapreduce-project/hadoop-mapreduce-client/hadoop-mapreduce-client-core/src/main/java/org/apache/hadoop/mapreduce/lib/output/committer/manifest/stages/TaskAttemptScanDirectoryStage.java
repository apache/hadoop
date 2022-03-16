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
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.EntryStatus;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_DEPTH_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_FILE_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_FILE_SIZE_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SCAN_DIRECTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createTaskManifest;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.maybeAddIOStatistics;

/**
 * Stage to scan a directory tree and build a task manifest.
 * This is executed by the task committer.
 */
public final class TaskAttemptScanDirectoryStage
    extends AbstractJobOrTaskStage<Void, TaskManifest> {

  private static final Logger LOG = LoggerFactory.getLogger(
      TaskAttemptScanDirectoryStage.class);

  public TaskAttemptScanDirectoryStage(
      final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SCAN_DIRECTORY, false);
  }

  /**
   * Build the Manifest.
   * @return the manifest
   * @throws IOException failure.
   */
  @Override
  protected TaskManifest executeStage(final Void arguments)
      throws IOException {

    final Path taskAttemptDir = getRequiredTaskAttemptDir();
    final TaskManifest manifest = createTaskManifest(getStageConfig());

    LOG.info("{}: scanning directory {}",
        getName(), taskAttemptDir);

    final int depth = scanDirectoryTree(manifest,
        taskAttemptDir,
        getDestinationDir(),
        0, true);
    List<FileEntry> filesToCommit = manifest.getFilesToCommit();
    LongSummaryStatistics fileSummary = filesToCommit.stream()
        .mapToLong(FileEntry::getSize)
        .summaryStatistics();
    long fileDataSize = fileSummary.getSum();
    long fileCount = fileSummary.getCount();
    int dirCount = manifest.getDestDirectories().size();
    LOG.info("{}: directory {} contained {} file(s); data size {}",
        getName(),
        taskAttemptDir,
        fileCount,
        fileDataSize);
    LOG.info("{}: Directory count = {}; maximum depth {}",
        getName(),
        dirCount,
        depth);
    // add statistics about the task output which, when aggregated, provides
    // insight into structure of job, task skew, etc.
    IOStatisticsStore iostats = getIOStatistics();
    iostats.addSample(COMMITTER_TASK_DIRECTORY_COUNT_MEAN, dirCount);
    iostats.addSample(COMMITTER_TASK_DIRECTORY_DEPTH_MEAN, depth);
    iostats.addSample(COMMITTER_TASK_FILE_COUNT_MEAN, fileCount);
    iostats.addSample(COMMITTER_TASK_FILE_SIZE_MEAN, fileDataSize);

    return manifest;
  }

  /**
   * Recursively scan a directory tree.
   * The manifest will contain all files to rename
   * (source and dest) and directories to create.
   * All files are processed before any of the subdirs are.
   * This helps in statistics gathering.
   * There's some optimizations which could be done with async
   * fetching of the iterators of those subdirs, but as this
   * is generally off-critical path then that "enhancement"
   * can be postponed until data suggests this needs improvement.
   * @param manifest manifest to update
   * @param srcDir dir to scan
   * @param destDir destination directory
   * @param depth depth from the task attempt dir.
   * @param parentDirExists does the parent dir exist?
   * @return the maximum depth of child directories
   * @throws IOException IO failure.
   */
  private int scanDirectoryTree(
      TaskManifest manifest,
      Path srcDir,
      Path destDir,
      int depth,
      boolean parentDirExists) throws IOException {

    // generate some task progress in case directory scanning is very slow.
    progress();

    int maxDepth = 0;
    int files = 0;
    boolean dirExists = parentDirExists;
    List<FileStatus> subdirs = new ArrayList<>();
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Task Attempt %s source dir %s, dest dir %s",
        getTaskAttemptId(), srcDir, destDir)) {

      // list the directory. This may block until the listing is complete,
      // or, if the FS does incremental or asynchronous fetching,
      // then the next()/hasNext() call will block for the results
      // unless turned off, ABFS does to this async
      final RemoteIterator<FileStatus> listing = listStatusIterator(srcDir);

      // when the FS (especially ABFS) does an asyn fetch of the listing,
      // we can probe for the status of the destination dir while that
      // page is being fetched.
      // probe for and add the dest dir entry for all but
      // the base dir

      if (depth > 0) {
        final EntryStatus status;
        if (parentDirExists) {
          final FileStatus destDirStatus = getFileStatusOrNull(destDir);
          status = EntryStatus.toEntryStatus(destDirStatus);
          dirExists = destDirStatus != null;
        } else {
          // if there is no parent dir, then there is no need to look
          // for this directory -report it as missing automatically.
          status = EntryStatus.not_found;
        }
        manifest.addDirectory(DirEntry.dirEntry(
            destDir,
            status,
            depth));
      }

      // process the listing; this is where abfs will block
      // to wait the result of the list call.
      while (listing.hasNext()) {
        final FileStatus st = listing.next();
        if (st.isFile()) {
          // this is a file, so add to the list of files to commit.
          files++;
          final FileEntry entry = fileEntry(st, destDir);
          manifest.addFileToCommit(entry);
          LOG.debug("To rename: {}", entry);
        } else {
          if (st.isDirectory()) {
            // will need to scan this directory too.
            subdirs.add(st);
          } else {
            // some other object. ignoring
            LOG.info("Ignoring FS object {}", st);
          }
        }
      }
      // add any statistics provided by the listing.
      maybeAddIOStatistics(getIOStatistics(), listing);
    }

    // now scan the subdirectories
    LOG.debug("{}: Number of subdirectories under {} found: {}; file count {}",
        getName(), srcDir, subdirs.size(), files);

    for (FileStatus st : subdirs) {
      Path destSubDir = new Path(destDir, st.getPath().getName());
      final int d = scanDirectoryTree(manifest,
          st.getPath(),
          destSubDir,
          depth + 1,
          dirExists);
      maxDepth = Math.max(maxDepth, d);
    }

    return 1 + maxDepth;
  }

}
