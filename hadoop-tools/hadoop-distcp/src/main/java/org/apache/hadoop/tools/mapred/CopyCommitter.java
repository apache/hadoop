/**
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

package org.apache.hadoop.tools.mapred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpContext;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.GlobbedCopyListing;
import org.apache.hadoop.tools.util.DistCpUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.tools.DistCpConstants.*;

/**
 * The CopyCommitter class is DistCp's OutputCommitter implementation. It is
 * responsible for handling the completion/cleanup of the DistCp run.
 * Specifically, it does the following:
 *  1. Cleanup of the meta-folder (where DistCp maintains its file-list, etc.)
 *  2. Preservation of user/group/replication-factor on any directories that
 *     have been copied. (Files are taken care of in their map-tasks.)
 *  3. Atomic-move of data from the temporary work-folder to the final path
 *     (if atomic-commit was opted for).
 *  4. Deletion of files from the target that are missing at source (if opted for).
 *  5. Cleanup of any partially copied files, from previous, failed attempts.
 */
public class CopyCommitter extends FileOutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(CopyCommitter.class);

  private final TaskAttemptContext taskAttemptContext;
  private boolean syncFolder = false;
  private boolean overwrite = false;
  private boolean targetPathExists = true;
  private boolean ignoreFailures = false;
  private boolean skipCrc = false;
  private int blocksPerChunk = 0;
  private boolean updateRoot = false;

  /**
   * Create a output committer
   *
   * @param outputPath the job's output path
   * @param context    the task's context
   * @throws IOException - Exception if any
   */
  public CopyCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    blocksPerChunk = context.getConfiguration().getInt(
        DistCpOptionSwitch.BLOCKS_PER_CHUNK.getConfigLabel(), 0);
    LOG.debug("blocks per chunk {}", blocksPerChunk);
    skipCrc = context.getConfiguration().getBoolean(
        DistCpOptionSwitch.SKIP_CRC.getConfigLabel(), false);
    LOG.debug("skip CRC is {}", skipCrc);
    this.taskAttemptContext = context;
  }

  /** {@inheritDoc} */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    syncFolder = conf.getBoolean(DistCpConstants.CONF_LABEL_SYNC_FOLDERS, false);
    overwrite = conf.getBoolean(DistCpConstants.CONF_LABEL_OVERWRITE, false);
    updateRoot =
        conf.getBoolean(CONF_LABEL_UPDATE_ROOT, false);
    targetPathExists = conf.getBoolean(
        DistCpConstants.CONF_LABEL_TARGET_PATH_EXISTS, true);
    ignoreFailures = conf.getBoolean(
        DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false);

    if (blocksPerChunk > 0) {
      concatFileChunks(conf);
    }

    super.commitJob(jobContext);

    cleanupTempFiles(jobContext);

    try {
      if (conf.getBoolean(DistCpConstants.CONF_LABEL_DELETE_MISSING, false)) {
        deleteMissing(conf);
      } else if (conf.getBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, false)) {
        commitData(conf);
      } else if (conf.get(CONF_LABEL_TRACK_MISSING) != null) {
        // save missing information to a directory
        trackMissing(conf);
      }
      // for HDFS-14621, should preserve status after -delete
      String attributes = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
      final boolean preserveRawXattrs = conf.getBoolean(
              DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);
      if ((attributes != null && !attributes.isEmpty()) || preserveRawXattrs) {
        preserveFileAttributesForDirectories(conf);
      }
      taskAttemptContext.setStatus("Commit Successful");
    }
    finally {
      cleanup(conf);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void abortJob(JobContext jobContext,
                       JobStatus.State state) throws IOException {
    try {
      super.abortJob(jobContext, state);
    } finally {
      cleanupTempFiles(jobContext);
      cleanup(jobContext.getConfiguration());
    }
  }

  private void cleanupTempFiles(JobContext context) {
    Configuration conf = context.getConfiguration();

    final boolean directWrite = conf.getBoolean(
        DistCpOptionSwitch.DIRECT_WRITE.getConfigLabel(), false);
    if (directWrite) {
      return;
    }

    try {
      Path targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
      FileSystem targetFS = targetWorkPath.getFileSystem(conf);

      String jobId = context.getJobID().toString();
      deleteAttemptTempFiles(targetWorkPath, targetFS, jobId);
      deleteAttemptTempFiles(targetWorkPath.getParent(), targetFS, jobId);
    } catch (Throwable t) {
      LOG.warn("Unable to cleanup temp files", t);
    }
  }

  private void deleteAttemptTempFiles(Path targetWorkPath,
                                      FileSystem targetFS,
                                      String jobId) throws IOException {
    if (targetWorkPath == null) {
      return;
    }

    FileStatus[] tempFiles = targetFS.globStatus(
        new Path(targetWorkPath, ".distcp.tmp." + jobId.replaceAll("job","attempt") + "*"));

    if (tempFiles != null && tempFiles.length > 0) {
      for (FileStatus file : tempFiles) {
        LOG.info("Cleaning up " + file.getPath());
        targetFS.delete(file.getPath(), false);
      }
    }
  }

  /**
   * Cleanup meta folder and other temporary files
   *
   * @param conf - Job Configuration
   */
  private void cleanup(Configuration conf) {
    Path metaFolder = new Path(conf.get(DistCpConstants.CONF_LABEL_META_FOLDER));
    try {
      FileSystem fs = metaFolder.getFileSystem(conf);
      LOG.info("Cleaning up temporary work folder: " + metaFolder);
      fs.delete(metaFolder, true);
    } catch (IOException ignore) {
      LOG.error("Exception encountered ", ignore);
    }
  }

  private boolean isFileNotFoundException(IOException e) {
    if (e instanceof FileNotFoundException) {
      return true;
    }

    if (e instanceof RemoteException) {
      return ((RemoteException)e).unwrapRemoteException()
          instanceof FileNotFoundException;
    }

    return false;
  }

  /**
   * Concat chunk files for the same file into one.
   * Iterate through copy listing, identify chunk files for the same file,
   * concat them into one.
   */
  private void concatFileChunks(Configuration conf) throws IOException {

    LOG.info("concat file chunks ...");

    String spath = conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH);
    if (spath == null || spath.isEmpty()) {
      return;
    }
    Path sourceListing = new Path(spath);
    SequenceFile.Reader sourceReader = new SequenceFile.Reader(conf,
                                      SequenceFile.Reader.file(sourceListing));
    Path targetRoot =
        new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));

    try {
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      Text srcRelPath = new Text();
      CopyListingFileStatus lastFileStatus = null;
      LinkedList<Path> allChunkPaths = new LinkedList<Path>();

      // Iterate over every source path that was copied.
      while (sourceReader.next(srcRelPath, srcFileStatus)) {
        if (srcFileStatus.isDirectory()) {
          continue;
        }
        Path targetFile = new Path(targetRoot.toString() + "/" + srcRelPath);
        Path targetFileChunkPath =
            DistCpUtils.getSplitChunkPath(targetFile, srcFileStatus);
        if (LOG.isDebugEnabled()) {
          LOG.debug("  add " + targetFileChunkPath + " to concat.");
        }
        allChunkPaths.add(targetFileChunkPath);
        if (srcFileStatus.getChunkOffset() + srcFileStatus.getChunkLength()
            == srcFileStatus.getLen()) {
          // This is the last chunk of the splits, consolidate allChunkPaths
          try {
            concatFileChunks(conf, srcFileStatus.getPath(), targetFile,
                allChunkPaths, srcFileStatus);
          } catch (IOException e) {
            // If the concat failed because a chunk file doesn't exist,
            // then we assume that the CopyMapper has skipped copying this
            // file, and we ignore the exception here.
            // If a chunk file should have been created but it was not, then
            // the CopyMapper would have failed.
            if (!isFileNotFoundException(e)) {
              String emsg = "Failed to concat chunk files for " + targetFile;
              if (!ignoreFailures) {
                throw new IOException(emsg, e);
              } else {
                LOG.warn(emsg, e);
              }
            }
          }
          allChunkPaths.clear();
          lastFileStatus = null;
        } else {
          if (lastFileStatus == null) {
            lastFileStatus = new CopyListingFileStatus(srcFileStatus);
          } else {
            // Two neighboring chunks have to be consecutive ones for the same
            // file, for them to be merged
            if (!srcFileStatus.getPath().equals(lastFileStatus.getPath()) ||
                srcFileStatus.getChunkOffset() !=
                (lastFileStatus.getChunkOffset() +
                lastFileStatus.getChunkLength())) {
              String emsg = "Inconsistent sequence file: current " +
                  "chunk file " + srcFileStatus + " doesnt match prior " +
                  "entry " + lastFileStatus;
              if (!ignoreFailures) {
                throw new IOException(emsg);
              } else {
                LOG.warn(emsg + ", skipping concat this set.");
              }
            } else {
              lastFileStatus.setChunkOffset(srcFileStatus.getChunkOffset());
              lastFileStatus.setChunkLength(srcFileStatus.getChunkLength());
            }
          }
        }
      }
    } finally {
      IOUtils.closeStream(sourceReader);
    }
  }

  // This method changes the target-directories' file-attributes (owner,
  // user/group permissions, etc.) based on the corresponding source directories.
  private void preserveFileAttributesForDirectories(Configuration conf)
      throws IOException {
    String attrSymbols = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    final boolean syncOrOverwrite = syncFolder || overwrite;

    LOG.info("About to preserve attributes: " + attrSymbols);

    EnumSet<FileAttribute> attributes = DistCpUtils.unpackAttributes(attrSymbols);
    final boolean preserveRawXattrs =
        conf.getBoolean(DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);

    Path sourceListing = new Path(conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    FileSystem clusterFS = sourceListing.getFileSystem(conf);
    SequenceFile.Reader sourceReader = new SequenceFile.Reader(conf,
                                      SequenceFile.Reader.file(sourceListing));
    long totalLen = clusterFS.getFileStatus(sourceListing).getLen();
    // For Atomic Copy the Final & Work Path are different & atomic copy has
    // already moved it to final path.
    Path targetRoot =
            new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));

    long preservedEntries = 0;
    try {
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      Text srcRelPath = new Text();

      // Iterate over every source path that was copied.
      while (sourceReader.next(srcRelPath, srcFileStatus)) {
        // File-attributes for files are set at the time of copy,
        // in the map-task.
        if (! srcFileStatus.isDirectory()) continue;

        Path targetFile = new Path(targetRoot.toString() + "/" + srcRelPath);
        //
        // Skip the root folder when skipRoot is true.
        //
        boolean skipRoot = syncOrOverwrite && !updateRoot;
        if (targetRoot.equals(targetFile) && skipRoot) {
          continue;
        }

        FileSystem targetFS = targetFile.getFileSystem(conf);
        DistCpUtils.preserve(targetFS, targetFile, srcFileStatus, attributes,
            preserveRawXattrs);

        taskAttemptContext.progress();
        taskAttemptContext.setStatus("Preserving status on directory entries. [" +
            sourceReader.getPosition() * 100 / totalLen + "%]");
      }
    } finally {
      IOUtils.closeStream(sourceReader);
    }
    LOG.info("Preserved status on " + preservedEntries + " dir entries on target");
  }

  /**
   * Track all the missing files by saving the listings to the tracking
   * directory.
   * This is the same as listing phase of the
   * {@link #deleteMissing(Configuration)} operation.
   * @param conf configuration to read options from, and for FS instantiation.
   * @throws IOException IO failure
   */
  private void trackMissing(Configuration conf) throws IOException {
    // destination directory for all output files
    Path trackDir = new Path(
        conf.get(DistCpConstants.CONF_LABEL_TRACK_MISSING));

    // where is the existing source listing?
    Path sourceListing = new Path(
        conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    LOG.info("Tracking file changes to directory {}", trackDir);

    // the destination path is under the track directory
    Path sourceSortedListing = new Path(trackDir,
        DistCpConstants.SOURCE_SORTED_FILE);
    LOG.info("Source listing {}", sourceSortedListing);

    DistCpUtils.sortListing(conf, sourceListing, sourceSortedListing);

    // Similarly, create the listing of target-files. Sort alphabetically.
    // target listing will be deleted after the sort
    Path targetListing = new Path(trackDir, TARGET_LISTING_FILE);
    Path sortedTargetListing = new Path(trackDir, TARGET_SORTED_FILE);
    // list the target
    listTargetFiles(conf, targetListing, sortedTargetListing);
    LOG.info("Target listing {}", sortedTargetListing);

    targetListing.getFileSystem(conf).delete(targetListing, false);
  }

  /**
   * Deletes "extra" files and directories from the target, if they're not
   * available at the source.
   * @param conf configuration to read options from, and for FS instantiation.
   * @throws IOException IO failure
   */
  private void deleteMissing(Configuration conf) throws IOException {
    LOG.info("-delete option is enabled. About to remove entries from " +
        "target that are missing in source");
    long listingStart = System.currentTimeMillis();

    // Sort the source-file listing alphabetically.
    Path sourceListing = new Path(conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    FileSystem clusterFS = sourceListing.getFileSystem(conf);
    Path sortedSourceListing = DistCpUtils.sortListing(conf, sourceListing);
    long sourceListingCompleted = System.currentTimeMillis();
    LOG.info("Source listing completed in {}",
        formatDuration(sourceListingCompleted - listingStart));

    // Similarly, create the listing of target-files. Sort alphabetically.
    Path targetListing = new Path(sourceListing.getParent(), "targetListing.seq");
    Path sortedTargetListing = new Path(targetListing.toString() + "_sorted");

    Path targetFinalPath = listTargetFiles(conf,
        targetListing, sortedTargetListing);
    long totalLen = clusterFS.getFileStatus(sortedTargetListing).getLen();

    SequenceFile.Reader sourceReader = new SequenceFile.Reader(conf,
                                 SequenceFile.Reader.file(sortedSourceListing));
    SequenceFile.Reader targetReader = new SequenceFile.Reader(conf,
                                 SequenceFile.Reader.file(sortedTargetListing));

    // Walk both source and target file listings.
    // Delete all from target that doesn't also exist on source.
    long deletionStart = System.currentTimeMillis();
    LOG.info("Destination listing completed in {}",
        formatDuration(deletionStart - sourceListingCompleted));

    long deletedEntries = 0;
    long filesDeleted = 0;
    long missingDeletes = 0;
    long failedDeletes = 0;
    long skippedDeletes = 0;
    long deletedDirectories = 0;
    // this is an arbitrary constant.
    final DeletedDirTracker tracker = new DeletedDirTracker(1000);
    try {
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      Text srcRelPath = new Text();
      CopyListingFileStatus trgtFileStatus = new CopyListingFileStatus();
      Text trgtRelPath = new Text();

      final FileSystem targetFS = targetFinalPath.getFileSystem(conf);
      boolean showProgress;
      boolean srcAvailable = sourceReader.next(srcRelPath, srcFileStatus);
      while (targetReader.next(trgtRelPath, trgtFileStatus)) {
        // Skip sources that don't exist on target.
        while (srcAvailable && trgtRelPath.compareTo(srcRelPath) > 0) {
          srcAvailable = sourceReader.next(srcRelPath, srcFileStatus);
        }
        Path targetEntry = trgtFileStatus.getPath();
        LOG.debug("Comparing {} and {}",
            srcFileStatus.getPath(), targetEntry);

        if (srcAvailable && trgtRelPath.equals(srcRelPath)) continue;

        // Target doesn't exist at source. Try to delete it.
        if (tracker.shouldDelete(trgtFileStatus)) {
          showProgress = true;
          try {
            if (targetFS.delete(targetEntry, true)) {
              // the delete worked. Unless the file is actually missing, this is the
              LOG.info("Deleted " + targetEntry + " - missing at source");
              deletedEntries++;
              if (trgtFileStatus.isDirectory()) {
                deletedDirectories++;
              } else {
                filesDeleted++;
              }
            } else {
              // delete returned false.
              // For all the filestores which implement the FS spec properly,
              // this means "the file wasn't there".
              // so track but don't worry about it.
              LOG.info("delete({}) returned false ({})",
                  targetEntry, trgtFileStatus);
              missingDeletes++;
            }
          } catch (IOException e) {
            if (!ignoreFailures) {
              throw e;
            } else {
              // failed to delete, but ignoring errors. So continue
              LOG.info("Failed to delete {}, ignoring exception {}",
                  targetEntry, e.toString());
              LOG.debug("Failed to delete {}", targetEntry, e);
              // count and break out the loop
              failedDeletes++;
            }
          }
        } else {
          LOG.debug("Skipping deletion of {}", targetEntry);
          skippedDeletes++;
          showProgress = false;
        }
        if (showProgress) {
          // update progress if there's been any FS IO/files deleted.
          taskAttemptContext.progress();
          taskAttemptContext.setStatus("Deleting removed files from target. [" +
              targetReader.getPosition() * 100 / totalLen + "%]");
        }
      }
      // if the FS toString() call prints statistics, they get logged here
      LOG.info("Completed deletion of files from {}", targetFS);
    } finally {
      IOUtils.closeStream(sourceReader);
      IOUtils.closeStream(targetReader);
    }
    long deletionEnd = System.currentTimeMillis();
    long deletedFileCount = deletedEntries - deletedDirectories;
    LOG.info("Deleted from target: files: {} directories: {};"
            + " skipped deletions {}; deletions already missing {};"
            + " failed deletes {}",
        deletedFileCount, deletedDirectories, skippedDeletes,
        missingDeletes, failedDeletes);
    LOG.info("Number of tracked deleted directories {}", tracker.size());
    LOG.info("Duration of deletions: {}",
        formatDuration(deletionEnd - deletionStart));
    LOG.info("Total duration of deletion operation: {}",
        formatDuration(deletionEnd - listingStart));
  }

  /**
   * Take a duration and return a human-readable duration of
   * hours:minutes:seconds.millis.
   * @param duration to process
   * @return a string for logging.
   */
  private String formatDuration(long duration) {

    long seconds = duration > 0 ? (duration / 1000) : 0;
    long minutes = (seconds / 60);
    long hours = (minutes / 60);
    return String.format("%d:%02d:%02d.%03d",
        hours, minutes % 60, seconds % 60, duration % 1000);
  }

  /**
   * Build a listing of the target files, sorted and unsorted.
   * @param conf configuration to work with
   * @param targetListing target listing
   * @param sortedTargetListing sorted version of the listing
   * @return the target path of the operation
   * @throws IOException IO failure.
   */
  private Path listTargetFiles(final Configuration conf,
      final Path targetListing,
      final Path sortedTargetListing) throws IOException {
    CopyListing target = new GlobbedCopyListing(new Configuration(conf), null);
    Path targetFinalPath = new Path(
        conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    List<Path> targets = new ArrayList<>(1);
    targets.add(targetFinalPath);
    //
    // Set up options to be the same from the CopyListing.buildListing's
    // perspective, so to collect similar listings as when doing the copy
    //
    // thread count is picked up from the job
    int threads = conf.getInt(DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS,
        DistCpConstants.DEFAULT_LISTSTATUS_THREADS);
    boolean useIterator =
        conf.getBoolean(DistCpConstants.CONF_LABEL_USE_ITERATOR, false);
    LOG.info("Scanning destination directory {} with thread count: {}",
        targetFinalPath, threads);
    DistCpOptions options = new DistCpOptions.Builder(targets, targetFinalPath)
        .withOverwrite(overwrite)
        .withSyncFolder(syncFolder)
        .withNumListstatusThreads(threads)
        .withUseIterator(useIterator)
        .build();
    DistCpContext distCpContext = new DistCpContext(options);
    distCpContext.setTargetPathExists(targetPathExists);

    target.buildListing(targetListing, distCpContext);
    DistCpUtils.sortListing(conf, targetListing, sortedTargetListing);
    return targetFinalPath;
  }

  private void commitData(Configuration conf) throws IOException {

    Path workDir = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
    Path finalDir = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    FileSystem targetFS = workDir.getFileSystem(conf);

    LOG.info("Atomic commit enabled. Moving " + workDir + " to " + finalDir);
    if (targetFS.exists(finalDir) && targetFS.exists(workDir)) {
      LOG.error("Pre-existing final-path found at: " + finalDir);
      throw new IOException("Target-path can't be committed to because it " +
          "exists at " + finalDir + ". Copied data is in temp-dir: " + workDir + ". ");
    }

    boolean result = targetFS.rename(workDir, finalDir);
    if (!result) {
      LOG.warn("Rename failed. Perhaps data already moved. Verifying...");
      result = targetFS.exists(finalDir) && !targetFS.exists(workDir);
    }
    if (result) {
      LOG.info("Data committed successfully to " + finalDir);
      taskAttemptContext.setStatus("Data committed successfully to " + finalDir);
    } else {
      LOG.error("Unable to commit data to " + finalDir);
      throw new IOException("Atomic commit failed. Temporary data in " + workDir +
        ", Unable to move to " + finalDir);
    }
  }

  /**
   * Concat the passed chunk files into one and rename it the targetFile.
   */
  private void concatFileChunks(Configuration conf, Path sourceFile,
                                Path targetFile, LinkedList<Path> allChunkPaths,
                                CopyListingFileStatus srcFileStatus)
      throws IOException {
    if (allChunkPaths.size() == 1) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("concat " + targetFile + " allChunkSize+ "
          + allChunkPaths.size());
    }
    FileSystem dstfs = targetFile.getFileSystem(conf);
    FileSystem srcfs = sourceFile.getFileSystem(conf);

    Path firstChunkFile = allChunkPaths.removeFirst();
    Path[] restChunkFiles = new Path[allChunkPaths.size()];
    allChunkPaths.toArray(restChunkFiles);
    if (LOG.isDebugEnabled()) {
      LOG.debug("concat: firstchunk: " + dstfs.getFileStatus(firstChunkFile));
      int i = 0;
      for (Path f : restChunkFiles) {
        LOG.debug("concat: other chunk: " + i + ": " + dstfs.getFileStatus(f));
        ++i;
      }
    }
    dstfs.concat(firstChunkFile, restChunkFiles);
    if (LOG.isDebugEnabled()) {
      LOG.debug("concat: result: " + dstfs.getFileStatus(firstChunkFile));
    }
    rename(dstfs, firstChunkFile, targetFile);
    DistCpUtils.compareFileLengthsAndChecksums(srcFileStatus.getLen(),
        srcfs, sourceFile, null, dstfs,
            targetFile, skipCrc, srcFileStatus.getLen());
  }

  /**
   * Rename tmp to dst on destFileSys.
   * @param destFileSys the file ssystem
   * @param tmp the source path
   * @param dst the destination path
   * @throws IOException if renaming failed
   */
  private static void rename(FileSystem destFileSys, Path tmp, Path dst)
      throws IOException {
    try {
      if (destFileSys.exists(dst)) {
        destFileSys.delete(dst, true);
      }
      destFileSys.rename(tmp, dst);
    } catch (IOException ioe) {
      throw new IOException("Fail to rename tmp file (=" + tmp
          + ") to destination file (=" + dst + ")", ioe);
    }
  }

}
