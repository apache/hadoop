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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.mapred.RetriableFileCopyCommand.CopyReadException;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_UPDATE_MOD_TIME_DEFAULT;

/**
 * Mapper class that executes the DistCp copy operation.
 * Implements the o.a.h.mapreduce.Mapper interface.
 */
public class CopyMapper extends Mapper<Text, CopyListingFileStatus, Text, Text> {

  /**
   * Hadoop counters for the DistCp CopyMapper.
   * (These have been kept identical to the old DistCp,
   * for backward compatibility.)
   */
  public static enum Counter {
    COPY,         // Number of files received by the mapper for copy.
    DIR_COPY,     // Number of directories received by the mapper for copy.
    SKIP,         // Number of files skipped.
    FAIL,         // Number of files that failed to be copied.
    BYTESCOPIED,  // Number of bytes actually copied by the copy-mapper, total.
    BYTESEXPECTED,// Number of bytes expected to be copied.
    BYTESFAILED,  // Number of bytes that failed to be copied.
    BYTESSKIPPED, // Number of bytes that were skipped from copy.
    SLEEP_TIME_MS, // Time map slept while trying to honor bandwidth cap.
    BANDWIDTH_IN_BYTES, // Effective transfer rate in B/s.
  }

  /**
   * Indicate the action for each file
   */
  static enum FileAction {
    SKIP,         // Skip copying the file since it's already in the target FS
    APPEND,       // Only need to append new data to the file in the target FS 
    OVERWRITE,    // Overwrite the whole file
  }

  /**
   * Indicates the checksum comparison result.
   */
  public enum ChecksumComparison {
    TRUE,           // checksum comparison is compatible and true.
    FALSE,          // checksum comparison is compatible and false.
    INCOMPATIBLE,   // checksum comparison is not compatible.
  }

  private static Logger LOG = LoggerFactory.getLogger(CopyMapper.class);

  private Configuration conf;

  private boolean syncFolders = false;
  private boolean ignoreFailures = false;
  private boolean skipCrc = false;
  private boolean overWrite = false;
  private boolean append = false;
  private boolean verboseLog = false;
  private boolean directWrite = false;
  private boolean useModTimeToUpdate;
  private boolean useFastCopy = false;
  private EnumSet<FileAttribute> preserve = EnumSet.noneOf(FileAttribute.class);

  private FileSystem targetFS = null;
  private Path targetWorkPath = null;
  private long startEpoch;
  private long totalBytesCopied = 0;

  /**
   * Implementation of the Mapper::setup() method. This extracts the DistCp-
   * options specified in the Job's configuration, to set up the Job.
   * @param context Mapper's context.
   * @throws IOException On IO failure.
   * @throws InterruptedException If the job is interrupted.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();

    syncFolders = conf.getBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false);
    ignoreFailures = conf.getBoolean(DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false);
    skipCrc = conf.getBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(), false);
    overWrite = conf.getBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(), false);
    append = conf.getBoolean(DistCpOptionSwitch.APPEND.getConfigLabel(), false);
    verboseLog = conf.getBoolean(
        DistCpOptionSwitch.VERBOSE_LOG.getConfigLabel(), false);
    preserve = DistCpUtils.unpackAttributes(conf.get(DistCpOptionSwitch.
        PRESERVE_STATUS.getConfigLabel()));
    directWrite = conf.getBoolean(
        DistCpOptionSwitch.DIRECT_WRITE.getConfigLabel(), false);
    useModTimeToUpdate =
        conf.getBoolean(DistCpConstants.CONF_LABEL_UPDATE_MOD_TIME,
            CONF_LABEL_UPDATE_MOD_TIME_DEFAULT);
    useFastCopy = conf.getBoolean(DistCpOptionSwitch.USE_FASTCOPY.getConfigLabel(), false);

    targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
    Path targetFinalPath = new Path(conf.get(
            DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    targetFS = targetFinalPath.getFileSystem(conf);

    try {
      overWrite = overWrite || targetFS.getFileStatus(targetFinalPath).isFile();
    } catch (FileNotFoundException ignored) {
    }

    startEpoch = System.currentTimeMillis();
  }

  /**
   * Implementation of the Mapper::map(). Does the copy.
   * @param relPath The target path.
   * @param sourceFileStatus The source path.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void map(Text relPath, CopyListingFileStatus sourceFileStatus,
          Context context) throws IOException, InterruptedException {
    Path sourcePath = sourceFileStatus.getPath();
    if (LOG.isDebugEnabled())
      LOG.debug("DistCpMapper::map(): Received " + sourcePath + ", " + relPath);

    Path target = new Path(targetWorkPath.makeQualified(targetFS.getUri(),
                          targetFS.getWorkingDirectory()) + relPath.toString());

    EnumSet<DistCpOptions.FileAttribute> fileAttributes
            = getFileAttributeSettings(context);
    final boolean preserveRawXattrs = context.getConfiguration().getBoolean(
        DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);

    final String description = "Copying " + sourcePath + " to " + target;
    context.setStatus(description);

    LOG.info(description);

    try {
      CopyListingFileStatus sourceCurrStatus;
      FileSystem sourceFS;
      FileStatus sourceStatus;
      try {
        sourceFS = sourcePath.getFileSystem(conf);
        sourceStatus = sourceFS.getFileStatus(sourcePath);
        final boolean preserveXAttrs =
            fileAttributes.contains(FileAttribute.XATTR);
        sourceCurrStatus = DistCpUtils.toCopyListingFileStatusHelper(sourceFS,
            sourceStatus,
            fileAttributes.contains(FileAttribute.ACL),
            preserveXAttrs, preserveRawXattrs,
            sourceFileStatus.getChunkOffset(),
            sourceFileStatus.getChunkLength());
      } catch (FileNotFoundException e) {
        throw new IOException(new RetriableFileCopyCommand.CopyReadException(e));
      }

      FileStatus targetStatus = null;

      try {
        targetStatus = targetFS.getFileStatus(target);
      } catch (FileNotFoundException ignore) {
        if (LOG.isDebugEnabled())
          LOG.debug("Path could not be found: " + target, ignore);
      }

      if (targetStatus != null &&
          (targetStatus.isDirectory() != sourceCurrStatus.isDirectory())) {
        throw new IOException("Can't replace " + target + ". Target is " +
            getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
      }

      if (sourceCurrStatus.isDirectory()) {
        createTargetDirsWithRetry(description, target, context, sourceStatus,
            sourceFS);
        return;
      }

      FileAction action = checkUpdate(sourceFS, sourceCurrStatus, target,
          targetStatus);

      Path tmpTarget = target;
      if (action == FileAction.SKIP) {
        LOG.info("Skipping copy of " + sourceCurrStatus.getPath()
                 + " to " + target);
        updateSkipCounters(context, sourceCurrStatus);
        context.write(null, new Text("SKIP: " + sourceCurrStatus.getPath()));

        if (verboseLog) {
          context.write(null,
              new Text("FILE_SKIPPED: source=" + sourceFileStatus.getPath()
              + ", size=" + sourceFileStatus.getLen() + " --> "
              + "target=" + target + ", size=" + (targetStatus == null ?
                  0 : targetStatus.getLen())));
        }
      } else {
        if (sourceCurrStatus.isSplit()) {
          tmpTarget = DistCpUtils.getSplitChunkPath(target, sourceCurrStatus);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("copying " + sourceCurrStatus + " " + tmpTarget);
        }
        copyFileWithRetry(description, sourceCurrStatus, tmpTarget,
            targetStatus, context, action, fileAttributes, sourceStatus);
      }
      DistCpUtils.preserve(target.getFileSystem(conf), tmpTarget,
          sourceCurrStatus, fileAttributes, preserveRawXattrs);
    } catch (IOException exception) {
      handleFailures(exception, sourceFileStatus, target, context);
    }
  }

  private String getFileType(CopyListingFileStatus fileStatus) {
    if (null == fileStatus) {
      return "N/A";
    }
    return fileStatus.isDirectory() ? "dir" : "file";
  }

  private String getFileType(FileStatus fileStatus) {
    if (null == fileStatus) {
      return "N/A";
    }
    return fileStatus.isDirectory() ? "dir" : "file";
  }

  static EnumSet<DistCpOptions.FileAttribute>
          getFileAttributeSettings(Mapper.Context context) {
    String attributeString = context.getConfiguration().get(
            DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel());
    return DistCpUtils.unpackAttributes(attributeString);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private void copyFileWithRetry(String description,
      CopyListingFileStatus sourceFileStatus, Path target,
      FileStatus targrtFileStatus, Context context, FileAction action,
      EnumSet<FileAttribute> fileAttributes, FileStatus sourceStatus)
      throws IOException, InterruptedException {
    long bytesCopied;
    try {
      if (!useFastCopy) {
        bytesCopied =
            (Long) new RetriableFileCopyCommand(skipCrc, description, action, directWrite).execute(
                sourceFileStatus, target, context, fileAttributes, sourceStatus);
      } else {
        bytesCopied = (Long) new RetriableFileFastCopyCommand(skipCrc, description, action,
            directWrite).execute(sourceFileStatus, target, context, fileAttributes, sourceStatus);
      }
    } catch (Exception e) {
      context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
      throw new IOException("File copy failed: " + sourceFileStatus.getPath() +
          " --> " + target, e);
    }
    incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
    incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
    incrementCounter(context, Counter.COPY, 1);
    totalBytesCopied += bytesCopied;

    if (verboseLog) {
      context.write(null,
          new Text("FILE_COPIED: source=" + sourceFileStatus.getPath() + ","
          + " size=" + sourceFileStatus.getLen() + " --> "
          + "target=" + target + ", size=" + (targrtFileStatus == null ?
              0 : targrtFileStatus.getLen())));
    }
  }

  private void createTargetDirsWithRetry(String description, Path target,
      Context context, FileStatus sourceStatus, FileSystem sourceFS) throws IOException {
    try {
      new RetriableDirectoryCreateCommand(description).execute(target, context,
          sourceStatus, sourceFS);
    } catch (Exception e) {
      throw new IOException("mkdir failed for " + target, e);
    }
    incrementCounter(context, Counter.DIR_COPY, 1);
  }

  private static void updateSkipCounters(Context context,
      CopyListingFileStatus sourceFile) {
    incrementCounter(context, Counter.SKIP, 1);
    incrementCounter(context, Counter.BYTESSKIPPED, sourceFile.getLen());

  }

  private void handleFailures(IOException exception,
      CopyListingFileStatus sourceFileStatus, Path target, Context context)
      throws IOException, InterruptedException {
    LOG.error("Failure in copying " + sourceFileStatus.getPath() +
        (sourceFileStatus.isSplit()? ","
            + " offset=" + sourceFileStatus.getChunkOffset()
            + " chunkLength=" + sourceFileStatus.getChunkLength()
            : "") +
        " to " + target, exception);

    if (ignoreFailures &&
        ExceptionUtils.indexOfType(exception, CopyReadException.class) != -1) {
      incrementCounter(context, Counter.FAIL, 1);
      incrementCounter(context, Counter.BYTESFAILED, sourceFileStatus.getLen());
      context.write(null, new Text("FAIL: " + sourceFileStatus.getPath() + " - " +
          StringUtils.stringifyException(exception)));
    }
    else
      throw exception;
  }

  private static void incrementCounter(Context context, Counter counter,
                                       long value) {
    context.getCounter(counter).increment(value);
  }

  private FileAction checkUpdate(FileSystem sourceFS,
      CopyListingFileStatus source, Path target, FileStatus targetFileStatus)
      throws IOException {
    if (targetFileStatus != null && !overWrite) {
      if (canSkip(sourceFS, source, targetFileStatus)) {
        return FileAction.SKIP;
      } else if (append) {
        long targetLen = targetFileStatus.getLen();
        if (targetLen < source.getLen()) {
          FileChecksum sourceChecksum = sourceFS.getFileChecksum(
              source.getPath(), targetLen);
          if (sourceChecksum != null
              && sourceChecksum.equals(targetFS.getFileChecksum(target))) {
            // We require that the checksum is not null. Thus currently only
            // DistributedFileSystem is supported
            return FileAction.APPEND;
          }
        }
      }
    }
    return FileAction.OVERWRITE;
  }

  private boolean canSkip(FileSystem sourceFS, CopyListingFileStatus source,
      FileStatus target) throws IOException {
    if (!syncFolders) {
      return true;
    }
    boolean sameLength = target.getLen() == source.getLen();
    boolean sameBlockSize = source.getBlockSize() == target.getBlockSize()
        || !preserve.contains(FileAttribute.BLOCKSIZE);
    // Skip the copy if a 0 size file is being copied.
    if (sameLength && source.getLen() == 0) {
      return true;
    }
    // If the src and target file have same size and block size, we would
    // check if the checkCrc flag is enabled or not. If enabled, and the
    // modTime comparison is enabled then return true if target file is older
    // than the source file, since this indicates that the target file is
    // recently updated and the source is not changed more recently than the
    // update, we can skip the copy else we would copy.
    // If skipCrc flag is disabled, we would check the checksum comparison
    // which is an enum representing 3 values, of which if the comparison
    // returns NOT_COMPATIBLE, we'll try to check modtime again, else return
    // the result of checksum comparison which are compatible(true or false).
    //
    // Note: Different object stores can have different checksum algorithms
    // resulting in no checksum comparison that results in return true
    // always, having the modification time enabled can help in these
    // scenarios to not incorrectly skip a copy. Refer: HADOOP-18596.

    if (sameLength && sameBlockSize) {
      if (skipCrc) {
        return maybeUseModTimeToCompare(source, target);
      } else {
        ChecksumComparison checksumComparison = DistCpUtils
            .checksumsAreEqual(sourceFS, source.getPath(), null,
                targetFS, target.getPath(), source.getLen());
        LOG.debug("Result of checksum comparison between src {} and target "
            + "{} : {}", source, target, checksumComparison);
        if (checksumComparison.equals(ChecksumComparison.INCOMPATIBLE)) {
          return maybeUseModTimeToCompare(source, target);
        }
        // if skipCrc is disabled and checksumComparison is compatible we
        // need not check the mod time.
        return checksumComparison.equals(ChecksumComparison.TRUE);
      }
    }
    return false;
  }

  /**
   * If the mod time comparison is enabled, check the mod time else return
   * false.
   * Comparison: If the target file perceives to have greater or equal mod time
   * (older) than the source file, we can assume that there has been no new
   * changes that occurred in the source file, hence we should return true to
   * skip the copy of the file.
   *
   * @param source Source fileStatus.
   * @param target Target fileStatus.
   * @return boolean representing result of modTime check.
   */
  private boolean maybeUseModTimeToCompare(
      CopyListingFileStatus source, FileStatus target) {
    if (useModTimeToUpdate) {
      return source.getModificationTime() <= target.getModificationTime();
    }
    // if we cannot check mod time, return true (skip the copy).
    return true;
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    super.cleanup(context);
    long secs = (System.currentTimeMillis() - startEpoch) / 1000;
    incrementCounter(context, Counter.BANDWIDTH_IN_BYTES,
        totalBytesCopied / ((secs == 0 ? 1 : secs)));
  }
}
