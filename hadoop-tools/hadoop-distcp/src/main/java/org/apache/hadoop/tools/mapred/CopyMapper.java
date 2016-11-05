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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private static Log LOG = LogFactory.getLog(CopyMapper.class);

  private Configuration conf;

  private boolean syncFolders = false;
  private boolean ignoreFailures = false;
  private boolean skipCrc = false;
  private boolean overWrite = false;
  private boolean append = false;
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
    preserve = DistCpUtils.unpackAttributes(conf.get(DistCpOptionSwitch.
        PRESERVE_STATUS.getConfigLabel()));

    targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
    Path targetFinalPath = new Path(conf.get(
            DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    targetFS = targetFinalPath.getFileSystem(conf);

    if (targetFS.exists(targetFinalPath) && targetFS.isFile(targetFinalPath)) {
      overWrite = true; // When target is an existing file, overwrite it.
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
      try {
        sourceFS = sourcePath.getFileSystem(conf);
        final boolean preserveXAttrs =
            fileAttributes.contains(FileAttribute.XATTR);
        sourceCurrStatus = DistCpUtils.toCopyListingFileStatus(sourceFS,
          sourceFS.getFileStatus(sourcePath),
          fileAttributes.contains(FileAttribute.ACL), 
          preserveXAttrs, preserveRawXattrs);
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

      if (targetStatus != null && (targetStatus.isDirectory() != sourceCurrStatus.isDirectory())) {
        throw new IOException("Can't replace " + target + ". Target is " +
            getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
      }

      if (sourceCurrStatus.isDirectory()) {
        createTargetDirsWithRetry(description, target, context);
        return;
      }

      FileAction action = checkUpdate(sourceFS, sourceCurrStatus, target, targetStatus);
      if (action == FileAction.SKIP) {
        LOG.info("Skipping copy of " + sourceCurrStatus.getPath()
                 + " to " + target);
        updateSkipCounters(context, sourceCurrStatus);
        context.write(null, new Text("SKIP: " + sourceCurrStatus.getPath()));
      } else {
        copyFileWithRetry(description, sourceCurrStatus, target, context,
            action, fileAttributes);
      }

      DistCpUtils.preserve(target.getFileSystem(conf), target, sourceCurrStatus,
          fileAttributes, preserveRawXattrs);
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

  private static EnumSet<DistCpOptions.FileAttribute>
          getFileAttributeSettings(Mapper.Context context) {
    String attributeString = context.getConfiguration().get(
            DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel());
    return DistCpUtils.unpackAttributes(attributeString);
  }

  private void copyFileWithRetry(String description,
      CopyListingFileStatus sourceFileStatus, Path target, Context context,
      FileAction action, EnumSet<DistCpOptions.FileAttribute> fileAttributes)
      throws IOException {
    long bytesCopied;
    try {
      bytesCopied = (Long) new RetriableFileCopyCommand(skipCrc, description,
          action).execute(sourceFileStatus, target, context, fileAttributes);
    } catch (Exception e) {
      context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
      throw new IOException("File copy failed: " + sourceFileStatus.getPath() +
          " --> " + target, e);
    }
    incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
    incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
    incrementCounter(context, Counter.COPY, 1);
    totalBytesCopied += bytesCopied;
  }

  private void createTargetDirsWithRetry(String description,
                   Path target, Context context) throws IOException {
    try {
      new RetriableDirectoryCreateCommand(description).execute(target, context);
    } catch (Exception e) {
      throw new IOException("mkdir failed for " + target, e);
    }
    incrementCounter(context, Counter.COPY, 1);
  }

  private static void updateSkipCounters(Context context,
      CopyListingFileStatus sourceFile) {
    incrementCounter(context, Counter.SKIP, 1);
    incrementCounter(context, Counter.BYTESSKIPPED, sourceFile.getLen());

  }

  private void handleFailures(IOException exception,
      CopyListingFileStatus sourceFileStatus, Path target, Context context)
      throws IOException, InterruptedException {
    LOG.error("Failure in copying " + sourceFileStatus.getPath() + " to " +
                target, exception);

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
    if (sameLength && sameBlockSize) {
      return skipCrc ||
          DistCpUtils.checksumsAreEqual(sourceFS, source.getPath(), null,
              targetFS, target.getPath());
    } else {
      return false;
    }
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
