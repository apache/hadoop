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

package org.apache.hadoop.tools;

import org.apache.hadoop.util.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.util.ProducerConsumer;
import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;
import org.apache.hadoop.tools.util.WorkRequestProcessor;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.LinkedList;
import java.util.Stack;

import static org.apache.hadoop.tools.DistCpConstants
        .HDFS_RESERVED_RAW_DIRECTORY_NAME;

/**
 * The SimpleCopyListing is responsible for making the exhaustive list of
 * all files/directories under its specified list of input-paths.
 * These are written into the specified copy-listing file.
 * Note: The SimpleCopyListing doesn't handle wild-cards in the input-paths.
 */
public class SimpleCopyListing extends CopyListing {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleCopyListing.class);

  public static final int DEFAULT_FILE_STATUS_SIZE = 1000;
  public static final boolean DEFAULT_RANDOMIZE_FILE_LISTING = true;

  private long totalPaths = 0;
  private long totalDirs = 0;
  private long totalBytesToCopy = 0;
  private int numListstatusThreads = 1;
  private final int fileStatusLimit;
  private final boolean randomizeFileListing;
  private final int maxRetries = 3;
  private CopyFilter copyFilter;
  private DistCpSync distCpSync;
  private final Random rnd = new Random();

  /**
   * Protected constructor, to initialize configuration.
   *
   * @param configuration The input configuration, with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  protected SimpleCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    numListstatusThreads = getConf().getInt(
        DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS,
        DistCpConstants.DEFAULT_LISTSTATUS_THREADS);
    fileStatusLimit = Math.max(1, getConf()
        .getInt(DistCpConstants.CONF_LABEL_SIMPLE_LISTING_FILESTATUS_SIZE,
        DEFAULT_FILE_STATUS_SIZE));
    randomizeFileListing = getConf().getBoolean(
        DistCpConstants.CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES,
        DEFAULT_RANDOMIZE_FILE_LISTING);
    LOG.debug(
        "numListstatusThreads={}, fileStatusLimit={}, randomizeFileListing={}",
        numListstatusThreads, fileStatusLimit, randomizeFileListing);
    copyFilter = CopyFilter.getCopyFilter(getConf());
    copyFilter.initialize();
  }

  @VisibleForTesting
  protected SimpleCopyListing(Configuration configuration,
                              Credentials credentials,
                              int numListstatusThreads,
                              int fileStatusLimit,
                              boolean randomizeFileListing) {
    super(configuration, credentials);
    this.numListstatusThreads = numListstatusThreads;
    this.fileStatusLimit = Math.max(1, fileStatusLimit);
    this.randomizeFileListing = randomizeFileListing;
  }

  protected SimpleCopyListing(Configuration configuration,
                              Credentials credentials,
                              DistCpSync distCpSync) {
    this(configuration, credentials);
    this.distCpSync = distCpSync;
  }

  @Override
  protected void validatePaths(DistCpContext context)
      throws IOException, InvalidInputException {

    Path targetPath = context.getTargetPath();
    FileSystem targetFS = targetPath.getFileSystem(getConf());
    boolean targetExists = false;
    boolean targetIsFile = false;
    try {
      targetIsFile = targetFS.getFileStatus(targetPath).isFile();
      targetExists = true;
    } catch (FileNotFoundException ignored) {
    }
    targetPath = targetFS.makeQualified(targetPath);
    final boolean targetIsReservedRaw =
        Path.getPathWithoutSchemeAndAuthority(targetPath).toString().
            startsWith(HDFS_RESERVED_RAW_DIRECTORY_NAME);

    //If target is a file, then source has to be single file
    if (targetIsFile) {
      if (context.getSourcePaths().size() > 1) {
        throw new InvalidInputException("Multiple source being copied to a file: " +
            targetPath);
      }

      Path srcPath = context.getSourcePaths().get(0);
      FileSystem sourceFS = srcPath.getFileSystem(getConf());
      if (!sourceFS.isFile(srcPath)) {
        throw new InvalidInputException("Cannot copy " + srcPath +
            ", which is not a file to " + targetPath);
      }
    }

    if (context.shouldAtomicCommit() && targetExists) {
      throw new InvalidInputException("Target path for atomic-commit already exists: " +
        targetPath + ". Cannot atomic-commit to pre-existing target-path.");
    }

    for (Path path: context.getSourcePaths()) {
      FileSystem fs = path.getFileSystem(getConf());
      if (!fs.exists(path)) {
        throw new InvalidInputException(path + " doesn't exist");
      }
      if (Path.getPathWithoutSchemeAndAuthority(path).toString().
          startsWith(HDFS_RESERVED_RAW_DIRECTORY_NAME)) {
        if (!targetIsReservedRaw) {
          final String msg = "The source path '" + path + "' starts with " +
              HDFS_RESERVED_RAW_DIRECTORY_NAME + " but the target path '" +
              targetPath + "' does not. Either all or none of the paths must " +
              "have this prefix.";
          throw new InvalidInputException(msg);
        }
      } else if (targetIsReservedRaw) {
        final String msg = "The target path '" + targetPath + "' starts with " +
                HDFS_RESERVED_RAW_DIRECTORY_NAME + " but the source path '" +
                path + "' does not. Either all or none of the paths must " +
                "have this prefix.";
        throw new InvalidInputException(msg);
      }
    }

    if (targetIsReservedRaw) {
      context.setPreserveRawXattrs(true);
      getConf().setBoolean(DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, true);
    }

    /* This is requires to allow map tasks to access each of the source
       clusters. This would retrieve the delegation token for each unique
       file system and add them to job's private credential store
     */
    Credentials credentials = getCredentials();
    if (credentials != null) {
      Path[] inputPaths = context.getSourcePaths()
          .toArray(new Path[1]);
      TokenCache.obtainTokensForNamenodes(credentials, inputPaths, getConf());
    }
  }

  @Override
  protected void doBuildListing(Path pathToListingFile,
                                DistCpContext context) throws IOException {
    if (context.shouldUseSnapshotDiff()) {
      doBuildListingWithSnapshotDiff(getWriter(pathToListingFile), context);
    } else {
      doBuildListing(getWriter(pathToListingFile), context);
    }
  }

  /**
   * Get a path with its scheme and authority.
   */
  private Path getPathWithSchemeAndAuthority(Path path) throws IOException {
    FileSystem fs= path.getFileSystem(getConf());
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      scheme = fs.getUri().getScheme();
    }

    String authority = path.toUri().getAuthority();
    if (authority == null) {
      authority = fs.getUri().getAuthority();
    }

    return new Path(scheme, authority, makeQualified(path).toUri().getPath());
  }

  /**
   * Write a single file/directory to the sequence file.
   * @param fileListWriter the list for holding processed results
   * @param sourceRoot the source dir path for copyListing
   * @param path add the given path to the file list.
   * @param context The DistCp context with associated input options
   * @throws IOException if it fails to add it to the fileList
   */
  private void addToFileListing(SequenceFile.Writer fileListWriter,
      Path sourceRoot, Path path, DistCpContext context) throws IOException {
    sourceRoot = getPathWithSchemeAndAuthority(sourceRoot);
    path = getPathWithSchemeAndAuthority(path);
    path = makeQualified(path);

    FileSystem sourceFS = sourceRoot.getFileSystem(getConf());
    FileStatus fileStatus = sourceFS.getFileStatus(path);
    final boolean preserveAcls = context.shouldPreserve(FileAttribute.ACL);
    final boolean preserveXAttrs = context.shouldPreserve(FileAttribute.XATTR);
    final boolean preserveRawXAttrs = context.shouldPreserveRawXattrs();
    LinkedList<CopyListingFileStatus> fileCopyListingStatus =
        DistCpUtils.toCopyListingFileStatus(sourceFS, fileStatus,
            preserveAcls, preserveXAttrs, preserveRawXAttrs,
            context.getBlocksPerChunk());
    writeToFileListingRoot(fileListWriter, fileCopyListingStatus,
        sourceRoot, context);
  }

  /**
   * Build a copy list based on the snapshot diff report.
   *
   * Any file/directory changed or created will be in the list. Deleted
   * files/directories will not be in the list, since they are handled by
   * {@link org.apache.hadoop.tools.DistCpSync#sync}. An item can be
   * created/modified and renamed, in which case, the target path is put
   * into the list.
   * @param fileListWriter the list for holding processed results
   * @param context The DistCp context with associated input options
   * @throws IOException if unable to construct the fileList
   */
  @VisibleForTesting
  protected void doBuildListingWithSnapshotDiff(
      SequenceFile.Writer fileListWriter, DistCpContext context)
      throws IOException {
    ArrayList<DiffInfo> diffList = distCpSync.prepareDiffListForCopyListing();
    Path sourceRoot = context.getSourcePaths().get(0);
    FileSystem sourceFS = sourceRoot.getFileSystem(getConf());
    boolean traverseDirectory = getConf().getBoolean(
        DistCpConstants.CONF_LABEL_DIFF_COPY_LISTING_TRAVERSE_DIRECTORY, true);

    try {
      List<FileStatusInfo> fileStatuses = Lists.newArrayList();
      for (DiffInfo diff : diffList) {
        // add snapshot paths prefix
        diff.setTarget(
            new Path(context.getSourcePaths().get(0), diff.getTarget()));
        if (diff.getType() == SnapshotDiffReport.DiffType.MODIFY) {
          addToFileListing(fileListWriter,
              sourceRoot, diff.getTarget(), context);
        } else if (diff.getType() == SnapshotDiffReport.DiffType.CREATE) {
          addCreateDiffsToFileListing(fileListWriter, context, sourceRoot,
              sourceFS, fileStatuses, diff, traverseDirectory);
        }
      }
      if (randomizeFileListing) {
        writeToFileListing(fileStatuses, fileListWriter);
      }
      fileListWriter.close();
      fileListWriter = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, fileListWriter);
    }
  }

  /**
   * Handle create Diffs and add to the copyList.
   * If the path is a directory, iterate it recursively and add the paths
   * to the result copyList.
   *
   * @param fileListWriter the list for holding processed results
   * @param context The DistCp context with associated input options
   * @param sourceRoot The rootDir of the source snapshot
   * @param sourceFS the source Filesystem
   * @param fileStatuses store the result fileStatuses to add to the copyList
   * @param diff the SnapshotDiff report
   * @param traverseDirectory traverse directory recursively and add paths to the copyList
   * @throws IOException
   */
  private void addCreateDiffsToFileListing(SequenceFile.Writer fileListWriter,
      DistCpContext context, Path sourceRoot, FileSystem sourceFS,
      List<FileStatusInfo> fileStatuses, DiffInfo diff, boolean traverseDirectory) throws IOException {
    addToFileListing(fileListWriter, sourceRoot, diff.getTarget(), context);

    if (traverseDirectory) {
      FileStatus sourceStatus = sourceFS.getFileStatus(diff.getTarget());
      if (sourceStatus.isDirectory()) {
        LOG.debug("Adding source dir for traverse: {}", sourceStatus.getPath());

        HashSet<String> excludeList =
            distCpSync.getTraverseExcludeList(diff.getSource(),
                context.getSourcePaths().get(0));

        ArrayList<FileStatus> sourceDirs = new ArrayList<>();
        sourceDirs.add(sourceStatus);

        new TraverseDirectory(fileListWriter, sourceFS, sourceDirs, sourceRoot,
            context, excludeList, fileStatuses).traverseDirectory();
      }
    }
  }

  /**
   * Collect the list of 
   *   {@literal <sourceRelativePath, sourceFileStatus>}
   * to be copied and write to the sequence file. In essence, any file or
   * directory that need to be copied or sync-ed is written as an entry to the
   * sequence file, with the possible exception of the source root:
   *     when either -update (sync) or -overwrite switch is specified, and if
   *     the the source root is a directory, then the source root entry is not 
   *     written to the sequence file, because only the contents of the source
   *     directory need to be copied in this case.
   * See {@link org.apache.hadoop.tools.util.DistCpUtils#getRelativePath} for
   *     how relative path is computed.
   * See computeSourceRootPath method for how the root path of the source is
   *     computed.
   * @param fileListWriter
   * @param context The distcp context with associated input options
   * @throws IOException if unable to construct the fileList
   */
  @VisibleForTesting
  protected void doBuildListing(SequenceFile.Writer fileListWriter,
      DistCpContext context) throws IOException {
    if (context.getNumListstatusThreads() > 0) {
      numListstatusThreads = context.getNumListstatusThreads();
    }

    try {
      List<FileStatusInfo> statusList = Lists.newArrayList();
      for (Path path: context.getSourcePaths()) {
        FileSystem sourceFS = path.getFileSystem(getConf());
        final boolean preserveAcls =
            context.shouldPreserve(FileAttribute.ACL);
        final boolean preserveXAttrs =
            context.shouldPreserve(FileAttribute.XATTR);
        final boolean preserveRawXAttrs =
            context.shouldPreserveRawXattrs();
        path = makeQualified(path);

        FileStatus rootStatus = sourceFS.getFileStatus(path);
        Path sourcePathRoot = computeSourceRootPath(rootStatus, context);

        FileStatus[] sourceFiles = sourceFS.listStatus(path);
        boolean explore = (sourceFiles != null && sourceFiles.length > 0);
        if (!explore || rootStatus.isDirectory()) {
          LinkedList<CopyListingFileStatus> rootCopyListingStatus =
              DistCpUtils.toCopyListingFileStatus(sourceFS, rootStatus,
                  preserveAcls, preserveXAttrs, preserveRawXAttrs,
                  context.getBlocksPerChunk());
          writeToFileListingRoot(fileListWriter, rootCopyListingStatus,
              sourcePathRoot, context);
        }
        if (explore) {
          ArrayList<FileStatus> sourceDirs = new ArrayList<FileStatus>();
          for (FileStatus sourceStatus: sourceFiles) {
            LOG.debug("Recording source-path: {} for copy.",
                sourceStatus.getPath());
            LinkedList<CopyListingFileStatus> sourceCopyListingStatus =
                DistCpUtils.toCopyListingFileStatus(sourceFS, sourceStatus,
                    preserveAcls && sourceStatus.isDirectory(),
                    preserveXAttrs && sourceStatus.isDirectory(),
                    preserveRawXAttrs && sourceStatus.isDirectory(),
                    context.getBlocksPerChunk());
            for (CopyListingFileStatus fs : sourceCopyListingStatus) {
              if (randomizeFileListing) {
                addToFileListing(statusList,
                    new FileStatusInfo(fs, sourcePathRoot), fileListWriter);
              } else {
                writeToFileListing(fileListWriter, fs, sourcePathRoot);
              }
            }
            if (sourceStatus.isDirectory()) {
              LOG.debug("Adding source dir for traverse: {}",
                  sourceStatus.getPath());
              sourceDirs.add(sourceStatus);
            }
          }
          new TraverseDirectory(fileListWriter, sourceFS, sourceDirs,
              sourcePathRoot, context, null, statusList).traverseDirectory();
        }
      }
      if (randomizeFileListing) {
        writeToFileListing(statusList, fileListWriter);
      }
      fileListWriter.close();
      printStats();
      LOG.info("Build file listing completed.");
      fileListWriter = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, fileListWriter);
    }
  }

  private void addToFileListing(List<FileStatusInfo> fileStatusInfoList,
      FileStatusInfo statusInfo, SequenceFile.Writer fileListWriter)
      throws IOException {
    fileStatusInfoList.add(statusInfo);
    if (fileStatusInfoList.size() > fileStatusLimit) {
      writeToFileListing(fileStatusInfoList, fileListWriter);
    }
  }

  @VisibleForTesting
  void setSeedForRandomListing(long seed) {
    this.rnd.setSeed(seed);
  }

  private void writeToFileListing(List<FileStatusInfo> fileStatusInfoList,
      SequenceFile.Writer fileListWriter) throws IOException {
    /**
     * In cloud storage systems, it is possible to get region hotspot.
     * Shuffling paths can avoid such cases and also ensure that
     * some mappers do not get lots of similar paths.
     */
    Collections.shuffle(fileStatusInfoList, rnd);
    for (FileStatusInfo fileStatusInfo : fileStatusInfoList) {
      LOG.debug("Adding {}", fileStatusInfo.fileStatus.getPath());
      writeToFileListing(fileListWriter, fileStatusInfo.fileStatus,
          fileStatusInfo.sourceRootPath);
    }
    LOG.debug("Number of paths written to fileListing={}",
        fileStatusInfoList.size());
    fileStatusInfoList.clear();
  }

  private static class FileStatusInfo {
    private CopyListingFileStatus fileStatus;
    private Path sourceRootPath;

    FileStatusInfo(CopyListingFileStatus fileStatus, Path sourceRootPath) {
      this.fileStatus = fileStatus;
      this.sourceRootPath = sourceRootPath;
    }
  }

  private Path computeSourceRootPath(FileStatus sourceStatus,
      DistCpContext context) throws IOException {

    Path target = context.getTargetPath();
    FileSystem targetFS = target.getFileSystem(getConf());
    final boolean targetPathExists = context.isTargetPathExists();

    boolean solitaryFile = context.getSourcePaths().size() == 1
                                                && !sourceStatus.isDirectory();

    if (solitaryFile) {
      if (!targetPathExists || targetFS.isFile(target)) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    } else {
      boolean specialHandling =
          (context.getSourcePaths().size() == 1 &&
              !targetPathExists) ||
              context.shouldSyncFolder() ||
              context.shouldOverwrite();

      if ((specialHandling && sourceStatus.isDirectory()) ||
          sourceStatus.getPath().isRoot()) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    }
  }

  /**
   * Provide an option to skip copy of a path, Allows for exclusion
   * of files such as {@link org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#SUCCEEDED_FILE_NAME}
   * @param path - Path being considered for copy while building the file listing
   * @return - True if the path should be considered for copy, false otherwise
   */
  protected boolean shouldCopy(Path path) {
    return copyFilter.shouldCopy(path);
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return totalBytesToCopy;
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return totalPaths;
  }

  private Path makeQualified(Path path) throws IOException {
    final FileSystem fs = path.getFileSystem(getConf());
    return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  private SequenceFile.Writer getWriter(Path pathToListFile) throws IOException {
    FileSystem fs = pathToListFile.getFileSystem(getConf());
    fs.delete(pathToListFile, false);
    return SequenceFile.createWriter(getConf(),
            SequenceFile.Writer.file(pathToListFile),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
            SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
  }

  /*
   *  Private class to implement WorkRequestProcessor interface. It processes
   *  each directory (represented by FileStatus item) and returns a list of all
   *  file-system objects in that directory (files and directories). In case of
   *  retriable exceptions it increments retry counter and returns the same
   *  directory for later retry.
   */
  private static class FileStatusProcessor
      implements WorkRequestProcessor<FileStatus, FileStatus[]> {
    private FileSystem fileSystem;
    private HashSet<String> excludeList;

    public FileStatusProcessor(FileSystem fileSystem,
                               HashSet<String> excludeList) {
      this.fileSystem = fileSystem;
      this.excludeList = excludeList;
    }

    /**
     * Get FileStatuses for a given path.
     * Exclude the some renamed FileStatuses since they are already handled by
     * {@link org.apache.hadoop.tools.DistCpSync#sync}.
     * @return an array of file status
     */
    private FileStatus[] getFileStatus(Path path) throws IOException {
      FileStatus[] fileStatuses = fileSystem.listStatus(path);
      if (excludeList != null && excludeList.size() > 0) {
        ArrayList<FileStatus> fileStatusList = new ArrayList<>();
        for(FileStatus status : fileStatuses) {
          if (!excludeList.contains(status.getPath().toUri().getPath())) {
            fileStatusList.add(status);
          }
        }
        fileStatuses = fileStatusList.toArray(
                new FileStatus[fileStatusList.size()]);
      }
      return fileStatuses;
    }

    /*
     *  Processor for FileSystem.listStatus().
     *
     *  @param workRequest  Input work item that contains FileStatus item which
     *                      is a parent directory we want to list.
     *  @return Outputs WorkReport<FileStatus[]> with a list of objects in the
     *          directory (array of objects, empty if parent directory is
     *          empty). In case of intermittent exception we increment retry
     *          counter and return the list containing the parent directory).
     */
    public WorkReport<FileStatus[]> processItem(
        WorkRequest<FileStatus> workRequest) {
      FileStatus parent = workRequest.getItem();
      int retry = workRequest.getRetry();
      WorkReport<FileStatus[]> result = null;
      try {
        if (retry > 0) {
          int sleepSeconds = 2;
          for (int i = 1; i < retry; i++) {
            sleepSeconds *= 2;
          }
          try {
            Thread.sleep(1000 * sleepSeconds);
          } catch (InterruptedException ie) {
            LOG.debug("Interrupted while sleeping in exponential backoff.");
          }
        }
        result = new WorkReport<FileStatus[]>(getFileStatus(parent.getPath()),
                retry, true);
      } catch (FileNotFoundException fnf) {
        LOG.error("FileNotFoundException exception in listStatus: {}",
            fnf.getMessage());
        result = new WorkReport<FileStatus[]>(new FileStatus[0], retry, true,
                                              fnf);
      } catch (Exception e) {
        LOG.error("Exception in listStatus. Will send for retry.");
        FileStatus[] parentList = new FileStatus[1];
        parentList[0] = parent;
        result = new WorkReport<FileStatus[]>(parentList, retry + 1, false, e);
      }
      return result;
    }
  }

  private void printStats() {
    LOG.info("Paths (files+dirs) cnt = {}; dirCnt = {}", totalPaths, totalDirs);
  }

  private void maybePrintStats() {
    if (totalPaths % 100000 == 0) {
      printStats();
    }
  }

  private void writeToFileListingRoot(SequenceFile.Writer fileListWriter,
      LinkedList<CopyListingFileStatus> fileStatus, Path sourcePathRoot,
      DistCpContext context) throws IOException {
    boolean syncOrOverwrite = context.shouldSyncFolder() ||
        context.shouldOverwrite();
    boolean skipRootPath = syncOrOverwrite && !context.shouldUpdateRoot();
    for (CopyListingFileStatus fs : fileStatus) {
      if (fs.getPath().equals(sourcePathRoot) &&
          fs.isDirectory() && skipRootPath) {
        // Skip the root-paths when skipRootPath (syncOrOverwrite and
        // update root directory is not a must).
        LOG.debug("Skip {}", fs.getPath());
        return;
      }
      writeToFileListing(fileListWriter, fs, sourcePathRoot);
    }
  }

  private void writeToFileListing(SequenceFile.Writer fileListWriter,
                                  CopyListingFileStatus fileStatus,
                                  Path sourcePathRoot) throws IOException {
    LOG.debug("REL PATH: {}, FULL PATH: {}",
        DistCpUtils.getRelativePath(sourcePathRoot, fileStatus.getPath()),
        fileStatus.getPath());

    if (!shouldCopy(fileStatus.getPath())) {
      return;
    }

    fileListWriter.append(getFileListingKey(sourcePathRoot, fileStatus),
        getFileListingValue(fileStatus));
    fileListWriter.sync();

    if (!fileStatus.isDirectory()) {
      totalBytesToCopy += fileStatus.getSizeToCopy();
    } else {
      totalDirs++;
    }
    totalPaths++;
    maybePrintStats();
  }

  /**
   * A utility class to traverse a directory.
   */
  private final class TraverseDirectory {

    private SequenceFile.Writer fileListWriter;
    private FileSystem sourceFS;
    private ArrayList<FileStatus> sourceDirs;
    private Path sourcePathRoot;
    private DistCpContext context;
    private HashSet<String> excludeList;
    private List<FileStatusInfo> fileStatuses;
    private final boolean preserveAcls;
    private final boolean preserveXAttrs;
    private final boolean preserveRawXattrs;

    private TraverseDirectory(SequenceFile.Writer fileListWriter,
        FileSystem sourceFS, ArrayList<FileStatus> sourceDirs,
        Path sourcePathRoot, DistCpContext context, HashSet<String> excludeList,
        List<FileStatusInfo> fileStatuses) {
      this.fileListWriter = fileListWriter;
      this.sourceFS = sourceFS;
      this.sourceDirs = sourceDirs;
      this.sourcePathRoot = sourcePathRoot;
      this.context = context;
      this.excludeList = excludeList;
      this.fileStatuses = fileStatuses;
      this.preserveAcls = context.shouldPreserve(FileAttribute.ACL);
      this.preserveXAttrs = context.shouldPreserve(FileAttribute.XATTR);
      this.preserveRawXattrs = context.shouldPreserveRawXattrs();
    }

    public void traverseDirectory() throws IOException {
      if (context.shouldUseIterator()) {
        try (DurationInfo ignored = new DurationInfo(LOG,
            "Building listing using iterator mode for %s", sourcePathRoot)) {
          traverseDirectoryLegacy();
        }
      } else {
        try (DurationInfo ignored = new DurationInfo(LOG,
            "Building listing using multi threaded approach for %s",
            sourcePathRoot)) {
          traverseDirectoryMultiThreaded();
        }
      }
    }

    public void traverseDirectoryMultiThreaded() throws IOException {
      assert numListstatusThreads > 0;

      LOG.debug("Starting thread pool of {} listStatus workers.",
          numListstatusThreads);

      ProducerConsumer<FileStatus, FileStatus[]> workers =
          new ProducerConsumer<FileStatus, FileStatus[]>(numListstatusThreads);
      try {
        for (int i = 0; i < numListstatusThreads; i++) {
          workers.addWorker(
              new FileStatusProcessor(sourcePathRoot.getFileSystem(getConf()),
                  excludeList));
        }

        for (FileStatus status : sourceDirs) {
          workers.put(new WorkRequest<FileStatus>(status, 0));
        }

        while (workers.hasWork()) {
          try {
            WorkReport<FileStatus[]> workResult = workers.take();
            int retry = workResult.getRetry();
            for (FileStatus child : workResult.getItem()) {
              LOG.debug("Recording source-path: {} for copy.", child.getPath());
              boolean isChildDirectory = child.isDirectory();
              if (workResult.getSuccess()) {
                LinkedList<CopyListingFileStatus> childCopyListingStatus =
                    DistCpUtils.toCopyListingFileStatus(sourceFS, child,
                        preserveAcls && isChildDirectory,
                        preserveXAttrs && isChildDirectory,
                        preserveRawXattrs && isChildDirectory,
                        context.getBlocksPerChunk());

                for (CopyListingFileStatus fs : childCopyListingStatus) {
                  if (randomizeFileListing) {
                    addToFileListing(fileStatuses,
                        new FileStatusInfo(fs, sourcePathRoot), fileListWriter);
                  } else {
                    writeToFileListing(fileListWriter, fs, sourcePathRoot);
                  }
                }
              }
              if (retry < maxRetries) {
                if (isChildDirectory) {
                  LOG.debug("Traversing into source dir: {}", child.getPath());
                  workers.put(new WorkRequest<FileStatus>(child, retry));
                }
              } else {
                LOG.error("Giving up on {} after {} retries.", child.getPath(),
                    retry);
              }
            }
          } catch (InterruptedException ie) {
            LOG.error("Could not get item from childQueue. Retrying...");
          }
        }
      } finally {
        workers.shutdown();
      }
    }

    private void traverseDirectoryLegacy() throws IOException {
      Stack<FileStatus> pathStack = new Stack<FileStatus>();
      for (FileStatus fs : sourceDirs) {
        if (excludeList == null || !excludeList
            .contains(fs.getPath().toUri().getPath())) {
          pathStack.add(fs);
        }
      }
      while (!pathStack.isEmpty()) {
        prepareListing(pathStack.pop().getPath());
      }
    }

    private void prepareListing(Path path) throws IOException {
      LOG.debug("Recording source-path: {} for copy.", path);
      RemoteIterator<FileStatus> listStatus = RemoteIterators
          .filteringRemoteIterator(sourceFS.listStatusIterator(path),
              i -> excludeList == null || !excludeList
                  .contains(i.getPath().toUri().getPath()));
      while (listStatus.hasNext()) {
        FileStatus child = listStatus.next();
        LinkedList<CopyListingFileStatus> childCopyListingStatus = DistCpUtils
            .toCopyListingFileStatus(sourceFS, child,
                preserveAcls && child.isDirectory(),
                preserveXAttrs && child.isDirectory(),
                preserveRawXattrs && child.isDirectory(),
                context.getBlocksPerChunk());
        for (CopyListingFileStatus fs : childCopyListingStatus) {
          if (randomizeFileListing) {
            addToFileListing(fileStatuses,
                new FileStatusInfo(fs, sourcePathRoot), fileListWriter);
          } else {
            writeToFileListing(fileListWriter, fs, sourcePathRoot);
          }
        }
        if (child.isDirectory()) {
          LOG.debug("Traversing into source dir: {}", child.getPath());
          prepareListing(child.getPath());
        }
      }
      IOStatisticsLogging
          .logIOStatisticsAtDebug(LOG, "RemoteIterator Statistics: {}",
              listStatus);
    }
  }
}
