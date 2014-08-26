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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;

import com.google.common.annotations.VisibleForTesting;

import java.io.*;
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
  private static final Log LOG = LogFactory.getLog(SimpleCopyListing.class);

  private long totalPaths = 0;
  private long totalBytesToCopy = 0;

  /**
   * Protected constructor, to initialize configuration.
   *
   * @param configuration The input configuration, with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  protected SimpleCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
  }

  @Override
  protected void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException {

    Path targetPath = options.getTargetPath();
    FileSystem targetFS = targetPath.getFileSystem(getConf());
    boolean targetIsFile = targetFS.isFile(targetPath);
    targetPath = targetFS.makeQualified(targetPath);
    final boolean targetIsReservedRaw =
        Path.getPathWithoutSchemeAndAuthority(targetPath).toString().
            startsWith(HDFS_RESERVED_RAW_DIRECTORY_NAME);

    //If target is a file, then source has to be single file
    if (targetIsFile) {
      if (options.getSourcePaths().size() > 1) {
        throw new InvalidInputException("Multiple source being copied to a file: " +
            targetPath);
      }

      Path srcPath = options.getSourcePaths().get(0);
      FileSystem sourceFS = srcPath.getFileSystem(getConf());
      if (!sourceFS.isFile(srcPath)) {
        throw new InvalidInputException("Cannot copy " + srcPath +
            ", which is not a file to " + targetPath);
      }
    }

    if (options.shouldAtomicCommit() && targetFS.exists(targetPath)) {
      throw new InvalidInputException("Target path for atomic-commit already exists: " +
        targetPath + ". Cannot atomic-commit to pre-existing target-path.");
    }

    for (Path path: options.getSourcePaths()) {
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
      options.preserveRawXattrs();
      getConf().setBoolean(DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, true);
    }

    /* This is requires to allow map tasks to access each of the source
       clusters. This would retrieve the delegation token for each unique
       file system and add them to job's private credential store
     */
    Credentials credentials = getCredentials();
    if (credentials != null) {
      Path[] inputPaths = options.getSourcePaths().toArray(new Path[1]);
      TokenCache.obtainTokensForNamenodes(credentials, inputPaths, getConf());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void doBuildListing(Path pathToListingFile, DistCpOptions options) throws IOException {
    doBuildListing(getWriter(pathToListingFile), options);
  }
  /**
   * Collect the list of 
   *   <sourceRelativePath, sourceFileStatus>
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
   * @param options
   * @throws IOException
   */
  @VisibleForTesting
  public void doBuildListing(SequenceFile.Writer fileListWriter,
      DistCpOptions options) throws IOException {
    try {
      for (Path path: options.getSourcePaths()) {
        FileSystem sourceFS = path.getFileSystem(getConf());
        final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
        final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
        final boolean preserveRawXAttrs = options.shouldPreserveRawXattrs();
        path = makeQualified(path);

        FileStatus rootStatus = sourceFS.getFileStatus(path);
        Path sourcePathRoot = computeSourceRootPath(rootStatus, options);

        FileStatus[] sourceFiles = sourceFS.listStatus(path);
        boolean explore = (sourceFiles != null && sourceFiles.length > 0);
        if (!explore || rootStatus.isDirectory()) {
          CopyListingFileStatus rootCopyListingStatus =
            DistCpUtils.toCopyListingFileStatus(sourceFS, rootStatus,
                preserveAcls, preserveXAttrs, preserveRawXAttrs);
          writeToFileListingRoot(fileListWriter, rootCopyListingStatus,
              sourcePathRoot, options);
        }
        if (explore) {
          for (FileStatus sourceStatus: sourceFiles) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Recording source-path: " + sourceStatus.getPath() + " for copy.");
            }
            CopyListingFileStatus sourceCopyListingStatus =
              DistCpUtils.toCopyListingFileStatus(sourceFS, sourceStatus,
                  preserveAcls && sourceStatus.isDirectory(),
                  preserveXAttrs && sourceStatus.isDirectory(),
                  preserveRawXAttrs && sourceStatus.isDirectory());
            writeToFileListing(fileListWriter, sourceCopyListingStatus,
                sourcePathRoot, options);

            if (isDirectoryAndNotEmpty(sourceFS, sourceStatus)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Traversing non-empty source dir: " + sourceStatus.getPath());
              }
              traverseNonEmptyDirectory(fileListWriter, sourceStatus, sourcePathRoot,
                  options);
            }
          }
        }
      }
      fileListWriter.close();
      fileListWriter = null;
    } finally {
      IOUtils.cleanup(LOG, fileListWriter);
    }
  }

  private Path computeSourceRootPath(FileStatus sourceStatus,
                                     DistCpOptions options) throws IOException {

    Path target = options.getTargetPath();
    FileSystem targetFS = target.getFileSystem(getConf());
    final boolean targetPathExists = options.getTargetPathExists();

    boolean solitaryFile = options.getSourcePaths().size() == 1
                                                && !sourceStatus.isDirectory();

    if (solitaryFile) {
      if (targetFS.isFile(target) || !targetPathExists) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    } else {
      boolean specialHandling = (options.getSourcePaths().size() == 1 && !targetPathExists) ||
          options.shouldSyncFolder() || options.shouldOverwrite();

      return specialHandling && sourceStatus.isDirectory() ? sourceStatus.getPath() :
          sourceStatus.getPath().getParent();
    }
  }

  /**
   * Provide an option to skip copy of a path, Allows for exclusion
   * of files such as {@link org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#SUCCEEDED_FILE_NAME}
   * @param path - Path being considered for copy while building the file listing
   * @param options - Input options passed during DistCp invocation
   * @return - True if the path should be considered for copy, false otherwise
   */
  protected boolean shouldCopy(Path path, DistCpOptions options) {
    return true;
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
    if (fs.exists(pathToListFile)) {
      fs.delete(pathToListFile, false);
    }
    return SequenceFile.createWriter(getConf(),
            SequenceFile.Writer.file(pathToListFile),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
            SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
  }

  private static boolean isDirectoryAndNotEmpty(FileSystem fileSystem,
                                    FileStatus fileStatus) throws IOException {
    return fileStatus.isDirectory() && getChildren(fileSystem, fileStatus).length > 0;
  }

  private static FileStatus[] getChildren(FileSystem fileSystem,
                                         FileStatus parent) throws IOException {
    return fileSystem.listStatus(parent.getPath());
  }

  private void traverseNonEmptyDirectory(SequenceFile.Writer fileListWriter,
                                         FileStatus sourceStatus,
                                         Path sourcePathRoot,
                                         DistCpOptions options)
                                         throws IOException {
    FileSystem sourceFS = sourcePathRoot.getFileSystem(getConf());
    final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
    final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
    final boolean preserveRawXattrs = options.shouldPreserveRawXattrs();
    Stack<FileStatus> pathStack = new Stack<FileStatus>();
    pathStack.push(sourceStatus);

    while (!pathStack.isEmpty()) {
      for (FileStatus child: getChildren(sourceFS, pathStack.pop())) {
        if (LOG.isDebugEnabled())
          LOG.debug("Recording source-path: "
                    + sourceStatus.getPath() + " for copy.");
        CopyListingFileStatus childCopyListingStatus =
          DistCpUtils.toCopyListingFileStatus(sourceFS, child,
            preserveAcls && child.isDirectory(),
            preserveXAttrs && child.isDirectory(),
            preserveRawXattrs && child.isDirectory());
        writeToFileListing(fileListWriter, childCopyListingStatus,
             sourcePathRoot, options);
        if (isDirectoryAndNotEmpty(sourceFS, child)) {
          if (LOG.isDebugEnabled())
            LOG.debug("Traversing non-empty source dir: "
                       + sourceStatus.getPath());
          pathStack.push(child);
        }
      }
    }
  }
  
  private void writeToFileListingRoot(SequenceFile.Writer fileListWriter,
      CopyListingFileStatus fileStatus, Path sourcePathRoot,
      DistCpOptions options) throws IOException {
    boolean syncOrOverwrite = options.shouldSyncFolder() ||
        options.shouldOverwrite();
    if (fileStatus.getPath().equals(sourcePathRoot) && 
        fileStatus.isDirectory() && syncOrOverwrite) {
      // Skip the root-paths when syncOrOverwrite
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip " + fileStatus.getPath());
      }      
      return;
    }
    writeToFileListing(fileListWriter, fileStatus, sourcePathRoot, options);
  }

  private void writeToFileListing(SequenceFile.Writer fileListWriter,
                                  CopyListingFileStatus fileStatus,
                                  Path sourcePathRoot,
                                  DistCpOptions options) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("REL PATH: " + DistCpUtils.getRelativePath(sourcePathRoot,
        fileStatus.getPath()) + ", FULL PATH: " + fileStatus.getPath());
    }

    FileStatus status = fileStatus;

    if (!shouldCopy(fileStatus.getPath(), options)) {
      return;
    }

    fileListWriter.append(new Text(DistCpUtils.getRelativePath(sourcePathRoot,
        fileStatus.getPath())), status);
    fileListWriter.sync();

    if (!fileStatus.isDirectory()) {
      totalBytesToCopy += fileStatus.getLen();
    }
    totalPaths++;
  }
}
