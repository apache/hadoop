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

package org.apache.hadoop.fs.impl;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DurationInfo;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FSExceptionMessages.*;

/**
 * Rename Support.
 * <p></p>
 * This is to support different implementations of {@link FileSystem#rename(Path, Path, Options.Rename...)}
 * with tuning for their underlying APIs.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemRename3Action {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemRename3Action.class);

  public FileSystemRename3Action() {
  }

  /**
   * Validate all the preconditions of
   * {@link FileSystem#rename(Path, Path, Options.Rename...)}.
   * <p></p>
   * @param params rename arguments
   * @throws FileNotFoundException source path does not exist, or the parent
   * path of dest does not exist.
   * @throws FileAlreadyExistsException dest path exists and is a file
   * @throws ParentNotDirectoryException if the parent path of dest is not
   * a directory
   * @throws IOException other failure
   */
  public void rename(final RenameActionParams params)
      throws IOException {
    final Path sourcePath = params.getSourcePath();
    final Path destPath = params.getDestPath();
    final Options.Rename[] renameOptions = params.getRenameOptions();
    final FileStatus sourceStatus = params.getSourceStatus();
    final FileStatus destStatus = params.getDestStatus();
    final RenameCallbacks callbacks = params.getRenameCallbacks();

    LOG.debug("Rename {} tp {}",
        sourcePath,
        destPath);

    if (sourceStatus == null) {
      throw new FileNotFoundException(
          String.format(RENAME_SOURCE_NOT_FOUND, sourcePath));
    }
    final String srcStr = sourcePath.toUri().getPath();
    final String dstStr = destPath.toUri().getPath();
    if (dstStr.startsWith(srcStr)
        && dstStr.charAt(srcStr.length() - 1) == Path.SEPARATOR_CHAR) {
      throw new PathIOException(srcStr,
          String.format(RENAME_DEST_UNDER_SOURCE, srcStr,
              dstStr));
    }
    if ("/".equals(srcStr)) {
      throw new PathIOException(srcStr, RENAME_SOURCE_IS_ROOT);
    }
    if ("/".equals(dstStr)) {
      throw new PathIOException(srcStr, RENAME_DEST_IS_ROOT);
    }
    if (srcStr.equals(dstStr)) {
      throw new FileAlreadyExistsException(
          String.format(RENAME_DEST_EQUALS_SOURCE,
              srcStr,
              dstStr));
    }

    boolean overwrite = false;
    boolean deleteEmptyDestDirectory = false;

    if (null != renameOptions) {
      for (Options.Rename option : renameOptions) {
        if (option == Options.Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    if (destStatus != null) {
      if (sourceStatus.isDirectory() != destStatus.isDirectory()) {
        // if the source is a directory, so must any existing
        // destination
        throw new PathIOException(sourcePath.toString(),
            String.format(RENAME_SOURCE_DEST_DIFFERENT_TYPE,
                sourcePath,
                destPath));
      }
      if (!overwrite) {
        // the destination exists but overwrite = false
        throw new FileAlreadyExistsException(
            String.format(RENAME_DEST_EXISTS, destPath));
      }
      // Delete the destination that is a file or an empty directory
      if (destStatus.isDirectory()) {
        // list children. This may be expensive in time or memory.
        if (callbacks.directoryHasChildren(destStatus)) {
          throw new PathIOException(sourcePath.toString(),
              String.format(RENAME_DEST_NOT_EMPTY,
                  destPath));
        }
      }
      // its an empty directory, delete.
      deleteEmptyDestDirectory = true;
    } else {
      // verify the parent of the dest being a directory.
      // this is implicit if the source and dest share the same parent,
      // otherwise a probe is is needed.
      // uses isDirectory so those stores which have optimised that
      // are slightly more efficient on the success path.
      // This is at the expense of a second check during failure
      // to distinguish parent dir not existing from parent
      // not being a file.
      final Path destParent = destPath.getParent();
      final Path srcParent = sourcePath.getParent();
      if (!destParent.equals(srcParent)) {
        // check that any non-root parent is a directory
        if (!destParent.isRoot()) {
          callbacks.verifyIsDirectory(destParent);
        }
      }
    }


    // and now, finally, the rename.
    // log duration @ debug for the benefit of anyone wondering why
    // renames are slow against object stores.
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "rename(%s %s, %s)",
        sourceStatus.isFile() ? "file" : "directory",
        srcStr,
        destPath)) {
      callbacks.rename(sourceStatus, destPath, destStatus, renameOptions,
          deleteEmptyDestDirectory);
    }
  }

  /**
   * Arguments for the rename validation operation.
   */
  public static class RenameActionParams {

    private final Path sourcePath;

    private final FileStatus sourceStatus;

    private final Path destPath;

    private final FileStatus destStatus;

    private final RenameCallbacks renameCallbacks;

    private final Options.Rename[] renameOptions;

    /**
     * @param sourcePath qualified source
     * @param sourceStatus qualified source status.
     * @param destPath qualified dest.
     * @param destStatus qualified dest status.
     * @param renameCallbacks callbacks
     * @param renameOptions options
     */
    private RenameActionParams(final Path sourcePath,
        final FileStatus sourceStatus,
        final Path destPath,
        final FileStatus destStatus,
        final RenameCallbacks renameCallbacks,
        final Options.Rename... renameOptions) {
      this.sourcePath = sourcePath;
      this.sourceStatus = sourceStatus;
      this.destPath = destPath;
      this.destStatus = destStatus;
      this.renameCallbacks = renameCallbacks;
      this.renameOptions = renameOptions;
    }

    Path getSourcePath() {
      return sourcePath;
    }

    FileStatus getSourceStatus() {
      return sourceStatus;
    }

    Path getDestPath() {
      return destPath;
    }

    FileStatus getDestStatus() {
      return destStatus;
    }

    public RenameCallbacks getRenameCallbacks() {
      return renameCallbacks;
    }

    Options.Rename[] getRenameOptions() {
      return renameOptions;
    }
  }

  /**
   * Builder for the {@link RenameActionParams} arguments.
   */
  public static class RenameValidationBuilder {

    private Path sourcePath;

    private FileStatus sourceStatus;

    private Path destPath;

    private FileStatus destStatus;

    private RenameCallbacks renameCallbacks;

    private Options.Rename[] renameOptions;

    public RenameValidationBuilder withSourcePath(final Path sourcePath) {
      this.sourcePath = sourcePath;
      return this;
    }

    public RenameValidationBuilder withSourceStatus(final FileStatus sourceStatus) {
      this.sourceStatus = sourceStatus;
      return this;
    }

    public RenameValidationBuilder withDestPath(final Path destPath) {
      this.destPath = destPath;
      return this;
    }

    public RenameValidationBuilder withDestStatus(
        final FileStatus destStatus) {
      this.destStatus = destStatus;
      return this;
    }

    public RenameValidationBuilder withRenameCallbacks(
        final RenameCallbacks renameCallbacks) {
      this.renameCallbacks = renameCallbacks;
      return this;
    }


    public RenameValidationBuilder withRenameOptions(
        final Options.Rename... renameOptions) {
      this.renameOptions = renameOptions;
      return this;
    }

    public RenameActionParams build() {
      return new RenameActionParams(sourcePath, sourceStatus, destPath,
          destStatus, renameCallbacks,
          renameOptions);
    }
  }

  /**
   * Result of the rename.
   * Extensible in case there's a desire to add data in future
   * (statistics, results of FileStatus calls, etc)
   */
  public static class RenameValidationResult {

  }

  /**
   * Callbacks which must be implemented to support rename.
   * <p></p>
   * These are used to validate/prepare the operation and the
   * final action.
   */
  public interface RenameCallbacks {

    /**
     * Test for a directory having children. This is used by the
     * base implementation of
     * @param directory the file status of the destination.
     * @return true if the path has one or more child entries.
     * @throws IOException for IO problems.
     */
    boolean directoryHasChildren(FileStatus directory)
        throws IOException;

    /**
     * Verify that the parent path of a rename is a directory
     * @param destParent parent path
     * @throws ParentNotDirectoryException it isn't
     * @throws IOException any other IO Failure
     */
    void verifyIsDirectory(Path destParent)
        throws ParentNotDirectoryException, IOException;

    /**
     * Get the status of the source. 
     * @param sourcePath source
     * @return status of source
     * @throws FileNotFoundException no source found
     * @throws IOException any other IO failure
     */
    FileStatus getSourceStatus(Path sourcePath)
        throws FileNotFoundException, IOException;

    /**
     * Get the destination status.
     * Any failure in the probe is considered to mean "no destination"
     * @param destPath destination path.
     * @return the file status, or null for a failure.
     */
    FileStatus getDestStatusOrNull(Path destPath);

    /**
     * Execute the final rename, by invoking
     * {@link FileSystem#rename(Path, Path)}.
     * If the method returns "false", a
     * PathIOException is raised.
     * <p></p>
     * If a FileSystem can throw more meaningful
     * exceptions here, users will appreciate it.
     * @param sourceStatus source FS status
     * @param destPath path of destination
     * @param destStatus status of destination
     * @param renameOptions any rename options
     * @param deleteEmptyDestDirectory
     * @throws IOException IO failure
     * @throws PathIOException IO failure
     *
     */
    void rename(
        FileStatus sourceStatus,
        Path destPath,
        @Nullable FileStatus destStatus,
        Options.Rename[] renameOptions, final boolean deleteEmptyDestDirectory)
        throws PathIOException, IOException;
  }

  /**
   * Create the rename callbacks from a filesystem.
   * @param fileSystem filesystm to invoke.
   * @return callbacks.
   */
  public static RenameCallbacks callbacksFromFileSystem(FileSystem fileSystem) {
    return new RenameCallbacksToFileSystem(fileSystem);
  }

  /**
   * Implementation of {@link RenameCallbacks} which uses the
   * FileSystem APIs. This may be suboptimal, if FS implementations
   * can implement empty directory probes/deletes better themselves.
   */
  private static final class RenameCallbacksToFileSystem
      implements RenameCallbacks {

    /**
     * FS to invoke.
     */
    private final FileSystem fileSystem;

    /**
     * Construct with the given filesystem.
     * @param fileSystem target FS.
     */
    private RenameCallbacksToFileSystem(final FileSystem fileSystem) {
      this.fileSystem = requireNonNull(fileSystem);
    }

    /**
     * {@inheritDoc}
     * @param directory the file status of the destination.
     * @return true if there are children.
     * @throws IOException
     */
    @Override
    public boolean directoryHasChildren(final FileStatus directory)
        throws IOException {
      Path path = directory.getPath();
      // get an iterator and then see if it is of size
      // one or more.
      // we use this for more efficiency with larger directories
      // on those clients which do paged downloads.
      final RemoteIterator<FileStatus> it
          = fileSystem.listStatusIterator(path);
      try {
        if (!it.hasNext()) {
          // no children.
          return false;
        }
        final FileStatus first = it.next();
        return !path.equals(first.getPath());
      } finally {
        // in case the iterator is closeable.
        if (it instanceof Closeable) {
          IOUtils.cleanupWithLogger(LOG, (Closeable) it);
        }
      }
    }

    @Override
    public void verifyIsDirectory(final Path destParent)
        throws ParentNotDirectoryException, IOException {
      // check
      final FileStatus parentStatus = fileSystem.getFileStatus(destParent);
      if (!parentStatus.isDirectory()) {
        throw new ParentNotDirectoryException(
            String.format(RENAME_DEST_PARENT_NOT_DIRECTORY, destParent));
      }
    }

    @Override
    public FileStatus getSourceStatus(Path sourcePath)
        throws FileNotFoundException, IOException {
      return fileSystem.getFileLinkStatus(sourcePath);
    }

    @Override
    public FileStatus getDestStatusOrNull(Path destPath) {
      FileStatus destStatus;
      try {
        destStatus = fileSystem.getFileLinkStatus(destPath);
      } catch (IOException e) {
        destStatus = null;
      }
      return destStatus;
    }

    /**
     * Execute the final rename, by invoking
     * {@link FileSystem#rename(Path, Path)}.
     * If the method returns "false", a
     * PathIOException is raised.
     * <p></p>
     * If a FileSystem can throw more meaningful
     * exceptions here, users will appreciate it.
     * {@inheritDoc}.
     */
    @Override
    public void rename(
        final FileStatus sourceStatus,
        final Path destPath,
        @Nullable final FileStatus destStatus,
        final Options.Rename[] renameOptions,
        final boolean deleteEmptyDestDirectory)
        throws PathIOException, IOException {

      final Path sourcePath = sourceStatus.getPath();
      if (deleteEmptyDestDirectory) {
        fileSystem.delete(destStatus.getPath(), false);
      }
      if (!fileSystem.rename(sourcePath, destPath)) {
        // inner rename failed, no obvious cause
        throw new PathIOException(sourcePath.toString(),
            String.format(RENAME_FAILED, sourcePath, destPath));
      }
    }

  }


}
