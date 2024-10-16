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
import org.apache.hadoop.fs.AbstractFileSystem;
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
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FSExceptionMessages.*;

/**
 * Rename Support.
 * <p>
 * This is to support different implementations o
 * {@link FileSystem#rename(Path, Path, Options.Rename...)}
 * with tuning for their underlying APIs.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemRename3Action
  implements FunctionRaisingIOE<
    FileSystemRename3Action.RenameActionParams,
    FileSystemRename3Action.RenameResult> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemRename3Action.class);

  public static final Path ROOT = new Path("/");

  public FileSystemRename3Action() {
  }


  /**
   * Execute
   * {@link FileSystem#rename(Path, Path, Options.Rename...)}, by first
   * validating the arguments, then using the supplied
   * {@link RenameCallbacks} implementation to probe the store and
   * then execute the rename.
   * <p>
   * @param params rename arguments
   * @throws FileNotFoundException source path does not exist, or the parent
   * path of dest does not exist.
   * @throws FileAlreadyExistsException dest path exists and is a file
   * @throws ParentNotDirectoryException if the parent path of dest is not
   * a directory
   * @throws IOException other failure
   */
  @Override
  public RenameResult apply(final RenameActionParams params)
      throws IOException {
    final Path sourcePath = params.getSourcePath();
    final Path destPath = params.getDestPath();
    final Options.Rename[] renameOptions = params.getRenameOptions();
    final RenameCallbacks callbacks = params.getRenameCallbacks();

    LOG.debug("Rename {} tp {}",
        sourcePath,
        destPath);

    try {
      final String srcStr = sourcePath.toUri().getPath();
      final String dstStr = destPath.toUri().getPath();
      if (dstStr.startsWith(srcStr)
          && dstStr.charAt(srcStr.length() - 1) == Path.SEPARATOR_CHAR) {
        throw new PathIOException(srcStr,
            String.format(RENAME_DEST_UNDER_SOURCE, srcStr,
                dstStr));
      }
      if (sourcePath.isRoot()) {
        throw new PathIOException(srcStr, RENAME_SOURCE_IS_ROOT);
      }
      if (destPath.isRoot()) {
        throw new PathIOException(srcStr, RENAME_DEST_IS_ROOT);
      }
      if (sourcePath.equals(destPath)) {
        throw new FileAlreadyExistsException(
            String.format(RENAME_DEST_EQUALS_SOURCE,
                srcStr,
                dstStr));
      }

      boolean overwrite = false;
      boolean deleteDestination = false;

      if (null != renameOptions) {
        for (Options.Rename option : renameOptions) {
          if (option == Options.Rename.OVERWRITE) {
            overwrite = true;
          }
        }
      }
      final FileStatus sourceStatus = callbacks.getSourceStatus(sourcePath);
      if (sourceStatus == null) {
        throw new FileNotFoundException(
            String.format(RENAME_SOURCE_NOT_FOUND, sourcePath));
      }

      final FileStatus destStatus = callbacks.getDestStatusOrNull(destPath);

      if (destStatus != null) {
        // the destination exists.

        // The source and destination types must match.
        if (sourceStatus.isDirectory() != destStatus.isDirectory()) {
          throw new PathIOException(sourcePath.toString(),
              String.format(RENAME_SOURCE_DEST_DIFFERENT_TYPE,
                  sourcePath,
                  destPath));
        }
        // and the rename must permit overwrite
        if (!overwrite) {
          // the destination exists but overwrite = false
          throw new FileAlreadyExistsException(
              String.format(RENAME_DEST_EXISTS, destPath));
        }
        // If the destination exists,is a directory, it must be empty
        if (destStatus.isDirectory()) {
          if (callbacks.directoryHasChildren(destStatus)) {
            throw new PathIOException(sourcePath.toString(),
                String.format(RENAME_DEST_NOT_EMPTY,
                    destPath));
          }
        }
        // declare that the destination must be deleted
        deleteDestination = true;
      } else {
        // there was no destination status; the path does not
        // exist.

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
        if (!destParent.equals(srcParent) && destParent.isRoot()) {
          // check that any non-root parent is a directory
          callbacks.verifyIsDirectory(destParent);
        }
      }

      // and now, finally, the rename.
      // log duration @ debug for the benefit of anyone wondering why
      // renames are slow against object stores.
      try (DurationInfo ignored = new DurationInfo(LOG, false,
          "executing rename(%s %s, %s)",
          sourceStatus.isFile() ? "file" : "directory",
          srcStr,
          destPath)) {
        callbacks.executeRename(
            new ExecuteRenameParams(sourceStatus, destPath, destStatus,
                renameOptions, deleteDestination));
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, callbacks);

    }
    return new RenameResult();
  }

  /**
   * Create the rename callbacks from a filesystem.
   * @param fileSystem filesystem to invoke.
   * @return callbacks.
   */
  public static RenameCallbacks callbacksFromFileSystem(
      FileSystem fileSystem) {
    return new RenameCallbacksToFileSystem(fileSystem);
  }

  /**
   * Create the rename callbacks into an AbstractFileSystem
   * instance.
   * @param abstractFileSystem filesystm to invoke.
   * @return callbacks.
   */
  public static RenameCallbacks callbacksFromFileContext(
      AbstractFileSystem abstractFileSystem) {
    return new RenameCallbacksToFileContext(abstractFileSystem);
  }


  /**
   * Arguments for the rename validation operation.
   */
  public static class RenameActionParams {

    private final Path sourcePath;

    private final Path destPath;

    private final RenameCallbacks renameCallbacks;

    private final Options.Rename[] renameOptions;

    /**
     * @param sourcePath qualified source
     * @param destPath qualified dest.
     * @param renameCallbacks callbacks
     * @param renameOptions options
     */
    private RenameActionParams(final Path sourcePath,
        final Path destPath,
        final RenameCallbacks renameCallbacks,
        final Options.Rename[] renameOptions) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      this.renameCallbacks = renameCallbacks;
      this.renameOptions = renameOptions;
    }

    Path getSourcePath() {
      return sourcePath;
    }

    Path getDestPath() {
      return destPath;
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


    private Path destPath;

    private RenameCallbacks renameCallbacks;

    private Options.Rename[] renameOptions;

    public RenameValidationBuilder withSourcePath(final Path sourcePath) {
      this.sourcePath = sourcePath;
      return this;
    }


    public RenameValidationBuilder withDestPath(final Path destPath) {
      this.destPath = destPath;
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
      return new RenameActionParams(sourcePath,
          destPath,
          renameCallbacks,
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
   * <p>
   * These are used to validate/prepare the operation and the
   * final action.
   */
  public interface RenameCallbacks extends Closeable {

    /**
     * Test for a directory having children.
     * <p>
     * This is invoked knowing that the destination exists
     * and is a directory.
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
    FileStatus getDestStatusOrNull(Path destPath) throws IOException;

    /**
     * Execute the final rename, by invoking
     * {@link FileSystem#rename(Path, Path)}.
     * If the method returns "false", a
     * PathIOException is raised.
     * <p>
     * If a FileSystem can throw more meaningful
     * exceptions here, users will appreciate it.
     *
     * @param params @throws IOException IO failure
     * @throws PathIOException IO failure
     *
     */
    void executeRename(final ExecuteRenameParams params)
        throws PathIOException, IOException;
  }

  /**
   * Implementation of {@link RenameCallbacks} which uses the
   * FileSystem APIs.
   * <p>
   * If FS implementations can do this more efficiently
   * or fail rename better than {@link FileSystem#rename(Path, Path)}
   * does, they should subclass/reimplement this.
   */
  public static class RenameCallbacksToFileSystem
      implements RenameCallbacks {

    /**
     * FS to invoke.
     */
    private final FileSystem fileSystem;

    /**
     * Construct with the given filesystem.
     * @param fileSystem target FS.
     */
    protected RenameCallbacksToFileSystem(final FileSystem fileSystem) {
      this.fileSystem = requireNonNull(fileSystem);
    }

    /**
     * Get the filesystem.
     * @return
     */
    protected FileSystem getFileSystem() {
      return fileSystem;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean directoryHasChildren(final FileStatus directory)
        throws IOException {
      // get an iterator and then see if it is of size
      // one or more.
      // we use this for more efficiency with larger directories
      // on those clients which do paged downloads.
      RemoteIterator<FileStatus> it
          = fileSystem.listStatusIterator(directory.getPath());
      boolean b = it
          .hasNext();
      if (it instanceof Closeable) {
        ((Closeable) it).close();
      }
      return b;
    }

    @Override
    public void verifyIsDirectory(final Path destParent)
        throws ParentNotDirectoryException, IOException {
      if (!fileSystem.getFileStatus(destParent).isDirectory()) {
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
    public FileStatus getDestStatusOrNull(Path destPath) throws IOException {
      try {
        return fileSystem.getFileLinkStatus(destPath);
      } catch (FileNotFoundException e) {
        return null;
      }
    }

    /**
     * Execute the final rename, by invoking
     * {@link FileSystem#rename(Path, Path)}.
     * If the method returns "false", a
     * PathIOException is raised.
     * <p>
     * If a FileSystem can throw more meaningful
     * exceptions here, users will appreciate it.
     * {@inheritDoc}.
     * @param params
     */
    @Override
    public void executeRename(final ExecuteRenameParams params)
        throws PathIOException, IOException {

      Path sourcePath = params.getSourcePath();
      Path destPath = params.getDestPath();
      if (params.isDeleteDest()) {
        fileSystem.delete(destPath, false);
      }
      if (!fileSystem.rename(sourcePath, destPath)) {
        // inner rename failed, no obvious cause
        throw new PathIOException(sourcePath.toString(),
            String.format(RENAME_FAILED, sourcePath,
                destPath));
      }
    }

  }

  /**
   * Implementation of {@link RenameCallbacks} which uses the
   * FileSystem APIs. This may be suboptimal, if FS implementations
   * can implement empty directory probes/deletes better themselves.
   */
  private static final class RenameCallbacksToFileContext
      implements RenameCallbacks {

    /**
     * FS to invoke.
     */
    private final AbstractFileSystem fileSystem;


    /**
     * Construct with the given filesystem.
     * @param fileSystem target FS.
     */
    private RenameCallbacksToFileContext(
        final AbstractFileSystem fileSystem) {
      this.fileSystem = requireNonNull(fileSystem);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean directoryHasChildren(final FileStatus directory)
        throws IOException {
      // get an iterator and then see if it is of size
      // one or more.
      // we use this for more efficiency with larger directories
      // on those clients which do paged downloads.
      RemoteIterator<FileStatus> it
          = fileSystem.listStatusIterator(directory.getPath());
      boolean b = it
          .hasNext();
      if (it instanceof Closeable) {
        ((Closeable) it).close();
      }
      return b;
    }

    @Override
    public void verifyIsDirectory(final Path destParent)
        throws ParentNotDirectoryException, IOException {
      // check
      if (!fileSystem.getFileStatus(destParent).isDirectory()) {
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
    public FileStatus getDestStatusOrNull(Path destPath) throws IOException {
      try {
        return fileSystem.getFileLinkStatus(destPath);
      } catch (FileNotFoundException e) {
        return null;
      }
    }

    /**
     * Execute the final rename, by invoking
     * {@link FileSystem#rename(Path, Path)}.
     * If the method returns "false", a
     * PathIOException is raised.
     * <p>
     * If a FileSystem can throw more meaningful
     * exceptions here, users will appreciate it.
     * {@inheritDoc}.
     * @param params
     */
    @Override
    public void executeRename(final ExecuteRenameParams params)
        throws PathIOException, IOException {
      Path sourcePath = params.getSourcePath();
      Path destPath = params.getDestPath();
      if (params.isDeleteDest()) {
        fileSystem.delete(destPath, false);
      }
      fileSystem.renameInternal(sourcePath, destPath);
    }

  }

  /**
   * Parameters for {@link RenameCallbacks#executeRename(ExecuteRenameParams)}.
   * <p>
   * Made a parameter object so if we extend it, external implementations
   * of the {@link RenameCallbacks} interface won't encounter link
   * problems.
   */
  public static class ExecuteRenameParams {

    private final FileStatus sourceStatus;

    private final Path destPath;

    @Nullable private final FileStatus destStatus;

    private final Options.Rename[] renameOptions;

    private final boolean deleteDest;

    /**
     * @param sourceStatus source FS status
     * @param destPath path of destination
     * @param destStatus status of destination
     * @param renameOptions any rename options
     * @param deleteDest delete destination path
     */
    public ExecuteRenameParams(final FileStatus sourceStatus,
        final Path destPath,
        @Nullable final FileStatus destStatus,
        final Options.Rename[] renameOptions,
        final boolean deleteDest) {
      this.sourceStatus = sourceStatus;
      this.destPath = destPath;
      this.destStatus = destStatus;
      this.renameOptions = renameOptions;
      this.deleteDest = deleteDest;
    }

    public FileStatus getSourceStatus() {
      return sourceStatus;
    }

    public Path getSourcePath() {
      return sourceStatus.getPath();
    }

    public FileStatus getDestStatus() {
      return destStatus;
    }

    public Path getDestPath() {
      return destPath;
    }

    public Options.Rename[] getRenameOptions() {
      return renameOptions;
    }

    public boolean isDeleteDest() {
      return deleteDest;
    }
  }

  /**
   * Result.
   */
  public static class RenameResult {

  }
}
