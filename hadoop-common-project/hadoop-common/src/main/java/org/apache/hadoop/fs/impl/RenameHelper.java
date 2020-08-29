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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_EQUALS_SOURCE;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_EXISTS;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_IS_ROOT;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_NOT_EMPTY;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_PARENT_NOT_DIRECTORY;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_UNDER_SOURCE;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_SOURCE_DEST_DIFFERENT_TYPE;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_SOURCE_IS_ROOT;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_SOURCE_NOT_FOUND;

/**
 * Rename Support.
 * <p></p>
 * This is to support different implementations of the rename() operation
 * with all the upfront validation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RenameHelper {

  final Logger log;

  final FileSystem fileSystem;

  public RenameHelper(
      final FileSystem fileSystem,
      final Logger log) {
    this.log = log;
    this.fileSystem = fileSystem;
  }

  /**
   * Validate all the preconditions of
   * {@link FileSystem#rename(Path, Path, Options.Rename...)}.
   *
   * This was pull out of FileSystem so that subclasses can implement more
   * efficient versions of the rename operation, ones where the filestatus
   * facts are
   *
   * @param args@throws FileNotFoundException source path does not exist, or the parent
   * path of dest does not exist.
   * @throws FileAlreadyExistsException dest path exists and is a file
   * @throws ParentNotDirectoryException if the parent path of dest is not
   * a directory
   * @throws IOException other failure
   */
  public RenameValidationResult validateRenameOptions(final RenameValidationParams args)
      throws IOException {
    final Path sourcePath = args.getSourcePath();
    final Path destPath = args.getDestPath();
    log.debug("Rename {} tp {}",
        sourcePath,
        destPath);
    final FileStatus sourceStatus = args.getSourceStatus();
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
    final Options.Rename[] renameOptions = args.getRenameOptions();
    if (null != renameOptions) {
      for (Options.Rename option : renameOptions) {
        if (option == Options.Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    final FileStatus destStatus = args.getDestStatus();
    if (destStatus != null) {
      if (sourceStatus.isDirectory() != destStatus.isDirectory()) {
        throw new PathIOException(sourcePath.toString(),
            String.format(RENAME_SOURCE_DEST_DIFFERENT_TYPE,
                sourcePath,
                destPath));
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException(
            String.format(RENAME_DEST_EXISTS, destPath));
      }
      // Delete the destination that is a file or an empty directory
      if (destStatus.isDirectory()) {
        // list children. This may be expensive in time or memory.
        if (args.getHasChildrenFunction().apply(destStatus)) {
          throw new PathIOException(sourcePath.toString(),
              String.format(RENAME_DEST_NOT_EMPTY,
                  destPath));
        }
      }
      // its an empty directory, delete.
      args.getDeleteEmptyDirectoryFunction().apply(destStatus);
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
        if (!destParent.isRoot()
            && !fileSystem.isDirectory(destParent)) {
          // not a dir, so we fail. Do the full getFileStatus to trigger
          // an FNFE if it is not there
          fileSystem.getFileStatus(destParent);
          throw new ParentNotDirectoryException(
              String.format(RENAME_DEST_PARENT_NOT_DIRECTORY, destParent));
        }
      }
    }
    return new RenameValidationResult();
  }


  /**
   * Arguments for the rename validation operation.
   */
  public static class RenameValidationParams {

    private final Path sourcePath;

    private final FileStatus sourceStatus;

    private final Path destPath;

    private final FileStatus destStatus;

    private final FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean>
        hasChildrenFunction;

    private final FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean>
        deleteEmptyDirectoryFunction;

    private final Options.Rename[] renameOptions;

    /**
     * @param sourcePath qualified source
     * @param sourceStatus qualified source status.
     * @param destPath qualified dest.
     * @param destStatus qualified dest status.
     * @param hasChildrenFunction function to probe for a directory having
     * children. Generally {@link FileSystem#hasChildren(FileStatus)}.
     * @param renameOptions options
     */
    private RenameValidationParams(final Path sourcePath,
        final FileStatus sourceStatus,
        final Path destPath,
        final FileStatus destStatus,
        final FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> hasChildrenFunction,
        final FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> deleteEmptyDirectoryFunction,
        final Options.Rename... renameOptions) {
      this.sourcePath = sourcePath;
      this.sourceStatus = sourceStatus;
      this.destPath = destPath;
      this.destStatus = destStatus;
      this.hasChildrenFunction = hasChildrenFunction;
      this.deleteEmptyDirectoryFunction = deleteEmptyDirectoryFunction;
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

    FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> getHasChildrenFunction() {
      return hasChildrenFunction;
    }

    FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> getDeleteEmptyDirectoryFunction() {
      return deleteEmptyDirectoryFunction;
    }

    Options.Rename[] getRenameOptions() {
      return renameOptions;
    }
  }

  /**
   * Builder for the {@link RenameValidationParams} arguments.
   */
  public static class RenameValidationBuilder {

    private Path sourcePath;

    private FileStatus sourceStatus;

    private Path destPath;

    private FileStatus destStatus;

    private FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> hasChildrenFunction;

    private FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean>
        deleteEmptyDirectoryFunction;

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

    public RenameValidationBuilder withHasChildrenFunction(
        final FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> hasChildrenFunction) {
      this.hasChildrenFunction = hasChildrenFunction;
      return this;
    }

    public RenameValidationBuilder withDeleteEmptyDirectoryFunction(
        final FunctionsRaisingIOE.FunctionRaisingIOE<FileStatus, Boolean> deleteEmptyDirectoryFunction) {
      this.deleteEmptyDirectoryFunction = deleteEmptyDirectoryFunction;
      return this;
    }

    public RenameValidationBuilder withRenameOptions(
        final Options.Rename... renameOptions) {
      this.renameOptions = renameOptions;
      return this;
    }

    public RenameValidationParams createRenameValidation() {
      return new RenameValidationParams(sourcePath, sourceStatus, destPath,
          destStatus, hasChildrenFunction, deleteEmptyDirectoryFunction,
          renameOptions);
    }
  }

  /**
   * Result of the rename.
   * Extensible in case there's a desire to add data in future
   * (statistics, results of FileStatus calls, etc)
   */
  public class RenameValidationResult {

  }
}
