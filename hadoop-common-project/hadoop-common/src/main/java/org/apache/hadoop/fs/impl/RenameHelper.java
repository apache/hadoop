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
import java.util.Optional;

import org.slf4j.Logger;

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
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_NO_PARENT;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_PARENT_NOT_DIRECTORY;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_UNDER_SOURCE;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_SOURCE_DEST_DIFFERENT_TYPE;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_SOURCE_IS_ROOT;
import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_SOURCE_NOT_FOUND;

/**
 * Pulls out rename logic into its own operation.
 * This is to enable different implementations of the rename() operation
 * to do all the upfront validation.
 */
public class RenameHelper {

  final Logger log;
  final FileSystem fileSystem;

  public RenameHelper(final FileSystem fileSystem,
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
   * @param srcPath qualified source
   * @param srcStatus qualified source status.
   * @param destPath qualified dest.
   * @param destStatus qualified dest status.
   * @param hasChildrenFunction function to probe for a directory having
   * children. Generally {@code FileSystem.hasChildren(Path)}.
   * @param options options
   * @throws FileNotFoundException source path does not exist, or the parent
   * path of dest does not exist.
   * @throws FileAlreadyExistsException dest path exists and is a file
   * @throws ParentNotDirectoryException if the parent path of dest is not
   * a directory
   * @throws IOException other failure
   */
  public void validateRenameOptions(
      final Path srcPath,
      final FileStatus srcStatus,
      final Path destPath,
      final Optional<FileStatus> destStatus,
      final FunctionWithIOE<FileStatus, Boolean> hasChildrenFunction,
      final FunctionWithIOE<FileStatus, Boolean> deleteEmptyDirectoryFunction,
      final Options.Rename... options) throws IOException {
    log.debug("Rename {} tp {}", srcPath, destPath);
    if (srcStatus == null) {
      throw new FileNotFoundException(
          String.format(RENAME_SOURCE_NOT_FOUND, srcPath));
    }
    final String srcStr = srcPath.toUri().getPath();
    final String dstStr = destPath.toUri().getPath();
    if (dstStr.startsWith(srcStr)
        && dstStr.charAt(srcStr.length() - 1) == Path.SEPARATOR_CHAR) {
      throw new PathIOException(srcPath.toString(),
          String.format(RENAME_DEST_UNDER_SOURCE, srcPath, destPath));
    }
    if ("/".equals(srcStr)) {
      throw new PathIOException(srcPath.toString(), RENAME_SOURCE_IS_ROOT);
    }
    if ("/".equals(dstStr)) {
      throw new PathIOException(srcPath.toString(), RENAME_DEST_IS_ROOT);
    }
    if (srcStr.equals(dstStr)) {
      throw new FileAlreadyExistsException(
          String.format(RENAME_DEST_EQUALS_SOURCE, srcPath, destPath));
    }

    boolean overwrite = false;
    if (null != options) {
      for (Options.Rename option : options) {
        if (option == Options.Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    if (destStatus.isPresent()) {
      FileStatus dest = destStatus.get();
      if (srcStatus.isDirectory() != dest.isDirectory()) {
        throw new PathIOException(srcPath.toString(),
            String.format(RENAME_SOURCE_DEST_DIFFERENT_TYPE, srcPath, destPath));
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException(
            String.format(RENAME_DEST_EXISTS, destPath));
      }
      // Delete the destination that is a file or an empty directory
      if (dest.isDirectory()) {
        // list children. This may be expensive in time or memory.
        if (hasChildrenFunction.apply(dest)) {
          throw new PathIOException(srcPath.toString(),
              String.format(RENAME_DEST_NOT_EMPTY, destPath));
        }
      }
      // its an empty directory, delete.
      deleteEmptyDirectoryFunction.apply(dest);
    } else {
      // verify the parent of the dest being a directory.
      // this is implicit if the source and dest share the same parent,
      // otherwise a getFileStatus call is needed.
      final Path destParent = destPath.getParent();
      final Path srcParent = srcPath.getParent();
      if (!destParent.equals(srcParent)) {
        // check
        final FileStatus parentStatus = fileSystem.getFileStatus(destParent);
        if (parentStatus == null) {
          throw new FileNotFoundException(
              String.format(RENAME_DEST_NO_PARENT, destParent));
        }
        if (!parentStatus.isDirectory()) {
          throw new ParentNotDirectoryException(
              String.format(RENAME_DEST_PARENT_NOT_DIRECTORY, destParent));
        }
      }
    }
  }

  @FunctionalInterface
  public interface FunctionWithIOE<T, R> {

    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     */
    R apply(T t) throws IOException;
  }
}
