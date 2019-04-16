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
import java.util.concurrent.Callable;
import java.util.function.Function;

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
 * Pulls out rename logic into is
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
   * @param src qualified source
   * @param srcStatus qualified source status.
   * @param dst qualified dest.
   * @param dstStatus qualified dest status (may be null)
   * @param options options
   * @throws FileNotFoundException source path does not exist, or the parent
   * path of dest does not exist.
   * @throws FileAlreadyExistsException dest path exists and is a file
   * @throws ParentNotDirectoryException if the parent path of dest is not
   * a directory
   * @throws IOException other failure
   */
  void validate(final Path src, final FileStatus srcStatus,
      final Path dst, final FileStatus dstStatus,
      final FileStatus parentStatus,
      final Function<Path, Boolean> hasChildren,
      final Options.Rename... options
  ) throws IOException {
    final String srcStr = src.toUri().getPath();
    final String dstStr = dst.toUri().getPath();
    if (dstStr.startsWith(srcStr)
        && dstStr.charAt(srcStr.length() - 1) == Path.SEPARATOR_CHAR) {
      throw new PathIOException(src.toString(),
          String.format(RENAME_DEST_UNDER_SOURCE, src, dst));
    }
    if ("/".equals(srcStr)) {
      throw new PathIOException(src.toString(), RENAME_SOURCE_IS_ROOT);
    }
    if ("/".equals(dstStr)) {
      throw new PathIOException(src.toString(), RENAME_DEST_IS_ROOT);
    }
    if (srcStr.equals(dstStr)) {
      throw new FileAlreadyExistsException(
          String.format(RENAME_DEST_EQUALS_SOURCE, src, dst));
    }
    if (srcStatus == null) {
      throw new FileNotFoundException(
          String.format(RENAME_SOURCE_NOT_FOUND, src));
    }

    boolean overwrite = false;
    if (null != options) {
      for (Options.Rename option : options) {
        if (option == Options.Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    if (dstStatus != null) {
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new PathIOException(src.toString(),
            String.format(RENAME_SOURCE_DEST_DIFFERENT_TYPE, src, dst));
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException(
            String.format(RENAME_DEST_EXISTS, dst));
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDirectory()) {
        // list children. This may be expensive in time or memory.
        if (hasChildren(dst)) {
          throw new PathIOException(src.toString(),
              String.format(RENAME_DEST_NOT_EMPTY, dst));
        }
      }
      delete(dst, false);
    } else {
      final Path parent = dst.getParent();
      final FileStatus parentStatus = getFileStatus(parent);
      if (parentStatus == null) {
        throw new FileNotFoundException(
            String.format(RENAME_DEST_NO_PARENT, parent));
      }
      if (!parentStatus.isDirectory()) {
        throw new ParentNotDirectoryException(
            String.format(RENAME_DEST_PARENT_NOT_DIRECTORY, parent));
      }
    }


  }
}
