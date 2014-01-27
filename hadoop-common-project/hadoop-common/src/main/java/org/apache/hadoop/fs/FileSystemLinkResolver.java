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
package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * FileSystem-specific class used to operate on and resolve symlinks in a path.
 * Operation can potentially span multiple {@link FileSystem}s.
 * 
 * @see FSLinkResolver
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class FileSystemLinkResolver<T> {

  /**
   * FileSystem subclass-specific implementation of superclass method.
   * Overridden on instantiation to perform the actual method call, which throws
   * an UnresolvedLinkException if called on an unresolved {@link Path}.
   * @param p Path on which to perform an operation
   * @return Generic type returned by operation
   * @throws IOException
   * @throws UnresolvedLinkException
   */
  abstract public T doCall(final Path p) throws IOException,
      UnresolvedLinkException;

  /**
   * Calls the abstract FileSystem call equivalent to the specialized subclass
   * implementation in {@link #doCall(Path)}. This is used when retrying the
   * call with a newly resolved Path and corresponding new FileSystem.
   * 
   * @param fs
   *          FileSystem with which to retry call
   * @param p
   *          Resolved Target of path
   * @return Generic type determined by implementation
   * @throws IOException
   */
  abstract public T next(final FileSystem fs, final Path p) throws IOException;

  /**
   * Attempt calling overridden {@link #doCall(Path)} method with
   * specified {@link FileSystem} and {@link Path}. If the call fails with an
   * UnresolvedLinkException, it will try to resolve the path and retry the call
   * by calling {@link #next(FileSystem, Path)}.
   * @param filesys FileSystem with which to try call
   * @param path Path with which to try call
   * @return Generic type determined by implementation
   * @throws IOException
   */
  public T resolve(final FileSystem filesys, final Path path)
      throws IOException {
    int count = 0;
    T in = null;
    Path p = path;
    // Assumes path belongs to this FileSystem.
    // Callers validate this by passing paths through FileSystem#checkPath
    FileSystem fs = filesys;
    for (boolean isLink = true; isLink;) {
      try {
        in = doCall(p);
        isLink = false;
      } catch (UnresolvedLinkException e) {
        if (!filesys.resolveSymlinks) {
          throw new IOException("Path " + path + " contains a symlink"
              + " and symlink resolution is disabled ("
              + CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY
              + ").", e);
        }
        if (!FileSystem.areSymlinksEnabled()) {
          throw new IOException("Symlink resolution is disabled in" +
              " this version of Hadoop.");
        }
        if (count++ > FsConstants.MAX_PATH_LINKS) {
          throw new IOException("Possible cyclic loop while " +
                                "following symbolic link " + path);
        }
        // Resolve the first unresolved path component
        p = FSLinkResolver.qualifySymlinkTarget(fs.getUri(), p,
            filesys.resolveLink(p));
        fs = FileSystem.getFSofPath(p, filesys.getConf());
        // Have to call next if it's a new FS
        if (!fs.equals(filesys)) {
          return next(fs, p);
        }
        // Else, we keep resolving with this filesystem
      }
    }
    // Successful call, path was fully resolved
    return in;
  }
}
