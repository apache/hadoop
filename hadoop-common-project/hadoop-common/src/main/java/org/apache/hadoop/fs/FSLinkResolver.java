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
import java.net.URI;

/**
 * Class used to perform an operation on and resolve symlinks in a
 * path. The operation may potentially span multiple file systems.
 */
public abstract class FSLinkResolver<T> {

  private static final int MAX_PATH_LINKS = 32;

  /**
   * See {@link #qualifySymlinkTarget(URI, Path, Path)}
   */
  public static Path qualifySymlinkTarget(final AbstractFileSystem pathFS,
    Path pathWithLink, Path target) {
    return qualifySymlinkTarget(pathFS.getUri(), pathWithLink, target);
  }

  /**
   * See {@link #qualifySymlinkTarget(URI, Path, Path)}
   */
  public static Path qualifySymlinkTarget(final FileSystem pathFS,
      Path pathWithLink, Path target) {
    return qualifySymlinkTarget(pathFS.getUri(), pathWithLink, target);
  }
  
  /**
   * Return a fully-qualified version of the given symlink target if it
   * has no scheme and authority. Partially and fully-qualified paths
   * are returned unmodified.
   * @param pathURI URI of the filesystem of pathWithLink
   * @param pathWithLink Path that contains the symlink
   * @param target The symlink's absolute target
   * @return Fully qualified version of the target.
   */
  private static Path qualifySymlinkTarget(final URI pathURI,
      Path pathWithLink, Path target) {
    // NB: makeQualified uses the target's scheme and authority, if
    // specified, and the scheme and authority of pathURI, if not.
    final URI targetUri = target.toUri();
    final String scheme = targetUri.getScheme();
    final String auth = targetUri.getAuthority();
    return (scheme == null && auth == null) ? target.makeQualified(pathURI,
        pathWithLink.getParent()) : target;
  }

  // FileContext / AbstractFileSystem resolution methods

  /**
   * Generic helper function overridden on instantiation to perform a
   * specific operation on the given file system using the given path
   * which may result in an UnresolvedLinkException.
   * @param fs AbstractFileSystem to perform the operation on.
   * @param p Path given the file system.
   * @return Generic type determined by the specific implementation.
   * @throws UnresolvedLinkException If symbolic link <code>path</code> could
   *           not be resolved
   * @throws IOException an I/O error occurred
   */
  public T next(final AbstractFileSystem fs, final Path p)
      throws IOException, UnresolvedLinkException {
    throw new AssertionError("Should not be called without first overriding!");
  }

  /**
   * Performs the operation specified by the next function, calling it
   * repeatedly until all symlinks in the given path are resolved.
   * @param fc FileContext used to access file systems.
   * @param path The path to resolve symlinks on.
   * @return Generic type determined by the implementation of next.
   * @throws IOException
   */
  public T resolve(final FileContext fc, final Path path) throws IOException {
    int count = 0;
    T in = null;
    Path p = path;
    // NB: More than one AbstractFileSystem can match a scheme, eg 
    // "file" resolves to LocalFs but could have come by RawLocalFs.
    AbstractFileSystem fs = fc.getFSofPath(p);

    // Loop until all symlinks are resolved or the limit is reached
    for (boolean isLink = true; isLink;) {
      try {
        in = next(fs, p);
        isLink = false;
      } catch (UnresolvedLinkException e) {
        if (count++ > MAX_PATH_LINKS) {
          throw new IOException("Possible cyclic loop while " +
                                "following symbolic link " + path);
        }
        // Resolve the first unresolved path component
        p = FSLinkResolver.qualifySymlinkTarget(fs, p, fs.getLinkTarget(p));
        fs = fc.getFSofPath(p);
      }
    }
    return in;
  }
}
