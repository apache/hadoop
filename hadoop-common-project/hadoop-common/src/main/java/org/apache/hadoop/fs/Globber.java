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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Globber {
  public static final Log LOG = LogFactory.getLog(Globber.class.getName());

  private final FileSystem fs;
  private final FileContext fc;
  private final Path pathPattern;
  private final PathFilter filter;
  
  public Globber(FileSystem fs, Path pathPattern, PathFilter filter) {
    this.fs = fs;
    this.fc = null;
    this.pathPattern = pathPattern;
    this.filter = filter;
  }

  public Globber(FileContext fc, Path pathPattern, PathFilter filter) {
    this.fs = null;
    this.fc = fc;
    this.pathPattern = pathPattern;
    this.filter = filter;
  }

  private FileStatus getFileStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.getFileStatus(path);
      } else {
        return fc.getFileStatus(path);
      }
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  private FileStatus[] listStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.listStatus(path);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  private Path fixRelativePart(Path path) {
    if (fs != null) {
      return fs.fixRelativePart(path);
    } else {
      return fc.fixRelativePart(path);
    }
  }

  /**
   * Translate an absolute path into a list of path components.
   * We merge double slashes into a single slash here.
   * POSIX root path, i.e. '/', does not get an entry in the list.
   */
  private static List<String> getPathComponents(String path)
      throws IOException {
    ArrayList<String> ret = new ArrayList<String>();
    for (String component : path.split(Path.SEPARATOR)) {
      if (!component.isEmpty()) {
        ret.add(component);
      }
    }
    return ret;
  }

  private String schemeFromPath(Path path) throws IOException {
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      if (fs != null) {
        scheme = fs.getUri().getScheme();
      } else {
        scheme = fc.getDefaultFileSystem().getUri().getScheme();
      }
    }
    return scheme;
  }

  private String authorityFromPath(Path path) throws IOException {
    String authority = path.toUri().getAuthority();
    if (authority == null) {
      if (fs != null) {
        authority = fs.getUri().getAuthority();
      } else {
        authority = fc.getDefaultFileSystem().getUri().getAuthority();
      }
    }
    return authority ;
  }

  public FileStatus[] glob() throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    String scheme = schemeFromPath(pathPattern);
    String authority = authorityFromPath(pathPattern);

    // Next we strip off everything except the pathname itself, and expand all
    // globs.  Expansion is a process which turns "grouping" clauses,
    // expressed as brackets, into separate path patterns.
    String pathPatternString = pathPattern.toUri().getPath();
    List<String> flattenedPatterns = GlobExpander.expand(pathPatternString);

    // Now loop over all flattened patterns.  In every case, we'll be trying to
    // match them to entries in the filesystem.
    ArrayList<FileStatus> results = 
        new ArrayList<FileStatus>(flattenedPatterns.size());
    boolean sawWildcard = false;
    for (String flatPattern : flattenedPatterns) {
      // Get the absolute path for this flattened pattern.  We couldn't do 
      // this prior to flattening because of patterns like {/,a}, where which
      // path you go down influences how the path must be made absolute.
      Path absPattern = fixRelativePart(new Path(
          flatPattern.isEmpty() ? Path.CUR_DIR : flatPattern));
      // Now we break the flattened, absolute pattern into path components.
      // For example, /a/*/c would be broken into the list [a, *, c]
      List<String> components =
          getPathComponents(absPattern.toUri().getPath());
      // Starting out at the root of the filesystem, we try to match
      // filesystem entries against pattern components.
      ArrayList<FileStatus> candidates = new ArrayList<FileStatus>(1);
      if (Path.WINDOWS && !components.isEmpty()
          && Path.isWindowsAbsolutePath(absPattern.toUri().getPath(), true)) {
        // On Windows the path could begin with a drive letter, e.g. /E:/foo.
        // We will skip matching the drive letter and start from listing the
        // root of the filesystem on that drive.
        String driveLetter = components.remove(0);
        candidates.add(new FileStatus(0, true, 0, 0, 0, new Path(scheme,
            authority, Path.SEPARATOR + driveLetter + Path.SEPARATOR)));
      } else {
        candidates.add(new FileStatus(0, true, 0, 0, 0,
            new Path(scheme, authority, Path.SEPARATOR)));
      }
      
      for (String component : components) {
        ArrayList<FileStatus> newCandidates =
            new ArrayList<FileStatus>(candidates.size());
        GlobFilter globFilter = new GlobFilter(component);
        if (globFilter.hasPattern()) {
          sawWildcard = true;
        }
        if (candidates.isEmpty() && sawWildcard) {
          break;
        }
        for (FileStatus candidate : candidates) {
          FileStatus resolvedCandidate = candidate;
          if (candidate.isSymlink()) {
            // We have to resolve symlinks, because otherwise we don't know
            // whether they are directories.
            resolvedCandidate = getFileStatus(candidate.getPath());
          }
          if (resolvedCandidate == null ||
              resolvedCandidate.isDirectory() == false) {
            continue;
          }
          FileStatus[] children = listStatus(candidate.getPath());
          for (FileStatus child : children) {
            // Set the child path based on the parent path.
            // This keeps the symlinks in our path.
            child.setPath(new Path(candidate.getPath(),
                    child.getPath().getName()));
            if (globFilter.accept(child.getPath())) {
              newCandidates.add(child);
            }
          }
        }
        candidates = newCandidates;
      }
      for (FileStatus status : candidates) {
        // HADOOP-3497 semantics: the user-defined filter is applied at the
        // end, once the full path is built up.
        if (filter.accept(status.getPath())) {
          results.add(status);
        }
      }
    }
    /*
     * When the input pattern "looks" like just a simple filename, and we
     * can't find it, we return null rather than an empty array.
     * This is a special case which the shell relies on.
     *
     * To be more precise: if there were no results, AND there were no
     * groupings (aka brackets), and no wildcards in the input (aka stars),
     * we return null.
     */
    if ((!sawWildcard) && results.isEmpty() &&
        (flattenedPatterns.size() <= 1)) {
      return null;
    }
    return results.toArray(new FileStatus[0]);
  }
}
