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

package org.apache.hadoop.fs.s3a.impl;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;

/**
 * Tracks directory markers which have been reported in object listings.
 * This is needed for auditing and cleanup, including during rename
 * operations.
 * <p></p>
 * Designed to be used while scanning through the results of listObject
 * calls, where are we assume the results come in alphanumeric sort order
 * and parent entries before children.
 * <p></p>
 * This lets as assume that we can identify all leaf markers as those
 * markers which were added to set of leaf markers and not subsequently
 * removed as a child entries were discovered.
 * <p></p>
 * To avoid scanning datastructures excessively, the path of the parent
 * directory of the last file added is cached. This allows for a
 * quick bailout when many children of the same directory are
 * returned in a listing.
 */
public class DirMarkerTracker {

  private final Map<Path, Pair<String, S3ALocatedFileStatus>> leafMarkers
      = new TreeMap<>();

  private final Map<Path, Pair<String, S3ALocatedFileStatus>> surplusMarkers
      = new TreeMap<>();

  private Path lastDirChecked;

  public int markerFound(Path path,
      final String key,
      final S3ALocatedFileStatus source) {
    leafMarkers.put(path, Pair.of(key, source));
    return fileFound(path, key, source);
  }

  public int fileFound(Path path,
      final String key,
      final S3ALocatedFileStatus source) {
    // all parent entries are superfluous
    final Path parent = path.getParent();
    if (parent == null || parent.equals(lastDirChecked)) {
      // short cut exit
      return 0;
    }
    final int markers = removeParentMarkers(parent);
    lastDirChecked = parent;
    return markers;
  }

  /**
   * Remove all markers from the path and its parents.
   * @param path path to start at
   * @return number of markers removed.
   */
  private int removeParentMarkers(final Path path) {
    if (path == null || path.isRoot()) {
      return 0;
    }
    int parents = removeParentMarkers(path.getParent());
    final Pair<String, S3ALocatedFileStatus> value = leafMarkers.remove(path);
    if (value != null) {
      // marker is surplus
      surplusMarkers.put(path, value);
      parents++;
    }
    return parents;
  }

  public Map<Path, Pair<String, S3ALocatedFileStatus>> getLeafMarkers() {
    return leafMarkers;
  }

  public Map<Path, Pair<String, S3ALocatedFileStatus>> getSurplusMarkers() {
    return surplusMarkers;
  }
}
