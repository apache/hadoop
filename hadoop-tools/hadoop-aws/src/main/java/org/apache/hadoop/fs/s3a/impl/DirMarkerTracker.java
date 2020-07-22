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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

  /**
   * all leaf markers.
   */
  private final Map<Path, Marker> leafMarkers
      = new TreeMap<>();

  /**
   * all surplus markers.
   */
  private final Map<Path, Marker> surplusMarkers
      = new TreeMap<>();

  /**
   * last parent directory checked.
   */
  private Path lastDirChecked;

  /**
   * Count of scans; used for test assertions.
   */
  private int scanCount;

  private int filesFound;

  private int markersFound;

  /**
   * A marker has been found; this may or may not be a leaf.
   * Trigger a move of all markers above it into the surplus map.
   * @param path marker path
   * @param key object key
   * @param source listing source
   * @return the surplus markers found.
   */
  public List<Marker> markerFound(Path path,
      final String key,
      final S3ALocatedFileStatus source) {
    markersFound++;
    leafMarkers.put(path, new Marker(path, key, source));
    return pathFound(path, key, source);
  }

  /**
   * A file has been found. Trigger a move of all
   * markers above it into the surplus map.
   * @param path marker path
   * @param key object key
   * @param source listing source
   * @return the surplus markers found.
   */
  public List<Marker> fileFound(Path path,
      final String key,
      final S3ALocatedFileStatus source) {
    filesFound++;
    return pathFound(path, key, source);
  }

  /**
   * A path has been found. Trigger a move of all
   * markers above it into the surplus map.
   * @param path marker path
   * @param key object key
   * @param source listing source
   * @return the surplus markers found.
   */
  public List<Marker> pathFound(Path path,
      final String key,
      final S3ALocatedFileStatus source) {
    List<Marker> removed = new ArrayList<>();

    // all parent entries are superfluous
    final Path parent = path.getParent();
    if (parent == null || parent.equals(lastDirChecked)) {
      // short cut exit
      return removed;
    }
    removeParentMarkers(parent, removed);
    lastDirChecked = parent;
    return removed;
  }

  /**
   * Remove all markers from the path and its parents.
   * @param path path to start at
   * @param removed list of markers removed; is built up during the
   * recursive operation.
   */
  private void removeParentMarkers(final Path path,
      List<Marker> removed) {
    if (path == null || path.isRoot()) {
      return;
    }
    scanCount++;
    removeParentMarkers(path.getParent(), removed);
    final Marker value = leafMarkers.remove(path);
    if (value != null) {
      // marker is surplus
      surplusMarkers.put(path, value);
      removed.add(value);
    }
  }

  /**
   * get the map of leaf markers.
   * @return all leaf markers.
   */
  public Map<Path, Marker> getLeafMarkers() {
    return leafMarkers;
  }

  /**
   * get the map of surplus markers.
   * @return all surplus markers.
   */
  public Map<Path, Marker> getSurplusMarkers() {
    return surplusMarkers;
  }

  public Path getLastDirChecked() {
    return lastDirChecked;
  }

  public int getScanCount() {
    return scanCount;
  }

  public int getFilesFound() {
    return filesFound;
  }

  public int getMarkersFound() {
    return markersFound;
  }

  @Override
  public String toString() {
    return "DirMarkerTracker{" +
        "leafMarkers=" + leafMarkers.size() +
        ", surplusMarkers=" + surplusMarkers.size() +
        ", lastDirChecked=" + lastDirChecked +
        ", filesFound=" + filesFound +
        ", scanCount=" + scanCount +
        '}';
  }

  /**
   * This is a marker entry stored in the map and
   * returned as markers are deleted.
   */
  public static final class Marker {
    /** Path of the marker. */
    private final Path path;

    /**
     * Key in the store.
     */
    private final String key;

    /**
     * The file status of the marker.
     */
    private final S3ALocatedFileStatus status;

    public Marker(final Path path,
        final String key,
        final S3ALocatedFileStatus status) {
      this.path = path;
      this.key = key;
      this.status = status;
    }

    public Path getPath() {
      return path;
    }

    public String getKey() {
      return key;
    }

    public S3ALocatedFileStatus getStatus() {
      return status;
    }

    @Override
    public String toString() {
      return "Marker{" +
          "path=" + path +
          ", key='" + key + '\'' +
          ", status=" + status +
          '}';
    }

  }

}
