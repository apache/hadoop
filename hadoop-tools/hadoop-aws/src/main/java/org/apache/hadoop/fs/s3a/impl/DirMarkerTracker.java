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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p></p>
 * Consult the directory_markers document for details on this feature,
 * including terminology.
 */
public class DirMarkerTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(DirMarkerTracker.class);

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
   * Base path of the tracking operation.
   */
  private final Path basePath;

  /**
   * Should surplus markers be recorded in
   * the {@link #surplusMarkers} map?
   */
  private final boolean recordSurplusMarkers;

  /**
   * last parent directory checked.
   */
  private Path lastDirChecked;

  /**
   * Count of scans; used for test assertions.
   */
  private int scanCount;

  /**
   * How many files were found.
   */
  private int filesFound;

  /**
   * How many markers were found.
   */
  private int markersFound;

  /**
   * How many objects of any kind were found?
   */
  private int objectsFound;

  /**
   * Construct.
   * <p></p>
   * The base path is currently only used for information rather than
   * validating paths supplied in other methods.
   * @param basePath base path of track
   * @param recordSurplusMarkers save surplus markers to a map?
   */
  public DirMarkerTracker(final Path basePath,
      boolean recordSurplusMarkers) {
    this.basePath = basePath;
    this.recordSurplusMarkers = recordSurplusMarkers;
  }

  /**
   * Get the base path of the tracker.
   * @return the path
   */
  public Path getBasePath() {
    return basePath;
  }

  /**
   * A marker has been found; this may or may not be a leaf.
   * <p></p>
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
   * A path has been found.
   * <p></p>
   * Declare all markers above it as surplus
   * @param path marker path
   * @param key object key
   * @param source listing source
   * @return the surplus markers found.
   */
  private List<Marker> pathFound(Path path,
      final String key,
      final S3ALocatedFileStatus source) {
    objectsFound++;
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
   * Remove all markers from the path and its parents from the
   * {@link #leafMarkers} map.
   * <p></p>
   * if {@link #recordSurplusMarkers} is true, the marker is
   * moved to the surplus map. Not doing this is simply an
   * optimisation designed to reduce risk of excess memory consumption
   * when renaming (hypothetically) large directory trees.
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
      removed.add(value);
      if (recordSurplusMarkers) {
        surplusMarkers.put(path, value);
      }
    }
  }

  /**
   * Get the map of leaf markers.
   * @return all leaf markers.
   */
  public Map<Path, Marker> getLeafMarkers() {
    return leafMarkers;
  }

  /**
   * Get the map of surplus markers.
   * <p></p>
   * Empty if they were not being recorded.
   * @return all surplus markers.
   */
  public Map<Path, Marker> getSurplusMarkers() {
    return surplusMarkers;
  }

  public Path getLastDirChecked() {
    return lastDirChecked;
  }


  /**
   * How many objects were found.
   * @return count
   */
  public int getObjectsFound() {
    return objectsFound;
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
   * Scan the surplus marker list and remove from it all where the directory
   * policy says "keep". This is useful when auditing
   * @param policy policy to use when auditing markers for
   * inclusion/exclusion.
   * @return list of markers stripped
   */
  public List<Path> removeAllowedMarkers(DirectoryPolicy policy) {
    List<Path> removed = new ArrayList<>();
    Iterator<Map.Entry<Path, Marker>> entries =
        surplusMarkers.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<Path, Marker> entry = entries.next();
      Path path = entry.getKey();
      if (policy.keepDirectoryMarkers(path)) {
        // there's a match
        // remove it from the map.
        entries.remove();
        LOG.debug("Removing {}", entry.getValue());
        removed.add(path);
      }
    }
    return removed;
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

    private Marker(final Path path,
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

    /**
     * Get the version ID of the status object; may be null.
     * @return a version ID, if known.
     */
    public String getVersionId() {
      return status.getVersionId();
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
