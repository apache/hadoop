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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;

/**
 * Comparator of path ordering for sorting collections.
 *
 * The definition of "topmost" is:
 * <ol>
 *   <li>The depth of a path is the primary comparator.</li>
 *   <li>Root is topmost, "0"</li>
 *   <li>If two paths are of equal depth, {@link Path#compareTo(Path)}</li>
 *   is used. This delegates to URI compareTo.
 *   <li>repeated sorts do not change the order</li>
 * </ol>
 */
final class PathOrderComparators {

  private PathOrderComparators() {
  }

  /**
   * The shallowest paths come first.
   * This is to be used when adding entries.
   */
  static final Comparator<Path> TOPMOST_PATH_FIRST
      = new TopmostFirst();

  /**
   * The leaves come first.
   * This is to be used when deleting entries.
   */
  static final Comparator<Path> TOPMOST_PATH_LAST
      = new TopmostLast();

  /**
   * The shallowest paths come first.
   * This is to be used when adding entries.
   */
  static final Comparator<PathMetadata> TOPMOST_PM_FIRST
      = new PathMetadataComparator(TOPMOST_PATH_FIRST);

  /**
   * The leaves come first.
   * This is to be used when deleting entries.
   */
  static final Comparator<PathMetadata> TOPMOST_PM_LAST
      = new PathMetadataComparator(TOPMOST_PATH_LAST);

  private static class TopmostFirst implements Comparator<Path>, Serializable {

    @Override
    public int compare(Path pathL, Path pathR) {
      // exit fast on equal values.
      if (pathL.equals(pathR)) {
        return 0;
      }
      int depthL = pathL.depth();
      int depthR = pathR.depth();
      if (depthL < depthR) {
        // left is higher up than the right.
        return -1;
      }
      if (depthR < depthL) {
        // right is higher up than the left
        return 1;
      }
      // and if they are of equal depth, use the "classic" comparator
      // of paths.
      return pathL.compareTo(pathR);
    }
  }

  /**
   * Compare the topmost last.
   * For some reason the .reverse() option wasn't giving the
   * correct outcome.
   */
  private static final class TopmostLast extends TopmostFirst {

    @Override
    public int compare(final Path pathL, final Path pathR) {
      int compare = super.compare(pathL, pathR);
      if (compare < 0) {
        return 1;
      }
      if (compare > 0) {
        return -1;
      }
      return 0;
    }
  }

  /**
   * Compare on path status.
   */
  static final class PathMetadataComparator implements
      Comparator<PathMetadata>, Serializable {

    private final Comparator<Path> inner;

    PathMetadataComparator(final Comparator<Path> inner) {
      this.inner = inner;
    }

    @Override
    public int compare(final PathMetadata o1, final PathMetadata o2) {
      return inner.compare(o1.getFileStatus().getPath(),
          o2.getFileStatus().getPath());
    }
  }
}
