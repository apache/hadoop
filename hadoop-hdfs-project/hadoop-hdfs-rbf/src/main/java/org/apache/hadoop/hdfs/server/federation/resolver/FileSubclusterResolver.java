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

package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * Interface to map a file path in the global name space to a specific
 * subcluster and path in an HDFS name space.
 * <p>
 * Each path in the global/federated namespace may map to 1-N different HDFS
 * locations.  Each location specifies a single nameservice and a single HDFS
 * path.  The behavior is similar to MergeFS and Nfly and allows the merger
 * of multiple HDFS locations into a single path.  See HADOOP-8298 and
 * HADOOP-12077
 * <p>
 * For example, a directory listing will fetch listings for each destination
 * path and combine them into a single set of results.
 * <p>
 * When multiple destinations are available for a path, the destinations are
 * prioritized in a consistent manner.  This allows the proxy server to
 * guess the best/most likely destination and attempt it first.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FileSubclusterResolver {

  /**
   * Get the destinations for a global path. Results are from the mount table
   * cache.  If multiple destinations are available, the first result is the
   * highest priority destination.
   *
   * @param path Global path.
   * @return Location in a destination namespace or null if it does not exist.
   * @throws IOException Throws exception if the data is not available.
   */
  PathLocation getDestinationForPath(String path) throws IOException;

  /**
   * Get a list of mount points for a path. Results are from the mount table
   * cache.
   *
   * @param path Path to get the mount points under.
   * @return List of mount points present at this path. Return zero-length
   *         list if the path is a mount point but there are no mount points
   *         under the path. Return null if the path is not a mount point
   *         and there are no mount points under the path.
   * @throws IOException Throws exception if the data is not available.
   */
  List<String> getMountPoints(String path) throws IOException;

  /**
   * Get the default namespace for the cluster.
   *
   * @return Default namespace identifier.
   */
  String getDefaultNamespace();

  /**
   * Get all namespaces which have mountpoints for the cluster.
   *
   * @return a set of namespace identifiers.
   */
  Set<String> getAllNamespaces();

  /**
   * Get a list of mount points for a path.
   *
   * @param path Path to get the mount points under.
   * @param mountPoints the mount points to choose.
   * @return Return empty list if the path is a mount point but there are no
   *         mount points under the path. Return null if the path is not a mount
   *         point and there are no mount points under the path.
   */
  static List<String> getMountPoints(String path,
      Collection<String> mountPoints) {
    Set<String> children = new TreeSet<>();
    boolean exists = false;
    for (String subPath : mountPoints) {
      String child = subPath;

      // Special case for /
      if (!path.equals(Path.SEPARATOR)) {
        // Get the children
        int ini = path.length();
        child = subPath.substring(ini);
      }

      if (child.isEmpty()) {
        // This is a mount point but without children
        exists = true;
      } else if (child.startsWith(Path.SEPARATOR)) {
        // This is a mount point with children
        exists = true;
        child = child.substring(1);

        // We only return immediate children
        int fin = child.indexOf(Path.SEPARATOR);
        if (fin > -1) {
          child = child.substring(0, fin);
        }
        if (!child.isEmpty()) {
          children.add(child);
        }
      }
    }
    if (!exists) {
      return null;
    }
    return new LinkedList<>(children);
  }
}
