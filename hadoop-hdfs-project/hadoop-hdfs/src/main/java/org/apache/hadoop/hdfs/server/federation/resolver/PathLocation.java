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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A map of the properties and target destinations (name space + path) for
 * a path in the global/federated namespace.
 * This data is generated from the @see MountTable records.
 */
public class PathLocation {

  /** Source path in global namespace. */
  private final String sourcePath;

  /** Remote paths in the target namespaces. */
  private final List<RemoteLocation> destinations;

  /** List of name spaces present. */
  private final Set<String> namespaces;


  /**
   * Create a new PathLocation.
   *
   * @param source Source path in the global name space.
   * @param dest Destinations of the mount table entry.
   * @param namespaces Unique identifier representing the combination of
   *          name spaces present in the destination list.
   */
  public PathLocation(
      String source, List<RemoteLocation> dest, Set<String> nss) {
    this.sourcePath = source;
    this.destinations = dest;
    this.namespaces = nss;
  }

  /**
   * Create a path location from another path.
   *
   * @param other Other path location to copy from.
   */
  public PathLocation(PathLocation other) {
    this.sourcePath = other.sourcePath;
    this.destinations = new LinkedList<RemoteLocation>(other.destinations);
    this.namespaces = new HashSet<String>(other.namespaces);
  }

  /**
   * Get the source path in the global namespace for this path location.
   *
   * @return The path in the global namespace.
   */
  public String getSourcePath() {
    return this.sourcePath;
  }

  /**
   * Get the list of subclusters defined for the destinations.
   */
  public Set<String> getNamespaces() {
    return Collections.unmodifiableSet(this.namespaces);
  }

  @Override
  public String toString() {
    RemoteLocation loc = getDefaultLocation();
    return loc.getNameserviceId() + "->" + loc.getDest();
  }

  /**
   * Check if this location supports multiple clusters/paths.
   *
   * @return If it has multiple destinations.
   */
  public boolean hasMultipleDestinations() {
    return this.destinations.size() > 1;
  }

  /**
   * Get the list of locations found in the mount table.
   * The first result is the highest priority path.
   *
   * @return List of remote locations.
   */
  public List<RemoteLocation> getDestinations() {
    return Collections.unmodifiableList(this.destinations);
  }

  /**
   * Get the default or highest priority location.
   *
   * @return The default location.
   */
  public RemoteLocation getDefaultLocation() {
    if (destinations.isEmpty() || destinations.get(0).getDest() == null) {
      throw new UnsupportedOperationException(
          "Unsupported path " + sourcePath + " please check mount table");
    }
    return destinations.get(0);
  }
}