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

import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A map of the properties and target destinations (name space + path) for
 * a path in the global/federated name space.
 * This data is generated from the @see MountTable records.
 */
public class PathLocation {

  private static final Logger LOG = LoggerFactory.getLogger(PathLocation.class);


  /** Source path in global namespace. */
  private final String sourcePath;

  /** Remote paths in the target name spaces. */
  private final List<RemoteLocation> destinations;
  /** Order for the destinations. */
  private final DestinationOrder destOrder;


  /**
   * Create a new PathLocation.
   *
   * @param source Source path in the global name space.
   * @param dest Destinations of the mount table entry.
   * @param order Order of the locations.
   */
  public PathLocation(
      String source, List<RemoteLocation> dest, DestinationOrder order) {
    this.sourcePath = source;
    this.destinations = Collections.unmodifiableList(dest);
    this.destOrder = order;
  }

  /**
   * Create a new PathLocation with default HASH order.
   *
   * @param source Source path in the global name space.
   * @param dest Destinations of the mount table entry.
   */
  public PathLocation(String source, List<RemoteLocation> dest) {
    this(source, dest, DestinationOrder.HASH);
  }

  /**
   * Create a path location from another path.
   *
   * @param other Other path location to copy from.
   */
  public PathLocation(final PathLocation other) {
    this.sourcePath = other.sourcePath;
    this.destinations = Collections.unmodifiableList(other.destinations);
    this.destOrder = other.destOrder;
  }

  /**
   * Create a path location from another path with the destinations sorted.
   *
   * @param other Other path location to copy from.
   * @param firstNsId Identifier of the namespace to place first.
   */
  public PathLocation(PathLocation other, String firstNsId) {
    this.sourcePath = other.sourcePath;
    this.destOrder = other.destOrder;
    this.destinations = orderedNamespaces(other.destinations, firstNsId);
  }

  /**
   * Prioritize a location/destination by its name space/nameserviceId.
   * This destination might be used by other threads, so the source is not
   * modifiable.
   *
   * @param original List of destinations to order.
   * @param nsId The name space/nameserviceID to prioritize.
   * @return Prioritized list of detinations that cannot be modified.
   */
  private static List<RemoteLocation> orderedNamespaces(
      final List<RemoteLocation> original, final String nsId) {
    if (original.size() <= 1) {
      return original;
    }

    LinkedList<RemoteLocation> newDestinations = new LinkedList<>();
    boolean found = false;
    for (RemoteLocation dest : original) {
      if (dest.getNameserviceId().equals(nsId)) {
        found = true;
        newDestinations.addFirst(dest);
      } else {
        newDestinations.add(dest);
      }
    }

    if (!found) {
      LOG.debug("Cannot find location with namespace {} in {}",
          nsId, original);
    }
    return Collections.unmodifiableList(newDestinations);
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
   * Get the subclusters defined for the destinations.
   *
   * @return Set containing the subclusters.
   */
  public Set<String> getNamespaces() {
    Set<String> namespaces = new HashSet<>();
    List<RemoteLocation> locations = this.getDestinations();
    for (RemoteLocation location : locations) {
      String nsId = location.getNameserviceId();
      namespaces.add(nsId);
    }
    return namespaces;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (RemoteLocation destination : this.destinations) {
      String nsId = destination.getNameserviceId();
      String path = destination.getDest();
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(nsId + "->" + path);
    }
    if (this.destinations.size() > 1) {
      sb.append(" [")
          .append(this.destOrder.toString())
          .append("]");
    }
    return sb.toString();
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
   * Get the order for the destinations.
   *
   * @return Order for the destinations.
   */
  public DestinationOrder getDestinationOrder() {
    return this.destOrder;
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