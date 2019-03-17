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
package org.apache.hadoop.hdds.scm.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class to facilitate network topology functions.
 */
public final class NetUtils {
  public static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);
  private NetUtils() {
    // Prevent instantiation
  }
  /**
   * Normalize a path by stripping off any trailing.
   * {@link NetConstants#PATH_SEPARATOR}
   * @param path path to normalize.
   * @return the normalised path
   * If <i>path</i>is empty or null, then {@link NetConstants#ROOT} is returned
   */
  public static String normalize(String path) {
    if (path == null || path.length() == 0) {
      return NetConstants.ROOT;
    }

    if (path.charAt(0) != NetConstants.PATH_SEPARATOR) {
      throw new IllegalArgumentException(
          "Network Location path does not start with "
              + NetConstants.PATH_SEPARATOR_STR + ": " + path);
    }

    // Remove any trailing NetConstants.PATH_SEPARATOR
    return path.length() == 1 ? path :
        path.replaceAll(NetConstants.PATH_SEPARATOR_STR + "+$", "");
  }

  /**
   *  Given a network topology location string, return its network topology
   *  depth, E.g. the depth of /dc1/rack1/ng1/node1 is 5.
   */
  public static int locationToDepth(String location) {
    String newLocation = normalize(location);
    return newLocation.equals(NetConstants.PATH_SEPARATOR_STR) ? 1 :
        newLocation.split(NetConstants.PATH_SEPARATOR_STR).length;
  }


  /**
   *  Remove node from mutableExcludedNodes if it's covered by excludedScope.
   *  Please noted that mutableExcludedNodes content might be changed after the
   *  function call.
   * @return the new excludedScope
   */
  public static String removeDuplicate(NetworkTopology topology,
      Collection<Node> mutableExcludedNodes, String excludedScope,
      int ancestorGen) {
    if (mutableExcludedNodes == null || mutableExcludedNodes.size() == 0 ||
        excludedScope == null || topology == null) {
      return excludedScope;
    }

    Iterator<Node> iterator = mutableExcludedNodes.iterator();
    while (iterator.hasNext()) {
      Node node = iterator.next();
      Node ancestor = topology.getAncestor(node, ancestorGen);
      if (ancestor == null) {
        LOG.warn("Fail to get ancestor generation " + ancestorGen +
            " of node :" + node);
        continue;
      }
      if (excludedScope.startsWith(ancestor.getNetworkFullPath())) {
        // reset excludedScope if it's covered by exclude node's ancestor
        return null;
      }
      if (ancestor.getNetworkFullPath().startsWith(excludedScope)) {
        // remove exclude node if it's covered by excludedScope
        iterator.remove();
      }
    }
    return excludedScope;
  }

  /**
   *  Remove node from mutableExcludedNodes if it's not part of scope
   *  Please noted that mutableExcludedNodes content might be changed after the
   *  function call.
   */
  public static void removeOutscope(Collection<Node> mutableExcludedNodes,
      String scope) {
    if (mutableExcludedNodes == null || scope == null) {
      return;
    }
    synchronized (mutableExcludedNodes) {
      Iterator<Node> iterator = mutableExcludedNodes.iterator();
      while (iterator.hasNext()) {
        Node next = iterator.next();
        if (!next.getNetworkFullPath().startsWith(scope)) {
          iterator.remove();
        }
      }
    }
  }

  /**
   * Get a ancestor list for nodes on generation <i>generation</i>.
   *
   * @param nodes a collection of leaf nodes
   * @param generation  the ancestor generation
   * @return the ancestor list. If no ancestor is found, then a empty list is
   * returned.
   */
  public static List<Node> getAncestorList(NetworkTopology topology,
      Collection<Node> nodes, int generation) {
    List<Node> ancestorList = new ArrayList<>();
    if (topology == null ||nodes == null || nodes.size() == 0 ||
        generation == 0) {
      return ancestorList;
    }
    Iterator<Node> iterator = nodes.iterator();
    while (iterator.hasNext()) {
      Node node = iterator.next();
      Node ancestor = topology.getAncestor(node, generation);
      if (ancestor == null) {
        LOG.warn("Fail to get ancestor generation " + generation +
            " of node :" + node);
        continue;
      }
      if (!ancestorList.contains(ancestor)) {
        ancestorList.add(ancestor);
      }
    }
    return ancestorList;
  }
}
