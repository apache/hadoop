/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.ArrayList;
import java.util.HashMap;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Wrapper class for handling Ozone prefix path lookup of ACL APIs
 * with radix tree.
 */
public class RadixTree<T> {

  /**
   * create a empty radix tree with root only.
   */
  public RadixTree() {
    root = new RadixNode<T>(PATH_DELIMITER);
  }

  /**
   * If the Radix tree contains root only.
   * @return true if the radix tree contains root only.
   */
  public boolean isEmpty() {
    return root.hasChildren();
  }

  /**
   * Insert prefix tree node without value, value can be ACL or other metadata
   * of the prefix path.
   * @param path
   */
  public void insert(String path) {
    insert(path, null);
  }

  /**
   * Insert prefix tree node with value, value can be ACL or other metadata
   * of the prefix path.
   * @param path
   * @param val
   */
  public void insert(String path, T val) {
    // all prefix path inserted should end with "/"
    RadixNode<T> n = root;
    Path p = Paths.get(path);
    for (int level = 0; level < p.getNameCount(); level++) {
      HashMap<String, RadixNode> child = n.getChildren();
      String component = p.getName(level).toString();
      if (child.containsKey(component)) {
        n = child.get(component);
      } else {
        RadixNode tmp = new RadixNode(component);
        child.put(component, tmp);
        n = tmp;
      }
    }
    if (val != null) {
      n.setValue(val);
    }
  }

  /**
   * Get the last node in the exact prefix path that matches in the tree.
   * @param path - prefix path
   * @return last node in the prefix tree or null if non exact prefix matchl
   */
  public RadixNode<T> getLastNodeInPrefixPath(String path) {
    List<RadixNode<T>> lpp = getLongestPrefixPath(path);
    Path p = Paths.get(path);
    if (lpp.size() != p.getNameCount() + 1) {
      return null;
    } else {
      return lpp.get(p.getNameCount());
    }
  }

  /**
   * Remove prefix path.
   * @param path
   */
  public void removePrefixPath(String path) {
    Path p = Paths.get(path);
    removePrefixPathInternal(root, p, 0);
  }

  /**
   * Recursively remove non-overlapped part of the prefix path from radix tree.
   * @param current current radix tree node.
   * @param path prefix path to be removed.
   * @param level current recursive level.
   * @return true if current radix node can be removed.
   *             (not overlapped with other path),
   *         false otherwise.
   */
  private boolean removePrefixPathInternal(RadixNode<T> current,
      Path path, int level) {
    // last component is processed
    if (level == path.getNameCount()) {
      return current.hasChildren();
    }

    // not last component, recur for next component
    String name = path.getName(level).toString();
    RadixNode<T> node = current.getChildren().get(name);
    if (node == null)  {
      return false;
    }

    if (removePrefixPathInternal(node, path, level+1)) {
      current.getChildren().remove(name);
      return current.hasChildren();
    }
    return false;
  }

  /**
   * Get the longest prefix path.
   * @param path - prefix path.
   * @return longest prefix path as list of RadixNode.
   */
  public List<RadixNode<T>> getLongestPrefixPath(String path) {
    RadixNode n = root;
    Path p = Paths.get(path);
    int level = 0;
    List<RadixNode<T>> result = new ArrayList<>();
    result.add(root);
    while (level < p.getNameCount()) {
      HashMap<String, RadixNode> children = n.getChildren();
      if (children.isEmpty()) {
        break;
      }
      String component = p.getName(level).toString();
      if (children.containsKey(component)) {
        n = children.get(component);
        result.add(n);
        level++;
      } else {
        break;
      }
    }
    return result;
  }

  @VisibleForTesting
  /**
   * Convert radix path to string format for output.
   * @param path - radix path represented by list of radix nodes.
   * @return radix path as string separated by "/".
   * Note: the path will always be normalized with and ending "/".
   */
  public static String radixPathToString(List<RadixNode<Integer>> path) {
    StringBuilder sb = new StringBuilder();
    for (RadixNode n : path) {
      sb.append(n.getName());
      sb.append(n.getName().equals(PATH_DELIMITER) ? "" : PATH_DELIMITER);
    }
    return sb.toString();
  }

  /**
   * Get the longest prefix path.
   * @param path - prefix path.
   * @return longest prefix path as String separated by "/".
   */
  public String getLongestPrefix(String path) {
    RadixNode<T> n = root;
    Path p = Paths.get(path);
    int level = 0;
    while (level < p.getNameCount()) {
      HashMap<String, RadixNode> children = n.getChildren();
      if (children.isEmpty()) {
        break;
      }
      String component = p.getName(level).toString();
      if (children.containsKey(component)) {
        n = children.get(component);
        level++;
      } else {
        break;
      }
    }

    if (level >= 1) {
      Path longestMatch =
          Paths.get(root.getName()).resolve(p.subpath(0, level));
      String ret = longestMatch.toString();
      return path.endsWith("/") ?  ret + "/" : ret;
    } else {
      return root.getName();
    }
  }

  // root of a radix tree has a name of "/" and may optionally has it value.
  private RadixNode root;

  private final static String PATH_DELIMITER = OzoneConsts.OZONE_URI_DELIMITER;
}
