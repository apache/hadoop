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
package org.apache.hadoop.net;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A base class that implements interface Node
 * 
 */

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NodeBase implements Node {
  /** Path separator {@value} */
  public final static char PATH_SEPARATOR = '/';
  /** Path separator as a string {@value} */
  public final static String PATH_SEPARATOR_STR = "/";
  /** string representation of root {@value} */
  public final static String ROOT = "";
  
  protected String name; //host:port#
  protected String location; //string representation of this node's location
  protected int level; //which level of the tree the node resides
  protected Node parent; //its parent
  
  /** Default constructor */
  public NodeBase() {
  }
  
  /** Construct a node from its path
   * @param path 
   *   a concatenation of this node's location, the path seperator, and its name 
   */
  public NodeBase(String path) {
    path = normalize(path);
    int index = path.lastIndexOf(PATH_SEPARATOR);
    if (index== -1) {
      set(ROOT, path);
    } else {
      set(path.substring(index+1), path.substring(0, index));
    }
  }
  
  /** Construct a node from its name and its location
   * @param name this node's name (can be null, must not contain {@link #PATH_SEPARATOR})
   * @param location this node's location 
   */
  public NodeBase(String name, String location) {
    set(name, normalize(location));
  }
  
  /** Construct a node from its name and its location
   * @param name this node's name (can be null, must not contain {@link #PATH_SEPARATOR})
   * @param location this node's location 
   * @param parent this node's parent node
   * @param level this node's level in the tree
   */
  public NodeBase(String name, String location, Node parent, int level) {
    set(name, normalize(location));
    this.parent = parent;
    this.level = level;
  }

  /**
   * set this node's name and location
   * @param name the (nullable) name -which cannot contain the {@link #PATH_SEPARATOR}
   * @param location the location
   */
  private void set(String name, String location) {
    if (name != null && name.contains(PATH_SEPARATOR_STR))
      throw new IllegalArgumentException(
                                         "Network location name contains /: "+name);
    this.name = (name==null)?"":name;
    this.location = location;      
  }
  
  /** @return this node's name */
  @Override
  public String getName() { return name; }
  
  /** @return this node's network location */
  @Override
  public String getNetworkLocation() { return location; }
  
  /** Set this node's network location
   * @param location the location
   */
  @Override
  public void setNetworkLocation(String location) { this.location = location; }
  
  /**
   * Get the path of a node
   * @param node a non-null node
   * @return the path of a node
   */
  public static String getPath(Node node) {
    return node.getNetworkLocation() + PATH_SEPARATOR_STR + node.getName();
  }

  /**
   * Get the path components of a node.
   * @param node a non-null node
   * @return the path of a node
   */
  public static String[] getPathComponents(Node node) {
    return getPath(node).split(PATH_SEPARATOR_STR);
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    }
    if (!(to instanceof NodeBase)) {
      return false;
    }
    return getPath(this).equals(getPath((NodeBase)to));
  }

  @Override
  public int hashCode() {
    return getPath(this).hashCode();
  }

  /** @return this node's path as its string representation */
  @Override
  public String toString() {
    return getPath(this);
  }

  /** Normalize a path by stripping off any trailing {@link #PATH_SEPARATOR}
   * @param path path to normalize.
   * @return the normalised path
   * If <i>path</i>is null or empty {@link #ROOT} is returned
   * @throws IllegalArgumentException if the first character of a non empty path
   * is not {@link #PATH_SEPARATOR}
   */
  public static String normalize(String path) {
    if (path == null) {
      throw new IllegalArgumentException(
          "Network Location is null ");
    }

    if (path.length() == 0) {
      return ROOT;
    }
    
    if (path.charAt(0) != PATH_SEPARATOR) {
      throw new IllegalArgumentException(
                                         "Network Location path does not start with "
                                         +PATH_SEPARATOR_STR+ ": "+path);
    }
    
    int len = path.length();
    if (path.charAt(len-1) == PATH_SEPARATOR) {
      return path.substring(0, len-1);
    }
    return path;
  }
  
  /** @return this node's parent */
  @Override
  public Node getParent() { return parent; }
  
  /** Set this node's parent
   * @param parent the parent
   */
  @Override
  public void setParent(Node parent) {
    this.parent = parent;
  }
  
  /** @return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  @Override
  public int getLevel() { return level; }
  
  /** Set this node's level in the tree
   * @param level the level
   */
  @Override
  public void setLevel(int level) {
    this.level = level;
  }
  
  public static int locationToDepth(String location) {
    String normalizedLocation = normalize(location);
    int length = normalizedLocation.length();
    int depth = 0;
    for (int i = 0; i < length; i++) {
      if (normalizedLocation.charAt(i) == PATH_SEPARATOR) {
        depth++;
      }
    }
    return depth;
  }
}
