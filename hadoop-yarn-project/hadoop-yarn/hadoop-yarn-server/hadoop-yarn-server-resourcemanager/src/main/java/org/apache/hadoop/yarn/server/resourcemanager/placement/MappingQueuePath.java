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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * This is a helper class which represents a queue path, and has easy access
 * methods to get the path's parent or leaf part, or as a whole.
 */
public class MappingQueuePath {
  /**
   * The parent part of the queue path.
   */
  private String parent;

  /**
   * The leaf part of the parent path.
   */
  private String leaf;

  /**
   * Constructor to create mapping queue path from parent path and leaf name.
   * @param parent Parent path of the queue
   * @param leaf Name of the leaf queue
   */
  public MappingQueuePath(String parent, String leaf) {
    this.parent = parent;
    this.leaf = leaf;
  }

  /**
   * Constructor creates a MappingQueuePath object using the queue's full path.
   * @param fullPath Full path of the queue
   */
  public MappingQueuePath(String fullPath) {
    setFromFullPath(fullPath);
  }

  /**
   * This method is responsible for splitting up a full queue path into parent
   * path and leaf name.
   * @param fullPath Full path of the queue to be processed
   */
  private void setFromFullPath(String fullPath) {
    parent = null;
    leaf = fullPath;

    int lastDotIdx = fullPath.lastIndexOf(DOT);
    if (lastDotIdx > -1) {
      parent = fullPath.substring(0, lastDotIdx).trim();
      leaf = fullPath.substring(lastDotIdx + 1).trim();
    }
  }

  /**
   * Simple helper method to determine if the path contains any empty parts.
   * @return true if there is at least one empty part of the path
   */
  public boolean hasEmptyPart() {
    String[] parts = getFullPath().split("\\.");
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].equals("")) {
        return true;
      }
    }

    return false;
  }

  /**
   * Getter for the parent part of the path.
   * @return Parent path of the queue, null if there is no parent.
   */
  public String getParent() {
    return parent;
  }

  /**
   * Getter for the leaf part of the path.
   * @return The leaf queue name
   */
  public String getLeafName() {
    return leaf;
  }

  /**
   * Getter for the full path of the queue.
   * @return Full path of the queue
   */
  public String getFullPath() {
    return hasParent() ? (parent + DOT + leaf) : leaf;
  }

  /**
   * Convenience getter to check if the queue has a parent path defined.
   * @return True if there is a parent path provided
   */
  public boolean hasParent() {
    return parent != null;
  }

  @Override
  public String toString() {
    return getFullPath();
  }
}
