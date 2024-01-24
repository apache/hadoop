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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.WILDCARD_QUEUE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

/**
 * This is a helper class which represents a queue path, and has easy access
 * methods to get the path's parent or leaf part, or as a whole.
 */
public class QueuePath implements Iterable<String> {
  private static final String QUEUE_REGEX_DELIMITER = "\\.";
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
  public QueuePath(String parent, String leaf) {
    this.parent = parent;
    this.leaf = leaf;
  }

  /**
   * Constructor creates a MappingQueuePath object using the queue's full path.
   * @param fullPath Full path of the queue
   */
  public QueuePath(String fullPath) {
    setFromFullPath(fullPath);
  }

  /**
   * Constructor to create Queue path from queue names.
   * The provided queue names will be concatenated by dots, giving a full queue path.
   * @param parts Parts of queue path
   * @return QueuePath object
   */
  public static QueuePath createFromQueues(String... parts) {
    return new QueuePath(String.join(DOT, parts));
  }

  /**
   * This method is responsible for splitting up a full queue path into parent
   * path and leaf name.
   * @param fullPath Full path of the queue to be processed
   */
  private void setFromFullPath(String fullPath) {
    parent = null;
    leaf = fullPath;

    if (leaf == null) {
      leaf = "";
      return;
    }

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
    // iterator will not contain an empty leaf queue, so check directly
    if (leaf.isEmpty()) {
      return true;
    }

    for (String part : this) {
      if (part.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Simple helper method to determine if the queue path is invalid or not.
   * @return true if the queue path is invalid.
   */
  public boolean isInvalid() {
    return getPathComponents().length <= 1 && !isRoot();
  }

  /**
   * Getter for the parent part of the path.
   * @return Parent path of the queue, null if there is no parent.
   */
  public String getParent() {
    return parent;
  }

  /**
   * Getter for the parent object of the path.
   * @return Parent QueuePath object of the queue, null if there is no parent.
   */
  public QueuePath getParentObject() {
    return hasParent() ? new QueuePath(parent) : null;
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

  /**
   * Convenience getter to check if the queue is the root queue.
   * @return True if the path is root
   */
  public boolean isRoot() {
    return !hasParent() && leaf.equals(ROOT);
  }

  /**
   * Creates a new {@code QueuePath} from the current full path as parent, and
   * the appended child queue path as leaf.
   * @param childQueue path of leaf queue
   * @return new queue path made of current full path and appended leaf path
   */
  public QueuePath createNewLeaf(String childQueue) {
    return new QueuePath(getFullPath(), childQueue);
  }

  /**
   * Returns an iterator of queue path parts, starting from the highest level
   * (generally root).
   * @return queue part iterator
   */
  @Override
  public Iterator<String> iterator() {
    return Arrays.asList(getPathComponents()).iterator();
  }

  /**
   * Returns an iterator that provides a way to traverse the queue path from
   * current queue through its parents.
   * @return queue path iterator
   */
  public Iterator<String> reverseIterator() {

    return new Iterator<String>() {
      private String current = getFullPath();

      @Override
      public boolean hasNext() {
        return current != null;
      }

      @Override
      public String next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        int parentQueueNameEndIndex = current.lastIndexOf(".");
        String old = current;
        if (parentQueueNameEndIndex > -1) {
          current = current.substring(0, parentQueueNameEndIndex).trim();
        } else {
          current = null;
        }

        return old;
      }
    };
  }

  /**
   * Returns the list of wildcarded queue paths based on the autoCreatedQueueDepth config value.
   * An example template precedence hierarchy for root.a ParentQueue from highest to lowest:
   * yarn.scheduler.capacity.root.a.auto-queue-creation-v2.template.capacity
   * yarn.scheduler.capacity.root.*.auto-queue-creation-v2.template.capacity
   * @param maxAutoCreatedQueueDepth the maximum depth of auto-created queues stored in the
   *                                 configuration
   * @return list of wildcarded QueuePath objects
   */
  public List<QueuePath> getWildcardedQueuePaths(int maxAutoCreatedQueueDepth) {
    List<QueuePath> wildcardedPaths = new ArrayList<>();
    // Start with the most explicit format (without wildcard)
    wildcardedPaths.add(this);

    String[] pathComponents = getPathComponents();
    int supportedWildcardLevel = getSupportedWildcardLevel(maxAutoCreatedQueueDepth);

    // Collect all template entries
    for (int wildcardLevel = 1; wildcardLevel <= supportedWildcardLevel; wildcardLevel++) {
      int wildcardedComponentIndex = pathComponents.length - wildcardLevel;
      pathComponents[wildcardedComponentIndex] = WILDCARD_QUEUE;
      QueuePath wildcardedPath = createFromQueues(pathComponents);
      wildcardedPaths.add(wildcardedPath);
    }

    return wildcardedPaths;
  }

  /**
   * Returns the supported wildcard level for this queue path.
   * @param maxAutoCreatedQueueDepth the maximum depth of auto-created queues stored in the
   *                                 configuration
   * @return int value of the supported wildcard level
   */
  private int getSupportedWildcardLevel(int maxAutoCreatedQueueDepth) {
    int queuePathMaxIndex = getPathComponents().length - 1;
    // Allow root to have template properties
    return isRoot() ? 0 : Math.min(queuePathMaxIndex, maxAutoCreatedQueueDepth);
  }

  /**
   * Returns queue path components.
   * @return String array containing the queue names.
   */
  public String[] getPathComponents() {
    return getFullPath().split(QUEUE_REGEX_DELIMITER);
  }

  @Override
  public String toString() {
    return getFullPath();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueuePath other = (QueuePath) o;
    return Objects.equals(parent, other.parent) &&
        Objects.equals(leaf, other.leaf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parent, leaf);
  }
}
