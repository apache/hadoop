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
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * Turns the {@code explain} query parameter of
 * {@code POST /scheduler-conf/validate/v2} into the set of queues to explain.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ValidationExplainScope {

  /** Query parameter value selecting the queues affected by the mutation. */
  public static final String AFFECTED = "affected";

  private static final String ROOT = "root";
  private static final String PREFIX = CapacitySchedulerConfiguration.PREFIX;
  private static final String QUEUES_SUFFIX =
      CapacitySchedulerConfiguration.DOT + CapacitySchedulerConfiguration.QUEUES;

  private ValidationExplainScope() {
  }

  /**
   * @param explainParam the raw query parameter, may be null
   * @param mutation the requested mutation
   * @param proposed the proposed configuration
   * @return the full queue paths to explain in report order, or null when
   *         explain was not requested
   */
  public static List<String> resolve(String explainParam,
      SchedConfUpdateInfo mutation, ConfigSnapshot proposed) {
    if (explainParam == null || explainParam.trim().isEmpty()) {
      return null;
    }
    if (AFFECTED.equals(explainParam.trim())) {
      return affectedQueues(mutation, proposed);
    }
    Set<String> paths = new LinkedHashSet<>();
    for (String path : explainParam.split(",")) {
      String trimmed = path.trim();
      if (!trimmed.isEmpty()) {
        paths.add(trimmed);
      }
    }
    return new ArrayList<>(paths);
  }

  /**
   * Computes the queues whose resolved values the mutation can change.
   * <ul>
   *   <li>An updated or added queue and its descendants.</li>
   *   <li>The parent of an added or removed queue, without its other
   *   children: only its queue list changes, which nothing inherits.</li>
   *   <li>For a global update of a queue-scoped key (for example
   *   {@code yarn.scheduler.capacity.root.a.capacity} or a template key), the
   *   queue with the longest matching path and its descendants.</li>
   *   <li>For any other global update, every queue, since scheduler-wide
   *   defaults are inherited everywhere.</li>
   * </ul>
   * Queues are returned in depth-first order of the proposed hierarchy,
   * followed by paths that are not part of it in lexical order.
   *
   * @param mutation the requested mutation
   * @param proposed the proposed configuration
   * @return the affected full queue paths
   */
  public static List<String> affectedQueues(SchedConfUpdateInfo mutation,
      ConfigSnapshot proposed) {
    List<String> tree = queueTree(proposed);
    Set<String> roots = new TreeSet<>();
    Set<String> single = new TreeSet<>();

    for (QueueConfigInfo update : mutation.getUpdateQueueInfo()) {
      if (update != null && update.getQueue() != null) {
        roots.add(update.getQueue());
      }
    }
    for (QueueConfigInfo add : mutation.getAddQueueInfo()) {
      if (add != null && add.getQueue() != null) {
        roots.add(add.getQueue());
        addParent(add.getQueue(), single);
      }
    }
    for (String removed : mutation.getRemoveQueueInfo()) {
      if (removed != null) {
        addParent(removed, single);
      }
    }
    for (String key : mutation.getGlobalParams().keySet()) {
      roots.add(queueOfKey(key, tree));
    }

    Set<String> affected = new LinkedHashSet<>();
    Set<String> outside = new TreeSet<>();
    for (String path : roots) {
      if (!tree.contains(path)) {
        outside.add(path);
      }
    }
    for (String path : single) {
      if (!tree.contains(path)) {
        outside.add(path);
      }
    }
    for (String path : tree) {
      if (single.contains(path) || isSelfOrDescendant(path, roots)) {
        affected.add(path);
      }
    }
    affected.addAll(outside);
    return new ArrayList<>(affected);
  }

  private static void addParent(String path, Set<String> parents) {
    int lastDot = path.lastIndexOf('.');
    if (lastDot > 0) {
      parents.add(path.substring(0, lastDot));
    }
  }

  private static boolean isSelfOrDescendant(String path, Set<String> roots) {
    for (String root : roots) {
      if (path.equals(root) || path.startsWith(root + ".")) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return the queue with the longest path that scopes the key, or root
   *         when the key is not queue-scoped
   */
  private static String queueOfKey(String key, List<String> tree) {
    String best = ROOT;
    if (key == null || !key.startsWith(PREFIX)) {
      return best;
    }
    String rest = key.substring(PREFIX.length());
    for (String path : tree) {
      if (rest.startsWith(path + ".") && path.length() > best.length()) {
        best = path;
      }
    }
    return best;
  }

  /** @return the configured queue paths in depth-first order */
  private static List<String> queueTree(ConfigSnapshot proposed) {
    List<String> paths = new ArrayList<>();
    collect(ROOT, proposed, paths, new LinkedHashSet<String>());
    return paths;
  }

  private static void collect(String path, ConfigSnapshot proposed,
      List<String> paths, Set<String> seen) {
    if (!seen.add(path)) {
      return;
    }
    paths.add(path);
    String children = proposed.get(PREFIX + path + QUEUES_SUFFIX);
    if (children == null) {
      return;
    }
    for (String child : children.split(",")) {
      String name = child.trim();
      if (!name.isEmpty()) {
        collect(path + "." + name, proposed, paths, seen);
      }
    }
  }
}
