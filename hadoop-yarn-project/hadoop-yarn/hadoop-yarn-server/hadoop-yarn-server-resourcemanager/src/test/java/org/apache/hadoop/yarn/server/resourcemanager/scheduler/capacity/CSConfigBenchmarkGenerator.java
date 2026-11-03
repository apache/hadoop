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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueACL;

import static org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet.of;

/**
 * Generates synthetic but structurally realistic capacity-scheduler
 * configuration content for a requested queue count.
 * It feeds {@link TestCapacitySchedulerConfigBenchmark} (scheduler load,
 * refresh and validation timings) and {@link TestCSConfigBenchmarkGenerator}.
 *
 * Tree shape (mixed depth 3 and 4):
 * <pre>
 *   root
 *     p0 (pct children, node label, one legacy managed parent leaf)
 *     p1 (weight children, AQC v2 template on sub-parent s0 when its
 *         children use weights)
 *     p2 (pct children, unlabeled)
 *     p3 (weight children)
 *     ...
 *       each pI: 2 direct leaves + up to 5 sub-parents with up to 9 leaves each
 * </pre>
 *
 * Capacity modes:
 * - PCT_WEIGHT_MIXED (default): root children are percentage; even-indexed
 *   subtrees use percentage children, odd-indexed use weight children; the last
 *   sub-parent of a weight subtree flips its children back to percentage
 *   (legal per the legacy-mode parent/child mode table in AbstractParentQueue).
 * - ALL_ABSOLUTE: every queue uses absolute [memory=..,vcores=..] resources,
 *   with parent minimums equal to the sum of their children. This is a separate
 *   mode because in legacy queue mode absolute resources cannot be mixed with
 *   percentage/weight anywhere in one hierarchy (absolute children require an
 *   absolute parent, and siblings must be mode-uniform).
 *
 * Extras sprinkled for realism: accessible-node-labels + per-label capacities
 * on every fourth subtree (labels "blue"/"ssd"), legacy managed-parent
 * (auto-create-child-queue) leaf templates, auto-queue-creation-v2 templates,
 * legacy queue-mapping placement rules, ACLs, user-limit-factor,
 * maximum-capacity and ordering policies on a subset of queues.
 */
public final class CSConfigBenchmarkGenerator {

  public enum CapacityMode {
    PCT_WEIGHT_MIXED,
    ALL_ABSOLUTE
  }

  /** Everything the harness needs to know about a generated config. */
  public static final class GeneratedConfig {
    private final CapacitySchedulerConfiguration conf;
    private final int queueCount;
    private final List<String> leafPaths;
    private final String mutationLeafA;
    private final String mutationLeafB;
    private final List<String> labels;
    private final List<String> aqcV2Parents;

    private GeneratedConfig(CapacitySchedulerConfiguration conf, int queueCount,
        List<String> leafPaths, String mutationLeafA, String mutationLeafB,
        List<String> labels, List<String> aqcV2Parents) {
      this.conf = conf;
      this.queueCount = queueCount;
      this.leafPaths = leafPaths;
      this.mutationLeafA = mutationLeafA;
      this.mutationLeafB = mutationLeafB;
      this.labels = labels;
      this.aqcV2Parents = aqcV2Parents;
    }

    public CapacitySchedulerConfiguration getConf() {
      return conf;
    }

    /** Number of generated queues, root excluded. */
    public int getQueueCount() {
      return queueCount;
    }

    public List<String> getLeafPaths() {
      return Collections.unmodifiableList(leafPaths);
    }

    /** Two sibling leaves whose capacities can be swapped for a valid change. */
    public String getMutationLeafA() {
      return mutationLeafA;
    }

    public String getMutationLeafB() {
      return mutationLeafB;
    }

    public List<String> getLabels() {
      return Collections.unmodifiableList(labels);
    }

    /** Parents that carry an auto-queue-creation-v2 template. */
    public List<String> getAqcV2Parents() {
      return Collections.unmodifiableList(aqcV2Parents);
    }
  }

  private static final int DIRECT_LEAVES = 2;
  private static final int SUB_PARENTS = 5;
  private static final int LEAVES_PER_SUB_PARENT = 9;
  private static final int MAX_MANAGED_PARENTS = 4;
  private static final int MAX_AQC_V2_PARENTS = 4;
  private static final String LABEL_BLUE = "blue";
  private static final String LABEL_SSD = "ssd";
  private static final String CS_PREFIX = CapacitySchedulerConfiguration.PREFIX;

  private CSConfigBenchmarkGenerator() {
  }

  private static final class Node {
    private final String name;
    private final String path;
    private final List<Node> children = new ArrayList<>();
    private boolean managedParent;

    Node(String parentPath, String name) {
      this.name = name;
      this.path = parentPath + "." + name;
    }

    boolean isLeaf() {
      return children.isEmpty() && !managedParent;
    }
  }

  public static GeneratedConfig generate(int targetQueueCount) {
    return generate(targetQueueCount, CapacityMode.PCT_WEIGHT_MIXED);
  }

  public static GeneratedConfig generate(int targetQueueCount, CapacityMode mode) {
    if (targetQueueCount <= 0) {
      throw new IllegalArgumentException(
          "targetQueueCount must be positive, got " + targetQueueCount);
    }
    List<Node> tops = buildForest(targetQueueCount);
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);

    markManagedParents(tops);
    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);
    conf.setQueues(rootPath, names(tops));

    List<String> aqcV2Parents = new ArrayList<>();
    if (mode == CapacityMode.ALL_ABSOLUTE) {
      emitAbsolute(conf, rootPath, tops);
    } else {
      emitMixed(conf, tops, aqcV2Parents);
    }

    List<String> leafPaths = new ArrayList<>();
    int count = 0;
    for (Node top : tops) {
      count += collect(top, leafPaths);
    }
    String[] mutationPair = pickMutationSiblings(tops);
    setPlacementRules(conf, leafPaths);

    List<String> labels = new ArrayList<>();
    if (mode == CapacityMode.PCT_WEIGHT_MIXED) {
      for (int i = 0; i < tops.size(); i++) {
        String label = labelFor(i);
        if (label != null && !labels.contains(label)) {
          labels.add(label);
        }
      }
    }
    return new GeneratedConfig(conf, count, leafPaths, mutationPair[0],
        mutationPair[1], labels, aqcV2Parents);
  }

  /**
   * Returns a copy of {@code base} with a valid change applied. With
   * percentage capacities one percent is moved from mutation leaf B to
   * mutation leaf A (and likewise for their per-label capacities), so
   * per-parent sums stay at 100. Absolute capacities cannot be shifted that
   * way, so in {@link CapacityMode#ALL_ABSOLUTE} the change sets
   * maximum-applications on mutation leaf A instead.
   */
  public static Configuration createMutatedCopy(Configuration base,
      GeneratedConfig gen) {
    Configuration copy = new Configuration(base);
    moveOnePercent(copy, gen.getMutationLeafA(), gen.getMutationLeafB(), null);
    for (String label : gen.getLabels()) {
      moveOnePercent(copy, gen.getMutationLeafA(), gen.getMutationLeafB(), label);
    }
    return copy;
  }

  private static void moveOnePercent(Configuration conf, String leafA,
      String leafB, String label) {
    if (leafA == null || leafB == null) {
      return;
    }
    String suffix = (label == null) ? ".capacity"
        : ".accessible-node-labels." + label + ".capacity";
    String keyA = CS_PREFIX + leafA + suffix;
    String keyB = CS_PREFIX + leafB + suffix;
    String a = conf.get(keyA);
    String b = conf.get(keyB);
    if (a == null || b == null) {
      return;
    }
    try {
      conf.set(keyA, String.valueOf(Float.parseFloat(a) + 1.0f));
      conf.set(keyB, String.valueOf(Float.parseFloat(b) - 1.0f));
    } catch (NumberFormatException e) {
      // Absolute capacities mutate a real queue limit, never a no-op.
      conf.set(CS_PREFIX + leafA + ".maximum-applications", "2345");
    }
  }

  // ---------------------------------------------------------------------
  // Tree construction
  // ---------------------------------------------------------------------

  private static List<Node> buildForest(int targetQueueCount) {
    List<Node> tops = new ArrayList<>();
    int fullSubtree = 1 + DIRECT_LEAVES + SUB_PARENTS * (1 + LEAVES_PER_SUB_PARENT);
    int remaining = targetQueueCount;
    int i = 0;
    while (remaining > 0) {
      int budget = Math.min(fullSubtree, remaining);
      Node top = new Node(CapacitySchedulerConfiguration.ROOT, "p" + i);
      budget--;
      int directLeaves = Math.min(DIRECT_LEAVES, budget);
      for (int l = 0; l < directLeaves; l++) {
        top.children.add(new Node(top.path, "l" + l));
      }
      budget -= directLeaves;
      int s = 0;
      while (budget > 0) {
        Node sub = new Node(top.path, "s" + s);
        top.children.add(sub);
        budget--;
        int subLeaves = Math.min(LEAVES_PER_SUB_PARENT, budget);
        for (int l = 0; l < subLeaves; l++) {
          sub.children.add(new Node(sub.path, "l" + l));
        }
        budget -= subLeaves;
        s++;
      }
      remaining -= subtreeSize(top);
      tops.add(top);
      i++;
    }
    return tops;
  }

  private static int subtreeSize(Node node) {
    int size = 1;
    for (Node child : node.children) {
      size += subtreeSize(child);
    }
    return size;
  }

  /**
   * Converts the last direct leaf of every other percentage subtree into a
   * legacy managed parent (auto-create-child-queue.enabled), capped globally.
   */
  private static void markManagedParents(List<Node> tops) {
    int marked = 0;
    for (int i = 0; i < tops.size() && marked < MAX_MANAGED_PARENTS; i += 4) {
      Node top = tops.get(i);
      for (int c = top.children.size() - 1; c >= 0; c--) {
        Node child = top.children.get(c);
        if (child.isLeaf()) {
          child.managedParent = true;
          marked++;
          break;
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // Mixed percentage/weight emission
  // ---------------------------------------------------------------------

  private static void emitMixed(CapacitySchedulerConfiguration conf, List<Node> tops,
      List<String> aqcV2Parents) {
    float[] topCaps = splitPercentages(tops.size());
    Map<String, List<Integer>> labelMembers = new LinkedHashMap<>();
    for (int i = 0; i < tops.size(); i++) {
      String label = labelFor(i);
      if (label != null) {
        labelMembers.computeIfAbsent(label, k -> new ArrayList<>()).add(i);
      }
    }
    // Per-label percentage shares among the labeled root children; the sums per
    // label are 100 among label-accessible siblings, as legacy mode requires.
    Map<Integer, Float> topLabelCap = new LinkedHashMap<>();
    for (List<Integer> members : labelMembers.values()) {
      float[] shares = splitPercentages(members.size());
      for (int m = 0; m < members.size(); m++) {
        topLabelCap.put(members.get(m), shares[m]);
      }
    }

    int aqcV2Marked = 0;
    for (int i = 0; i < tops.size(); i++) {
      Node top = tops.get(i);
      QueuePath topPath = new QueuePath(top.path);
      conf.setCapacity(topPath, topCaps[i]);
      conf.setMaximumCapacity(topPath, 100f);
      String label = labelFor(i);
      if (label != null) {
        conf.setAccessibleNodeLabels(topPath, of(label));
        conf.setCapacityByLabel(topPath, label, topLabelCap.get(i));
      }
      boolean weightMode = (i % 2 == 1);
      boolean markAqcV2 = weightMode && (i % 6 == 1) && aqcV2Marked < MAX_AQC_V2_PARENTS;
      if (markAqcV2) {
        aqcV2Marked++;
      }
      emitChildren(conf, top, weightMode, label, true, markAqcV2, aqcV2Parents);
    }
  }

  /**
   * Emits capacities (and per-label capacities) for the children of
   * {@code parent}, then recurses one level into sub-parents.
   */
  private static void emitChildren(CapacitySchedulerConfiguration conf, Node parent,
      boolean weightMode, String label, boolean topLevel, boolean markAqcV2,
      List<String> aqcV2Parents) {
    if (parent.children.isEmpty()) {
      return;
    }
    conf.setQueues(new QueuePath(parent.path), names(parent.children));
    float[] caps = splitPercentages(parent.children.size());
    for (int c = 0; c < parent.children.size(); c++) {
      Node child = parent.children.get(c);
      QueuePath childPath = new QueuePath(child.path);
      // The last sub-parent of a top-level weight subtree flips its children
      // back to percentage mode; mode must stay uniform among siblings only.
      boolean childrenWeightMode = weightMode
          && !(topLevel && c == parent.children.size() - 1 && !child.children.isEmpty());
      if (weightMode) {
        conf.setNonLabeledQueueWeight(childPath, (c % 4) + 1f);
      } else {
        conf.setCapacity(childPath, caps[c]);
        conf.setMaximumCapacity(childPath, 100f);
      }
      if (label != null) {
        conf.setAccessibleNodeLabels(childPath, of(label));
        conf.setCapacityByLabel(childPath, label, caps[c]);
      }
      decorate(conf, child, c);
      if (child.managedParent) {
        emitManagedParent(conf, childPath, label);
      } else if (!child.children.isEmpty()) {
        // AQC v2 under a legacy parent needs weight children to create queues.
        if (markAqcV2 && c == DIRECT_LEAVES && childrenWeightMode) {
          emitAqcV2Template(conf, childPath);
          aqcV2Parents.add(child.path);
        }
        emitChildren(conf, child, childrenWeightMode, label, false, false,
            aqcV2Parents);
      }
    }
  }

  private static void emitManagedParent(CapacitySchedulerConfiguration conf,
      QueuePath path, String label) {
    conf.setAutoCreateChildQueueEnabled(path, true);
    conf.setAutoCreatedLeafQueueConfigCapacity(path, 25f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(path, 100f);
    if (label != null) {
      conf.setAutoCreatedLeafQueueTemplateCapacityByLabel(path, label, 25f);
    }
  }

  private static void emitAqcV2Template(CapacitySchedulerConfiguration conf,
      QueuePath path) {
    conf.setAutoQueueCreationV2Enabled(path, true);
    conf.set(CS_PREFIX + path.getFullPath()
        + ".auto-queue-creation-v2.template.capacity", "2w");
    conf.set(CS_PREFIX + path.getFullPath()
        + ".auto-queue-creation-v2.leaf-template.maximum-applications", "100");
  }

  /** Realistic per-queue extras on a deterministic subset of queues. */
  private static void decorate(CapacitySchedulerConfiguration conf, Node node,
      int childIndex) {
    QueuePath path = new QueuePath(node.path);
    if (childIndex % 3 == 0) {
      conf.setUserLimitFactor(path, 2.0f);
    }
    if (childIndex % 5 == 1) {
      conf.setAcl(path, QueueACL.SUBMIT_APPLICATIONS, "benchuser benchgroup");
      conf.setAcl(path, QueueACL.ADMINISTER_QUEUE, "benchadmin");
    }
    if (node.isLeaf() && childIndex % 4 == 2) {
      conf.setOrderingPolicy(path, CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY);
    }
  }

  // ---------------------------------------------------------------------
  // Absolute-mode emission
  // ---------------------------------------------------------------------

  private static void emitAbsolute(CapacitySchedulerConfiguration conf,
      QueuePath rootPath, List<Node> tops) {
    // Root keeps its fixed 100 percent capacity; setting an absolute capacity
    // on root is rejected by CapacitySchedulerConfiguration.
    for (Node top : tops) {
      emitAbsoluteSubtree(conf, top);
    }
  }

  /** Returns {memoryMb, vcores} configured for the subtree minimum. */
  private static long[] emitAbsoluteSubtree(CapacitySchedulerConfiguration conf,
      Node node) {
    QueuePath path = new QueuePath(node.path);
    if (node.children.isEmpty()) {
      conf.setCapacity(path, "[memory=1024,vcores=1]");
      conf.set(CS_PREFIX + node.path + ".maximum-capacity",
          "[memory=2048,vcores=2]");
      if (node.managedParent) {
        conf.setAutoCreateChildQueueEnabled(path, true);
      }
      return new long[]{1024L, 1L};
    }
    conf.setQueues(path, names(node.children));
    long mem = 0;
    long vcores = 0;
    for (Node child : node.children) {
      long[] used = emitAbsoluteSubtree(conf, child);
      mem += used[0];
      vcores += used[1];
    }
    conf.setCapacity(path, "[memory=" + mem + ",vcores=" + vcores + "]");
    conf.set(CS_PREFIX + node.path + ".maximum-capacity",
        "[memory=" + (2 * mem) + ",vcores=" + (2 * vcores) + "]");
    return new long[]{mem, vcores};
  }

  // ---------------------------------------------------------------------
  // Placement rules and helpers
  // ---------------------------------------------------------------------

  private static void setPlacementRules(CapacitySchedulerConfiguration conf,
      List<String> leafPaths) {
    if (leafPaths.isEmpty()) {
      return;
    }
    List<String> mappings = new ArrayList<>();
    mappings.add("u:benchuser1:" + leafPaths.get(0));
    mappings.add("u:benchuser2:" + leafPaths.get(leafPaths.size() / 2));
    mappings.add("g:benchgroup:" + leafPaths.get(leafPaths.size() - 1));
    if (leafPaths.size() > 3) {
      mappings.add("u:%user:" + leafPaths.get(1));
    }
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        String.join(",", mappings));
  }

  private static int collect(Node node, List<String> leafPaths) {
    int count = 1;
    if (node.isLeaf()) {
      leafPaths.add(node.path);
    }
    for (Node child : node.children) {
      count += collect(child, leafPaths);
    }
    return count;
  }

  /**
   * Picks two plain sibling leaves whose capacity keys can be swapped to
   * produce a valid changed configuration. Prefers an unlabeled percentage
   * subtree; falls back to any sub-parent, then to direct leaves.
   */
  private static String[] pickMutationSiblings(List<Node> tops) {
    for (int pass = 0; pass < 2; pass++) {
      for (int i = 0; i < tops.size(); i += 2) {
        if (pass == 0 && labelFor(i) != null) {
          continue;
        }
        Node top = tops.get(i);
        for (Node child : top.children) {
          List<Node> leaves = new ArrayList<>();
          for (Node leaf : child.children) {
            if (leaf.isLeaf()) {
              leaves.add(leaf);
            }
          }
          if (leaves.size() >= 2) {
            return new String[]{leaves.get(0).path, leaves.get(1).path};
          }
        }
        List<Node> direct = new ArrayList<>();
        for (Node child : top.children) {
          if (child.isLeaf()) {
            direct.add(child);
          }
        }
        if (direct.size() >= 2) {
          return new String[]{direct.get(0).path, direct.get(1).path};
        }
      }
    }
    return new String[]{null, null};
  }

  private static String labelFor(int topIndex) {
    if (topIndex % 4 != 0) {
      return null;
    }
    return (topIndex % 8 == 0) ? LABEL_BLUE : LABEL_SSD;
  }

  private static String[] names(List<Node> nodes) {
    String[] names = new String[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      names[i] = nodes.get(i).name;
    }
    return names;
  }

  /**
   * Splits 100 percent into n two-decimal shares summing to exactly 100.00.
   */
  private static float[] splitPercentages(int n) {
    float[] out = new float[n];
    int base = 10000 / n;
    int remainder = 10000 - base * n;
    for (int i = 0; i < n; i++) {
      out[i] = (base + (i == 0 ? remainder : 0)) / 100.0f;
    }
    return out;
  }
}
