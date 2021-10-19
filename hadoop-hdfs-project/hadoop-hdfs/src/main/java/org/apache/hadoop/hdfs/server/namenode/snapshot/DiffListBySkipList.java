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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.
    DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.
    DirectoryWithSnapshotFeature.ChildrenDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * SkipList is an implementation of a data structure for storing a sorted list
 * of Directory Diff elements, using a hierarchy of linked lists that connect
 * increasingly sparse subsequences(defined by skip interval here) of the diffs.
 * The elements contained in the tree must be mutually comparable.
 * <p>
 * Consider  a case where we have 10 snapshots for a directory starting from s0
 * to s9 each associated with certain change records in terms of inodes deleted
 * and created after a particular snapshot and before the next snapshot. The
 * sequence will look like this:
 * <p>
 * {@literal s0->s1->s2->s3->s4->s5->s6->s7->s8->s9}.
 * <p>
 * Assuming a skip interval of 3, which means a new diff will be added at a
 * level higher than the current level after we have  ore than 3 snapshots.
 * Next level promotion happens after 9 snapshots and so on.
 * <p>
 * level 2:   {@literal s08------------------------------->s9}
 * level 1:   {@literal S02------->s35-------->s68-------->s9}
 * level 0:  {@literal s0->s1->s2->s3->s4->s5->s6->s7->s8->s9}
 * <p>
 * s02 will be created by combining diffs for s0, s1, s2 once s3 gets created.
 * Similarly, s08 will be created by combining s02, s35 and s68 once s9 gets
 * created.So, for constructing the children list fot s0, we have  to combine
 * s08, s9 and reverse apply to the live fs.
 * <p>
 * Similarly, for constructing the children list for s2, s2, s35, s68 and s9
 * need to get combined(or added) and reverse applied to current fs.
 * <p>
 * This approach will improve the snapshot deletion and snapshot diff
 * calculation.
 * <p>
 * Once a snapshot gets deleted, the list needs to be balanced.
 */
public class DiffListBySkipList implements DiffList<DirectoryDiff> {
  public static final Logger LOG =
      LoggerFactory.getLogger(DiffListBySkipList.class);

  static String childrenDiff2String(ChildrenDiff diff) {
    if (diff == null) {
      return "null";
    }
    return "@" + Integer.toHexString(System.identityHashCode(diff));
  }

  static String skip2String(SkipListNode skipTo, ChildrenDiff diff) {
    return "->" + skipTo + ":diff=" + childrenDiff2String(diff);
  }

  private static class SkipDiff {
    static final SkipDiff[] EMPTY_ARRAY = {};

    /**
     * The references to the subsequent nodes.
     */
    private SkipListNode skipTo;
    /**
     * combined diff over a skip Interval.
     */
    private ChildrenDiff diff;

    SkipDiff(ChildrenDiff diff) {
      this.diff = diff;
    }

    public ChildrenDiff getDiff() {
      return diff;
    }

    public SkipListNode getSkipTo() {
      return skipTo;
    }

    public void setSkipTo(SkipListNode node) {
      skipTo = node;
    }

    public void setDiff(ChildrenDiff diff) {
      this.diff = diff;
    }

    @Override
    public String toString() {
      return skip2String(skipTo, diff);
    }
  }

  /**
   * SkipListNode is an implementation of a DirectoryDiff List node,
   * which stores a Directory Diff and references to subsequent nodes.
   */
  final static class SkipListNode implements Comparable<Integer> {

    /**
     * The data element stored in this node.
     */
    private final DirectoryDiff diff;

    /** Next node. */
    private SkipListNode next;
    /**
     * Array containing combined children diffs over a skip interval.
     */
    private SkipDiff[] skips;

    /**
     * Constructs a new instance of SkipListNode with the specified data element
     * and level.
     *
     * @param diff The element to be stored in the node.
     * @param level
     */
    SkipListNode(DirectoryDiff diff, int level) {
      this.diff = diff;

      this.skips = level > 0? new SkipDiff[level]: SkipDiff.EMPTY_ARRAY;
      for(int i = 0; i < skips.length; i++) {
        skips[i] = new SkipDiff(null);
      }
    }

    /**
     * Returns the level of this SkipListNode.
     */
    public int level() {
      return skips.length;
    }

    void trim() {
      int n = skips.length - 1;
      for (; n >= 0 && skips[n] == null; n--) {
        continue;
      }
      n++;
      if (n < skips.length) {
        skips = n > 0 ? Arrays.copyOf(skips, n) : SkipDiff.EMPTY_ARRAY;
      }
    }

    public DirectoryDiff getDiff() {
      return diff;
    }

    /**
     * Compare diffs with snapshot ID.
     */
    @Override
    public int compareTo(Integer that) {
      return diff.compareTo(that);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SkipListNode that = (SkipListNode) o;
      return Objects.equals(diff, that.diff);
    }

    @Override
    public int hashCode() {
      return Objects.hash(diff);
    }

    public void setSkipDiff(ChildrenDiff cDiff, int level) {
      Preconditions.checkArgument(level > 0);
      resize(level);
      skips[level - 1].setDiff(cDiff);
    }

    void setSkipDiff4Target(
        SkipListNode target, int startLevel, ChildrenDiff childrenDiff) {
      for(int i = startLevel; i <= level(); i++) {
        if (getSkipNode(i) != target) {
          return;
        }
        setSkipDiff(childrenDiff, i);
      }
    }

    private void resize(int newLevel) {
      int i = skips.length;
      if (i < newLevel) {
        skips = Arrays.copyOf(skips, newLevel);
        for (; i < newLevel; i++) {
          skips[i] = new SkipDiff(null);
        }
      }
    }

    public void setSkipTo(SkipListNode node, int level) {
      if (level == 0) {
        next = node;
      } else {
        resize(level);
        skips[level - 1].setSkipTo(node);
      }
    }

    public ChildrenDiff getChildrenDiff(int level) {
      if (level == 0) {
        return diff != null? diff.getChildrenDiff(): null;
      } else {
        return skips[level - 1].getDiff();
      }
    }

    SkipListNode getSkipNode(int level) {
      return level == 0? next
          : level <= skips.length? skips[level - 1].getSkipTo()
          : null;
    }

    @Override
    public String toString() {
      return diff != null ? "" + diff.getSnapshotId() : "?";
    }

    StringBuilder appendTo(StringBuilder b) {
      b.append(this).append(": ").append(skip2String(next, getChildrenDiff(0)));
      for(int i = 0; i < skips.length; i++) {
        b.append(", ").append(skips[i]);
      }
      return b;
    }
  }

  /**
   * The reference to the first node of the list.
   * The list will grow linearly once a new Directory diff gets added.
   * All the list inteface defined methods provide a linear view of the list.
   */
  private final List<SkipListNode> skipNodeList;

  /**
   * The head node to the list.
   */
  private SkipListNode head;

  /**
   * Constructs a new, empty instance of SkipList.
   */
  public DiffListBySkipList(int capacity) {
    skipNodeList = new ArrayList<>(capacity);
    head = new SkipListNode(null, 0);
  }

  /**
   * Adds the specified data element to the beginning of the SkipList,
   * if the element is not already present.
   * @param diff the element to be inserted
   */
  @Override
  public void addFirst(DirectoryDiff diff) {
    final int nodeLevel = DirectoryDiffListFactory.randomLevel();
    final SkipListNode[] nodePath = new SkipListNode[nodeLevel + 1];
    Arrays.fill(nodePath, head);

    final SkipListNode newNode = new SkipListNode(diff, nodeLevel);
    for (int level = 0; level <= nodeLevel; level++) {
      if (level > 0) {
        // Case : S0 is added at the beginning and it has 3 levels
        //  suppose the list is like:
        //  level 1: head ------------------->s5------------->NULL
        //  level 0:head->    s1->s2->s3->s4->s5->s6->s7->s8->s9
        //  in this case:
        //  level 2: head -> s0 -------------------------------->NULL
        //  level 1: head -> s0'---------------->s5------------->NULL
        //  level 0:head->   s0->s1->s2->s3->s4->s5->s6->s7->s8->s9
        //  At level 1, we need to combine s0, s1, s2, s3, s4 and s5 and store
        //  as s0'. At level 2, s0 of next is pointing to null;
        //  Note: in this case, the diff of element being added is included
        //  while combining the diffs.
        final SkipListNode nextNode = head.getSkipNode(level);
        if (nextNode != null) {
          ChildrenDiff combined = combineDiff(newNode, nextNode, level);
          if (combined != null) {
            newNode.setSkipDiff(combined, level);
          }
        }
      }
      //insert to the linked list
      newNode.setSkipTo(nodePath[level].getSkipNode(level), level);
      nodePath[level].setSkipTo(newNode, level);
    }
    skipNodeList.add(0, newNode);
  }

  private SkipListNode[] findPreviousNodes(SkipListNode node, int nodeLevel) {
    final SkipListNode[] nodePath = new SkipListNode[nodeLevel + 1];
    SkipListNode cur = head;
    final int headLevel = head.level();
    for (int level = headLevel < nodeLevel ? headLevel : nodeLevel;
         level >= 0; level--) {
      while (cur.getSkipNode(level) != node) {
        cur = cur.getSkipNode(level);
      }
      nodePath[level] = cur;
    }
    for (int level = headLevel + 1; level <= nodeLevel; level++) {
      nodePath[level] = head;
    }
    return nodePath;
  }

  /**
   * Adds the specified data element to the end of the SkipList,
   * if the element is not already present.
   * @param diff the element to be inserted
   */
  @Override
  public boolean addLast(DirectoryDiff diff) {
    final int nodeLevel = DirectoryDiffListFactory.randomLevel();
    final SkipListNode[] nodePath = findPreviousNodes(null, nodeLevel);

    final SkipListNode newNode = new SkipListNode(diff, nodeLevel);
    for (int level = 0; level <= nodeLevel; level++) {
      if (level > 0 && nodePath[level] != head) {
        //  suppose the list is like:
        //  level 2: head ->  s1----------------------------->NULL
        //  level 1: head ->  s1---->s3'------>s5------------->NULL
        //  level 0:head->    s1->s2->s3->s4->s5->s6->s7->s8->s9

        // case : s10 is added at the end the let the level for this node = 4
        //  in this case,
        //  level 2: head ->  s1''------------------------------------>s10
        //  level 1: head ->  s1'---->s3'------>s5'-------------------->s10
        //  level 0:head->    s1->s2->s3->s4->s5->s6->s7->s8->s9---->s10
        //  At level 1, we combine s5, s6, s7, s8, s9 and store as s5'
        //  At level 2, we combine s1', s3', s5' and form s1'' and store at s1.
        // Note : the last element(element being added) diff is not added while
        // combining the diffs.
        ChildrenDiff combined = combineDiff(nodePath[level], newNode, level);
        if (combined != null) {
          nodePath[level].setSkipDiff(combined, level);
        }
      }
      nodePath[level].setSkipTo(newNode, level);
      newNode.setSkipTo(null, level);
    }
    return skipNodeList.add(newNode);
  }

  private static ChildrenDiff combineDiff(SkipListNode from, SkipListNode to,
      int level) {
    ChildrenDiff combined = null;
    ChildrenDiff first = null;

    SkipListNode cur = from;
    for (int i = level - 1; i >= 0; i--) {
      while (cur != to) {
        final SkipListNode next = cur.getSkipNode(i);
        if (next == null) {
          break;
        }

        if (first == null) {
          first = cur.getChildrenDiff(i);
        } else {
          if (combined == null) {
            combined = new ChildrenDiff();
            combined.combinePosterior(first, null);
          }
          combined.combinePosterior(cur.getChildrenDiff(i), null);
        }
        cur = next;
      }
    }
    return combined != null? combined: first;
  }

  /**
   * Returns the data element at the specified index in this SkipList.
   *
   * @param index The index of the element to be returned.
   * @return The element at the specified index in this SkipList.
   */
  @Override
  public DirectoryDiff get(int index) {
    return skipNodeList.get(index).getDiff();
  }

  SkipListNode getSkipListNode(int i) {
    return skipNodeList.get(i);
  }

  /**
   * Removes the element at the specified position in this list.
   *
   * @param index the index of the element to be removed
   * @return the removed DirectoryDiff
   */
  @Override
  public DirectoryDiff remove(int index) {
    final SkipListNode node = getNode(index);

    int headLevel = head.level();
    int nodeLevel = node.level();
    final SkipListNode[] nodePath = findPreviousNodes(node, nodeLevel);

    for (int level = 0; level <= nodeLevel; level++) {
      final SkipListNode previous = nodePath[level];
      final SkipListNode next = node.getSkipNode(level);
      if (level == 0) {
        if (next != null) {
          previous.setSkipDiff4Target(next, 1, previous.getChildrenDiff(0));
        }
      } else if (previous != head) {
        // if the last snapshot is deleted, for all the skip level nodes
        // pointing to the last one, the combined children diff at each level
        // > 0 should be made null and skip pointers will be updated to null.
        // if the snapshot being deleted is not the last one, we have to merge
        // the diff of deleted node at each level to the previous skip level
        // node at that level and the skip pointers will be updated to point to
        // the skip nodes of the deleted node.
        if (next == null) {
          previous.setSkipDiff(null, level);
        } else {
          /* Ideally at level 0, the deleted diff will be combined with
           * the previous diff , and deleted inodes will be cleaned up
           * by passing a deleted processor here while combining the diffs.
           * Level 0 merge with previous diff will be handled inside the
           * {@link AbstractINodeDiffList#deleteSnapshotDiff} function.
           */
          if (node.getChildrenDiff(level) != null) {
            final ChildrenDiff combined;
            if (previous == nodePath[level - 1]
                && next == node.getSkipNode(level - 1)) {
              combined = nodePath[level - 1].getChildrenDiff(level - 1);
              previous.setSkipDiff4Target(next, level + 1, combined);
            } else if (next == previous.getSkipNode(level + 1)) {
              combined = previous.getChildrenDiff(level + 1);
            } else {
              combined = new ChildrenDiff();
              combined.combinePosterior(previous.getChildrenDiff(level), null);
              combined.combinePosterior(node.getChildrenDiff(level), null);
            }
            previous.setSkipDiff(combined, level);
          }
        }
      }
      previous.setSkipTo(next, level);
    }
    if (nodeLevel == headLevel) {
      head.trim();
    }
    return skipNodeList.remove(index).getDiff();
  }

  /**
   * Returns true if this SkipList contains no data elements. In other words,
   * returns true if the size of this SkipList is zero.
   *
   * @return True if this SkipList contains no elements.
   */
  @Override
  public boolean isEmpty() {
    return skipNodeList.isEmpty();
  }

  /**
   * Returns the number of data elements in this SkipList.
   *
   * @return The number of elements in this SkipList.
   */
  @Override
  public int size() {
    return skipNodeList.size();
  }

  /**
   * Iterator is an iterator over the SkipList. This should
   * always provide a linear view of the list.
   */
  @Override
  public Iterator<DirectoryDiff> iterator() {
    final Iterator<SkipListNode> i = skipNodeList.iterator();
    return new Iterator<DirectoryDiff>() {

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public DirectoryDiff next() {
        return i.next().getDiff();
      }
    };
  }

  @Override
  public int binarySearch(int key) {
    return Collections.binarySearch(skipNodeList, key);
  }

  private SkipListNode getNode(int index) {
    return skipNodeList.get(index);
  }


  /**
   * This function returns the minimal set of diffs required to combine in
   * order to generate all the changes occurred between fromIndex and
   * toIndex.
   *
   * @param fromIndex index from where the summation has to start(inclusive)
   * @param toIndex   index till where the summation has to end(exclusive)
   * @return list of Directory Diff
   */
  @Override
  public List<DirectoryDiff> getMinListForRange(int fromIndex, int toIndex,
      INodeDirectory dir) {
    final List<DirectoryDiff> subList = new ArrayList<>();
    final int toSnapshotId = get(toIndex - 1).getSnapshotId();
    for (SkipListNode current = getNode(fromIndex); current != null;) {
      SkipListNode next = null;
      ChildrenDiff childrenDiff = null;
      for (int level = current.level(); level >= 0; level--) {
        next = current.getSkipNode(level);
        if (next != null && next.getDiff().compareTo(toSnapshotId) <= 0) {
          childrenDiff = current.getChildrenDiff(level);
          break;
        }
      }
      final DirectoryDiff curDiff = current.getDiff();
      subList.add(childrenDiff == null ? curDiff :
          new DirectoryDiff(curDiff.getSnapshotId(), dir, childrenDiff));

      if (current.getDiff().compareTo(toSnapshotId) == 0) {
        break;
      }
      current = next;
    }
    return subList;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder().append(" head: ");
    head.appendTo(b);
    for (SkipListNode n : skipNodeList) {
      n.appendTo(b.append("\n  "));
    }
    return b.toString();
  }
}
