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
package org.apache.hadoop.hdfs.net;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.InnerNode;
import org.apache.hadoop.net.InnerNodeImpl;
import org.apache.hadoop.net.Node;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;

/**
 * The HDFS-specific representation of a network topology inner node. The
 * difference is this class includes the information about the storage type
 * info of this subtree. This info will be used when selecting subtrees
 * in block placement.
 */
public class DFSTopologyNodeImpl extends InnerNodeImpl {

  static final InnerNodeImpl.Factory FACTORY
      = new DFSTopologyNodeImpl.Factory();

  static final class Factory extends InnerNodeImpl.Factory {
    private Factory() {}

    @Override
    public InnerNodeImpl newInnerNode(String path) {
      return new DFSTopologyNodeImpl(path);
    }
  }

  /**
   * The core data structure of this class. The information about what storage
   * types this subtree has. Basically, a map whose key is a child
   * id, value is a enum map including the counts of each storage type. e.g.
   * DISK type has count 5 means there are 5 leaf datanodes with DISK type
   * available. This value is set/updated upon datanode joining and leaving.
   *
   * NOTE : It might be sufficient to keep only a map from storage type
   * to count, omitting the child node id. But this might make it hard to keep
   * consistency when there are updates from children.
   *
   * For example, if currently R has two children A and B with storage X, Y, and
   * A : X=1 Y=1
   * B : X=2 Y=2
   * so we store X=3 Y=3 as total on R.
   *
   * Now say A has a new X plugged in and becomes X=2 Y=1.
   *
   * If we know that "A adds one X", it is easy to update R by +1 on X. However,
   * if we don't know "A adds one X", but instead got "A now has X=2 Y=1",
   * (which seems to be the case in current heartbeat) we will not know how to
   * update R. While if we store on R "A has X=1 and Y=1" then we can simply
   * update R by completely replacing the A entry and all will be good.
   */
  private final HashMap
      <String, EnumMap<StorageType, Integer>> childrenStorageInfo;

  /**
   * This map stores storage type counts of the subtree. We can always get this
   * info by iterate over the childrenStorageInfo variable. But for optimization
   * purpose, we store this info directly to avoid the iteration.
   */
  private final EnumMap<StorageType, Integer> storageTypeCounts;

  DFSTopologyNodeImpl(String path) {
    super(path);
    childrenStorageInfo = new HashMap<>();
    storageTypeCounts = new EnumMap<>(StorageType.class);
  }

  DFSTopologyNodeImpl(
      String name, String location, InnerNode parent, int level) {
    super(name, location, parent, level);
    childrenStorageInfo = new HashMap<>();
    storageTypeCounts = new EnumMap<>(StorageType.class);
  }

  public int getSubtreeStorageCount(StorageType type) {
    if (storageTypeCounts.containsKey(type)) {
      return storageTypeCounts.get(type);
    } else {
      return 0;
    }
  }

  int getNumOfChildren() {
    return children.size();
  }

  private void incStorageTypeCount(StorageType type) {
    // no locking because the caller is synchronized already
    if (storageTypeCounts.containsKey(type)) {
      storageTypeCounts.put(type, storageTypeCounts.get(type)+1);
    } else {
      storageTypeCounts.put(type, 1);
    }
  }

  private void decStorageTypeCount(StorageType type) {
    // no locking because the caller is synchronized already
    int current = storageTypeCounts.get(type);
    current -= 1;
    if (current == 0) {
      storageTypeCounts.remove(type);
    } else {
      storageTypeCounts.put(type, current);
    }
  }

  @Override
  public boolean add(Node n) {
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(n.getName()
          + ", which is located at " + n.getNetworkLocation()
          + ", is not a descendant of " + getPath(this));
    }
    // In HDFS topology, the leaf node should always be DatanodeDescriptor
    if (!(n instanceof DatanodeDescriptor)) {
      throw new IllegalArgumentException("Unexpected node type "
          + n.getClass().getName());
    }
    DatanodeDescriptor dnDescriptor = (DatanodeDescriptor) n;
    if (isParent(n)) {
      // this node is the parent of n; add n directly
      n.setParent(this);
      n.setLevel(this.level + 1);
      Node prev = childrenMap.put(n.getName(), n);
      if (prev != null) {
        for(int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.set(i, n);
            return false;
          }
        }
      }
      children.add(n);
      numOfLeaves++;
      if (!childrenStorageInfo.containsKey(dnDescriptor.getName())) {
        childrenStorageInfo.put(
            dnDescriptor.getName(), new EnumMap<>(StorageType.class));
      }
      for (StorageType st : dnDescriptor.getStorageTypes()) {
        childrenStorageInfo.get(dnDescriptor.getName()).put(st, 1);
        incStorageTypeCount(st);
      }
      return true;
    } else {
      // find the next ancestor node
      String parentName = getNextAncestorName(n);
      InnerNode parentNode = (InnerNode)childrenMap.get(parentName);
      if (parentNode == null) {
        // create a new InnerNode
        parentNode = createParentNode(parentName);
        children.add(parentNode);
        childrenMap.put(parentNode.getName(), parentNode);
      }
      // add n to the subtree of the next ancestor node
      if (parentNode.add(n)) {
        numOfLeaves++;
        if (!childrenStorageInfo.containsKey(parentNode.getName())) {
          childrenStorageInfo.put(
              parentNode.getName(), new EnumMap<>(StorageType.class));
          for (StorageType st : dnDescriptor.getStorageTypes()) {
            childrenStorageInfo.get(parentNode.getName()).put(st, 1);
          }
        } else {
          EnumMap<StorageType, Integer> currentCount =
              childrenStorageInfo.get(parentNode.getName());
          for (StorageType st : dnDescriptor.getStorageTypes()) {
            if (currentCount.containsKey(st)) {
              currentCount.put(st, currentCount.get(st) + 1);
            } else {
              currentCount.put(st, 1);
            }
          }
        }
        for (StorageType st : dnDescriptor.getStorageTypes()) {
          incStorageTypeCount(st);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  @VisibleForTesting
  HashMap <String, EnumMap<StorageType, Integer>> getChildrenStorageInfo() {
    return childrenStorageInfo;
  }


  private DFSTopologyNodeImpl createParentNode(String parentName) {
    return new DFSTopologyNodeImpl(
        parentName, getPath(this), this, this.getLevel() + 1);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean remove(Node n) {
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(n.getName()
          + ", which is located at " + n.getNetworkLocation()
          + ", is not a descendant of " + getPath(this));
    }
    // In HDFS topology, the leaf node should always be DatanodeDescriptor
    if (!(n instanceof DatanodeDescriptor)) {
      throw new IllegalArgumentException("Unexpected node type "
          + n.getClass().getName());
    }
    DatanodeDescriptor dnDescriptor = (DatanodeDescriptor) n;
    if (isParent(n)) {
      // this node is the parent of n; remove n directly
      if (childrenMap.containsKey(n.getName())) {
        for (int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.remove(i);
            childrenMap.remove(n.getName());
            childrenStorageInfo.remove(dnDescriptor.getName());
            for (StorageType st : dnDescriptor.getStorageTypes()) {
              decStorageTypeCount(st);
            }
            numOfLeaves--;
            n.setParent(null);
            return true;
          }
        }
      }
      return false;
    } else {
      // find the next ancestor node: the parent node
      String parentName = getNextAncestorName(n);
      DFSTopologyNodeImpl parentNode =
          (DFSTopologyNodeImpl)childrenMap.get(parentName);
      if (parentNode == null) {
        return false;
      }
      // remove n from the parent node
      boolean isRemoved = parentNode.remove(n);
      if (isRemoved) {
        // if the parent node has no children, remove the parent node too
        EnumMap<StorageType, Integer> currentCount =
            childrenStorageInfo.get(parentNode.getName());
        EnumSet<StorageType> toRemove = EnumSet.noneOf(StorageType.class);
        for (StorageType st : dnDescriptor.getStorageTypes()) {
          int newCount = currentCount.get(st) - 1;
          if (newCount == 0) {
            toRemove.add(st);
          }
          currentCount.put(st, newCount);
        }
        for (StorageType st : toRemove) {
          currentCount.remove(st);
        }
        for (StorageType st : dnDescriptor.getStorageTypes()) {
          decStorageTypeCount(st);
        }
        if (parentNode.getNumOfChildren() == 0) {
          for(int i=0; i < children.size(); i++) {
            if (children.get(i).getName().equals(parentName)) {
              children.remove(i);
              childrenMap.remove(parentName);
              childrenStorageInfo.remove(parentNode.getName());
              break;
            }
          }
        }
        numOfLeaves--;
      }
      return isRemoved;
    }
  }
}