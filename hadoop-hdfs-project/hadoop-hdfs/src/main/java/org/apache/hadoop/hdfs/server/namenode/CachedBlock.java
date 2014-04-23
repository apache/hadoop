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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.IntrusiveCollection.Element;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

/**
 * Represents a cached block.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class CachedBlock implements Element, 
    LightWeightGSet.LinkedElement {
  private static final Object[] EMPTY_ARRAY = new Object[0];

  /**
   * Block id.
   */
  private final long blockId;

  /**
   * Used to implement #{LightWeightGSet.LinkedElement}
   */
  private LinkedElement nextElement;

  /**
   * Bit 15: Mark
   * Bit 0-14: cache replication factor.
   */
  private short replicationAndMark;

  /**
   * Used to implement the CachedBlocksList.
   *
   * Since this CachedBlock can be in multiple CachedBlocksList objects,
   * we need to be able to store multiple 'prev' and 'next' pointers.
   * The triplets array does this.
   *
   * Each triplet contains a CachedBlockList object followed by a
   * prev pointer, followed by a next pointer.
   */
  private Object[] triplets;

  public CachedBlock(long blockId, short replication, boolean mark) {
    this.blockId = blockId;
    this.triplets = EMPTY_ARRAY;
    setReplicationAndMark(replication, mark);
  }

  public long getBlockId() {
    return blockId;
  }

  @Override
  public int hashCode() {
    return (int)(blockId^(blockId>>>32));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CachedBlock other = (CachedBlock)o;
    return other.blockId == blockId;
  }

  public void setReplicationAndMark(short replication, boolean mark) {
    assert replication >= 0;
    replicationAndMark = (short)((replication << 1) | (mark ? 0x1 : 0x0));
  }

  public boolean getMark() {
    return ((replicationAndMark & 0x1) != 0);
  }

  public short getReplication() {
    return (short) (replicationAndMark >>> 1);
  }

  /**
   * Return true if this CachedBlock is present on the given list.
   */
  public boolean isPresent(CachedBlocksList cachedBlocksList) {
    for (int i = 0; i < triplets.length; i += 3) {
      CachedBlocksList list = (CachedBlocksList)triplets[i];
      if (list == cachedBlocksList) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get a list of the datanodes which this block is cached,
   * planned to be cached, or planned to be uncached on.
   *
   * @param type      If null, this parameter is ignored.
   *                  If it is non-null, we match only datanodes which
   *                  have it on this list.
   *                  See {@link DatanodeDescriptor.CachedBlocksList.Type}
   *                  for a description of all the lists.
   *                  
   * @return          The list of datanodes.  Modifying this list does not
   *                  alter the state of the CachedBlock.
   */
  public List<DatanodeDescriptor> getDatanodes(Type type) {
    List<DatanodeDescriptor> nodes = new LinkedList<DatanodeDescriptor>();
    for (int i = 0; i < triplets.length; i += 3) {
      CachedBlocksList list = (CachedBlocksList)triplets[i];
      if ((type == null) || (list.getType() == type)) {
        nodes.add(list.getDatanode());
      }
    }
    return nodes;
  }

  @Override
  public void insertInternal(IntrusiveCollection<? extends Element> list, Element prev,
      Element next) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        throw new RuntimeException("Trying to re-insert an element that " +
            "is already in the list.");
      }
    }
    Object newTriplets[] = Arrays.copyOf(triplets, triplets.length + 3);
    newTriplets[triplets.length] = list;
    newTriplets[triplets.length + 1] = prev;
    newTriplets[triplets.length + 2] = next;
    triplets = newTriplets;
  }
  
  @Override
  public void setPrev(IntrusiveCollection<? extends Element> list, Element prev) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        triplets[i + 1] = prev;
        return;
      }
    }
    throw new RuntimeException("Called setPrev on an element that wasn't " +
        "in the list.");
  }

  @Override
  public void setNext(IntrusiveCollection<? extends Element> list, Element next) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        triplets[i + 2] = next;
        return;
      }
    }
    throw new RuntimeException("Called setNext on an element that wasn't " +
        "in the list.");
  }

  @Override
  public void removeInternal(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        Object[] newTriplets = new Object[triplets.length - 3];
        System.arraycopy(triplets, 0, newTriplets, 0, i);
        System.arraycopy(triplets, i + 3, newTriplets, i,
            triplets.length - (i + 3));
        triplets = newTriplets;
        return;
      }
    }
    throw new RuntimeException("Called remove on an element that wasn't " +
        "in the list.");
  }

  @Override
  public Element getPrev(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        return (Element)triplets[i + 1];
      }
    }
    throw new RuntimeException("Called getPrev on an element that wasn't " +
        "in the list.");
  }

  @Override
  public Element getNext(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        return (Element)triplets[i + 2];
      }
    }
    throw new RuntimeException("Called getNext on an element that wasn't " +
        "in the list.");
  }

  @Override
  public boolean isInList(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public String toString() {
    return new StringBuilder().append("{").
        append("blockId=").append(blockId).append(", ").
        append("replication=").append(getReplication()).append(", ").
        append("mark=").append(getMark()).append("}").
        toString();
  }

  @Override // LightWeightGSet.LinkedElement 
  public void setNext(LinkedElement next) {
    this.nextElement = next;
  }

  @Override // LightWeightGSet.LinkedElement 
  public LinkedElement getNext() {
    return nextElement;
  }
}
