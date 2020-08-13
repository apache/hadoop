/*
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
package org.apache.hadoop.hdfs.server.namenode.visitor;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For validating {@link org.apache.hadoop.hdfs.server.namenode.FSImage}s.
 */
public class INodeCountVisitor implements NamespaceVisitor {
  public interface Counts {
    int getCount(INode inode);
  }

  public static Counts countTree(INode root) {
    return new INodeCountVisitor().count(root);
  }

  private static class SetElement {
    private final INode inode;
    private final AtomicInteger count = new AtomicInteger();

    SetElement(INode inode) {
      this.inode = inode;
    }

    int getCount() {
      return count.get();
    }

    int incrementAndGet() {
      return count.incrementAndGet();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final SetElement that = (SetElement) obj;
      return this.inode.getId() == that.inode.getId();
    }

    @Override
    public int hashCode() {
      return Long.hashCode(inode.getId());
    }
  }

  static class INodeSet implements Counts {
    private final ConcurrentMap<SetElement, SetElement> map
        = new ConcurrentHashMap<>();

    int put(INode inode, int snapshot) {
      final SetElement key = new SetElement(inode);
      final SetElement previous = map.putIfAbsent(key, key);
      final SetElement current = previous != null? previous: key;
      return current.incrementAndGet();
    }

    @Override
    public int getCount(INode inode) {
      final SetElement key = new SetElement(inode);
      final SetElement value = map.get(key);
      return value != null? value.getCount(): 0;
    }
  }

  private final INodeSet inodes = new INodeSet();

  @Override
  public INodeVisitor getDefaultVisitor() {
    return new INodeVisitor() {
      @Override
      public void visit(INode iNode, int snapshot) {
        inodes.put(iNode, snapshot);
      }
    };
  }

  private Counts count(INode root) {
    root.accept(this, Snapshot.CURRENT_STATE_ID);
    return inodes;
  }
}
