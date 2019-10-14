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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Traversal yielding a hierarchical sequence of paths.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class TreeWalk implements Iterable<TreePath> {

  /**
   * @param path path to the node being explored.
   * @param id the id of the node.
   * @param iterator the {@link TreeIterator} to use.
   * @return paths representing the children of the current node.
   */
  protected abstract Iterable<TreePath> getChildren(
      TreePath path, long id, TreeWalk.TreeIterator iterator);

  public abstract TreeIterator iterator();

  /**
   * Enumerator class for hierarchies. Implementations SHOULD support a fork()
   * operation yielding a subtree of the current cursor.
   */
  public abstract class TreeIterator implements Iterator<TreePath> {

    private final Deque<TreePath> pending;

    public TreeIterator() {
      this(new ArrayDeque<TreePath>());
    }

    protected TreeIterator(Deque<TreePath> pending) {
      this.pending = pending;
    }

    public abstract TreeIterator fork();

    @Override
    public boolean hasNext() {
      return !pending.isEmpty();
    }

    @Override
    public TreePath next() {
      return pending.removeFirst();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    protected void onAccept(TreePath p, long id) {
      for (TreePath k : getChildren(p, id, this)) {
        pending.addFirst(k);
      }
    }

    /**
     * @return the Deque containing the pending paths.
     */
    protected Deque<TreePath> getPendingQueue() {
      return pending;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{ Treewalk=\"").append(TreeWalk.this.toString());
      sb.append(", pending=[");
      Iterator<TreePath> i = pending.iterator();
      if (i.hasNext()) {
        sb.append("\"").append(i.next()).append("\"");
      }
      while (i.hasNext()) {
        sb.append(", \"").append(i.next()).append("\"");
      }
      sb.append("]");
      sb.append(" }");
      return sb.toString();
    }
  }
}
