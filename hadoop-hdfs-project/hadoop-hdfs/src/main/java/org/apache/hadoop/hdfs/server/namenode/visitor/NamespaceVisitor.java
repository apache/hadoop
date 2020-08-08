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
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.util.Iterator;

/**
 * For visiting namespace trees.
 */
public interface NamespaceVisitor {
  void visit(INodeFile file, int snapshot);

  void visit(INodeSymlink symlink, int snapshot);

  void visit(INodeReference reference, int snapshot);

  void visit(INodeDirectory directory, int snapshot);

  void visit(INodeDirectory dir, DirectorySnapshottableFeature snapshottable);

  void preVisitNextLevel(int index, boolean isLast);

  void postVisitNextLevel(int index, boolean isLast);

  default void visitRecursively(Iterable<Element> subs) {
    if (subs == null) {
      return;
    }
    int index = 0;
    for(final Iterator<Element> i = subs.iterator(); i.hasNext();) {
      final Element e = i.next();
      final boolean isList = !i.hasNext();
      preVisitNextLevel(index, isList);
      e.getInode().accept(this, e.getSnapshotId());
      postVisitNextLevel(index, isList);
      index++;
    }
  }

  default void visitRecursively(INodeDirectory dir, int snapshot) {
    visit(dir, snapshot);
    visitRecursively(getChildren(dir, snapshot));

    if (snapshot == Snapshot.CURRENT_STATE_ID) {
      final DirectorySnapshottableFeature snapshottable
          = dir.getDirectorySnapshottableFeature();
      if (snapshottable != null) {
        visit(dir, snapshottable);
        visitRecursively(getSnapshots(snapshottable));
      }
    }
  }

  static Iterable<Element> getChildren(INodeDirectory dir, int snapshot) {
    return new Iterable<Element>() {
      final Iterator<INode> i = dir.getChildrenList(snapshot).iterator();

      @Override
      public Iterator<Element> iterator() {
        return new Iterator<Element>() {
          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public Element next() {
            return new Element(snapshot, i.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  static Iterable<Element> getSnapshots(
      DirectorySnapshottableFeature snapshottable) {
    return new Iterable<Element>() {
      @Override
      public Iterator<Element> iterator() {
        return new Iterator<Element>() {
          final Iterator<DirectoryWithSnapshotFeature.DirectoryDiff> i
              = snapshottable.getDiffs().iterator();
          private DirectoryWithSnapshotFeature.DirectoryDiff next = findNext();

          private DirectoryWithSnapshotFeature.DirectoryDiff findNext() {
            for(; i.hasNext(); ) {
              final DirectoryWithSnapshotFeature.DirectoryDiff diff = i.next();
              if (diff.isSnapshotRoot()) {
                return diff;
              }
            }
            return null;
          }

          @Override
          public boolean hasNext() {
            return next != null;
          }

          @Override
          public Element next() {
            final int id = next.getSnapshotId();
            final Element e = new Element(id,
                snapshottable.getSnapshotById(id).getRoot());
            next = findNext();
            return e;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /** Snapshot and INode. */
  class Element {
    private final int snapshotId;
    private final INode inode;

    public Element(int snapshot, INode inode) {
      this.snapshotId = snapshot;
      this.inode = inode;
    }

    public INode getInode() {
      return inode;
    }

    public int getSnapshotId() {
      return snapshotId;
    }
  }
}
