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
  /** For visiting any {@link INode}. */
  interface INodeVisitor {
    INodeVisitor DEFAULT = new INodeVisitor() {};

    /** Visiting the given {@link INode}. */
    default void visit(INode iNode, int snapshot) {
    }
  }

  /** @return the default (non-recursive) {@link INodeVisitor}. */
  default INodeVisitor getDefaultVisitor() {
    return INodeVisitor.DEFAULT;
  }

  /** Visiting the given {@link INodeFile}. */
  default void visitFile(INodeFile file, int snapshot) {
    getDefaultVisitor().visit(file, snapshot);
  }

  /** Visiting the given {@link INodeSymlink}. */
  default void visitSymlink(INodeSymlink symlink, int snapshot) {
    getDefaultVisitor().visit(symlink, snapshot);
  }

  /** Visiting the given {@link INodeReference} (non-recursively). */
  default void visitReference(INodeReference ref, int snapshot) {
    getDefaultVisitor().visit(ref, snapshot);
  }

  /** First visit the given {@link INodeReference} and then the referred. */
  default void visitReferenceRecursively(INodeReference ref, int snapshot) {
    visitReference(ref, snapshot);

    final INode referred = ref.getReferredINode();
    preVisitReferred(referred);
    referred.accept(this, snapshot);
    postVisitReferred(referred);
  }

  /** Right before visiting the given referred {@link INode}. */
  default void preVisitReferred(INode referred) {
  }

  /** Right after visiting the given referred {@link INode}. */
  default void postVisitReferred(INode referred) {
  }

  /** Visiting the given {@link INodeDirectory} (non-recursively). */
  default void visitDirectory(INodeDirectory dir, int snapshot) {
    getDefaultVisitor().visit(dir, snapshot);
  }

  /**
   * First visit the given {@link INodeDirectory};
   * then the children;
   * and then, if snapshottable, the snapshots. */
  default void visitDirectoryRecursively(INodeDirectory dir, int snapshot) {
    visitDirectory(dir, snapshot);
    visitSubs(getChildren(dir, snapshot));

    if (snapshot == Snapshot.CURRENT_STATE_ID) {
      final DirectorySnapshottableFeature snapshottable
          = dir.getDirectorySnapshottableFeature();
      if (snapshottable != null) {
        visitSnapshottable(dir, snapshottable);
        visitSubs(getSnapshots(snapshottable));
      }
    }
  }

  /**
   * Right before visiting the given sub {@link Element}.
   * The sub element may be a child of an {@link INodeDirectory}
   * or a snapshot in {@link DirectorySnapshottableFeature}.
   *
   * @param sub the element to be visited.
   * @param index the index of the sub element.
   * @param isLast is the sub element the last element?
   */
  default void preVisitSub(Element sub, int index, boolean isLast) {
  }

  /**
   * Right after visiting the given sub {@link Element}.
   * The sub element may be a child of an {@link INodeDirectory}
   * or a snapshot in {@link DirectorySnapshottableFeature}.
   *
   * @param sub the element just visited.
   * @param index the index of the sub element.
   * @param isLast is the sub element the last element?
   */
  default void postVisitSub(Element sub, int index, boolean isLast) {
  }

  /** Visiting a {@link DirectorySnapshottableFeature}. */
  default void visitSnapshottable(INodeDirectory dir,
      DirectorySnapshottableFeature snapshottable) {
  }

  /**
   * Visiting the sub {@link Element}s recursively.
   *
   * @param subs the children of an {@link INodeDirectory}
   *             or the snapshots in {@link DirectorySnapshottableFeature}.
   */
  default void visitSubs(Iterable<Element> subs) {
    if (subs == null) {
      return;
    }
    int index = 0;
    for(final Iterator<Element> i = subs.iterator(); i.hasNext();) {
      final Element e = i.next();
      final boolean isList = !i.hasNext();
      preVisitSub(e, index, isList);
      e.getInode().accept(this, e.getSnapshotId());
      postVisitSub(e, index, isList);
      index++;
    }
  }

  /** @return the children as {@link Element}s. */
  static Iterable<Element> getChildren(INodeDirectory dir, int snapshot) {
    final Iterator<INode> i = dir.getChildrenList(snapshot).iterator();
    return new Iterable<Element>() {
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

  /** @return the snapshots as {@link Element}s. */
  static Iterable<Element> getSnapshots(
      DirectorySnapshottableFeature snapshottable) {
    final Iterator<DirectoryWithSnapshotFeature.DirectoryDiff> i
        = snapshottable.getDiffs().iterator();
    return new Iterable<Element>() {
      @Override
      public Iterator<Element> iterator() {
        return new Iterator<Element>() {
          private DirectoryWithSnapshotFeature.DirectoryDiff next = findNext();

          private DirectoryWithSnapshotFeature.DirectoryDiff findNext() {
            for(; i.hasNext();) {
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

    Element(int snapshot, INode inode) {
      this.snapshotId = snapshot;
      this.inode = inode;
    }

    INode getInode() {
      return inode;
    }

    int getSnapshotId() {
      return snapshotId;
    }
  }
}
