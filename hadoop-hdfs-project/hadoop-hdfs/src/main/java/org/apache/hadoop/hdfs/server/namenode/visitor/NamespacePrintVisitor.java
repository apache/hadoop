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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * To print the namespace tree recursively for testing.
 *
 *      \- foo   (INodeDirectory@33dd2717)
 *        \- sub1   (INodeDirectory@442172)
 *          +- file1   (INodeFile@78392d4)
 *          +- file2   (INodeFile@78392d5)
 *          +- sub11   (INodeDirectory@8400cff)
 *            \- file3   (INodeFile@78392d6)
 *          \- z_file4   (INodeFile@45848712)
 */
public final class NamespacePrintVisitor implements NamespaceVisitor {
  static final String NON_LAST_ITEM = "+-";
  static final String LAST_ITEM = "\\-";

  /** @return string of the tree in the given {@link FSNamesystem}. */
  public static String print2Sting(FSNamesystem ns) {
    return print2Sting(ns.getFSDirectory().getRoot());
  }

  /** @return string of the tree from the given root. */
  public static String print2Sting(INode root) {
    final StringWriter out = new StringWriter();
    new NamespacePrintVisitor(new PrintWriter(out)).print(root);
    return out.getBuffer().toString();
  }

  private final PrintWriter out;
  private final StringBuilder prefix = new StringBuilder();

  private NamespacePrintVisitor(PrintWriter out) {
    this.out = out;
  }

  private void print(INode root) {
    root.accept(this, Snapshot.CURRENT_STATE_ID);
  }

  private void printINode(INode iNode, int snapshot) {
    iNode.dumpINode(out, prefix, snapshot);
  }

  @Override
  public void visitFile(INodeFile file, int snapshot) {
    file.dumpINodeFile(out, prefix, snapshot);
  }

  @Override
  public void visitSymlink(INodeSymlink symlink, int snapshot) {
    printINode(symlink, snapshot);
    out.print(" ~> ");
    out.println(symlink.getSymlinkString());
  }

  @Override
  public void visitReference(INodeReference ref, int snapshot) {
    printINode(ref, snapshot);

    if (ref instanceof INodeReference.DstReference) {
      out.print(", dstSnapshotId=" + ref.getDstSnapshotId());
    } else if (ref instanceof INodeReference.WithCount) {
      out.print(", " + ((INodeReference.WithCount)ref).getCountDetails());
    }
    out.println();
  }

  @Override
  public void preVisitReferred(INode referred) {
    prefix.setLength(prefix.length() - 2);
    prefix.append("  ->");
  }

  @Override
  public void postVisitReferred(INode referred) {
    prefix.setLength(prefix.length() - 2);
  }

  @Override
  public void visitDirectory(INodeDirectory dir, int snapshot) {
    printINode(dir, snapshot);

    out.print(", childrenSize=" + dir.getChildrenList(snapshot).size());
    final DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
    if (q != null) {
      out.print(", " + q);
    }
    if (dir instanceof Snapshot.Root) {
      out.print(", snapshotId=" + snapshot);
    }
    out.println();

    if (prefix.length() >= 2) {
      prefix.setLength(prefix.length() - 2);
      prefix.append("  ");
    }

    final DirectoryWithSnapshotFeature snapshotFeature
        = dir.getDirectoryWithSnapshotFeature();
    if (snapshotFeature != null) {
      out.print(prefix);
      out.print(snapshotFeature);
    }
    out.println();
  }

  @Override
  public void visitSnapshottable(INodeDirectory dir,
      DirectorySnapshottableFeature snapshottable) {
    out.println();
    out.print(prefix);

    out.print("Snapshot of ");
    final String name = dir.getLocalName();
    out.print(name != null && name.isEmpty()? "/": name);
    out.print(": quota=");
    out.print(snapshottable.getSnapshotQuota());

    int n = 0;
    for(DirectoryDiff diff : snapshottable.getDiffs()) {
      if (diff.isSnapshotRoot()) {
        n++;
      }
    }
    final int numSnapshots = snapshottable.getNumSnapshots();
    Preconditions.checkState(n == numSnapshots,
        "numSnapshots = " + numSnapshots + " != " + n);
    out.print(", #snapshot=");
    out.println(n);
  }

  @Override
  public void preVisitSub(Element sub, int index, boolean isLast) {
    prefix.append(isLast? LAST_ITEM : NON_LAST_ITEM);
  }

  @Override
  public void postVisitSub(Element sub, int index, boolean isLast) {
    prefix.setLength(prefix.length() - 2);
  }
}
