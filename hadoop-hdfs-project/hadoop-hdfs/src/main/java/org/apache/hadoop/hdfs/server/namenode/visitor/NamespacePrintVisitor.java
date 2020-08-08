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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.PrintWriter;

/**
 * To print the tree recursively for testing.
 *
 *      \- foo   (INodeDirectory@33dd2717)
 *        \- sub1   (INodeDirectory@442172)
 *          +- file1   (INodeFile@78392d4)
 *          +- file2   (INodeFile@78392d5)
 *          +- sub11   (INodeDirectory@8400cff)
 *            \- file3   (INodeFile@78392d6)
 *          \- z_file4   (INodeFile@45848712)
 */
public class NamespacePrintVisitor implements NamespaceVisitor {
  static final String NON_LAST_ITEM = "+-";
  static final String LAST_ITEM = "\\-";

  private final PrintWriter out;
  private final StringBuffer prefix = new StringBuffer();

  public NamespacePrintVisitor(PrintWriter out) {
    this.out = out;
  }

  void print(INode iNode, int snapshot) {
    out.print(prefix);
    out.print(" ");
    final String name = iNode.getLocalName();
    out.print(name != null && name.isEmpty()? "/": name);
    out.print("   (");
    out.print(iNode.getObjectString());
    out.print("), ");
    out.print(iNode.getParentString());
    out.print(", " + iNode.getPermissionStatus(snapshot));
  }

  @Override
  public void visit(INodeFile file, int snapshot) {
    print(file, snapshot);

    out.print(", fileSize=" + file.computeFileSize(snapshot));
    // only compare the first block
    out.print(", blocks=");
    final BlockInfo[] blocks = file.getBlocks();
    out.print(blocks.length == 0 ? null: blocks[0]);
    out.println();

    final FileWithSnapshotFeature snapshotFeature
        = file.getFileWithSnapshotFeature();
    if (snapshotFeature != null) {
      if (prefix.length() >= 2) {
        prefix.setLength(prefix.length() - 2);
        prefix.append("  ");
      }
      out.print(prefix);
      out.print(snapshotFeature);
    }
    out.println();
  }

  @Override
  public void visit(INodeSymlink symlink, int snapshot) {
    print(symlink, snapshot);
    out.print(" ~> ");
    out.println(symlink.getSymlinkString());
  }

  @Override
  public void visit(INodeReference ref, int snapshot) {
    print(ref, snapshot);

    if (ref instanceof INodeReference.DstReference) {
      out.print(", dstSnapshotId=" + ref.getDstSnapshotId());
    } else if (ref instanceof INodeReference.WithCount) {
      out.print(", " + ((INodeReference.WithCount)ref).getCountDetails());
    }
    out.println();

    prefix.setLength(prefix.length() - 2);
    prefix.append("  ->");
    ref.getReferredINode().accept(this, snapshot);
    prefix.setLength(prefix.length() - 2);
  }

  @Override
  public void visit(INodeDirectory dir, int snapshot) {
    print(dir, snapshot);

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
  public void visit(INodeDirectory dir,
      DirectorySnapshottableFeature snapshottable) {
    out.println();
    out.print(prefix);

    out.print("Snapshot of ");
    final String name = dir.getLocalName();
    out.print(name != null && name.isEmpty()? "/": name);
    out.print(": quota=");
    out.print(snapshottable.getSnapshotQuota());

    int n = 0;
    for(DirectoryWithSnapshotFeature.DirectoryDiff diff : snapshottable.getDiffs()) {
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
  public void preVisitNextLevel(int index, boolean isLast) {
    prefix.append(isLast? LAST_ITEM : NON_LAST_ITEM);
  }

  @Override
  public void postVisitNextLevel(int index, boolean isLast) {
    prefix.setLength(prefix.length() - 2);
  }
}
