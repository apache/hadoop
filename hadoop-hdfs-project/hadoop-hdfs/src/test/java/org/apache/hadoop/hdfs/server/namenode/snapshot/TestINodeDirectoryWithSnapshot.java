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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.Diff;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link INodeDirectoryWithSnapshot}, especially, {@link Diff}.
 */
public class TestINodeDirectoryWithSnapshot {
  private static Random RANDOM = new Random();
  private static final PermissionStatus PERM = PermissionStatus.createImmutable(
      "user", "group", FsPermission.createImmutable((short)0));

  static int nextStep(int n) {
    return n == 0? 1: 10*n;
  }

  /** Test directory diff. */
  @Test
  public void testDiff() throws Exception {
    for(int startSize = 0; startSize <= 1000; startSize = nextStep(startSize)) {
      for(int m = 0; m <= 10000; m = nextStep(m)) {
        runDiffTest(startSize, m, true);
      }
    }
  }

  /**
   * The following are the step of the diff test:
   * 1) Initialize the previous list and add s elements to it,
   *    where s = startSize.
   * 2) Initialize the current list by coping all elements from the previous list
   * 3) Initialize an empty diff object.
   * 4) Make m modifications to the current list, where m = numModifications.
   *    Record the modifications in diff at the same time.
   * 5) Test if current == previous + diff and previous == current - diff.
   * 6) Test accessPrevious and accessCurrent.
   *
   * @param startSize
   * @param numModifications
   * @param computeDiff
   */
  void runDiffTest(int startSize, int numModifications, boolean computeDiff) {
    final int width = findWidth(startSize + numModifications);
    System.out.println("\nstartSize=" + startSize
        + ", numModifications=" + numModifications
        + ", width=" + width);

    // initialize previous
    final List<INode> previous = new ArrayList<INode>();
    int n = 0;
    for(; n < startSize; n++) {
      previous.add(newINode(n, width));
    }

    // make modifications to current and record the diff
    final List<INode> current = new ArrayList<INode>(previous);
    final Diff diff = computeDiff? new Diff(0): null;

    for(int m = 0; m < numModifications; m++) {
      // if current is empty, the next operation must be create;
      // otherwise, randomly pick an operation.
      final int nextOperation = current.isEmpty()? 1: RANDOM.nextInt(3) + 1;
      switch(nextOperation) {
      case 1: // create
      {
        final INode i = newINode(n++, width);
        create(i, current, diff);
        break;
      }
      case 2: // delete
      {
        final INode i = current.get(RANDOM.nextInt(current.size()));
        delete(i, current, diff);
        break;
      }
      case 3: // modify
      {
        final INode i = current.get(RANDOM.nextInt(current.size()));
        modify(i, current, diff);
        break;
      }
      }
    }

    if (computeDiff) {
      // check if current == previous + diff
      final List<INode> c = diff.apply2Previous(previous);
      if (!hasIdenticalElements(current, c)) {
        System.out.println("previous = " + Diff.toString(previous));
        System.out.println();
        System.out.println("current  = " + Diff.toString(current));
        System.out.println("c        = " + Diff.toString(c));
        System.out.println();
        System.out.println("diff     = " + diff);
        throw new AssertionError("current and c are not identical.");
      }

      // check if previous == current - diff
      final List<INode> p = diff.apply2Current(current);
      if (!hasIdenticalElements(previous, p)) {
        System.out.println("previous = " + Diff.toString(previous));
        System.out.println("p        = " + Diff.toString(p));
        System.out.println();
        System.out.println("current  = " + Diff.toString(current));
        System.out.println();
        System.out.println("diff     = " + diff);
        throw new AssertionError("previous and p are not identical.");
      }
    }

    if (computeDiff) {
      for(int m = 0; m < n; m++) {
        final INode inode = newINode(m, width);
        {// test accessPrevious
          final int i = Diff.search(current, inode);
          final INode inodeInCurrent = i < 0? null: current.get(i);
          final INode computed = diff.accessPrevious(
              inode.getLocalNameBytes(), inodeInCurrent);

          final int j = Diff.search(previous, inode);
          final INode expected = j < 0? null: previous.get(j);
          // must be the same object (equals is not enough)
          Assert.assertTrue(computed == expected);
        }

        {// test accessCurrent
          final int i = Diff.search(previous, inode);
          final INode inodeInPrevious = i < 0? null: previous.get(i);
          final INode computed = diff.accessCurrent(
              inode.getLocalNameBytes(), inodeInPrevious);

          final int j = Diff.search(current, inode);
          final INode expected = j < 0? null: current.get(j);
          // must be the same object (equals is not enough)
          Assert.assertTrue(computed == expected);
        }
      }
    }
  }

  static boolean hasIdenticalElements(final List<INode> expected,
      final List<INode> computed) {
    if (expected == null) {
      return computed == null;
    }
    if (expected.size() != computed.size()) {
      return false;
    }
    for(int i = 0; i < expected.size(); i++) {
      // must be the same object (equals is not enough)
      if (expected.get(i) != computed.get(i)) {
        return false;
      }
    }
    return true;
  }

  static String toString(INode inode) {
    return inode == null? null
        : inode.getLocalName() + ":" + inode.getModificationTime();
  }

  static int findWidth(int max) {
    int w = 1;
    for(long n = 10; n < max; n *= 10, w++);
    return w;
  }

  static INode newINode(int n, int width) {
    return new INodeDirectory(String.format("n%0" + width + "d", n), PERM);
  }

  static void create(INode inode, final List<INode> current, Diff diff) {
    final int i = Diff.search(current, inode);
    Assert.assertTrue(i < 0);
    current.add(-i - 1, inode);
    if (diff != null) {
      diff.create(inode);
    }
  }

  static void delete(INode inode, final List<INode> current, Diff diff) {
    final int i = Diff.search(current, inode);
    Assert.assertTrue("i=" + i + ", inode=" + inode + "\ncurrent=" + current,
        i >= 0);
    current.remove(i);
    if (diff != null) {
      diff.delete(inode);
    }
  }

  static void modify(INode inode, final List<INode> current, Diff diff) {
    final int i = Diff.search(current, inode);
    Assert.assertTrue(i >= 0);
    final INodeDirectory oldinode = (INodeDirectory)current.get(i);
    final INodeDirectory newinode = new INodeDirectory(oldinode);
    newinode.setModificationTime(oldinode.getModificationTime() + 1);

    current.set(i, newinode);
    if (diff != null) {
      diff.modify(oldinode, newinode);
    }
  }
}
