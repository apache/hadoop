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
package org.apache.hadoop.hdfs.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.hdfs.util.Diff.Container;
import org.apache.hadoop.hdfs.util.Diff.UndoInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link Diff} with {@link INode}.
 */
public class TestDiff {
  private static final Random RANDOM = new Random();
  private static final int UNDO_TEST_P = 10;
  private static final PermissionStatus PERM = PermissionStatus.createImmutable(
      "user", "group", FsPermission.createImmutable((short)0));

  static int nextStep(int n) {
    return n == 0? 1: 10*n;
  }

  /** Test directory diff. */
  @Test(timeout=60000)
  public void testDiff() throws Exception {
    for(int startSize = 0; startSize <= 10000; startSize = nextStep(startSize)) {
      for(int m = 0; m <= 10000; m = nextStep(m)) {
        runDiffTest(startSize, m);
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
  void runDiffTest(int startSize, int numModifications) {
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
    
    final List<Diff<byte[], INode>> diffs = 
        new ArrayList<Diff<byte[], INode>>();
    for(int j = 0; j < 5; j++) {
      diffs.add(new Diff<byte[], INode>());
    }

    for(int m = 0; m < numModifications; m++) {
      final int j = m * diffs.size() / numModifications;

      // if current is empty, the next operation must be create;
      // otherwise, randomly pick an operation.
      final int nextOperation = current.isEmpty()? 1: RANDOM.nextInt(3) + 1;
      switch(nextOperation) {
      case 1: // create
      {
        final INode i = newINode(n++, width);
        create(i, current, diffs.get(j));
        break;
      }
      case 2: // delete
      {
        final INode i = current.get(RANDOM.nextInt(current.size()));
        delete(i, current, diffs.get(j));
        break;
      }
      case 3: // modify
      {
        final INode i = current.get(RANDOM.nextInt(current.size()));
        modify(i, current, diffs.get(j));
        break;
      }
      }
    }

    {
      // check if current == previous + diffs
      List<INode> c = previous;
      for(int i = 0; i < diffs.size(); i++) {
        c = diffs.get(i).apply2Previous(c);
      }
      if (!hasIdenticalElements(current, c)) {
        System.out.println("previous = " + previous);
        System.out.println();
        System.out.println("current  = " + current);
        System.out.println("c        = " + c);
        throw new AssertionError("current and c are not identical.");
      }

      // check if previous == current - diffs
      List<INode> p = current;
      for(int i = diffs.size() - 1; i >= 0; i--) {
        p = diffs.get(i).apply2Current(p);
      }
      if (!hasIdenticalElements(previous, p)) {
        System.out.println("previous = " + previous);
        System.out.println("p        = " + p);
        System.out.println();
        System.out.println("current  = " + current);
        throw new AssertionError("previous and p are not identical.");
      }
    }

    // combine all diffs
    final Diff<byte[], INode> combined = diffs.get(0);
    for(int i = 1; i < diffs.size(); i++) {
      combined.combinePosterior(diffs.get(i), null);
    }

    {
      // check if current == previous + combined
      final List<INode> c = combined.apply2Previous(previous);
      if (!hasIdenticalElements(current, c)) {
        System.out.println("previous = " + previous);
        System.out.println();
        System.out.println("current  = " + current);
        System.out.println("c        = " + c);
        throw new AssertionError("current and c are not identical.");
      }

      // check if previous == current - combined
      final List<INode> p = combined.apply2Current(current);
      if (!hasIdenticalElements(previous, p)) {
        System.out.println("previous = " + previous);
        System.out.println("p        = " + p);
        System.out.println();
        System.out.println("current  = " + current);
        throw new AssertionError("previous and p are not identical.");
      }
    }

    {
      for(int m = 0; m < n; m++) {
        final INode inode = newINode(m, width);
        {// test accessPrevious
          final Container<INode> r = combined.accessPrevious(inode.getKey());
          final INode computed;
          if (r != null) {
            computed = r.getElement();
          } else {
            final int i = Diff.search(current, inode.getKey());
            computed = i < 0? null: current.get(i);
          }

          final int j = Diff.search(previous, inode.getKey());
          final INode expected = j < 0? null: previous.get(j);
          // must be the same object (equals is not enough)
          Assert.assertTrue(computed == expected);
        }

        {// test accessCurrent
          final Container<INode> r = combined.accessCurrent(inode.getKey());
          final INode computed;
          if (r != null) {
            computed = r.getElement(); 
          } else {
            final int i = Diff.search(previous, inode.getKey());
            computed = i < 0? null: previous.get(i);
          }

          final int j = Diff.search(current, inode.getKey());
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
    byte[] name = DFSUtil.string2Bytes(String.format("n%0" + width + "d", n));
    return new INodeDirectory(n, name, PERM, 0L);
  }

  static void create(INode inode, final List<INode> current,
      Diff<byte[], INode> diff) {
    final int i = Diff.search(current, inode.getKey());
    Assert.assertTrue(i < 0);
    current.add(-i - 1, inode);
    if (diff != null) {
      //test undo with 1/UNDO_TEST_P probability
      final boolean testUndo = RANDOM.nextInt(UNDO_TEST_P) == 0;
      String before = null;
      if (testUndo) {
        before = diff.toString();
      }

      final int undoInfo = diff.create(inode);

      if (testUndo) {
        final String after = diff.toString();
        //undo
        diff.undoCreate(inode, undoInfo);
        assertDiff(before, diff);
        //re-do
        diff.create(inode);
        assertDiff(after, diff);
      }
    }
  }

  static void delete(INode inode, final List<INode> current,
      Diff<byte[], INode> diff) {
    final int i = Diff.search(current, inode.getKey());
    current.remove(i);
    if (diff != null) {
      //test undo with 1/UNDO_TEST_P probability
      final boolean testUndo = RANDOM.nextInt(UNDO_TEST_P) == 0;
      String before = null;
      if (testUndo) {
        before = diff.toString();
      }

      final UndoInfo<INode> undoInfo = diff.delete(inode);

      if (testUndo) {
        final String after = diff.toString();
        //undo
        diff.undoDelete(inode, undoInfo);
        assertDiff(before, diff);
        //re-do
        diff.delete(inode);
        assertDiff(after, diff);
      }
    }
  }

  static void modify(INode inode, final List<INode> current,
      Diff<byte[], INode> diff) {
    final int i = Diff.search(current, inode.getKey());
    Assert.assertTrue(i >= 0);
    final INodeDirectory oldinode = (INodeDirectory)current.get(i);
    final INodeDirectory newinode = new INodeDirectory(oldinode, false,
      oldinode.getFeatures());
    newinode.setModificationTime(oldinode.getModificationTime() + 1);

    current.set(i, newinode);
    if (diff != null) {
      //test undo with 1/UNDO_TEST_P probability
      final boolean testUndo = RANDOM.nextInt(UNDO_TEST_P) == 0;
      String before = null;
      if (testUndo) {
        before = diff.toString();
      }

      final UndoInfo<INode> undoInfo = diff.modify(oldinode, newinode);

      if (testUndo) {
        final String after = diff.toString();
        //undo
        diff.undoModify(oldinode, newinode, undoInfo);
        assertDiff(before, diff);
        //re-do
        diff.modify(oldinode, newinode);
        assertDiff(after, diff);
      }
    }
  }
  
  static void assertDiff(String s, Diff<byte[], INode> diff) {
    Assert.assertEquals(s, diff.toString());
  }
}
