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
import org.apache.hadoop.hdfs.server.namenode.INode.Triple;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.Diff;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link INodeDirectoryWithSnapshot}, especially, {@link Diff}.
 */
public class TestINodeDirectoryWithSnapshot {
  private static final Random RANDOM = new Random();
  private static final int UNDO_TEST_P = 10;
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
    
    final Diff[] diffs = new Diff[5];
    for(int j = 0; j < diffs.length; j++) {
      diffs[j] = new Diff();
    }

    for(int m = 0; m < numModifications; m++) {
      final int j = m * diffs.length / numModifications;

      // if current is empty, the next operation must be create;
      // otherwise, randomly pick an operation.
      final int nextOperation = current.isEmpty()? 1: RANDOM.nextInt(3) + 1;
      switch(nextOperation) {
      case 1: // create
      {
        final INode i = newINode(n++, width);
        create(i, current, diffs[j]);
        break;
      }
      case 2: // delete
      {
        final INode i = current.get(RANDOM.nextInt(current.size()));
        delete(i, current, diffs[j]);
        break;
      }
      case 3: // modify
      {
        final INode i = current.get(RANDOM.nextInt(current.size()));
        modify(i, current, diffs[j]);
        break;
      }
      }
    }

    {
      // check if current == previous + diffs
      List<INode> c = previous;
      for(int i = 0; i < diffs.length; i++) {
        c = diffs[i].apply2Previous(c);
      }
      if (!hasIdenticalElements(current, c)) {
        System.out.println("previous = " + Diff.toString(previous));
        System.out.println();
        System.out.println("current  = " + Diff.toString(current));
        System.out.println("c        = " + Diff.toString(c));
        throw new AssertionError("current and c are not identical.");
      }

      // check if previous == current - diffs
      List<INode> p = current;
      for(int i = diffs.length - 1; i >= 0; i--) {
        p = diffs[i].apply2Current(p);
      }
      if (!hasIdenticalElements(previous, p)) {
        System.out.println("previous = " + Diff.toString(previous));
        System.out.println("p        = " + Diff.toString(p));
        System.out.println();
        System.out.println("current  = " + Diff.toString(current));
        throw new AssertionError("previous and p are not identical.");
      }
    }

    // combine all diffs
    final Diff combined = diffs[0];
    for(int i = 1; i < diffs.length; i++) {
      combined.combinePostDiff(diffs[i], null);
    }

    {
      // check if current == previous + combined
      final List<INode> c = combined.apply2Previous(previous);
      if (!hasIdenticalElements(current, c)) {
        System.out.println("previous = " + Diff.toString(previous));
        System.out.println();
        System.out.println("current  = " + Diff.toString(current));
        System.out.println("c        = " + Diff.toString(c));
        throw new AssertionError("current and c are not identical.");
      }

      // check if previous == current - combined
      final List<INode> p = combined.apply2Current(current);
      if (!hasIdenticalElements(previous, p)) {
        System.out.println("previous = " + Diff.toString(previous));
        System.out.println("p        = " + Diff.toString(p));
        System.out.println();
        System.out.println("current  = " + Diff.toString(current));
        throw new AssertionError("previous and p are not identical.");
      }
    }

    {
      for(int m = 0; m < n; m++) {
        final INode inode = newINode(m, width);
        {// test accessPrevious
          final INode[] array = combined.accessPrevious(inode.getLocalNameBytes());
          final INode computed;
          if (array != null) {
            computed = array[0];
          } else {
            final int i = Diff.search(current, inode);
            computed = i < 0? null: current.get(i);
          }

          final int j = Diff.search(previous, inode);
          final INode expected = j < 0? null: previous.get(j);
          // must be the same object (equals is not enough)
          Assert.assertTrue(computed == expected);
        }

        {// test accessCurrent
          final INode[] array = combined.accessCurrent(inode.getLocalNameBytes());
          final INode computed;
          if (array != null) {
            computed = array[0]; 
          } else {
            final int i = Diff.search(previous, inode);
            computed = i < 0? null: previous.get(i);
          }

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

  static String toString(Diff diff) {
    return diff.toString();
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
    return new INodeDirectory(n, String.format("n%0" + width + "d", n), PERM);
  }

  static void create(INode inode, final List<INode> current, Diff diff) {
    final int i = Diff.search(current, inode);
    Assert.assertTrue(i < 0);
    current.add(-i - 1, inode);
    if (diff != null) {
      //test undo with 1/UNDO_TEST_P probability
      final boolean testUndo = RANDOM.nextInt(UNDO_TEST_P) == 0;
      String before = null;
      if (testUndo) {
        before = toString(diff);
      }

      final int undoInfo = diff.create(inode);

      if (testUndo) {
        final String after = toString(diff);
        //undo
        diff.undoCreate(inode, undoInfo);
        assertDiff(before, diff);
        //re-do
        diff.create(inode);
        assertDiff(after, diff);
      }
    }
  }

  static void delete(INode inode, final List<INode> current, Diff diff) {
    final int i = Diff.search(current, inode);
    current.remove(i);
    if (diff != null) {
      //test undo with 1/UNDO_TEST_P probability
      final boolean testUndo = RANDOM.nextInt(UNDO_TEST_P) == 0;
      String before = null;
      if (testUndo) {
        before = toString(diff);
      }

      final Triple<Integer, INode, Integer> undoInfo = diff.delete(inode);

      if (testUndo) {
        final String after = toString(diff);
        //undo
        diff.undoDelete(inode, undoInfo);
        assertDiff(before, diff);
        //re-do
        diff.delete(inode);
        assertDiff(after, diff);
      }
    }
  }

  static void modify(INode inode, final List<INode> current, Diff diff) {
    final int i = Diff.search(current, inode);
    Assert.assertTrue(i >= 0);
    final INodeDirectory oldinode = (INodeDirectory)current.get(i);
    final INodeDirectory newinode = new INodeDirectory(oldinode, false);
    newinode.updateModificationTime(oldinode.getModificationTime() + 1, null);

    current.set(i, newinode);
    if (diff != null) {
      //test undo with 1/UNDO_TEST_P probability
      final boolean testUndo = RANDOM.nextInt(UNDO_TEST_P) == 0;
      String before = null;
      if (testUndo) {
        before = toString(diff);
      }

      final Triple<Integer, INode, Integer> undoInfo = diff.modify(oldinode, newinode);

      if (testUndo) {
        final String after = toString(diff);
        //undo
        diff.undoModify(oldinode, newinode, undoInfo);
        assertDiff(before, diff);
        //re-do
        diff.modify(oldinode, newinode);
        assertDiff(after, diff);
      }
    }
  }
  
  static void assertDiff(String s, Diff diff) {
    Assert.assertEquals(s, toString(diff));
  }

  /**
   * Test {@link Snapshot#ID_COMPARATOR}.
   */
  @Test
  public void testIdCmp() {
    final INodeDirectory dir = new INodeDirectory(0, "foo", PERM);
    final INodeDirectorySnapshottable snapshottable
        = new INodeDirectorySnapshottable(dir);
    final Snapshot[] snapshots = {
      new Snapshot(1, "s1", snapshottable),
      new Snapshot(1, "s1", snapshottable),
      new Snapshot(2, "s2", snapshottable),
      new Snapshot(2, "s2", snapshottable),
    };

    Assert.assertEquals(0, Snapshot.ID_COMPARATOR.compare(null, null));
    for(Snapshot s : snapshots) {
      Assert.assertTrue(Snapshot.ID_COMPARATOR.compare(null, s) < 0);
      Assert.assertTrue(Snapshot.ID_COMPARATOR.compare(s, null) > 0);
      
      for(Snapshot t : snapshots) {
        final int expected = s.getRoot().getLocalName().compareTo(
            t.getRoot().getLocalName());
        final int computed = Snapshot.ID_COMPARATOR.compare(s, t);
        Assert.assertEquals(expected > 0, computed > 0);
        Assert.assertEquals(expected == 0, computed == 0);
        Assert.assertEquals(expected < 0, computed < 0);
      }
    }
  }
}
