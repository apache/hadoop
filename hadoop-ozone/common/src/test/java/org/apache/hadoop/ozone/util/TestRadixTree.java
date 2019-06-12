/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.util;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test Ozone Radix tree operations.
 */
public class TestRadixTree {

  final static RadixTree<Integer> ROOT = new RadixTree<>();

  @BeforeClass
  public static void setupRadixTree() {
    // Test prefix paths with an empty tree
    assertEquals(true, ROOT.isEmpty());
    assertEquals("/", ROOT.getLongestPrefix("/a/b/c"));
    assertEquals("/", RadixTree.radixPathToString(
        ROOT.getLongestPrefixPath("/a/g")));
    // Build Radix tree below for testing.
    //                a
    //                |
    //                b
    //             /    \
    //            c        e
    //           / \    /   \   \
    //          d   f  g   dir1  dir2(1000)
    //          |
    //          g
    //          |
    //          h
    ROOT.insert("/a/b/c/d");
    ROOT.insert("/a/b/c/d/g/h");
    ROOT.insert("/a/b/c/f");
    ROOT.insert("/a/b/e/g");
    ROOT.insert("/a/b/e/dir1");
    ROOT.insert("/a/b/e/dir2", 1000);
  }

  /**
   * Tests if insert and build prefix tree is correct.
   */
  @Test
  public  void testGetLongestPrefix() {
    assertEquals("/a/b/c", ROOT.getLongestPrefix("/a/b/c"));
    assertEquals("/a/b", ROOT.getLongestPrefix("/a/b"));
    assertEquals("/a", ROOT.getLongestPrefix("/a"));
    assertEquals("/a/b/e/g", ROOT.getLongestPrefix("/a/b/e/g/h"));

    assertEquals("/", ROOT.getLongestPrefix("/d/b/c"));
    assertEquals("/a/b/e", ROOT.getLongestPrefix("/a/b/e/dir3"));
    assertEquals("/a/b/c/d", ROOT.getLongestPrefix("/a/b/c/d/p"));

    assertEquals("/a/b/c/f", ROOT.getLongestPrefix("/a/b/c/f/p"));
  }

  @Test
  public void testGetLongestPrefixPath() {
    List<RadixNode<Integer>> lpp =
        ROOT.getLongestPrefixPath("/a/b/c/d/g/p");
    RadixNode<Integer> lpn = lpp.get(lpp.size()-1);
    assertEquals("g", lpn.getName());
    lpn.setValue(100);

    List<RadixNode<Integer>> lpq =
        ROOT.getLongestPrefixPath("/a/b/c/d/g/q");
    RadixNode<Integer> lqn = lpp.get(lpq.size()-1);
    System.out.print(RadixTree.radixPathToString(lpq));
    assertEquals(lpn, lqn);
    assertEquals("g", lqn.getName());
    assertEquals(100, (int)lqn.getValue());

    assertEquals("/a/", RadixTree.radixPathToString(
        ROOT.getLongestPrefixPath("/a/g")));

  }

  @Test
  public void testGetLastNoeInPrefixPath() {
    assertEquals(null, ROOT.getLastNodeInPrefixPath("/a/g"));
    RadixNode<Integer> ln = ROOT.getLastNodeInPrefixPath("/a/b/e/dir1");
    assertEquals("dir1", ln.getName());
  }

  @Test
  public void testRemovePrefixPath() {

    // Remove, test and restore
    // Remove partially overlapped path
    ROOT.removePrefixPath("/a/b/c/d/g/h");
    assertEquals("/a/b/c", ROOT.getLongestPrefix("a/b/c/d"));
    ROOT.insert("/a/b/c/d/g/h");

    // Remove fully overlapped path
    ROOT.removePrefixPath("/a/b/c/d");
    assertEquals("/a/b/c/d", ROOT.getLongestPrefix("a/b/c/d"));
    ROOT.insert("/a/b/c/d");

    // Remove non existing path
    ROOT.removePrefixPath("/d/a");
    assertEquals("/a/b/c/d", ROOT.getLongestPrefix("a/b/c/d"));
  }


}
