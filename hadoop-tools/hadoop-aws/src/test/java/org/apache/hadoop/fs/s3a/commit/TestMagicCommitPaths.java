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

package org.apache.hadoop.fs.s3a.commit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.test.LambdaTestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.MagicCommitPaths.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Tests for {@link MagicCommitPaths} path operations.
 */
public class TestMagicCommitPaths extends Assert {

  private static final List<String> MAGIC_AT_ROOT =
      list(MAGIC);
  private static final List<String> MAGIC_AT_ROOT_WITH_CHILD =
      list(MAGIC, "child");
  private static final List<String> MAGIC_WITH_CHILD =
      list("parent", MAGIC, "child");
  private static final List<String> MAGIC_AT_WITHOUT_CHILD =
      list("parent", MAGIC);

  private static final List<String> DEEP_MAGIC =
      list("parent1", "parent2", MAGIC, "child1", "child2");

  public static final String[] EMPTY = {};

  @Test
  public void testSplitPathEmpty() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> splitPathToElements(new Path("")));
  }

  @Test
  public void testSplitPathDoubleBackslash() {
    assertPathSplits("//", EMPTY);
  }

  @Test
  public void testSplitRootPath() {
    assertPathSplits("/", EMPTY);
  }

  @Test
  public void testSplitBasic() {
    assertPathSplits("/a/b/c",
        new String[]{"a", "b", "c"});
  }

  @Test
  public void testSplitTrailingSlash() {
    assertPathSplits("/a/b/c/",
        new String[]{"a", "b", "c"});
  }

  @Test
  public void testSplitShortPath() {
    assertPathSplits("/a",
        new String[]{"a"});
  }

  @Test
  public void testSplitShortPathTrailingSlash() {
    assertPathSplits("/a/",
        new String[]{"a"});
  }

  @Test
  public void testParentsMagicRoot() {
    assertParents(EMPTY, MAGIC_AT_ROOT);
  }

  @Test
  public void testChildrenMagicRoot() {
    assertChildren(EMPTY, MAGIC_AT_ROOT);
  }

  @Test
  public void testParentsMagicRootWithChild() {
    assertParents(EMPTY, MAGIC_AT_ROOT_WITH_CHILD);
  }

  @Test
  public void testChildMagicRootWithChild() {
    assertChildren(a("child"), MAGIC_AT_ROOT_WITH_CHILD);
  }

  @Test
  public void testChildrenMagicWithoutChild() {
    assertChildren(EMPTY, MAGIC_AT_WITHOUT_CHILD);
  }

  @Test
  public void testChildMagicWithChild() {
    assertChildren(a("child"), MAGIC_WITH_CHILD);
  }

  @Test
  public void testParentMagicWithChild() {
    assertParents(a("parent"), MAGIC_WITH_CHILD);
  }

  @Test
  public void testParentDeepMagic() {
    assertParents(a("parent1", "parent2"), DEEP_MAGIC);
  }

  @Test
  public void testChildrenDeepMagic() {
    assertChildren(a("child1", "child2"), DEEP_MAGIC);
  }

  @Test
  public void testLastElementEmpty() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> lastElement(new ArrayList<>(0)));
  }

  @Test
  public void testLastElementSingle() {
    assertEquals("first", lastElement(l("first")));
  }

  @Test
  public void testLastElementDouble() {
    assertEquals("2", lastElement(l("first", "2")));
  }

  @Test
  public void testFinalDestinationNoMagic() {
    assertEquals(l("first", "2"),
        finalDestination(l("first", "2")));
  }

  @Test
  public void testFinalDestinationMagic1() {
    assertEquals(l("first", "2"),
        finalDestination(l("first", MAGIC, "2")));
  }

  @Test
  public void testFinalDestinationMagic2() {
    assertEquals(l("first", "3.txt"),
        finalDestination(l("first", MAGIC, "2", "3.txt")));
  }

  @Test
  public void testFinalDestinationRootMagic2() {
    assertEquals(l("3.txt"),
        finalDestination(l(MAGIC, "2", "3.txt")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFinalDestinationMagicNoChild() {
    finalDestination(l(MAGIC));
  }

  @Test
  public void testFinalDestinationBaseDirectChild() {
    finalDestination(l(MAGIC, BASE, "3.txt"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFinalDestinationBaseNoChild() {
    assertEquals(l(), finalDestination(l(MAGIC, BASE)));
  }

  @Test
  public void testFinalDestinationBaseSubdirsChild() {
    assertEquals(l("2", "3.txt"),
        finalDestination(l(MAGIC, "4", BASE, "2", "3.txt")));
  }

  /**
   * If the base is above the magic dir, it's ignored.
   */
  @Test
  public void testFinalDestinationIgnoresBaseBeforeMagic() {
    assertEquals(l(BASE, "home", "3.txt"),
        finalDestination(l(BASE, "home", MAGIC, "2", "3.txt")));
  }

  /** varargs to array. */
  private static String[] a(String... str) {
    return str;
  }

  /** list to array. */
  private static List<String> l(String... str) {
    return Arrays.asList(str);
  }

  /**
   * Varags to list.
   * @param args arguments
   * @return a list
   */
  private static List<String> list(String... args) {
    return Lists.newArrayList(args);
  }

  public void assertParents(String[] expected, List<String> elements) {
    assertListEquals(expected, magicPathParents(elements));
  }

  public void assertChildren(String[] expected, List<String> elements) {
    assertListEquals(expected, magicPathChildren(elements));
  }

  private void assertPathSplits(String pathString, String[] expected) {
    Path path = new Path(pathString);
    assertArrayEquals("From path " + path, expected,
        splitPathToElements(path).toArray());
  }

  private void assertListEquals(String[] expected, List<String> actual) {
    assertArrayEquals(expected, actual.toArray());
  }

}
