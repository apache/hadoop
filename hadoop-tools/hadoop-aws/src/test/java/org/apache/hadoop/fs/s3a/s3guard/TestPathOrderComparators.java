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

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.thirdparty.com.google.common.collect.Lists.newArrayList;
import static org.apache.hadoop.fs.s3a.s3guard.PathOrderComparators.TOPMOST_PATH_FIRST;
import static org.apache.hadoop.fs.s3a.s3guard.PathOrderComparators.TOPMOST_PATH_LAST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Test ordering of paths with the comparator matches requirements.
 */
public class TestPathOrderComparators {

  private static final Path ROOT = new Path("s3a://bucket/");

  public static final Path DIR_A = new Path(ROOT, "dirA");

  public static final Path DIR_B = new Path(ROOT, "dirB");

  public static final Path DIR_A_FILE_1 = new Path(DIR_A, "file1");

  public static final Path DIR_A_FILE_2 = new Path(DIR_A, "file2");

  public static final Path DIR_B_FILE_3 = new Path(DIR_B, "file3");

  public static final Path DIR_B_FILE_4 = new Path(DIR_B, "file4");

  @Test
  public void testRootEqual() throws Throwable {
    assertComparesEqual(ROOT, ROOT);
  }

  @Test
  public void testRootFirst() throws Throwable {
    assertComparesTopmost(ROOT, DIR_A_FILE_1);
  }

  @Test
  public void testDirOrdering() throws Throwable {
    assertComparesTopmost(DIR_A, DIR_B);
  }

  @Test
  public void testFilesEqual() throws Throwable {
    assertComparesEqual(DIR_A_FILE_1, DIR_A_FILE_1);
  }

  @Test
  public void testFilesInSameDir() throws Throwable {
    assertComparesTopmost(ROOT, DIR_A_FILE_1);
    assertComparesTopmost(DIR_A, DIR_A_FILE_1);
    assertComparesTopmost(DIR_A, DIR_A_FILE_2);
    assertComparesTopmost(DIR_A_FILE_1, DIR_A_FILE_2);
  }

  @Test
  public void testReversedFiles() throws Throwable {
    assertReverseOrder(DIR_A_FILE_1, ROOT);
    assertReverseOrder(DIR_A_FILE_1, DIR_A);
    assertReverseOrder(DIR_A_FILE_2, DIR_A);
    assertReverseOrder(DIR_A_FILE_2, DIR_A_FILE_1);
  }

  @Test
  public void testFilesAndDifferentShallowDir() throws Throwable {
    assertComparesTopmost(DIR_B, DIR_A_FILE_1);
    assertComparesTopmost(DIR_A, DIR_B_FILE_3);
  }

  @Test
  public void testOrderRoot() throws Throwable {
    verifySorted(ROOT);
  }

  @Test
  public void testOrderRootDirs() throws Throwable {
    verifySorted(ROOT, DIR_A, DIR_B);
  }

  @Test
  public void testOrderRootDirsAndFiles() throws Throwable {
    verifySorted(ROOT, DIR_A, DIR_B, DIR_A_FILE_1, DIR_A_FILE_2);
  }

  @Test
  public void testOrderRootDirsAndAllFiles() throws Throwable {
    verifySorted(ROOT, DIR_A, DIR_B,
        DIR_A_FILE_1, DIR_A_FILE_2,
        DIR_B_FILE_3, DIR_B_FILE_4);
  }

  @Test
  public void testSortOrderConstant() throws Throwable {
    List<Path> sort1 = verifySorted(ROOT, DIR_A, DIR_B,
        DIR_A_FILE_1, DIR_A_FILE_2,
        DIR_B_FILE_3, DIR_B_FILE_4);
    List<Path> sort2 = newArrayList(sort1);
    assertSortsTo(sort2, sort1, true);
  }

  @Test
  public void testSortReverse() throws Throwable {
    List<Path> sort1 = newArrayList(
        ROOT,
        DIR_A,
        DIR_B,
        DIR_A_FILE_1,
        DIR_A_FILE_2,
        DIR_B_FILE_3,
        DIR_B_FILE_4);
    List<Path> expected = newArrayList(
        DIR_B_FILE_4,
        DIR_B_FILE_3,
        DIR_A_FILE_2,
        DIR_A_FILE_1,
        DIR_B,
        DIR_A,
        ROOT);
    assertSortsTo(expected, sort1, false);
  }


  private List<Path> verifySorted(Path... paths) {
    List<Path> original = newArrayList(paths);
    List<Path> sorted = newArrayList(paths);
    assertSortsTo(original, sorted, true);
    return sorted;
  }

  private void assertSortsTo(
      final List<Path> original,
      final List<Path> sorted,
      boolean topmost) {
    sorted.sort(topmost ? TOPMOST_PATH_FIRST : TOPMOST_PATH_LAST);
    assertThat(sorted)
        .as("Sorted paths")
        .containsExactlyElementsOf(original);
  }

  private void assertComparesEqual(Path l, Path r) {
    assertOrder(0, l, r);
  }

  private void assertComparesTopmost(Path l, Path r) {
    assertOrder(-1, l, r);
    assertOrder(1, r, l);
  }

  private void assertReverseOrder(Path l, Path r) {
    assertComparesTo(-1, TOPMOST_PATH_LAST, l, r);
    assertComparesTo(1, TOPMOST_PATH_LAST, r, l);
  }

  private void assertOrder(int res,
      Path l, Path r) {
    assertComparesTo(res, TOPMOST_PATH_FIRST, l, r);
  }

  private void assertComparesTo(final int expected,
      final Comparator<Path> comparator,
      final Path l, final Path r) {
    int actual = comparator.compare(l, r);
    if (actual < -1) {
      actual = -1;
    }
    if (actual > 1) {
      actual = 1;
    }
    assertEquals("Comparing " + l + " to " + r,
        expected, actual);
  }
}
