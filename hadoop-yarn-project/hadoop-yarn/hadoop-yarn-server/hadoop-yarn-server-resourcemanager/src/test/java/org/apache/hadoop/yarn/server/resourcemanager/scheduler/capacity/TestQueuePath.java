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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestQueuePath {
  private static final String TEST_QUEUE = "root.level_1.level_2.level_3";
  private static final QueuePath TEST_QUEUE_PATH = new QueuePath(TEST_QUEUE);
  private static final QueuePath QUEUE_PATH_WITH_EMPTY_PART = new QueuePath("root..level_2");
  private static final QueuePath QUEUE_PATH_WITH_EMPTY_LEAF = new QueuePath("root.level_1.");
  private static final QueuePath ROOT_PATH = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final QueuePath EMPTY_PATH = new QueuePath("");
  private static final QueuePath ONE_LEVEL_WILDCARDED_TEST_PATH =
      new QueuePath("root.level_1.level_2.*");
  private static final QueuePath TWO_LEVEL_WILDCARDED_TEST_PATH =
      new QueuePath("root.level_1.*.*");
  private static final QueuePath THREE_LEVEL_WILDCARDED_TEST_PATH =
      new QueuePath("root.*.*.*");

  @Test
  public void testCreation() {
    assertEquals(TEST_QUEUE, TEST_QUEUE_PATH.getFullPath());
    assertEquals("root.level_1.level_2", TEST_QUEUE_PATH.getParent());
    assertEquals("level_3", TEST_QUEUE_PATH.getLeafName());

    assertNull(ROOT_PATH.getParent());

    QueuePath appendedPath = TEST_QUEUE_PATH.createNewLeaf("level_4");
    assertEquals(TEST_QUEUE + CapacitySchedulerConfiguration.DOT
        + "level_4", appendedPath.getFullPath());
    assertEquals("root.level_1.level_2.level_3", appendedPath.getParent());
    assertEquals("level_4", appendedPath.getLeafName());
  }

  @Test
  public void testEmptyPart() {
    assertTrue(QUEUE_PATH_WITH_EMPTY_PART.hasEmptyPart());
    assertTrue(QUEUE_PATH_WITH_EMPTY_LEAF.hasEmptyPart());
    assertFalse(TEST_QUEUE_PATH.hasEmptyPart());
  }

  @Test
  public void testNullPath() {
    QueuePath queuePathWithNullPath = new QueuePath(null);

    assertNull(queuePathWithNullPath.getParent());
    assertEquals("", queuePathWithNullPath.getLeafName());
    assertEquals("", queuePathWithNullPath.getFullPath());
    assertFalse(queuePathWithNullPath.isRoot());
  }

  @Test
  public void testIterator() {
    List<String> queuePathCollection = ImmutableList.copyOf(TEST_QUEUE_PATH.iterator());
    List<String> queuePathWithEmptyPartCollection = ImmutableList.copyOf(
        QUEUE_PATH_WITH_EMPTY_PART.iterator());
    List<String> rootPathCollection = ImmutableList.copyOf(ROOT_PATH.iterator());

    assertEquals(4, queuePathCollection.size());
    assertEquals(CapacitySchedulerConfiguration.ROOT, queuePathCollection.get(0));
    assertEquals("level_3", queuePathCollection.get(3));

    assertEquals(3, queuePathWithEmptyPartCollection.size());
    assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathWithEmptyPartCollection.get(0));
    assertEquals("level_2", queuePathWithEmptyPartCollection.get(2));

    assertEquals(1, rootPathCollection.size());
    assertEquals(CapacitySchedulerConfiguration.ROOT, rootPathCollection.get(0));
  }

  @Test
  public void testReversePathIterator() {
    List<String> queuePathCollection = ImmutableList.copyOf(TEST_QUEUE_PATH.reverseIterator());
    List<String> queuePathWithEmptyPartCollection = ImmutableList.copyOf(
        QUEUE_PATH_WITH_EMPTY_PART.reverseIterator());
    List<String> rootPathCollection = ImmutableList.copyOf(ROOT_PATH.reverseIterator());

    assertEquals(4, queuePathCollection.size());
    assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathCollection.get(3));
    assertEquals(TEST_QUEUE, queuePathCollection.get(0));

    assertEquals(3, queuePathWithEmptyPartCollection.size());
    assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathWithEmptyPartCollection.get(2));
    assertEquals("root..level_2", queuePathWithEmptyPartCollection.get(0));

    assertEquals(1, rootPathCollection.size());
    assertEquals(CapacitySchedulerConfiguration.ROOT,
        rootPathCollection.get(0));
  }

  @Test
  public void testEquals() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathSame = new QueuePath(TEST_QUEUE);

    QueuePath empty = new QueuePath("");
    QueuePath emptySame = new QueuePath("");

    assertEquals(queuePath, queuePathSame);
    assertEquals(empty, emptySame);
    assertNotEquals(null, queuePath);
  }

  @Test
  public void testInvalidPath() {
    assertFalse(TEST_QUEUE_PATH.isInvalid());
    assertFalse(ROOT_PATH.isInvalid());
    assertTrue(EMPTY_PATH.isInvalid());
    assertTrue(new QueuePath("invalidPath").isInvalid());
  }

  @Test
  public void testGetParentObject() {
    assertEquals(new QueuePath("root.level_1.level_2"),
        TEST_QUEUE_PATH.getParentObject());
    assertEquals(ROOT_PATH, new QueuePath("root.level_1").getParentObject());
    assertNull(ROOT_PATH.getParentObject());
  }

  @Test
  public void testGetPathComponents() {
    assertArrayEquals(TEST_QUEUE_PATH.getPathComponents(),
        new String[] {"root", "level_1", "level_2", "level_3"});
    assertArrayEquals(ROOT_PATH.getPathComponents(), new String[] {"root"});
    assertArrayEquals(EMPTY_PATH.getPathComponents(), new String[] {""});
  }

  @Test
  public void testWildcardedQueuePathsWithOneLevelWildCard() {
    int maxAutoCreatedQueueDepth = 1;

    List<QueuePath> expectedPaths = new ArrayList<>();
    expectedPaths.add(TEST_QUEUE_PATH);
    expectedPaths.add(ONE_LEVEL_WILDCARDED_TEST_PATH);

    List<QueuePath> wildcardedPaths = TEST_QUEUE_PATH
        .getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

    assertEquals(expectedPaths, wildcardedPaths);
  }

  @Test
  public void testWildcardedQueuePathsWithTwoLevelWildCard() {
    int maxAutoCreatedQueueDepth = 2;

    List<QueuePath> expectedPaths = new ArrayList<>();
    expectedPaths.add(TEST_QUEUE_PATH);
    expectedPaths.add(ONE_LEVEL_WILDCARDED_TEST_PATH);
    expectedPaths.add(TWO_LEVEL_WILDCARDED_TEST_PATH);

    List<QueuePath> wildcardedPaths = TEST_QUEUE_PATH
        .getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

    assertEquals(expectedPaths, wildcardedPaths);
  }

  @Test
  public void testWildcardedQueuePathsWithThreeLevelWildCard() {
    int maxAutoCreatedQueueDepth = 3;

    List<QueuePath> expectedPaths = new ArrayList<>();
    expectedPaths.add(TEST_QUEUE_PATH);
    expectedPaths.add(ONE_LEVEL_WILDCARDED_TEST_PATH);
    expectedPaths.add(TWO_LEVEL_WILDCARDED_TEST_PATH);
    expectedPaths.add(THREE_LEVEL_WILDCARDED_TEST_PATH);

    List<QueuePath> wildcardedPaths = TEST_QUEUE_PATH
        .getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

    assertEquals(expectedPaths, wildcardedPaths);
  }

  @Test
  public void testWildcardingWhenMaxACQDepthIsGreaterThanQueuePathDepth() {
    int maxAutoCreatedQueueDepth = 4;

    List<QueuePath> expectedPaths = new ArrayList<>();
    expectedPaths.add(TEST_QUEUE_PATH);
    expectedPaths.add(ONE_LEVEL_WILDCARDED_TEST_PATH);
    expectedPaths.add(TWO_LEVEL_WILDCARDED_TEST_PATH);
    expectedPaths.add(THREE_LEVEL_WILDCARDED_TEST_PATH);

    List<QueuePath> wildcardedPaths = TEST_QUEUE_PATH
        .getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

    assertEquals(expectedPaths, wildcardedPaths);
  }

  @Test
  public void testWildcardedQueuePathsWithRootPath() {
    int maxAutoCreatedQueueDepth = 1;

    List<QueuePath> expectedPaths = new ArrayList<>();
    expectedPaths.add(ROOT_PATH);

    List<QueuePath> wildcardedPaths = ROOT_PATH.getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

    assertEquals(expectedPaths, wildcardedPaths);
  }
}
