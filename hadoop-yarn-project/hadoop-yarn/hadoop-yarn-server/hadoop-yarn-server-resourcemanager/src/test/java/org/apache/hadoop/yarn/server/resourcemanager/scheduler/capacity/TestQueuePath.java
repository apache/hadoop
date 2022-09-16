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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestQueuePath {
  private static final String TEST_QUEUE = "root.level_1.level_2.level_3";

  @Test
  void testCreation() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);

    Assertions.assertEquals(TEST_QUEUE, queuePath.getFullPath());
    Assertions.assertEquals("root.level_1.level_2", queuePath.getParent());
    Assertions.assertEquals("level_3", queuePath.getLeafName());

    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);
    Assertions.assertNull(rootPath.getParent());

    QueuePath appendedPath = queuePath.createNewLeaf("level_4");
    Assertions.assertEquals(TEST_QUEUE + CapacitySchedulerConfiguration.DOT
        + "level_4", appendedPath.getFullPath());
    Assertions.assertEquals("root.level_1.level_2.level_3", appendedPath.getParent());
    Assertions.assertEquals("level_4", appendedPath.getLeafName());
  }

  @Test
  void testEmptyPart() {
    QueuePath queuePathWithEmptyPart = new QueuePath("root..level_2");
    QueuePath queuePathWithoutEmptyPart = new QueuePath(TEST_QUEUE);

    Assertions.assertTrue(queuePathWithEmptyPart.hasEmptyPart());
    Assertions.assertFalse(queuePathWithoutEmptyPart.hasEmptyPart());
  }

  @Test
  void testNullPath() {
    QueuePath queuePathWithNullPath = new QueuePath(null);

    Assertions.assertNull(queuePathWithNullPath.getParent());
    Assertions.assertEquals("", queuePathWithNullPath.getLeafName());
    Assertions.assertEquals("", queuePathWithNullPath.getFullPath());
    Assertions.assertFalse(queuePathWithNullPath.isRoot());
  }

  @Test
  void testIterator() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathWithEmptyPart = new QueuePath("root..level_2");
    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);

    List<String> queuePathCollection = ImmutableList.copyOf(queuePath.iterator());
    List<String> queuePathWithEmptyPartCollection = ImmutableList.copyOf(
        queuePathWithEmptyPart.iterator());
    List<String> rootPathCollection = ImmutableList.copyOf(rootPath.iterator());

    Assertions.assertEquals(4, queuePathCollection.size());
    Assertions.assertEquals(CapacitySchedulerConfiguration.ROOT, queuePathCollection.get(0));
    Assertions.assertEquals("level_3", queuePathCollection.get(3));

    Assertions.assertEquals(3, queuePathWithEmptyPartCollection.size());
    Assertions.assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathWithEmptyPartCollection.get(0));
    Assertions.assertEquals("level_2", queuePathWithEmptyPartCollection.get(2));

    Assertions.assertEquals(1, rootPathCollection.size());
    Assertions.assertEquals(CapacitySchedulerConfiguration.ROOT, rootPathCollection.get(0));
  }

  @Test
  void testReversePathIterator() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathWithEmptyPart = new QueuePath("root..level_2");
    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);

    List<String> queuePathCollection = ImmutableList.copyOf(queuePath.reverseIterator());
    List<String> queuePathWithEmptyPartCollection = ImmutableList.copyOf(
        queuePathWithEmptyPart.reverseIterator());
    List<String> rootPathCollection = ImmutableList.copyOf(rootPath.reverseIterator());

    Assertions.assertEquals(4, queuePathCollection.size());
    Assertions.assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathCollection.get(3));
    Assertions.assertEquals(TEST_QUEUE, queuePathCollection.get(0));

    Assertions.assertEquals(3, queuePathWithEmptyPartCollection.size());
    Assertions.assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathWithEmptyPartCollection.get(2));
    Assertions.assertEquals("root..level_2", queuePathWithEmptyPartCollection.get(0));

    Assertions.assertEquals(1, rootPathCollection.size());
    Assertions.assertEquals(CapacitySchedulerConfiguration.ROOT,
        rootPathCollection.get(0));
  }

  @Test
  void testEquals() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathSame = new QueuePath(TEST_QUEUE);

    QueuePath empty = new QueuePath("");
    QueuePath emptySame = new QueuePath("");

    Assertions.assertEquals(queuePath, queuePathSame);
    Assertions.assertEquals(empty, emptySame);
    Assertions.assertNotEquals(null, queuePath);
  }
}
