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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestQueuePath {
  private static final String TEST_QUEUE = "root.level_1.level_2.level_3";

  @Test
  public void testCreation() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);

    Assert.assertEquals(TEST_QUEUE, queuePath.getFullPath());
    Assert.assertEquals("root.level_1.level_2", queuePath.getParent());
    Assert.assertEquals("level_3", queuePath.getLeafName());

    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);
    Assert.assertNull(rootPath.getParent());

    QueuePath appendedPath = queuePath.createNewLeaf("level_4");
    Assert.assertEquals(TEST_QUEUE + CapacitySchedulerConfiguration.DOT
        + "level_4", appendedPath.getFullPath());
    Assert.assertEquals("root.level_1.level_2.level_3", appendedPath.getParent());
    Assert.assertEquals("level_4", appendedPath.getLeafName());
  }

  @Test
  public void testEmptyPart() {
    QueuePath queuePathWithEmptyPart = new QueuePath("root..level_2");
    QueuePath queuePathWithEmptyLeaf = new QueuePath("root.level_1.");
    QueuePath queuePathWithoutEmptyPart = new QueuePath(TEST_QUEUE);

    Assert.assertTrue(queuePathWithEmptyPart.hasEmptyPart());
    Assert.assertTrue(queuePathWithEmptyLeaf.hasEmptyPart());
    Assert.assertFalse(queuePathWithoutEmptyPart.hasEmptyPart());
  }

  @Test
  public void testNullPath() {
    QueuePath queuePathWithNullPath = new QueuePath(null);

    Assert.assertNull(queuePathWithNullPath.getParent());
    Assert.assertEquals("", queuePathWithNullPath.getLeafName());
    Assert.assertEquals("", queuePathWithNullPath.getFullPath());
    Assert.assertFalse(queuePathWithNullPath.isRoot());
  }

  @Test
  public void testIterator() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathWithEmptyPart = new QueuePath("root..level_2");
    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);

    List<String> queuePathCollection = ImmutableList.copyOf(queuePath.iterator());
    List<String> queuePathWithEmptyPartCollection = ImmutableList.copyOf(
        queuePathWithEmptyPart.iterator());
    List<String> rootPathCollection = ImmutableList.copyOf(rootPath.iterator());

    Assert.assertEquals(4, queuePathCollection.size());
    Assert.assertEquals(CapacitySchedulerConfiguration.ROOT, queuePathCollection.get(0));
    Assert.assertEquals("level_3", queuePathCollection.get(3));

    Assert.assertEquals(3, queuePathWithEmptyPartCollection.size());
    Assert.assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathWithEmptyPartCollection.get(0));
    Assert.assertEquals("level_2", queuePathWithEmptyPartCollection.get(2));

    Assert.assertEquals(1, rootPathCollection.size());
    Assert.assertEquals(CapacitySchedulerConfiguration.ROOT, rootPathCollection.get(0));
  }

  @Test
  public void testReversePathIterator() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathWithEmptyPart = new QueuePath("root..level_2");
    QueuePath rootPath = new QueuePath(CapacitySchedulerConfiguration.ROOT);

    List<String> queuePathCollection = ImmutableList.copyOf(queuePath.reverseIterator());
    List<String> queuePathWithEmptyPartCollection = ImmutableList.copyOf(
        queuePathWithEmptyPart.reverseIterator());
    List<String> rootPathCollection = ImmutableList.copyOf(rootPath.reverseIterator());

    Assert.assertEquals(4, queuePathCollection.size());
    Assert.assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathCollection.get(3));
    Assert.assertEquals(TEST_QUEUE, queuePathCollection.get(0));

    Assert.assertEquals(3, queuePathWithEmptyPartCollection.size());
    Assert.assertEquals(CapacitySchedulerConfiguration.ROOT,
        queuePathWithEmptyPartCollection.get(2));
    Assert.assertEquals("root..level_2", queuePathWithEmptyPartCollection.get(0));

    Assert.assertEquals(1, rootPathCollection.size());
    Assert.assertEquals(CapacitySchedulerConfiguration.ROOT,
        rootPathCollection.get(0));
  }

  @Test
  public void testEquals() {
    QueuePath queuePath = new QueuePath(TEST_QUEUE);
    QueuePath queuePathSame = new QueuePath(TEST_QUEUE);

    QueuePath empty = new QueuePath("");
    QueuePath emptySame = new QueuePath("");

    Assert.assertEquals(queuePath, queuePathSame);
    Assert.assertEquals(empty, emptySame);
    Assert.assertNotEquals(null, queuePath);
  }
}
