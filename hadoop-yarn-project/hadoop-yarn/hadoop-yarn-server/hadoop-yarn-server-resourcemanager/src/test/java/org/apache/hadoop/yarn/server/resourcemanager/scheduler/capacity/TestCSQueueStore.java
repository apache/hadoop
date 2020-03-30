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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestCSQueueStore {

  private final ResourceCalculator resourceCalculator =
          new DefaultResourceCalculator();

  private CSQueue root;
  private CapacitySchedulerContext csContext;

  @Before
  public void setUp() throws IOException {
    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration();

    RMContext rmContext = TestUtils.getMockRMContext();
    Resource clusterResource = Resources.createResource(
            10 * 16 * 1024, 10 * 32);

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
            thenReturn(Resources.createResource(1024, 1));
    when(csContext.getMaximumResourceCapability()).
            thenReturn(Resources.createResource(16*1024, 32));
    when(csContext.getClusterResource()).
            thenReturn(clusterResource);
    when(csContext.getResourceCalculator()).
            thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);

    CSQueueStore queues = new CSQueueStore();
    root = CapacitySchedulerQueueManager
            .parseQueue(csContext, csConf, null, "root",
                    queues, queues,
                    TestUtils.spyHook);
  }

  public CSQueue createLeafQueue(String name, CSQueue parent)
          throws IOException {
    return new LeafQueue(csContext, name, parent, null);
  }

  public CSQueue createParentQueue(String name, CSQueue parent)
          throws IOException {
    return new ParentQueue(csContext, name, parent, null);
  }

  /**
   * Asserts the queue can be accessed via it's full path and short name.
   * @param store Store against we do the assertion
   * @param queue The queue we need to look up
   */
  public void assertAccessibleByAllNames(CSQueueStore store, CSQueue queue) {
    assertEquals(queue, store.get(queue.getQueueShortName()));
    assertEquals(queue, store.get(queue.getQueuePath()));
  }

  /**
   * Asserts the queue can be accessed via it's full path only, using it's
   * short name supposed to return something else.
   *
   * This is a result of forcefully making root always referencing the root
   * since "root" is considered a full path, hence no other queue with short
   * name root should be accessible via root.
   *
   * @param store Store against we do the assertion
   * @param queue The queue we need to look up
   */
  public void assertAccessibleByFullNameOnly(
      CSQueueStore store, CSQueue queue) {
    assertFalse(store.isAmbiguous(queue.getQueueShortName()));
    assertNotEquals(queue, store.get(queue.getQueueShortName()));
    assertEquals(queue, store.get(queue.getQueuePath()));
  }

  /**
   * Asserts the queue can be accessed via it's full path only, using it's
   * short name supposed to return null, this is the case when there are two
   * leaf queues are present with the same name. (or two parent queues with
   * no leaf queue with the same name)
   * @param store Store against we do the assertion
   * @param queue The queue we need to look up
   */
  public void assertAmbiguous(CSQueueStore store, CSQueue queue) {
    assertTrue(store.isAmbiguous(queue.getQueueShortName()));
    assertNull(store.get(queue.getQueueShortName()));
    assertEquals(queue, store.get(queue.getQueuePath()));
  }

  /**
   * Asserts the queue is not present in the store.
   * @param store Store against we do the assertion
   * @param queue The queue we need to look up
   */
  public void assertQueueNotPresent(CSQueueStore store, CSQueue queue) {
    assertNotEquals(queue, store.get(queue.getQueueShortName()));
    assertNull(store.get(queue.getQueuePath()));
  }

  @Test
  public void testSimpleMapping() throws IOException {
    CSQueueStore store = new CSQueueStore();

    //root.main
    CSQueue main = createParentQueue("main", root);
    //root.main.A
    CSQueue mainA = createLeafQueue("A", main);
    //root.main.B
    CSQueue mainB = createParentQueue("B", main);
    //root.main.B.C
    CSQueue mainBC = createLeafQueue("C", mainB);

    store.add(main);
    store.add(mainA);
    store.add(mainB);
    store.add(mainBC);

    assertAccessibleByAllNames(store, main);
    assertAccessibleByAllNames(store, mainA);
    assertAccessibleByAllNames(store, mainB);
    assertAccessibleByAllNames(store, mainBC);
  }

  @Test
  public void testAmbiguousMapping() throws IOException {
    CSQueueStore store = new CSQueueStore();

    //root.main
    CSQueue main = createParentQueue("main", root);
    //root.main.A
    CSQueue mainA = createParentQueue("A", main);
    //root.main.A.C
    CSQueue mainAC = createLeafQueue("C", mainA);
    //root.main.A.D
    CSQueue mainAD = createParentQueue("D", mainA);
    //root.main.A.D.E
    CSQueue mainADE = createLeafQueue("E", mainAD);
    //root.main.A.D.F
    CSQueue mainADF = createLeafQueue("F", mainAD);
    //root.main.B
    CSQueue mainB = createParentQueue("B", main);
    //root.main.B.C
    CSQueue mainBC = createLeafQueue("C", mainB);
    //root.main.B.D
    CSQueue mainBD = createParentQueue("D", mainB);
    //root.main.B.D.E
    CSQueue mainBDE = createLeafQueue("E", mainBD);
    //root.main.B.D.G
    CSQueue mainBDG = createLeafQueue("G", mainBD);

    store.add(main);
    store.add(mainA);
    store.add(mainAC);
    store.add(mainAD);
    store.add(mainADE);
    store.add(mainADF);
    store.add(mainB);
    store.add(mainBC);
    store.add(mainBD);
    store.add(mainBDE);
    store.add(mainBDG);

    assertAccessibleByAllNames(store, main);
    assertAccessibleByAllNames(store, mainA);
    assertAccessibleByAllNames(store, mainB);
    assertAccessibleByAllNames(store, mainADF);
    assertAccessibleByAllNames(store, mainBDG);

    assertAmbiguous(store, mainAC);
    assertAmbiguous(store, mainAD);
    assertAmbiguous(store, mainADE);
    assertAmbiguous(store, mainBC);
    assertAmbiguous(store, mainBD);
    assertAmbiguous(store, mainBDE);
  }

  @Test
  public void testDynamicModifications() throws IOException {
    CSQueueStore store = new CSQueueStore();

    //root.main
    CSQueue main = createParentQueue("main", root);
    //root.main.A
    CSQueue mainA = createParentQueue("A", main);
    //root.main.B
    CSQueue mainB = createParentQueue("B", main);
    //root.main.A.C
    CSQueue mainAC = createLeafQueue("C", mainA);
    //root.main.B.C
    CSQueue mainBC = createLeafQueue("C", mainB);

    store.add(main);
    store.add(mainA);
    store.add(mainB);

    assertAccessibleByAllNames(store, main);
    assertAccessibleByAllNames(store, mainA);
    assertAccessibleByAllNames(store, mainB);

    assertQueueNotPresent(store, mainAC);
    assertQueueNotPresent(store, mainBC);

    store.add(mainAC);
    assertAccessibleByAllNames(store, mainAC);
    assertQueueNotPresent(store, mainBC);

    store.add(mainBC);
    assertAmbiguous(store, mainAC);
    assertAmbiguous(store, mainBC);

    store.remove(mainAC);
    assertQueueNotPresent(store, mainAC);
    assertAccessibleByAllNames(store, mainBC);

    store.remove(mainBC);
    assertQueueNotPresent(store, mainAC);
    assertQueueNotPresent(store, mainBC);
  }

  @Test
  public void testQueueOverwrites() throws IOException {
    CSQueueStore store = new CSQueueStore();

    //root.main
    CSQueue main = createParentQueue("main", root);
    //root.main.A
    CSQueue mainA = createLeafQueue("A", main);
    //root.main.B
    CSQueue newA = createLeafQueue("A", main);

    store.add(main);
    store.add(mainA);
    assertAccessibleByAllNames(store, mainA);

    store.add(newA);
    //this implicitly checks mainA is not longer accessible by it's names
    assertAccessibleByAllNames(store, newA);
  }

  @Test
  public void testQueueReferencePrecedence() throws IOException {
    CSQueueStore store = new CSQueueStore();

    //root.main.a.b
    //root.second.a.d.b.c
    // a - ambiguous both instances are parent queues
    // b - leaf queue b takes precedence over parent queue b

    //root.main
    CSQueue main = createParentQueue("main", root);
    //root.main.A
    CSQueue mainA = createParentQueue("A", main);
    //root.main.A.B
    CSQueue mainAB = createLeafQueue("B", mainA);
    //root.second
    CSQueue second = createParentQueue("second", root);
    //root.second.A
    CSQueue secondA = createParentQueue("A", second);
    //root.second.A.B
    CSQueue secondAD = createParentQueue("D", secondA);
    //root.second.A.B.D
    CSQueue secondADB = createParentQueue("B", secondAD);
    //root.second.A.B.D.C
    CSQueue secondADBC = createLeafQueue("C", secondADB);

    store.add(main);
    store.add(mainA);
    store.add(mainAB);
    store.add(second);
    store.add(secondA);
    store.add(secondAD);
    store.add(secondADB);
    store.add(secondADBC);

    assertAccessibleByAllNames(store, main);
    assertAccessibleByAllNames(store, second);
    assertAmbiguous(store, mainA);
    assertAmbiguous(store, secondA);
    assertAmbiguous(store, mainAB);
    assertAccessibleByAllNames(store, secondAD);
    assertAmbiguous(store, secondADB);
    assertAccessibleByAllNames(store, secondADBC);
  }

  @Test
  public void testRootIsAlwaysAccesible() throws IOException {
    CSQueueStore store = new CSQueueStore();

    //root.root
    CSQueue rootroot = createParentQueue("root", root);

    store.add(root);
    store.add(rootroot);

    assertAccessibleByAllNames(store, root);
    assertAccessibleByFullNameOnly(store, rootroot);
    assertFalse(store.isAmbiguous("root"));
  }
}