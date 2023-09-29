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

package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link WeakReferenceMap} and {@link WeakReferenceThreadMap}.
 * There's no attempt to force GC here, so the tests are
 * more about the basic behavior not the handling of empty references.
 */
public class TestWeakReferenceMap extends AbstractHadoopTestBase {

  public static final String FACTORY_STRING = "recreated %d";

  /**
   * The map to test.
   */
  private WeakReferenceMap<Integer, String> referenceMap;

  /**
   * List of references notified of loss.
   */
  private List<Integer> lostReferences;

  @Before
  public void setup() {
    lostReferences = new ArrayList<>();
    referenceMap = new WeakReferenceMap<>(
        this::factory,
        this::referenceLost);
  }

  /**
   * Reference lost callback.
   * @param key key lost
   */
  private void referenceLost(Integer key) {
    lostReferences.add(key);
  }


  /**
   * Basic insertions and lookups of those values.
   */
  @Test
  public void testBasicOperationsWithValidReferences() {

    referenceMap.put(1, "1");
    referenceMap.put(2, "2");
    assertMapSize(2);
    assertMapContainsKey(1);
    assertMapEntryEquals(1, "1");
    assertMapEntryEquals(2, "2");
    // overwrite
    referenceMap.put(1, "3");
    assertMapEntryEquals(1, "3");

    // remove an entry
    referenceMap.remove(1);
    assertMapDoesNotContainKey(1);
    assertMapSize(1);

    // clear the map
    referenceMap.clear();
    assertMapSize(0);
  }

  /**
   * pruning removes null entries, leaves the others alone.
   */
  @Test
  public void testPruneNullEntries() {
    referenceMap.put(1, "1");
    assertPruned(0);
    referenceMap.put(2, null);
    assertMapSize(2);
    assertPruned(1);
    assertMapSize(1);
    assertMapDoesNotContainKey(2);
    assertMapEntryEquals(1, "1");
    assertLostCount(1);
  }

  /**
   * Demand create entries.
   */
  @Test
  public void testDemandCreateEntries() {

    // ask for an unknown key and expect a generated value
    assertMapEntryEquals(1, factory(1));
    assertMapSize(1);
    assertMapContainsKey(1);
    assertLostCount(0);

    // an empty ref has the same outcome
    referenceMap.put(2, null);
    assertMapEntryEquals(2, factory(2));
    // but the lost coun goes up
    assertLostCount(1);

  }

  /**
   * It is an error to have a factory which returns null.
   */
  @Test
  public void testFactoryReturningNull() throws Throwable {
    referenceMap = new WeakReferenceMap<>(
        (k) -> null,
            null);
    intercept(NullPointerException.class, () ->
        referenceMap.get(0));
  }

  /**
   * Test the WeakReferenceThreadMap extension.
   */
  @Test
  public void testWeakReferenceThreadMapAssignment()
      throws Throwable {

    // counters foor the callbacks
    final AtomicLong created = new AtomicLong();
    final AtomicLong lost = new AtomicLong();

    WeakReferenceThreadMap<String> threadMap = new WeakReferenceThreadMap<>(
        id -> "Entry for thread ID " + id + " (" + created.incrementAndGet() + ")",
        id -> lost.incrementAndGet());

    Assertions.assertThat(threadMap.setForCurrentThread("hello"))
        .describedAs("current thread map value on first set")
        .isNull();

    // second attempt returns itself
    Assertions.assertThat(threadMap.setForCurrentThread("hello"))
        .describedAs("current thread map value on second set")
        .isEqualTo("hello");

    // it is forbidden to explicitly set to null via the set() call.
    intercept(NullPointerException.class, () ->
        threadMap.setForCurrentThread(null));

    // the map is unchanged
    Assertions.assertThat(threadMap.getForCurrentThread())
        .describedAs("current thread map value")
        .isEqualTo("hello");

    // remove the value and assert what the removed entry was
    Assertions.assertThat(threadMap.removeForCurrentThread())
        .describedAs("removed thread map value")
        .isEqualTo("hello");

    // remove the value again; this time the removed value is null
    Assertions.assertThat(threadMap.removeForCurrentThread())
        .describedAs("removed thread map value on second call")
        .isNull();

    // lookup will return a new instance created by the factory
    long c1 = created.get();
    String dynamicValue = threadMap.getForCurrentThread();
    Assertions.assertThat(dynamicValue)
        .describedAs("dynamically created thread map value")
        .startsWith("Entry for thread ID")
        .contains("(" + (c1 + 1) + ")");

    // and we can overwrite that
    Assertions.assertThat(threadMap.setForCurrentThread("hello2"))
        .describedAs("value before the thread entry is changed")
        .isEqualTo(dynamicValue);

    // simulate a weak gc
    long threadId = threadMap.currentThreadId();
    threadMap.put(threadId, null);
    String updated = threadMap.getForCurrentThread();
    Assertions.assertThat(lost.get())
        .describedAs("lost count")
        .isEqualTo(1);
    Assertions.assertThat(updated)
        .describedAs("dynamically created thread map value")
        .startsWith("Entry for thread ID")
        .contains("(" + (c1 + 2) + ")");
  }

  /**
   * Assert that the value of a map entry is as expected.
   * Will trigger entry creation if the key is absent.
   * @param key key
   * @param val expected value
   */
  private void assertMapEntryEquals(int key, String val) {
    Assertions.assertThat(referenceMap.get(key))
        .describedAs("map enty of key %d", key)
        .isEqualTo(val);
  }

  /**
   * Assert that a map entry is present.
   * @param key key
   */
  private void assertMapContainsKey(int key) {
    Assertions.assertThat(referenceMap.containsKey(key))
        .describedAs("map entry of key %d should be present", key)
        .isTrue();
  }

  /**
   * Assert that a map entry is not present.
   * @param key key
   */
  private void assertMapDoesNotContainKey(int key) {
    Assertions.assertThat(referenceMap.containsKey(key))
        .describedAs("map enty of key %d should be absent", key)
        .isFalse();
  }

  /**
   * Assert map size.
   * @param size expected size.
   */
  private void assertMapSize(int size) {
    Assertions.assertThat(referenceMap.size())
        .describedAs("size of map %s", referenceMap)
        .isEqualTo(size);
  }

  /**
   * Assert prune returned the given count.
   * @param count expected count.
   */
  private void assertPruned(int count) {
    Assertions.assertThat(referenceMap.prune())
        .describedAs("number of entries pruned from map %s", referenceMap)
        .isEqualTo(count);
  }

  /**
   * Assert number of entries lost matches expected count.
   * @param count expected count.
   */
  private void assertLostCount(int count) {
    Assertions.assertThat(lostReferences)
        .describedAs("number of entries lost from map %s", referenceMap)
        .hasSize(count);
  }

  /**
   * Factory operation.
   * @param key map key
   * @return a string
   */
  private String factory(Integer key) {
    return String.format(FACTORY_STRING, key);
  }

}
