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
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.CountersExceededException;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.Counters.GroupFactory;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup;
import org.apache.hadoop.mapreduce.counters.CounterGroupFactory.FrameworkGroupFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestCounters checks the sanity and recoverability of {@code Counters}
 */
public class TestCounters {
  enum myCounters {TEST1, TEST2};
  private static final long MAX_VALUE = 10;
  private static final Logger LOG = LoggerFactory.getLogger(TestCounters.class);
  
  static final Enum<?> FRAMEWORK_COUNTER = TaskCounter.CPU_MILLISECONDS;
  static final long FRAMEWORK_COUNTER_VALUE = 8;
  static final String FS_SCHEME = "HDFS";
  static final FileSystemCounter FS_COUNTER = FileSystemCounter.BYTES_READ;
  static final long FS_COUNTER_VALUE = 10;

  // Generates enum based counters
  private Counters getEnumCounters(Enum[] keys) {
    Counters counters = new Counters();
    for (Enum key : keys) {
      for (long i = 0; i < MAX_VALUE; ++i) {
        counters.incrCounter(key, i);
      }
    }
    return counters;
  }
  
  // Generate string based counters
  private Counters getEnumCounters(String[] gNames, String[] cNames) {
    Counters counters = new Counters();
    for (String gName : gNames) {
      for (String cName : cNames) {
        for (long i = 0; i < MAX_VALUE; ++i) {
          counters.incrCounter(gName, cName, i);
        }
      }
    }
    return counters;
  }
  
  /**
   * Test counter recovery
   */
  private void testCounter(Counters counter) throws ParseException {
    String compactEscapedString = counter.makeEscapedCompactString();
    assertFalse("compactEscapedString should not contain null",
                compactEscapedString.contains("null"));
    
    Counters recoveredCounter = 
      Counters.fromEscapedCompactString(compactEscapedString);
    // Check for recovery from string
    assertEquals("Recovered counter does not match on content", 
                 counter, recoveredCounter);
  }
  
  @Test
  public void testCounters() throws IOException {
    Enum[] keysWithResource = {TaskCounter.MAP_INPUT_RECORDS, 
                               TaskCounter.MAP_OUTPUT_BYTES};
    
    Enum[] keysWithoutResource = {myCounters.TEST1, myCounters.TEST2};
    
    String[] groups = {"group1", "group2", "group{}()[]"};
    String[] counters = {"counter1", "counter2", "counter{}()[]"};
    
    try {
      // I. Check enum counters that have resource bundler
      testCounter(getEnumCounters(keysWithResource));

      // II. Check enum counters that dont have resource bundler
      testCounter(getEnumCounters(keysWithoutResource));

      // III. Check string counters
      testCounter(getEnumCounters(groups, counters));
    } catch (ParseException pe) {
      throw new IOException(pe);
    }
  }
  
  /**
   * Verify counter value works
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCounterValue() {
    Counters counters = new Counters();
    final int NUMBER_TESTS = 100;
    final int NUMBER_INC = 10;
    final Random rand = new Random();
    for (int i = 0; i < NUMBER_TESTS; i++) {
      long initValue = rand.nextInt();
      long expectedValue = initValue;
      Counter counter = counters.findCounter("foo", "bar");
      counter.setValue(initValue);
      assertEquals("Counter value is not initialized correctly",
                   expectedValue, counter.getValue());
      for (int j = 0; j < NUMBER_INC; j++) {
        int incValue = rand.nextInt();
        counter.increment(incValue);
        expectedValue += incValue;
        assertEquals("Counter value is not incremented correctly",
                     expectedValue, counter.getValue());
      }
      expectedValue = rand.nextInt();
      counter.setValue(expectedValue);
      assertEquals("Counter value is not set correctly",
                   expectedValue, counter.getValue());
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testReadWithLegacyNames() {
    Counters counters = new Counters();
    counters.incrCounter(TaskCounter.MAP_INPUT_RECORDS, 1);
    counters.incrCounter(JobCounter.DATA_LOCAL_MAPS, 1);
    counters.findCounter("file", FileSystemCounter.BYTES_READ).increment(1);
    
    checkLegacyNames(counters);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testWriteWithLegacyNames() {
    Counters counters = new Counters();
    counters.incrCounter(Task.Counter.MAP_INPUT_RECORDS, 1);
    counters.incrCounter(JobInProgress.Counter.DATA_LOCAL_MAPS, 1);
    counters.findCounter("FileSystemCounters", "FILE_BYTES_READ").increment(1);
    
    checkLegacyNames(counters);
  }

  @SuppressWarnings("deprecation")
  private void checkLegacyNames(Counters counters) {
    assertEquals("New name", 1, counters.findCounter(
        TaskCounter.class.getName(), "MAP_INPUT_RECORDS").getValue());
    assertEquals("Legacy name", 1, counters.findCounter(
        "org.apache.hadoop.mapred.Task$Counter",
        "MAP_INPUT_RECORDS").getValue());
    assertEquals("Legacy enum", 1,
        counters.findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue());

    assertEquals("New name", 1, counters.findCounter(
        JobCounter.class.getName(), "DATA_LOCAL_MAPS").getValue());
    assertEquals("Legacy name", 1, counters.findCounter(
        "org.apache.hadoop.mapred.JobInProgress$Counter",
        "DATA_LOCAL_MAPS").getValue());
    assertEquals("Legacy enum", 1,
        counters.findCounter(JobInProgress.Counter.DATA_LOCAL_MAPS).getValue());

    assertEquals("New name", 1, counters.findCounter(
        FileSystemCounter.class.getName(), "FILE_BYTES_READ").getValue());
    assertEquals("New name and method", 1, counters.findCounter("file",
        FileSystemCounter.BYTES_READ).getValue());
    assertEquals("Legacy name", 1, counters.findCounter(
        "FileSystemCounters",
        "FILE_BYTES_READ").getValue());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testCounterIteratorConcurrency() {
    Counters counters = new Counters();
    counters.incrCounter("group1", "counter1", 1);
    Iterator<Group> iterator = counters.iterator();
    counters.incrCounter("group2", "counter2", 1);
    iterator.next();
  }
  
  
  @SuppressWarnings("deprecation")
  @Test
  public void testGroupIteratorConcurrency() {
    Counters counters = new Counters();
    counters.incrCounter("group1", "counter1", 1);
    Group group = counters.getGroup("group1");
    Iterator<Counter> iterator = group.iterator();
    counters.incrCounter("group1", "counter2", 1);
    iterator.next();
  }

  @Test
  public void testFileSystemGroupIteratorConcurrency() {
    Counters counters = new Counters();
    // create 2 filesystem counter groups
    counters.findCounter("fs1", FileSystemCounter.BYTES_READ).increment(1);
    counters.findCounter("fs2", FileSystemCounter.BYTES_READ).increment(1);
    
    // Iterate over the counters in this group while updating counters in
    // the group
    Group group = counters.getGroup(FileSystemCounter.class.getName());
    Iterator<Counter> iterator = group.iterator();
    counters.findCounter("fs3", FileSystemCounter.BYTES_READ).increment(1);
    assertTrue(iterator.hasNext());
    iterator.next();
    counters.findCounter("fs3", FileSystemCounter.BYTES_READ).increment(1);
    assertTrue(iterator.hasNext());
    iterator.next();
  }
  
  @Test
  public void testLegacyGetGroupNames() {
    Counters counters = new Counters();
    // create 2 filesystem counter groups
    counters.findCounter("fs1", FileSystemCounter.BYTES_READ).increment(1);
    counters.findCounter("fs2", FileSystemCounter.BYTES_READ).increment(1);
    counters.incrCounter("group1", "counter1", 1);
    
    HashSet<String> groups = new HashSet<String>(counters.getGroupNames());
    HashSet<String> expectedGroups = new HashSet<String>();
    expectedGroups.add("group1");
    expectedGroups.add("FileSystemCounters"); //Legacy Name
    expectedGroups.add("org.apache.hadoop.mapreduce.FileSystemCounter");

    assertEquals(expectedGroups, groups);
  }
  
  @Test
  public void testMakeCompactString() {
    final String GC1 = "group1.counter1:1";
    final String GC2 = "group2.counter2:3";
    Counters counters = new Counters();
    counters.incrCounter("group1", "counter1", 1);
    assertEquals("group1.counter1:1", counters.makeCompactString());
    counters.incrCounter("group2", "counter2", 3);
    String cs = counters.makeCompactString();
    assertTrue("Bad compact string",
        cs.equals(GC1 + ',' + GC2) || cs.equals(GC2 + ',' + GC1));
  }
  
  @Test
  public void testCounterLimits() {
    testMaxCountersLimits(new Counters());
    testMaxGroupsLimits(new Counters());
  }

  private void testMaxCountersLimits(final Counters counters) {
    for (int i = 0; i < org.apache.hadoop.mapred.Counters.MAX_COUNTER_LIMIT; ++i) {
      counters.findCounter("test", "test" + i);
    }
    setExpected(counters);
    shouldThrow(CountersExceededException.class, new Runnable() {
      public void run() {
        counters.findCounter("test", "bad");
      }
    });
    checkExpected(counters);
  }

  private void testMaxGroupsLimits(final Counters counters) {
    for (int i = 0; i < org.apache.hadoop.mapred.Counters.MAX_GROUP_LIMIT; ++i) {
      // assuming COUNTERS_MAX > GROUPS_MAX
      counters.findCounter("test" + i, "test");
    }
    setExpected(counters);
    shouldThrow(CountersExceededException.class, new Runnable() {
      public void run() {
        counters.findCounter("bad", "test");
      }
    });
    checkExpected(counters);
  }

  private void setExpected(Counters counters) {
    counters.findCounter(FRAMEWORK_COUNTER).setValue(FRAMEWORK_COUNTER_VALUE);
    counters.findCounter(FS_SCHEME, FS_COUNTER).setValue(FS_COUNTER_VALUE);
  }

  private void checkExpected(Counters counters) {
    assertEquals(FRAMEWORK_COUNTER_VALUE,
      counters.findCounter(FRAMEWORK_COUNTER).getValue());
    assertEquals(FS_COUNTER_VALUE, counters.findCounter(FS_SCHEME, FS_COUNTER)
      .getValue());
  }

  private void shouldThrow(Class<? extends Exception> ecls, Runnable runnable) {
    try {
      runnable.run();
    } catch (CountersExceededException e) {
      return;
    }
    Assert.fail("Should've thrown " + ecls.getSimpleName());
  }

  public static void main(String[] args) throws IOException {
    new TestCounters().testCounters();
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testFrameworkCounter() {
    GroupFactory groupFactory = new GroupFactoryForTest();
    FrameworkGroupFactory frameworkGroupFactory = 
        groupFactory.newFrameworkGroupFactory(JobCounter.class);
    Group group = (Group) frameworkGroupFactory.newGroup("JobCounter");
    
    FrameworkCounterGroup counterGroup = 
        (FrameworkCounterGroup) group.getUnderlyingGroup();
  
    org.apache.hadoop.mapreduce.Counter count1 = 
        counterGroup.findCounter(JobCounter.NUM_FAILED_MAPS.toString());
    Assert.assertNotNull(count1);
    
    // Verify no exception get thrown when finding an unknown counter
    org.apache.hadoop.mapreduce.Counter count2 = 
        counterGroup.findCounter("Unknown");
    Assert.assertNull(count2);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testTaskCounter() {
    GroupFactory groupFactory = new GroupFactoryForTest();
    FrameworkGroupFactory frameworkGroupFactory =
        groupFactory.newFrameworkGroupFactory(TaskCounter.class);
    Group group = (Group) frameworkGroupFactory.newGroup("TaskCounter");

    FrameworkCounterGroup counterGroup =
        (FrameworkCounterGroup) group.getUnderlyingGroup();

    org.apache.hadoop.mapreduce.Counter count1 =
        counterGroup.findCounter(
            TaskCounter.PHYSICAL_MEMORY_BYTES.toString());
    Assert.assertNotNull(count1);
    count1.increment(10);
    count1.increment(10);
    Assert.assertEquals(20, count1.getValue());

    // Verify no exception get thrown when finding an unknown counter
    org.apache.hadoop.mapreduce.Counter count2 =
        counterGroup.findCounter(
            TaskCounter.MAP_PHYSICAL_MEMORY_BYTES_MAX.toString());
    Assert.assertNotNull(count2);
    count2.increment(5);
    count2.increment(10);
    Assert.assertEquals(10, count2.getValue());
  }

  @Test
  public void testFilesystemCounter() {
    GroupFactory groupFactory = new GroupFactoryForTest();
    Group fsGroup = groupFactory.newFileSystemGroup();
  
    org.apache.hadoop.mapreduce.Counter count1 = 
        fsGroup.findCounter("ANY_BYTES_READ");
    Assert.assertNotNull(count1);
    
    // Verify no exception get thrown when finding an unknown counter
    org.apache.hadoop.mapreduce.Counter count2 = 
        fsGroup.findCounter("Unknown");
    Assert.assertNull(count2);
  }
  
}

  class GroupFactoryForTest extends GroupFactory {
    public <T extends Enum<T>>
        FrameworkGroupFactory<Group> newFrameworkGroupFactory(final Class<T> cls) {
      return super.newFrameworkGroupFactory(cls);
    }
    
    public Group newFileSystemGroup() {
      return super.newFileSystemGroup();
    }
    
  }


