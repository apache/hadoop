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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Simple tests for utility class Lists.
 */
public class TestLists {

  @Test
  public void testAddToEmptyArrayList() {
    List<String> list = Lists.newArrayList();
    list.add("record1");
    Assert.assertEquals(1, list.size());
    Assert.assertEquals("record1", list.get(0));
  }

  @Test
  public void testAddToEmptyLinkedList() {
    List<String> list = Lists.newLinkedList();
    list.add("record1");
    Assert.assertEquals(1, list.size());
    Assert.assertEquals("record1", list.get(0));
  }

  @Test
  public void testVarArgArrayLists() {
    List<String> list = Lists.newArrayList("record1", "record2", "record3");
    list.add("record4");
    Assert.assertEquals(4, list.size());
    Assert.assertEquals("record1", list.get(0));
    Assert.assertEquals("record2", list.get(1));
    Assert.assertEquals("record3", list.get(2));
    Assert.assertEquals("record4", list.get(3));
  }

  @Test
  public void testItrArrayLists() {
    Set<String> set = new HashSet<>();
    set.add("record1");
    set.add("record2");
    set.add("record3");
    List<String> list = Lists.newArrayList(set);
    list.add("record4");
    Assert.assertEquals(4, list.size());
  }

  @Test
  public void testItrLinkedLists() {
    Set<String> set = new HashSet<>();
    set.add("record1");
    set.add("record2");
    set.add("record3");
    List<String> list = Lists.newLinkedList(set);
    list.add("record4");
    Assert.assertEquals(4, list.size());
  }

  @Test
  public void testListsPartition() {
    List<String> list = new ArrayList<>();
    list.add("a");
    list.add("b");
    list.add("c");
    list.add("d");
    list.add("e");
    List<List<String>> res = Lists.
            partition(list, 2);
    Assertions.assertThat(res)
            .describedAs("Number of partitions post partition")
            .hasSize(3);
    Assertions.assertThat(res.get(0))
            .describedAs("Number of elements in first partition")
            .hasSize(2);
    Assertions.assertThat(res.get(2))
            .describedAs("Number of elements in last partition")
            .hasSize(1);

    List<List<String>> res2 = Lists.
            partition(list, 1);
    Assertions.assertThat(res2)
            .describedAs("Number of partitions post partition")
            .hasSize(5);
    Assertions.assertThat(res2.get(0))
            .describedAs("Number of elements in first partition")
            .hasSize(1);
    Assertions.assertThat(res2.get(4))
            .describedAs("Number of elements in last partition")
            .hasSize(1);

    List<List<String>> res3 = Lists.
            partition(list, 6);
    Assertions.assertThat(res3)
            .describedAs("Number of partitions post partition")
            .hasSize(1);
    Assertions.assertThat(res3.get(0))
            .describedAs("Number of elements in first partition")
            .hasSize(5);
  }

  @Test
  public void testArrayListWithSize() {
    List<String> list = Lists.newArrayListWithCapacity(3);
    list.add("record1");
    list.add("record2");
    list.add("record3");
    Assert.assertEquals(3, list.size());
    Assert.assertEquals("record1", list.get(0));
    Assert.assertEquals("record2", list.get(1));
    Assert.assertEquals("record3", list.get(2));
    list = Lists.newArrayListWithCapacity(3);
    list.add("record1");
    list.add("record2");
    list.add("record3");
    Assert.assertEquals(3, list.size());
    Assert.assertEquals("record1", list.get(0));
    Assert.assertEquals("record2", list.get(1));
    Assert.assertEquals("record3", list.get(2));
  }

}
