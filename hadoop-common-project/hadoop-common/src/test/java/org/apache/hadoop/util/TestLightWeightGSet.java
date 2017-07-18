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
package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.util.LightWeightGSet.LinkedElement;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Testing {@link LightWeightGSet} */
public class TestLightWeightGSet {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestLightWeightGSet.class);

  private static ArrayList<Integer> getRandomList(int length, int randomSeed) {
    Random random = new Random(randomSeed);
    ArrayList<Integer> list = new ArrayList<Integer>(length);
    for (int i = 0; i < length; i++) {
      list.add(random.nextInt());
    }
    return list;
  }
  
  private static class TestElement implements LightWeightGSet.LinkedElement {
    private final int val;
    private LinkedElement next;

    TestElement(int val) {
      this.val = val;
      this.next = null;
    }
    
    public int getVal() {
      return val;
    }

    @Override
    public void setNext(LinkedElement next) {
      this.next = next;
    }

    @Override
    public LinkedElement getNext() {
      return next;
    }
  }

  @Test(timeout=60000)
  public void testRemoveAllViaIterator() {
    ArrayList<Integer> list = getRandomList(100, 123);
    LightWeightGSet<TestElement, TestElement> set =
        new LightWeightGSet<TestElement, TestElement>(16);
    for (Integer i : list) {
      set.put(new TestElement(i));
    }
    for (Iterator<TestElement> iter = set.iterator();
        iter.hasNext(); ) {
      iter.next();
      iter.remove();
    }
    Assert.assertEquals(0, set.size());
  }

  @Test(timeout=60000)
  public void testRemoveSomeViaIterator() {
    ArrayList<Integer> list = getRandomList(100, 123);
    LightWeightGSet<TestElement, TestElement> set =
        new LightWeightGSet<TestElement, TestElement>(16);
    for (Integer i : list) {
      set.put(new TestElement(i));
    }
    long sum = 0;
    for (Iterator<TestElement> iter = set.iterator();
        iter.hasNext(); ) {
      sum += iter.next().getVal();
    }
    long mode = sum / set.size();
    LOG.info("Removing all elements above " + mode);
    for (Iterator<TestElement> iter = set.iterator();
        iter.hasNext(); ) {
      int item = iter.next().getVal();
      if (item > mode) {
        iter.remove();
      }
    }
    for (Iterator<TestElement> iter = set.iterator();
        iter.hasNext(); ) {
      Assert.assertTrue(iter.next().getVal() <= mode);
    }
  }
}
