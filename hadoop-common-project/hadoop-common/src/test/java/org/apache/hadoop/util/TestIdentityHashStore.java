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
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.util.IdentityHashStore;
import org.apache.hadoop.util.IdentityHashStore.Visitor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIdentityHashStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestIdentityHashStore.class.getName());

  private static class Key {
    private final String name;

    Key(String name) {
      this.name = name;
    }
    
    @Override
    public int hashCode() {
      throw new RuntimeException("should not be used!");
    }
    
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Key)) {
        return false;
      }
      Key other = (Key)o;
      return name.equals(other.name);
    }
  }

  @Test(timeout=60000)
  public void testStartingWithZeroCapacity() {
    IdentityHashStore<Key, Integer> store = 
        new IdentityHashStore<Key, Integer>(0);
    store.visitAll(new Visitor<Key, Integer>() {
      @Override
      public void accept(Key k, Integer v) {
        Assert.fail("found key " + k + " in empty IdentityHashStore.");
      }
    });
    Assert.assertTrue(store.isEmpty());
    final Key key1 = new Key("key1");
    Integer value1 = new Integer(100);
    store.put(key1, value1);
    Assert.assertTrue(!store.isEmpty());
    Assert.assertEquals(value1, store.get(key1));
    store.visitAll(new Visitor<Key, Integer>() {
      @Override
      public void accept(Key k, Integer v) {
        Assert.assertEquals(key1, k);
      }
    });
    Assert.assertEquals(value1, store.remove(key1));
    Assert.assertTrue(store.isEmpty());
  }
  
  @Test(timeout=60000)
  public void testDuplicateInserts() {
    IdentityHashStore<Key, Integer> store = 
        new IdentityHashStore<Key, Integer>(4);
    store.visitAll(new Visitor<Key, Integer>() {
      @Override
      public void accept(Key k, Integer v) {
        Assert.fail("found key " + k + " in empty IdentityHashStore.");
      }
    });
    Assert.assertTrue(store.isEmpty());
    Key key1 = new Key("key1");
    Integer value1 = new Integer(100);
    Integer value2 = new Integer(200);
    Integer value3 = new Integer(300);
    store.put(key1, value1);
    Key equalToKey1 = new Key("key1");

    // IdentityHashStore compares by object equality, not equals()
    Assert.assertNull(store.get(equalToKey1)); 

    Assert.assertTrue(!store.isEmpty());
    Assert.assertEquals(value1, store.get(key1));
    store.put(key1, value2);
    store.put(key1, value3);
    final List<Integer> allValues = new LinkedList<Integer>();
    store.visitAll(new Visitor<Key, Integer>() {
      @Override
      public void accept(Key k, Integer v) {
        allValues.add(v);
      }
    });
    Assert.assertEquals(3, allValues.size());
    for (int i = 0; i < 3; i++) {
      Integer value = store.remove(key1);
      Assert.assertTrue(allValues.remove(value));
    }
    Assert.assertNull(store.remove(key1));
    Assert.assertTrue(store.isEmpty());
  }
  
  @Test(timeout=60000)
  public void testAdditionsAndRemovals() {
    IdentityHashStore<Key, Integer> store = 
        new IdentityHashStore<Key, Integer>(0);
    final int NUM_KEYS = 1000;
    LOG.debug("generating " + NUM_KEYS + " keys");
    final List<Key> keys = new ArrayList<Key>(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; i++) {
      keys.add(new Key("key " + i));
    }
    for (int i = 0; i < NUM_KEYS; i++) {
      store.put(keys.get(i), i);
    }
    store.visitAll(new Visitor<Key, Integer>() {
      @Override
      public void accept(Key k, Integer v) {
        Assert.assertTrue(keys.contains(k));
      }
    });
    for (int i = 0; i < NUM_KEYS; i++) {
      Assert.assertEquals(Integer.valueOf(i),
          store.remove(keys.get(i)));
    }
    store.visitAll(new Visitor<Key, Integer>() {
      @Override
      public void accept(Key k, Integer v) {
        Assert.fail("expected all entries to be removed");
      }
    });
    Assert.assertTrue("expected the store to be " +
        "empty, but found " + store.numElements() + " elements.",
        store.isEmpty());
    Assert.assertEquals(1024, store.capacity());
  }
  
}
