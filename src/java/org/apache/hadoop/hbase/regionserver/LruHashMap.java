/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.io.HeapSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The LruHashMap is a memory-aware HashMap with a configurable maximum
 * memory footprint.
 * <p>
 * It maintains an ordered list of all entries in the map ordered by 
 * access time.  When space needs to be freed becase the maximum has been 
 * reached, or the application has asked to free memory, entries will be
 * evicted according to an LRU (least-recently-used) algorithm.  That is,
 * those entries which have not been accessed the longest will be evicted
 * first.
 * <p>
 * Both the Key and Value Objects used for this class must extend
 * <code>HeapSize</code> in order to track heap usage.
 * <p>
 * This class contains internal synchronization and is thread-safe.
 */
public class LruHashMap<K extends HeapSize, V extends HeapSize>
implements HeapSize, Map<K,V> {

  static final Log LOG = LogFactory.getLog(LruHashMap.class);
  
  /** The default size (in bytes) of the LRU */  
  private static final long DEFAULT_MAX_MEM_USAGE = 50000;
  /** The default capacity of the hash table */
  private static final int DEFAULT_INITIAL_CAPACITY = 16;
  /** The maxmum capacity of the hash table */
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  /** The default load factor to use */
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  
  /** Memory overhead of this Object (for HeapSize) */
  private static final int OVERHEAD = 5 * HeapSize.LONG + 2 * HeapSize.INT +
    2 * HeapSize.FLOAT + 3 * HeapSize.REFERENCE + 1 * HeapSize.ARRAY;
  
  /** Load factor allowed (usually 75%) */
  private final float loadFactor;
  /** Number of key/vals in the map */
  private int size;
  /** Size at which we grow hash */
  private int threshold;
  /** Entries in the map */
  private Entry [] entries;

  /** Pointer to least recently used entry */
  private Entry<K,V> headPtr;
  /** Pointer to most recently used entry */
  private Entry<K,V> tailPtr;

  /** Maximum memory usage of this map */
  private long memTotal = 0;
  /** Amount of available memory */
  private long memFree = 0;
  
  /** Number of successful (found) get() calls */
  private long hitCount = 0;
  /** Number of unsuccessful (not found) get() calls */
  private long missCount = 0;

  /**
   * Constructs a new, empty map with the specified initial capacity,
   * load factor, and maximum memory usage.
   *
   * @param initialCapacity the initial capacity
   * @param loadFactor the load factor
   * @param maxMemUsage the maximum total memory usage
   * @throws IllegalArgumentException if the initial capacity is less than one
   * @throws IllegalArgumentException if the initial capacity is greater than
   * the maximum capacity
   * @throws IllegalArgumentException if the load factor is <= 0
   * @throws IllegalArgumentException if the max memory usage is too small
   * to support the base overhead
   */
  public LruHashMap(int initialCapacity, float loadFactor,
  long maxMemUsage) {
    if (initialCapacity < 1) {
      throw new IllegalArgumentException("Initial capacity must be > 0");
    }
    if (initialCapacity > MAXIMUM_CAPACITY) {
      throw new IllegalArgumentException("Initial capacity is too large");
    }
    if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
      throw new IllegalArgumentException("Load factor must be > 0");
    }
    if (maxMemUsage <= (OVERHEAD + initialCapacity * HeapSize.REFERENCE)) {
      throw new IllegalArgumentException("Max memory usage too small to " +
      "support base overhead");
    }
  
    /** Find a power of 2 >= initialCapacity */
    int capacity = calculateCapacity(initialCapacity);
    this.loadFactor = loadFactor;
    this.threshold = calculateThreshold(capacity,loadFactor);
    this.entries = new Entry[capacity];
    this.memFree = maxMemUsage;
    this.memTotal = maxMemUsage;
    init();
  }

  /**
   * Constructs a new, empty map with the specified initial capacity and
   * load factor, and default maximum memory usage.
   *
   * @param initialCapacity the initial capacity
   * @param loadFactor the load factor
   * @throws IllegalArgumentException if the initial capacity is less than one
   * @throws IllegalArgumentException if the initial capacity is greater than
   * the maximum capacity
   * @throws IllegalArgumentException if the load factor is <= 0
   */
  public LruHashMap(int initialCapacity, float loadFactor) {
    this(initialCapacity, loadFactor, DEFAULT_MAX_MEM_USAGE);
  }
  
  /**
   * Constructs a new, empty map with the specified initial capacity and
   * with the default load factor and maximum memory usage.
   *
   * @param initialCapacity the initial capacity
   * @throws IllegalArgumentException if the initial capacity is less than one
   * @throws IllegalArgumentException if the initial capacity is greater than
   * the maximum capacity
   */
  public LruHashMap(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_MAX_MEM_USAGE);
  }

  /**
   * Constructs a new, empty map with the specified maximum memory usage
   * and with default initial capacity and load factor.
   *
   * @param maxMemUsage the maximum total memory usage
   * @throws IllegalArgumentException if the max memory usage is too small
   * to support the base overhead
   */
  public LruHashMap(long maxMemUsage) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR,
    maxMemUsage);
  }

  /**
   * Constructs a new, empty map with the default initial capacity, 
   * load factor and maximum memory usage.
   */
  public LruHashMap() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR,
    DEFAULT_MAX_MEM_USAGE);
  }
  
  //--------------------------------------------------------------------------
  /**
   * Get the currently available memory for this LRU in bytes.
   * This is (maxAllowed - currentlyUsed).
   *
   * @return currently available bytes
   */
  public long getMemFree() {
    return memFree;
  }
  
  /**
   * Get the maximum memory allowed for this LRU in bytes.
   *
   * @return maximum allowed bytes
   */
  public long getMemMax() {
    return memTotal;
  }
  
  /**
   * Get the currently used memory for this LRU in bytes.
   *
   * @return currently used memory in bytes
   */
  public long getMemUsed() {
    return (memTotal - memFree);
  }
  
  /**
   * Get the number of hits to the map.  This is the number of times
   * a call to get() returns a matched key.
   *
   * @return number of hits
   */
  public long getHitCount() {
    return hitCount;
  }
  
  /**
   * Get the number of misses to the map.  This is the number of times
   * a call to get() returns null.
   *
   * @return number of misses
   */
  public long getMissCount() {
    return missCount;
  }
  
  /**
   * Get the hit ratio.  This is the number of hits divided by the
   * total number of requests.
   *
   * @return hit ratio (double between 0 and 1)
   */
  public double getHitRatio() {
    return (double)((double)hitCount/
      ((double)(hitCount+missCount)));
  }
  
  /**
   * Free the requested amount of memory from the LRU map.
   *
   * This will do LRU eviction from the map until at least as much
   * memory as requested is freed.  This does not affect the maximum
   * memory usage parameter.
   *
   * @param requestedAmount memory to free from LRU in bytes
   * @return actual amount of memory freed in bytes
   */
  public synchronized long freeMemory(long requestedAmount) throws Exception {
    long minMemory = getMinimumUsage();
    if(requestedAmount > (getMemUsed() - getMinimumUsage())) {
      return clearAll();
    }
    long freedMemory = 0;
    while(freedMemory < requestedAmount) {
      freedMemory += evictFromLru();
    }
    return freedMemory;
  }
  
  /**
   * The total memory usage of this map
   *
   * @return memory usage of map in bytes
   */
  public long heapSize() {
    return (memTotal - memFree);
  }
  
  //--------------------------------------------------------------------------
  /**
   * Retrieves the value associated with the specified key.
   *
   * If an entry is found, it is updated in the LRU as the most recently
   * used (last to be evicted) entry in the map.
   *
   * @param key the key
   * @return the associated value, or null if none found
   * @throws NullPointerException if key is null
   */
  public synchronized V get(Object key) {
    checkKey((K)key);
    int hash = hash(key);
    int i = hashIndex(hash, entries.length);
    Entry<K,V> e = entries[i]; 
    while (true) {
      if (e == null) {
        missCount++;
        return null;
      }
      if (e.hash == hash && isEqual(key, e.key))  {
        // Hit!  Update position in LRU
        hitCount++;
        updateLru(e);
        return e.value;
      }
      e = e.next;
    }
  }

  /**
   * Insert a key-value mapping into the map.
   *
   * Entry will be inserted as the most recently used.
   *
   * Both the key and value are required to be Objects and must
   * implement the HeapSize interface.
   *
   * @param key the key
   * @param value the value
   * @return the value that was previously mapped to this key, null if none
   * @throws UnsupportedOperationException if either objects do not 
   * implement HeapSize
   * @throws NullPointerException if the key or value is null
   */
  public synchronized V put(K key, V value) {
    checkKey(key);
    checkValue(value);
    int hash = hash(key);
    int i = hashIndex(hash, entries.length);
    
    // For old values
    for (Entry<K,V> e = entries[i]; e != null; e = e.next) {
      if (e.hash == hash && isEqual(key, e.key)) {
        V oldValue = e.value;
        long memChange = e.replaceValue(value);
        checkAndFreeMemory(memChange);
        // If replacing an old value for this key, update in LRU
        updateLru(e);
        return oldValue;
      }
    }
    long memChange = addEntry(hash, key, value, i);
    checkAndFreeMemory(memChange);
    return null;
  }
  
  /**
   * Deletes the mapping for the specified key if it exists.
   *
   * @param key the key of the entry to be removed from the map
   * @return the value associated with the specified key, or null
   * if no mapping exists.
   */
  public synchronized V remove(Object key) {
    Entry<K,V> e = removeEntryForKey((K)key);
    if(e == null) return null;
    // Add freed memory back to available
    memFree += e.heapSize();
    return e.value;
  }

  /**
   * Gets the size (number of entries) of the map.
   *
   * @return size of the map
   */
  public int size() {
    return size;
  }

  /**
   * Checks whether the map is currently empty.
   *
   * @return true if size of map is zero
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Clears all entries from the map.
   *
   * This frees all entries, tracking memory usage along the way.
   * All references to entries are removed so they can be GC'd.
   */
  public synchronized void clear() {
    memFree += clearAll();
  }
  
  //--------------------------------------------------------------------------
  /**
   * Checks whether there is a value in the map for the specified key.
   *
   * Does not affect the LRU.
   *
   * @param key the key to check
   * @return true if the map contains a value for this key, false if not
   * @throws NullPointerException if the key is null
   */
  public synchronized boolean containsKey(Object key) {
    checkKey((K)key);
    int hash = hash(key);
    int i = hashIndex(hash, entries.length);
    Entry e = entries[i]; 
    while (e != null) {
      if (e.hash == hash && isEqual(key, e.key)) 
          return true;
      e = e.next;
    }
    return false;
  }

  /**
   * Checks whether this is a mapping which contains the specified value.
   * 
   * Does not affect the LRU.  This is an inefficient operation.
   *
   * @param value the value to check
   * @return true if the map contains an entry for this value, false
   * if not
   * @throws NullPointerException if the value is null
   */
  public synchronized boolean containsValue(Object value) {
    checkValue((V)value);
    Entry[] tab = entries;
    for (int i = 0; i < tab.length ; i++)
      for (Entry e = tab[i] ; e != null ; e = e.next)
          if (value.equals(e.value))
            return true;
    return false;
  }

  //--------------------------------------------------------------------------
  /**
   * Enforces key constraints.  Null keys are not permitted and key must
   * implement HeapSize.  It should not be necessary to verify the second
   * constraint because that's enforced on instantiation?
   *
   * Can add other constraints in the future.
   *
   * @param key the key
   * @throws NullPointerException if the key is null
   * @throws UnsupportedOperationException if the key class does not
   * implement the HeapSize interface
   */
  private void checkKey(K key) {
    if(key == null) {
      throw new NullPointerException("null keys are not allowed");
    }
  }
  
  /**
   * Enforces value constraints.  Null values are not permitted and value must
   * implement HeapSize.  It should not be necessary to verify the second
   * constraint because that's enforced on instantiation?
   *
   * Can add other contraints in the future.
   *
   * @param value the value
   * @throws NullPointerException if the value is null
   * @throws UnsupportedOperationException if the value class does not
   * implement the HeapSize interface
   */
  private void checkValue(V value) {
    if(value == null) {
      throw new NullPointerException("null values are not allowed");
    }
  }
  
  /**
   * Returns the minimum memory usage of the base map structure.
   *
   * @return baseline memory overhead of object in bytes
   */
  private long getMinimumUsage() {
    return OVERHEAD + (entries.length * HeapSize.REFERENCE);
  }
  
  //--------------------------------------------------------------------------
  /**
   * Evicts and frees based on LRU until at least as much memory as requested
   * is available.
   *
   * @param memNeeded the amount of memory needed in bytes
   */
  private void checkAndFreeMemory(long memNeeded) {
    while(memFree < memNeeded) {
      evictFromLru();
    }
    memFree -= memNeeded;
  }

  /**
   * Evicts based on LRU.  This removes all references and updates available
   * memory.
   *
   * @return amount of memory freed in bytes
   */
  private long evictFromLru() {
    long freed = headPtr.heapSize();
    memFree += freed;
    removeEntry(headPtr);
    return freed;
  }
  
  /**
   * Moves the specified entry to the most recently used slot of the
   * LRU.  This is called whenever an entry is fetched.
   *
   * @param e entry that was accessed
   */
  private void updateLru(Entry<K,V> e) {
    Entry<K,V> prev = e.getPrevPtr();
    Entry<K,V> next = e.getNextPtr();
    if(next != null) {
      if(prev != null) {
        prev.setNextPtr(next);
        next.setPrevPtr(prev);
      } else {
        headPtr = next;
        headPtr.setPrevPtr(null);
      }
      e.setNextPtr(null);
      e.setPrevPtr(tailPtr);
      tailPtr.setNextPtr(e);
      tailPtr = e;
    }
  }

  /**
   * Removes the specified entry from the map and LRU structure.
   *
   * @param entry entry to be removed
   */
  private void removeEntry(Entry<K,V> entry) {
    K k = entry.key;
    int hash = entry.hash;
    int i = hashIndex(hash, entries.length);
    Entry<K,V> prev = entries[i];
    Entry<K,V> e = prev;

    while (e != null) {
      Entry<K,V> next = e.next;
      if (e.hash == hash && isEqual(k, e.key)) {
          size--;
          if (prev == e) {
            entries[i] = next;
          } else {
            prev.next = next;
          }
          
          Entry<K,V> prevPtr = e.getPrevPtr();
          Entry<K,V> nextPtr = e.getNextPtr();
          
          if(prevPtr != null && nextPtr != null) {
            prevPtr.setNextPtr(nextPtr);
            nextPtr.setPrevPtr(prevPtr);
          } else if(prevPtr != null) {
            tailPtr = prevPtr;
            prevPtr.setNextPtr(null);
          } else if(nextPtr != null) {
            headPtr = nextPtr;
            nextPtr.setPrevPtr(null);
          }
          
          return;
      }
      prev = e;
      e = next;
    }
  }

  /**
   * Removes and returns the entry associated with the specified
   * key.
   *
   * @param key key of the entry to be deleted
   * @return entry that was removed, or null if none found
   */
  private Entry<K,V> removeEntryForKey(K key) {
    int hash = hash(key);
    int i = hashIndex(hash, entries.length);
    Entry<K,V> prev = entries[i];
    Entry<K,V> e = prev;

    while (e != null) {
      Entry<K,V> next = e.next;
      if (e.hash == hash && isEqual(key, e.key)) {
          size--;
          if (prev == e) {
            entries[i] = next;
          } else {
            prev.next = next;
          }
          
          // Updating LRU
          Entry<K,V> prevPtr = e.getPrevPtr();
          Entry<K,V> nextPtr = e.getNextPtr();
          if(prevPtr != null && nextPtr != null) {
            prevPtr.setNextPtr(nextPtr);
            nextPtr.setPrevPtr(prevPtr);
          } else if(prevPtr != null) {
            tailPtr = prevPtr;
            prevPtr.setNextPtr(null);
          } else if(nextPtr != null) {
            headPtr = nextPtr;
            nextPtr.setPrevPtr(null);
          }
          
          return e;
      }
      prev = e;
      e = next;
    }

    return e;
  }

 /**
  * Adds a new entry with the specified key, value, hash code, and
  * bucket index to the map.
  *
  * Also puts it in the bottom (most-recent) slot of the list and
  * checks to see if we need to grow the array.
  *
  * @param hash hash value of key
  * @param key the key
  * @param value the value
  * @param bucketIndex index into hash array to store this entry
  * @return the amount of heap size used to store the new entry
  */
  private long addEntry(int hash, K key, V value, int bucketIndex) {
    Entry<K,V> e = entries[bucketIndex];
    Entry<K,V> newE = new Entry<K,V>(hash, key, value, e, tailPtr);
    entries[bucketIndex] = newE;
    // add as most recently used in lru
    if (size == 0) {
      headPtr = newE;
      tailPtr = newE;
    } else {
      newE.setPrevPtr(tailPtr);
      tailPtr.setNextPtr(newE);
      tailPtr = newE;
    }
    // Grow table if we are past the threshold now
    if (size++ >= threshold) {
      growTable(2 * entries.length);
    }
    return newE.heapSize();
  }

  /**
   * Clears all the entries in the map.  Tracks the amount of memory being
   * freed along the way and returns the total.
   *
   * Cleans up all references to allow old entries to be GC'd.
   *
   * @return total memory freed in bytes
   */
  private long clearAll() {
    Entry cur;
    Entry prev;
    long freedMemory = 0;
    for(int i=0; i<entries.length; i++) {
      cur = entries[i];
      while(cur != null) {
        freedMemory += cur.heapSize();
        cur = cur.next;
      }
      entries[i] = null;
    }
    headPtr = null;
    tailPtr = null;
    size = 0;
    return freedMemory;
  }
  
  //--------------------------------------------------------------------------
  /**
   * Recreates the entire contents of the hashmap into a new array
   * with double the capacity.  This method is called when the number of
   * keys in the map reaches the current threshold.
   *
   * @param newCapacity the new size of the hash entries
   */
  private void growTable(int newCapacity) {
    Entry [] oldTable = entries;
    int oldCapacity = oldTable.length;
    
    // Do not allow growing the table beyond the max capacity
    if (oldCapacity == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    // Determine how much additional space will be required to grow the array
    long requiredSpace = (newCapacity - oldCapacity) * HeapSize.REFERENCE;
    
    // Verify/enforce we have sufficient memory to grow
    checkAndFreeMemory(requiredSpace);

    Entry [] newTable = new Entry[newCapacity];
    
    // Transfer existing entries to new hash table
    for(int i=0; i < oldCapacity; i++) {
      Entry<K,V> entry = oldTable[i];
      if(entry != null) {
        // Set to null for GC
        oldTable[i] = null;
        do {
          Entry<K,V> next = entry.next;
          int idx = hashIndex(entry.hash, newCapacity);
          entry.next = newTable[idx];
          newTable[idx] = entry;
          entry = next;
        } while(entry != null);
      }
    }

    entries = newTable;
    threshold = (int)(newCapacity * loadFactor);
  }

  /**
   * Gets the hash code for the specified key.
   * This implementation uses the additional hashing routine
   * from JDK 1.4.
   *
   * @param key the key to get a hash value for
   * @return the hash value
   */
  private int hash(Object key) {
    int h = key.hashCode();
    h += ~(h << 9);
    h ^=  (h >>> 14);
    h +=  (h << 4);
    h ^=  (h >>> 10);
    return h;
  }
  
  /**
   * Compares two objects for equality.  Method uses equals method and
   * assumes neither value is null.
   *
   * @param x the first value
   * @param y the second value
   * @return true if equal
   */
  private boolean isEqual(Object x, Object y) {
    return (x == y || x.equals(y));
  }
  
  /**
   * Determines the index into the current hash table for the specified
   * hashValue.
   *
   * @param hashValue the hash value
   * @param length the current number of hash buckets
   * @return the index of the current hash array to use
   */
  private int hashIndex(int hashValue, int length) {
    return hashValue & (length - 1);
  }

  /**
   * Calculates the capacity of the array backing the hash
   * by normalizing capacity to a power of 2 and enforcing
   * capacity limits.
   *
   * @param proposedCapacity the proposed capacity
   * @return the normalized capacity
   */
  private int calculateCapacity(int proposedCapacity) {
    int newCapacity = 1;
    if(proposedCapacity > MAXIMUM_CAPACITY) {
      newCapacity = MAXIMUM_CAPACITY;
    } else {
      while(newCapacity < proposedCapacity) {
        newCapacity <<= 1;
      }
      if(newCapacity > MAXIMUM_CAPACITY) {
        newCapacity = MAXIMUM_CAPACITY;
      }
    }
    return newCapacity;
  }
  
  /**
   * Calculates the threshold of the map given the capacity and load
   * factor.  Once the number of entries in the map grows to the
   * threshold we will double the size of the array.
   *
   * @param capacity the size of the array
   * @param factor the load factor of the hash
   */
  private int calculateThreshold(int capacity, float factor) {
    return (int)(capacity * factor);
  }

  /**
   * Set the initial heap usage of this class.  Includes class variable
   * overhead and the entry array.
   */
  private void init() {
    memFree -= OVERHEAD;
    memFree -= (entries.length * HeapSize.REFERENCE);
  }
  
  //--------------------------------------------------------------------------
  /**
   * Debugging function that returns a List sorted by access time.
   *
   * The order is oldest to newest (first in list is next to be evicted).
   *
   * @return Sorted list of entries
   */
  public List<Entry<K,V>> entryLruList() {
    List<Entry<K,V>> entryList = new ArrayList<Entry<K,V>>();
    Entry<K,V> entry = headPtr;
    while(entry != null) {
      entryList.add(entry);
      entry = entry.getNextPtr();
    }
    return entryList;
  }

  /**
   * Debugging function that returns a Set of all entries in the hash table.
   *
   * @return Set of entries in hash
   */
  public Set<Entry<K,V>> entryTableSet() {
    Set<Entry<K,V>> entrySet = new HashSet<Entry<K,V>>();
    Entry [] table = entries;
    for(int i=0;i<table.length;i++) {
      for(Entry e = table[i]; e != null; e = e.next) {
        entrySet.add(e);
      }
    }
    return entrySet;
  }
  
  /**
   * Get the head of the linked list (least recently used).
   *
   * @return head of linked list
   */
  public Entry getHeadPtr() {
    return headPtr;
  }
  
  /**
   * Get the tail of the linked list (most recently used).
   * 
   * @return tail of linked list
   */
  public Entry getTailPtr() {
    return tailPtr;
  }
  
  //--------------------------------------------------------------------------
  /**
   * To best optimize this class, some of the methods that are part of a
   * Map implementation are not supported.  This is primarily related
   * to being able to get Sets and Iterators of this map which require
   * significant overhead and code complexity to support and are
   * unnecessary for the requirements of this class.
   */
  
  /**
   * Intentionally unimplemented.
   */
  public Set<Map.Entry<K,V>> entrySet() {
    throw new UnsupportedOperationException(
    "entrySet() is intentionally unimplemented");
  }

  /**
   * Intentionally unimplemented.
   */
  public boolean equals(Object o) {
    throw new UnsupportedOperationException(
    "equals(Object) is intentionally unimplemented");
  }

  /**
   * Intentionally unimplemented.
   */
  public int hashCode() {
    throw new UnsupportedOperationException(
    "hashCode(Object) is intentionally unimplemented");
  }
  
  /**
   * Intentionally unimplemented.
   */
  public Set<K> keySet() {
    throw new UnsupportedOperationException(
    "keySet() is intentionally unimplemented");
  }
  
  /**
   * Intentionally unimplemented.
   */
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException(
    "putAll() is intentionally unimplemented");
  }
  
  /**
   * Intentionally unimplemented.
   */
  public Collection<V> values() {
    throw new UnsupportedOperationException(
    "values() is intentionally unimplemented");
  }

  //--------------------------------------------------------------------------
  /**
   * Entry to store key/value mappings.
   * <p>
   * Contains previous and next pointers for the doubly linked-list which is
   * used for LRU eviction.
   * <p>
   * Instantiations of this class are memory aware.  Both the key and value
   * classes used must also implement <code>HeapSize</code>.
   */
  protected static class Entry<K extends HeapSize, V extends HeapSize>
  implements Map.Entry<K,V>, HeapSize {
    /** The baseline overhead memory usage of this class */
    static final int OVERHEAD = 1 * HeapSize.LONG + 5 * HeapSize.REFERENCE + 
      2 * HeapSize.INT;
    
    /** The key */
    protected final K key;
    /** The value */
    protected V value;
    /** The hash value for this entries key */
    protected final int hash;
    /** The next entry in the hash chain (for collisions) */
    protected Entry<K,V> next;
    
    /** The previous entry in the LRU list (towards LRU) */
    protected Entry<K,V> prevPtr;
    /** The next entry in the LRU list (towards MRU) */
    protected Entry<K,V> nextPtr;
    
    /** The precomputed heap size of this entry */
    protected long heapSize;

    /**
     * Create a new entry.
     *
     * @param h the hash value of the key
     * @param k the key
     * @param v the value
     * @param nextChainPtr the next entry in the hash chain, null if none
     * @param prevLruPtr the previous entry in the LRU
     */
    Entry(int h, K k, V v, Entry<K,V> nextChainPtr, Entry<K,V> prevLruPtr) {
      value = v;
      next = nextChainPtr;
      key = k;
      hash = h;
      prevPtr = prevLruPtr;
      nextPtr = null;
      // Pre-compute heap size
      heapSize = OVERHEAD + k.heapSize() + v.heapSize();
    }

    /**
     * Get the key of this entry.
     *
     * @return the key associated with this entry
     */
    public K getKey() {
      return key;
    }

    /**
     * Get the value of this entry.
     *
     * @return the value currently associated with this entry
     */
    public V getValue() {
      return value;
    }
  
    /**
     * Set the value of this entry.
     *
     * It is not recommended to use this method when changing the value.
     * Rather, using <code>replaceValue</code> will return the difference
     * in heap usage between the previous and current values.
     *
     * @param newValue the new value to associate with this entry
     * @return the value previously associated with this entry
     */
    public V setValue(V newValue) {
      V oldValue = value;
      value = newValue;
      return oldValue;
    }
  
    /**
     * Replace the value of this entry.
     *
     * Computes and returns the difference in heap size when changing
     * the value associated with this entry.
     *
     * @param newValue the new value to associate with this entry
     * @return the change in heap usage of this entry in bytes
     */
    protected long replaceValue(V newValue) {
      long sizeDiff = newValue.heapSize() - value.heapSize();
      value = newValue;
      heapSize += sizeDiff;
      return sizeDiff;
    }
  
    /**
     * Returns true is the specified entry has the same key and the
     * same value as this entry.
     *
     * @param o entry to test against current
     * @return true is entries have equal key and value, false if no
     */
    public boolean equals(Object o) {
      if (!(o instanceof Map.Entry))
          return false;
      Map.Entry e = (Map.Entry)o;
      Object k1 = getKey();
      Object k2 = e.getKey();
      if (k1 == k2 || (k1 != null && k1.equals(k2))) {
          Object v1 = getValue();
          Object v2 = e.getValue();
          if (v1 == v2 || (v1 != null && v1.equals(v2))) 
            return true;
      }
      return false;
    }
    
    /** 
     * Returns the hash code of the entry by xor'ing the hash values
     * of the key and value of this entry.
     *
     * @return hash value of this entry
     */
    public int hashCode() {
      return (key.hashCode() ^ value.hashCode());
    }
  
    /**
     * Returns String representation of the entry in form "key=value"
     *
     * @return string value of entry
     */
    public String toString() {
      return getKey() + "=" + getValue();
    }

    //------------------------------------------------------------------------
    /**
     * Sets the previous pointer for the entry in the LRU.
     * @param prevPtr previous entry
     */
    protected void setPrevPtr(Entry<K,V> prevPtr){
      this.prevPtr = prevPtr;
    }
    
    /**
     * Returns the previous pointer for the entry in the LRU.
     * @return previous entry
     */
    protected Entry<K,V> getPrevPtr(){
      return prevPtr;
    }    
    
    /**
     * Sets the next pointer for the entry in the LRU.
     * @param nextPtr next entry
     */
    protected void setNextPtr(Entry<K,V> nextPtr){
      this.nextPtr = nextPtr;
    }
    
    /**
     * Returns the next pointer for the entry in teh LRU.
     * @return next entry
     */
    protected Entry<K,V> getNextPtr(){
      return nextPtr;
    }
    
    /**
     * Returns the pre-computed and "deep" size of the Entry
     * @return size of the entry in bytes
     */
    public long heapSize() {
      return heapSize;
    }
  }
}


