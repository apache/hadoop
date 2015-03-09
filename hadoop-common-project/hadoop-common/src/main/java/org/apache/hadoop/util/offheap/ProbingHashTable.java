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

package org.apache.hadoop.util.offheap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A hash table which can be off-heaped and uses probing.<p/>
 *
 * Not thread-safe.  Requires external synchronization.<p/>
 *
 * Each entry must be stored in a slot which takes a fixed number of bytes.  We
 * assume that slots which are zeroed are empty.<p/>
 *
 * This hash table does not implement the Java collection interface, because we
 * want to avoid some of the limitations of that interface.  For example, we
 * want to be able to have more than 2^^32 entries and to be able to use a hash
 * function which is wider than 32 bits.<p/>
 *
 * This hash table uses linear probing rather than separate chaining to handle
 * hash collisions.  When we hit a collision when inserting, we put the new
 * element into the next open slot.<p/>
 *
 * When the hash table gets more than a certain percent full, we double the size
 * of the table.  This requires moving all existing entries.<p/>
 */
@Public
@Unstable
public class ProbingHashTable<K extends ProbingHashTable.Key,
      E extends ProbingHashTable.Entry<K>> implements Closeable {
  static final Logger LOG =
      LoggerFactory.getLogger(ProbingHashTable.class);

  /**
   * Adapts a given entry class to work with this hash table.<p/>
   *
   * Specifically, the Adaptor handles storing elements into slots,
   * retrieving them, clearing slots, and getting the hash code for entries.<p/>
   */
  public interface Adaptor<E> {
    /**
     * Get the slot size to use for this hash table.
     *
     * @return            How many bytes each slot is in the hash table.
     */
    int getSlotSize();

    /**
     * Load an entry from memory.
     *
     * @param addr        The address to use to create the entry.
     *
     * @return            null if the slot was empty; the entry, otherwise.
     */
    E load(long addr);

    /**
     * Store an entry to memory.
     *
     * @param e           The element to store to memory.
     * @param addr        The address to store the element to.
     */
    void store(E e, long addr);

    /**
     * Clear a slot.
     *
     * @param addr        The address to clear.
     */
    void clear(long addr);
  }

  public interface Key {
    /**
     * Get a 64-bit hash code for this key.
     */
    long longHash();

    /**
     * Determine if this key equals another key.
     */
    boolean equals(Object other);

    /**
     * Get a human-readable representation of this key.
     */
    String toString();
  }

  /**
   * An entry in the ProbingHashTable.
   */
  public interface Entry<K extends Key> {
    /**
     * Get the key for this entry.
     */
    K getKey();
  }

  /**
   * The minimum size to allow.
   */
  private static long MIN_SIZE = 4;

  /**
   * The name of this hash table.
   */
  private final String name;

  /**
   * The memory manager for this hash table.
   */
  private final MemoryManager mman;

  /**
   * The size of each slot in bytes.
   */
  private final int slotSize;

  /**
   * The adaptor to use.
   */
  private final Adaptor<E> adaptor;

  /**
   * The base address of the hash table.
   */
  private long base;

  /**
   * The current number of slots in the hash table.
   */
  private long numSlots;

  /**
   * The current number of entries in the hash table.
   */
  private long numEntries;

  /**
   * The maximum load factor for this hash table.
   */
  private float maxLoadFactor;

  /**
   * The number of entries we should double at.
   */
  private long expansionThreshold;

  public static long roundUpToPowerOf2(long i) {
    long r = 1;
    while (r < i) {
      r = r << 1;
    }
    return r;
  }

  /**
   * Create a new ProbingHashTable.
   *
   * @param name              The name of the ProbingHashTable.
   * @param mman              The memory manager to use.
   * @param adaptor           The entry factory to use.
   * @param initialSize       The initial size of the hash table (in number of
   *                              slots, not elements.)  Will be rounded up to a
   *                              power of 2.
   * @param maxLoadFactor     The maximum load factor to allow before doubling
   *                              the hash table size.
   */
  public ProbingHashTable(String name, MemoryManager mman, Adaptor<E> adaptor,
                        long initialSize, float maxLoadFactor) {
    this.name = name;
    this.mman = mman;
    this.slotSize = adaptor.getSlotSize();
    this.adaptor = adaptor;
    if (initialSize < MIN_SIZE) {
      initialSize = MIN_SIZE;
    }
    this.numSlots = roundUpToPowerOf2((long)(initialSize / maxLoadFactor));
    long allocLen = numSlots * slotSize;
    this.base = mman.allocateZeroed(allocLen);
    this.numEntries = 0;
    this.maxLoadFactor = maxLoadFactor;
    this.expansionThreshold = (long)(numSlots * maxLoadFactor);
    LOG.debug("Created ProbingHashTable(name={}, mman={}, slotSize={}, " +
          "adaptor={}, numSlots={}, base=0x{}, allocLen=0x{}," +
          "maxLoadFactor={}, expansionThreshold={})",
          name, mman.toString(), slotSize,
          adaptor.getClass().getCanonicalName(), numSlots,
          Long.toHexString(base), Long.toHexString(allocLen), maxLoadFactor,
          expansionThreshold);
    Preconditions.checkArgument(maxLoadFactor > 0.0f);
    Preconditions.checkArgument(maxLoadFactor < 1.0f);
  }

  /**
   * Frees the memory associated with this hash table and does error checking.
   */
  public void close() throws IOException {
    ProbingHashTableIterator iter = iterator();
    if (iter.hasNext()) {
      StringBuilder bld = new StringBuilder();
      K k = iter.next();
      bld.append(k.toString());
      int numPrinted = 1;
      while (iter.hasNext()) {
        if (numPrinted >= 10) {
          bld.append("...");
          break;
        }
        bld.append(", ").append(iter.next().toString());
        numPrinted++;
      }
      throw new RuntimeException("Attempted to close the hash table " +
          " before all entries were removed.  There are still " + numEntries +
          " entries remaining, including " + bld.toString());
    }
    free();
  }

  /**
   * Frees the memory associated with this hash table.
   */
  void free() throws IOException {
    if (this.base != 0) {
      LOG.debug("Freeing {}.", this);
      mman.free(this.base);
      this.base = 0;
    }
  }

  protected void finalize() throws Throwable {
    try {
      if (this.base != 0) {
        LOG.error("Hash table {} was never closed.", this);
        free();
      }
    } finally {
      super.finalize();
    }
  }

  private long getSlot(K key, long nSlots) {
    long hash = key.longHash();
    if (hash < 0) {
      hash = -hash;
    }
    return hash % nSlots;
  }

  private E getInternal(K key, boolean remove) {
    long originalSlot = getSlot(key, numSlots);
    long slot = originalSlot;
    long addr;
    E target = null;
    K targetKey = null;
    while (true) {
      addr = this.base + (slot * slotSize);
      target = adaptor.load(addr);
      if (target == null) {
        // By the compactness invariant, we're done.  See below for more
        // discussion.
        LOG.trace("{}: getInternal(key={}, remove={}) found nothing.",
            this, key, remove);
        return null;
      }
      targetKey = target.getKey();
      if (targetKey.equals(key)) {
        break;
      }
      slot++;
      if (slot == numSlots) {
        slot = 0;
      }
      if (slot == originalSlot) {
        LOG.trace("{}: getInternal(key={}, remove={}) found nothing",
            this, key, remove);
        return null;
      }
    }
    if (remove) {
      adaptor.clear(addr);
      numEntries--;
      maintainCompactness(slot);
    }
    LOG.trace("{}: getInternal(key={}, remove={}) found {}",
        this, key, remove, targetKey);
    return target;
  }

  /**
   * Maintain the compactness invariant.<p/>
   *
   * In order to avoid doing a full array search when looking for an element
   * that may not be in the hash table, we maintain a compactness invariant.
   * The compactness invariant states that if we start at slot N and continue
   * searching until we hit an empty slot, we will have searched all the
   * possible places where the element could be.  We maintain the compactness
   * invariant by doing a little bit of extra work each time we delete an entry.
   * Specifically, we search forwards from the deleted entry, moving any keys
   * that need to be moved to maintain the invariant.  We can stop searching
   * when we hit an empty slot.<p/>
   *
   * Although maintaining the compactness invariant is O(N) in the worst case,
   * it should be O(1) in the average case.  This is because the hash table is
   * half empty at all times.  Assuming good hash dispersion, on average every
   * other slot should be empty.  Therefore, the average number of entries we
   * move here should be less than 1.<p/>
   */
  private void maintainCompactness(long startSlot) {
    long slot = startSlot;
    while (true) {
      slot++;
      if (slot == numSlots) {
        slot = 0;
      }
      if (slot == startSlot) {
        return;
      }
      long addr = this.base + (slot * slotSize);
      E e = adaptor.load(addr);
      if (e == null) {
        return;
      }
      E prevE = putInternal(e, false);
      if (prevE != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: {} was already in the right place.",
              this, e.getKey());
        }
      } else {
        // The put didn't actually add anything, it just moved something.
        // So decrement numEntries to its previous value.
        numEntries--;
        adaptor.clear(addr);
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: moved {} to the right place.",
              this, e.getKey());
        }
      }
    }
  }

  public E remove(K key) {
    return getInternal(key, true);
  }

  public E get(K key) {
    return getInternal(key, false);
  }

  private void expandTable(long newNumSlots) {
    LOG.info("{}: Expanding table from {} slots to {}...",
        this, numSlots, newNumSlots);
    long newBase = mman.allocateZeroed(newNumSlots * slotSize);
    long oldNumSlots = this.numSlots;
    long oldExpansionThreshold = this.expansionThreshold;
    long oldBase = this.base;
    long oldNumEntries = this.numEntries;
    try {
      // Switch the hash table over to using the new memory region.
      long entriesRemaining = oldNumEntries;
      this.numSlots = newNumSlots;
      this.expansionThreshold = (long)(newNumSlots * maxLoadFactor);
      this.base = newBase;
      this.numEntries = 0;

      for (long slot = 0; slot < oldNumSlots; slot++) {
        long addr = oldBase + (slot * slotSize);
        E e = adaptor.load(addr);
        if (e != null) {
          E prevEntry = putInternal(e, false);
          if (prevEntry != null) {
            LOG.error("{}: Unexpected duplicate encountered when resizing " +
                    "hash table: entry {} duplicates {}.", this,
                e.getKey(), prevEntry.getKey()
            );
          }
          entriesRemaining--;
        }
      }
      if (entriesRemaining != 0) {
        LOG.error("{}: Unexpectedly failed to locate {} entries that we " +
                "thought we needed to move when resizing the hash table.",
            this, entriesRemaining
        );
      }
      LOG.info("{}: Finished expanding hash table from {} slots to {}.  " +
              "Moved {} keys.  Freed old memory base 0x{}.  Using new memory " +
              "base 0x{}.", this, oldNumSlots, numSlots, numEntries,
              Long.toHexString(oldBase), Long.toHexString(newBase));
    } catch (Throwable t) {
      // In general we should never get here, since the functions used
      // above should not throw exceptions.  But it's nice to be safe.
      LOG.error("{}: expanding failed!  Restoring old memory region.", this, t);

      // Switch back to using the old memory region.
      this.numSlots = oldNumSlots;
      this.expansionThreshold = oldExpansionThreshold;
      this.base = oldBase;
      this.numEntries = oldNumEntries;
      mman.free(newBase);
      throw new RuntimeException("Failed to expand " + this, t);
    }
    mman.free(oldBase);
  }

  /**
   * Expand the hash table if it would need to expand to hold another key.
   */
  private void expandTableIfNeeded() {
    if (numEntries > expansionThreshold) {
      expandTable(numSlots * 2L);
    }
  }

  /**
   * Put the entry into the hash table if there is no entry in the hash table
   * which is equivalent.
   *
   * @param putEntry        The entry to add if absent.
   * @param overwrite       If true, we will overwrite the entry which is equal
   *                          to putEntry (if there is one.)  If false, we will
   *                          simply return that entry, but not overwrite it.
   *
   * @return                The previous entry in the hash table that was equal
   *                          to the one we wanted to insert.  null if there
   *                          was no such entry.
   */
  private E putInternal(E putEntry, boolean overwrite) {
    long slot = getSlot(putEntry.getKey(), numSlots);
    K putKey = putEntry.getKey();

    while (true) {
      long addr = this.base + (slot * slotSize);
      E e = adaptor.load(addr);
      if (e == null) {
        adaptor.store(putEntry, addr);
        numEntries++;
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: stored {} into slot {} (addr 0x{})",
              this, putKey, slot, Long.toHexString(addr));
        }
        return null;
      }
      K k = e.getKey();
      if (k.equals(putKey)) {
        if (!overwrite) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("{}: could not store {} because we found an " +
                "equivalent key {} in slot {} (addr 0x{})",
                this, putKey, k, slot, Long.toHexString(addr));
          }
          return e;
        }
        // Overwrite the existing entry.
        adaptor.store(putEntry, addr);
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: stored {} by overwriting the equivalent key {} " +
              "in slot {} (addr 0x{})", this, putKey, k, slot,
              Long.toHexString(addr));
        }
        return e;
      }
      slot++;
      if (slot == numSlots) {
        slot = 0;
      }
    }
  }

  /**
   * Put the entry into the hash table if there is no entry in the hash table
   * which is equivalent.
   *
   * @param putEntry    The entry to add.
   *
   * @return            Null if the element was inserted.
   *                      Otherwise, returns the previous element that compares
   *                      to be the same as the one we unsuccessfully tried to
   *                      add.
   */
  public E putIfAbsent(E putEntry) {
    expandTableIfNeeded(); // call this first in case it fails (very unlikely)
    return putInternal(putEntry, false);
  }

  /**
   * Put an entry into the hash table, overwriting any existing element
   * which is equivalent.
   *
   * @param putEntry    The entry to add.
   *
   * @return            null if there was no element in the table which was
   *                      equivalent... the existing element which was
   *                      equivalent, otherwise.  The existing element will
   *                      be removed.
   */
  public E put(E putEntry) {
    expandTableIfNeeded(); // call this first in case it fails (very unlikely)
    return putInternal(putEntry, true);
  }

  /**
   * Returns the current number of slots in the hash table.
   */
  public long numSlots() {
    return numSlots;
  }

  /**
   * Returns the size of the table.
   */
  public long size() {
    return numEntries;
  }

  /**
   * Returns true if the table is empty.
   */
  public boolean isEmpty() {
    return numEntries == 0;
  }

  /**
   * An iterator for the ProbingHashTable.<p/>
   *
   * Since ProbingHashTable has no internal synchronization, you are responsible
   * for ensuring that there are no concurrent write operations on the hash
   * table while an iterator function is being called.  The easiest way to do
   * this is with external locking.<p/>
   *
   * You can still perform write operations after creating this iterator
   * without invalidating the iterator object.  There are a few caveats:<p/>
   * 1. Keys inserted after the iterator was created may or may not be
   *    returned by the iterator.<p/>
   * 2. If the hash table is enlarged due to adding more keys, this iterator
   *    may return keys more than once, and return some keys not at all.<p/>
   */
  private class ProbingHashTableIterator implements Iterator<K> {
    private long slotId = 0;
    private K curKey;

    private boolean refillCurKey() {
      while (slotId < ProbingHashTable.this.numSlots) {
        long addr = base + (slotId * slotSize);
        E e = adaptor.load(addr);
        slotId++;
        if (e != null) {
          curKey = e.getKey();
          if (LOG.isTraceEnabled()) {
            LOG.trace("{}: iterator found another key {} at slot {} " +
                  "(address 0x{})", ProbingHashTable.this.toString(), curKey,
                  (slotId - 1), Long.toHexString(addr));
          }
          return true;
        }
      }
      LOG.trace("{}: no more keys to iterate over after reading all {} " +
                "slots.", ProbingHashTable.this.toString(), slotId);
      // Set slotId to Long.MAX_VALUE so that even if the hash table enlarges
      // in the future, this iterator will continue to be at the end.
      slotId = Long.MAX_VALUE;
      return false;
    }

    @Override
    public boolean hasNext() {
      if (curKey != null) {
        return true;
      }
      return refillCurKey();
    }

    @Override
    public K next() {
      if (curKey == null) {
        if (!refillCurKey()) {
          throw new IllegalStateException();
        }
      }
      K key = curKey;
      curKey = null;
      return key;
    }

    @Override
    public void remove() {
      if (curKey == null) {
        throw new IllegalStateException();
      }
      K key = curKey;
      curKey = null;
      if (ProbingHashTable.this.remove(key) == null) {
        throw new NoSuchElementException("No such element as " +
            key.toString());
      }
    }
  }

  public ProbingHashTableIterator iterator() {
    return new ProbingHashTableIterator();
  }

  @Override
  public String toString() {
    return "ProbingHashTable(" + name + ")";
  }
}
