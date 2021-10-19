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
package org.apache.hadoop.hdfs.shortcircuit;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;

import javax.annotation.Nonnull;

/**
 * A shared memory segment used to implement short-circuit reads.
 */
public class ShortCircuitShm {
  private static final Logger LOG = LoggerFactory.getLogger(
      ShortCircuitShm.class);

  protected static final int BYTES_PER_SLOT = 64;

  private static final Unsafe unsafe = safetyDance();

  private static Unsafe safetyDance() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return (Unsafe)f.get(null);
    } catch (Throwable e) {
      LOG.error("failed to load misc.Unsafe", e);
    }
    return null;
  }

  /**
   * Calculate the usable size of a shared memory segment.
   * We round down to a multiple of the slot size and do some validation.
   *
   * @param stream The stream we're using.
   * @return       The usable size of the shared memory segment.
   */
  private static int getUsableLength(FileInputStream stream)
      throws IOException {
    int intSize = Ints.checkedCast(stream.getChannel().size());
    int slots = intSize / BYTES_PER_SLOT;
    if (slots == 0) {
      throw new IOException("size of shared memory segment was " +
          intSize + ", but that is not enough to hold even one slot.");
    }
    return slots * BYTES_PER_SLOT;
  }

  /**
   * Identifies a DfsClientShm.
   */
  public static class ShmId implements Comparable<ShmId> {
    private static final Random random = new Random();
    private final long hi;
    private final long lo;

    /**
     * Generate a random ShmId.
     *
     * We generate ShmIds randomly to prevent a malicious client from
     * successfully guessing one and using that to interfere with another
     * client.
     */
    public static ShmId createRandom() {
      return new ShmId(random.nextLong(), random.nextLong());
    }

    public ShmId(long hi, long lo) {
      this.hi = hi;
      this.lo = lo;
    }

    public long getHi() {
      return hi;
    }

    public long getLo() {
      return lo;
    }

    @Override
    public boolean equals(Object o) {
      if ((o == null) || (o.getClass() != this.getClass())) {
        return false;
      }
      ShmId other = (ShmId)o;
      return new EqualsBuilder().
          append(hi, other.hi).
          append(lo, other.lo).
          isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(this.hi).
          append(this.lo).
          toHashCode();
    }

    @Override
    public String toString() {
      return String.format("%016x%016x", hi, lo);
    }

    @Override
    public int compareTo(@Nonnull ShmId other) {
      return ComparisonChain.start().
          compare(hi, other.hi).
          compare(lo, other.lo).
          result();
    }
  }

  /**
   * Uniquely identifies a slot.
   */
  public static class SlotId {
    private final ShmId shmId;
    private final int slotIdx;

    public SlotId(ShmId shmId, int slotIdx) {
      this.shmId = shmId;
      this.slotIdx = slotIdx;
    }

    public ShmId getShmId() {
      return shmId;
    }

    public int getSlotIdx() {
      return slotIdx;
    }

    @Override
    public boolean equals(Object o) {
      if ((o == null) || (o.getClass() != this.getClass())) {
        return false;
      }
      SlotId other = (SlotId)o;
      return new EqualsBuilder().
          append(shmId, other.shmId).
          append(slotIdx, other.slotIdx).
          isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(this.shmId).
          append(this.slotIdx).
          toHashCode();
    }

    @Override
    public String toString() {
      return String.format("SlotId(%s:%d)", shmId.toString(), slotIdx);
    }
  }

  public class SlotIterator implements Iterator<Slot> {
    int slotIdx = -1;

    @Override
    public boolean hasNext() {
      synchronized (ShortCircuitShm.this) {
        return allocatedSlots.nextSetBit(slotIdx + 1) != -1;
      }
    }

    @Override
    public Slot next() {
      synchronized (ShortCircuitShm.this) {
        int nextSlotIdx = allocatedSlots.nextSetBit(slotIdx + 1);
        if (nextSlotIdx == -1) {
          throw new NoSuchElementException();
        }
        slotIdx = nextSlotIdx;
        return slots[nextSlotIdx];
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("SlotIterator " +
          "doesn't support removal");
    }
  }

  /**
   * A slot containing information about a replica.
   *
   * The format is:
   * word 0
   *   bit 0:32   Slot flags (see below).
   *   bit 33:63  Anchor count.
   * word 1:7
   *   Reserved for future use, such as statistics.
   *   Padding is also useful for avoiding false sharing.
   *
   * Little-endian versus big-endian is not relevant here since both the client
   * and the server reside on the same computer and use the same orientation.
   */
  public class Slot {
    /**
     * Flag indicating that the slot is valid.
     *
     * The DFSClient sets this flag when it allocates a new slot within one of
     * its shared memory regions.
     *
     * The DataNode clears this flag when the replica associated with this slot
     * is no longer valid.  The client itself also clears this flag when it
     * believes that the DataNode is no longer using this slot to communicate.
     */
    private static final long VALID_FLAG =          1L<<63;

    /**
     * Flag indicating that the slot can be anchored.
     */
    private static final long ANCHORABLE_FLAG =     1L<<62;

    /**
     * The slot address in memory.
     */
    private final long slotAddress;

    /**
     * BlockId of the block this slot is used for.
     */
    private final ExtendedBlockId blockId;

    Slot(long slotAddress, ExtendedBlockId blockId) {
      this.slotAddress = slotAddress;
      this.blockId = blockId;
    }

    /**
     * Get the short-circuit memory segment associated with this Slot.
     *
     * @return      The enclosing short-circuit memory segment.
     */
    public ShortCircuitShm getShm() {
      return ShortCircuitShm.this;
    }

    /**
     * Get the ExtendedBlockId associated with this slot.
     *
     * @return      The ExtendedBlockId of this slot.
     */
    public ExtendedBlockId getBlockId() {
      return blockId;
    }

    /**
     * Get the SlotId of this slot, containing both shmId and slotIdx.
     *
     * @return      The SlotId of this slot.
     */
    public SlotId getSlotId() {
      return new SlotId(getShmId(), getSlotIdx());
    }

    /**
     * Get the Slot index.
     *
     * @return      The index of this slot.
     */
    public int getSlotIdx() {
      return Ints.checkedCast(
          (slotAddress - baseAddress) / BYTES_PER_SLOT);
    }

    /**
     * Clear the slot.
     */
    void clear() {
      unsafe.putLongVolatile(null, this.slotAddress, 0);
    }

    private boolean isSet(long flag) {
      long prev = unsafe.getLongVolatile(null, this.slotAddress);
      return (prev & flag) != 0;
    }

    private void setFlag(long flag) {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & flag) != 0) {
          return;
        }
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev | flag));
    }

    private void clearFlag(long flag) {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & flag) == 0) {
          return;
        }
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev & (~flag)));
    }

    public boolean isValid() {
      return isSet(VALID_FLAG);
    }

    public void makeValid() {
      setFlag(VALID_FLAG);
    }

    public void makeInvalid() {
      clearFlag(VALID_FLAG);
    }

    public boolean isAnchorable() {
      return isSet(ANCHORABLE_FLAG);
    }

    public void makeAnchorable() {
      setFlag(ANCHORABLE_FLAG);
    }

    public void makeUnanchorable() {
      clearFlag(ANCHORABLE_FLAG);
    }

    public boolean isAnchored() {
      long prev = unsafe.getLongVolatile(null, this.slotAddress);
      // Slot is no longer valid.
      return (prev & VALID_FLAG) != 0 && ((prev & 0x7fffffff) != 0);
    }

    /**
     * Try to add an anchor for a given slot.
     *
     * When a slot is anchored, we know that the block it refers to is resident
     * in memory.
     *
     * @return          True if the slot is anchored.
     */
    public boolean addAnchor() {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & VALID_FLAG) == 0) {
          // Slot is no longer valid.
          return false;
        }
        if ((prev & ANCHORABLE_FLAG) == 0) {
          // Slot can't be anchored right now.
          return false;
        }
        if ((prev & 0x7fffffff) == 0x7fffffff) {
          // Too many other threads have anchored the slot (2 billion?)
          return false;
        }
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev + 1));
      return true;
    }

    /**
     * Remove an anchor for a given slot.
     */
    public void removeAnchor() {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        Preconditions.checkState((prev & 0x7fffffff) != 0,
            "Tried to remove anchor for slot " + slotAddress +", which was " +
            "not anchored.");
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev - 1));
    }

    @Override
    public String toString() {
      return "Slot(slotIdx=" + getSlotIdx() + ", shm=" + getShm() + ")";
    }
  }

  /**
   * ID for this SharedMemorySegment.
   */
  private final ShmId shmId;

  /**
   * The base address of the memory-mapped file.
   */
  private final long baseAddress;

  /**
   * The mmapped length of the shared memory segment
   */
  private final int mmappedLength;

  /**
   * The slots associated with this shared memory segment.
   * slot[i] contains the slot at offset i * BYTES_PER_SLOT,
   * or null if that slot is not allocated.
   */
  private final Slot slots[];

  /**
   * A bitset where each bit represents a slot which is in use.
   */
  private final BitSet allocatedSlots;

  /**
   * Create the ShortCircuitShm.
   *
   * @param shmId       The ID to use.
   * @param stream      The stream that we're going to use to create this
   *                    shared memory segment.
   *
   *                    Although this is a FileInputStream, we are going to
   *                    assume that the underlying file descriptor is writable
   *                    as well as readable. It would be more appropriate to use
   *                    a RandomAccessFile here, but that class does not have
   *                    any public accessor which returns a FileDescriptor,
   *                    unlike FileInputStream.
   */
  public ShortCircuitShm(ShmId shmId, FileInputStream stream)
        throws IOException {
    if (!NativeIO.isAvailable()) {
      throw new UnsupportedOperationException("NativeIO is not available.");
    }
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException(
          "DfsClientShm is not yet implemented for Windows.");
    }
    if (unsafe == null) {
      throw new UnsupportedOperationException(
          "can't use DfsClientShm because we failed to " +
          "load misc.Unsafe.");
    }
    this.shmId = shmId;
    this.mmappedLength = getUsableLength(stream);
    this.baseAddress = POSIX.mmap(stream.getFD(),
        POSIX.MMAP_PROT_READ | POSIX.MMAP_PROT_WRITE, true, mmappedLength);
    this.slots = new Slot[mmappedLength / BYTES_PER_SLOT];
    this.allocatedSlots = new BitSet(slots.length);
    LOG.trace("creating {}(shmId={}, mmappedLength={}, baseAddress={}, "
        + "slots.length={})", this.getClass().getSimpleName(), shmId,
        mmappedLength, String.format("%x", baseAddress), slots.length);
  }

  public final ShmId getShmId() {
    return shmId;
  }

  /**
   * Determine if this shared memory object is empty.
   *
   * @return    True if the shared memory object is empty.
   */
  synchronized final public boolean isEmpty() {
    return allocatedSlots.nextSetBit(0) == -1;
  }

  /**
   * Determine if this shared memory object is full.
   *
   * @return    True if the shared memory object is full.
   */
  synchronized final public boolean isFull() {
    return allocatedSlots.nextClearBit(0) >= slots.length;
  }

  /**
   * Calculate the base address of a slot.
   *
   * @param slotIdx   Index of the slot.
   * @return          The base address of the slot.
   */
  private long calculateSlotAddress(int slotIdx) {
    long offset = slotIdx;
    offset *= BYTES_PER_SLOT;
    return this.baseAddress + offset;
  }

  /**
   * Allocate a new slot and register it.
   *
   * This function chooses an empty slot, initializes it, and then returns
   * the relevant Slot object.
   *
   * @return    The new slot.
   */
  synchronized public final Slot allocAndRegisterSlot(
      ExtendedBlockId blockId) {
    int idx = allocatedSlots.nextClearBit(0);
    if (idx >= slots.length) {
      throw new RuntimeException(this + ": no more slots are available.");
    }
    allocatedSlots.set(idx, true);
    Slot slot = new Slot(calculateSlotAddress(idx), blockId);
    slot.clear();
    slot.makeValid();
    slots[idx] = slot;
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": allocAndRegisterSlot " + idx + ": allocatedSlots=" + allocatedSlots +
                  StringUtils.getStackTrace(Thread.currentThread()));
    }
    return slot;
  }

  synchronized public final Slot getSlot(int slotIdx)
      throws InvalidRequestException {
    if (!allocatedSlots.get(slotIdx)) {
      throw new InvalidRequestException(this + ": slot " + slotIdx +
          " does not exist.");
    }
    return slots[slotIdx];
  }

  /**
   * Register a slot.
   *
   * This function looks at a slot which has already been initialized (by
   * another process), and registers it with us.  Then, it returns the
   * relevant Slot object.
   *
   * @return    The slot.
   *
   * @throws InvalidRequestException
   *            If the slot index we're trying to allocate has not been
   *            initialized, or is already in use.
   */
  synchronized public final Slot registerSlot(int slotIdx,
      ExtendedBlockId blockId) throws InvalidRequestException {
    if (slotIdx < 0) {
      throw new InvalidRequestException(this + ": invalid negative slot " +
          "index " + slotIdx);
    }
    if (slotIdx >= slots.length) {
      throw new InvalidRequestException(this + ": invalid slot " +
          "index " + slotIdx);
    }
    if (allocatedSlots.get(slotIdx)) {
      throw new InvalidRequestException(this + ": slot " + slotIdx +
          " is already in use.");
    }
    Slot slot = new Slot(calculateSlotAddress(slotIdx), blockId);
    if (!slot.isValid()) {
      throw new InvalidRequestException(this + ": slot " + slotIdx +
          " is not marked as valid.");
    }
    slots[slotIdx] = slot;
    allocatedSlots.set(slotIdx, true);
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": registerSlot " + slotIdx + ": allocatedSlots=" + allocatedSlots +
                  StringUtils.getStackTrace(Thread.currentThread()));
    }
    return slot;
  }

  /**
   * Unregisters a slot.
   *
   * This doesn't alter the contents of the slot.  It just means
   *
   * @param slotIdx  Index of the slot to unregister.
   */
  synchronized public final void unregisterSlot(int slotIdx) {
    Preconditions.checkState(allocatedSlots.get(slotIdx),
        "tried to unregister slot " + slotIdx + ", which was not registered.");
    allocatedSlots.set(slotIdx, false);
    slots[slotIdx] = null;
    LOG.trace("{}: unregisterSlot {}", this, slotIdx);
  }

  /**
   * Iterate over all allocated slots.
   *
   * Note that this method isn't safe if
   *
   * @return        The slot iterator.
   */
  public SlotIterator slotIterator() {
    return new SlotIterator();
  }

  public void free() {
    try {
      POSIX.munmap(baseAddress, mmappedLength);
    } catch (IOException e) {
      LOG.warn(this + ": failed to munmap", e);
    }
    LOG.trace(this + ": freed");
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + shmId + ")";
  }
}
