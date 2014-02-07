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
package org.apache.hadoop.hdfs.client;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.apache.hadoop.util.CloseableReferenceCount;
import org.apache.hadoop.util.Shell;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import sun.misc.Unsafe;

public class ShortCircuitSharedMemorySegment implements Closeable {
  private static final Log LOG =
    LogFactory.getLog(ShortCircuitSharedMemorySegment.class);

  private static final int BYTES_PER_SLOT = 64;

  private static final Unsafe unsafe;

  static {
    Unsafe theUnsafe = null;
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      theUnsafe = (Unsafe)f.get(null);
    } catch (Throwable e) {
      LOG.error("failed to load misc.Unsafe", e);
    }
    unsafe = theUnsafe;
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
  public class Slot implements Closeable {
    /**
     * Flag indicating that the slot is in use.
     */
    private static final long SLOT_IN_USE_FLAG =    1L<<63;

    /**
     * Flag indicating that the slot can be anchored.
     */
    private static final long ANCHORABLE_FLAG =     1L<<62;

    private long slotAddress;

    Slot(long slotAddress) {
      this.slotAddress = slotAddress;
    }

    /**
     * Make a given slot anchorable.
     */
    public void makeAnchorable() {
      Preconditions.checkState(slotAddress != 0,
          "Called makeAnchorable on a slot that was closed.");
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & ANCHORABLE_FLAG) != 0) {
          return;
        }
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev | ANCHORABLE_FLAG));
    }

    /**
     * Make a given slot unanchorable.
     */
    public void makeUnanchorable() {
      Preconditions.checkState(slotAddress != 0,
          "Called makeUnanchorable on a slot that was closed.");
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & ANCHORABLE_FLAG) == 0) {
          return;
        }
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev & (~ANCHORABLE_FLAG)));
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
        if ((prev & 0x7fffffff) == 0x7fffffff) {
          // Too many other threads have anchored the slot (2 billion?)
          return false;
        }
        if ((prev & ANCHORABLE_FLAG) == 0) {
          // Slot can't be anchored right now.
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

    /**
     * @return      The index of this slot.
     */
    public int getIndex() {
      Preconditions.checkState(slotAddress != 0);
      return Ints.checkedCast(
          (slotAddress - baseAddress) / BYTES_PER_SLOT);
    }

    @Override
    public void close() throws IOException {
      if (slotAddress == 0) return;
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        Preconditions.checkState((prev & SLOT_IN_USE_FLAG) != 0,
            "tried to close slot that wasn't open");
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, 0));
      slotAddress = 0;
      if (ShortCircuitSharedMemorySegment.this.refCount.unreference()) {
        ShortCircuitSharedMemorySegment.this.free();
      }
    }
  }

  /**
   * The stream that we're going to use to create this shared memory segment.
   *
   * Although this is a FileInputStream, we are going to assume that the
   * underlying file descriptor is writable as well as readable.
   * It would be more appropriate to use a RandomAccessFile here, but that class
   * does not have any public accessor which returns a FileDescriptor, unlike
   * FileInputStream.
   */
  private final FileInputStream stream;

  /**
   * Length of the shared memory segment.
   */
  private final int length;

  /**
   * The base address of the memory-mapped file.
   */
  private final long baseAddress;

  /**
   * Reference count and 'closed' status.
   */
  private final CloseableReferenceCount refCount = new CloseableReferenceCount();

  public ShortCircuitSharedMemorySegment(FileInputStream stream)
        throws IOException {
    if (!NativeIO.isAvailable()) {
      throw new UnsupportedOperationException("NativeIO is not available.");
    }
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException(
          "ShortCircuitSharedMemorySegment is not yet implemented " +
          "for Windows.");
    }
    if (unsafe == null) {
      throw new UnsupportedOperationException(
          "can't use ShortCircuitSharedMemorySegment because we failed to " +
          "load misc.Unsafe.");
    }
    this.refCount.reference();
    this.stream = stream;
    this.length = getEffectiveLength(stream);
    this.baseAddress = POSIX.mmap(this.stream.getFD(), 
      POSIX.MMAP_PROT_READ | POSIX.MMAP_PROT_WRITE, true, this.length);
  }

  /**
   * Calculate the effective usable size of the shared memory segment.
   * We round down to a multiple of the slot size and do some validation.
   *
   * @param stream The stream we're using.
   * @return       The effective usable size of the shared memory segment.
   */
  private static int getEffectiveLength(FileInputStream stream)
      throws IOException {
    int intSize = Ints.checkedCast(stream.getChannel().size());
    int slots = intSize / BYTES_PER_SLOT;
    Preconditions.checkState(slots > 0, "size of shared memory segment was " +
        intSize + ", but that is not enough to hold even one slot.");
    return slots * BYTES_PER_SLOT;
  }

  private boolean allocateSlot(long address) {
    long prev;
    do {
      prev = unsafe.getLongVolatile(null, address);
      if ((prev & Slot.SLOT_IN_USE_FLAG) != 0) {
        return false;
      }
    } while (!unsafe.compareAndSwapLong(null, address,
                prev, prev | Slot.SLOT_IN_USE_FLAG));
    return true;
  }

  /**
   * Allocate a new Slot in this shared memory segment.
   *
   * @return        A newly allocated Slot, or null if there were no available
   *                slots.
   */
  public Slot allocateNextSlot() throws IOException {
    ShortCircuitSharedMemorySegment.this.refCount.reference();
    Slot slot = null;
    try {
      final int numSlots = length / BYTES_PER_SLOT;
      for (int i = 0; i < numSlots; i++) {
        long address = this.baseAddress + (i * BYTES_PER_SLOT);
        if (allocateSlot(address)) {
          slot = new Slot(address);
          break;
        }
      }
    } finally {
      if (slot == null) {
        if (refCount.unreference()) {
          free();
        }
      }
    }
    return slot;
  }

  @Override
  public void close() throws IOException {
    refCount.setClosed();
    if (refCount.unreference()) {
      free();
    }
  }

  void free() throws IOException {
    IOUtils.cleanup(LOG, stream);
    POSIX.munmap(baseAddress, length);
  }
}
