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

import java.io.IOException;
import java.lang.Long;
import java.lang.RuntimeException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ByteArrayMemoryManager is a memory manager which keeps all memory on the Java
 * heap.  It is useful for testing, since it peforms validation of all memory
 * accesses and writes.  It also can be used if sun.misc.Unsafe is not
 * available, although its performance will be less than that of the off-heap
 * code.
 */
@Private
@Unstable
public class ByteArrayMemoryManager implements MemoryManager {
  static final Logger LOG =
      LoggerFactory.getLogger(ByteArrayMemoryManager.class);

  private final static long MAX_ADDRESS = 0x3fffffffffffffffL;

  private final TreeMap<Long, byte[]> buffers = new TreeMap<Long, byte[]>();

  private long curAddress = 1000;

  private final String name;

  public ByteArrayMemoryManager(String name) {
    this.name = name;
    LOG.debug("Created {}.", this);
  }

  @Override
  public synchronized void close() throws IOException {
    Iterator<Entry<Long, byte[]>> iter = buffers.entrySet().iterator();
    if (iter.hasNext()) {
      StringBuilder bld = new StringBuilder();
      Entry<Long, byte[]> entry = iter.next();
      bld.append(entryToString(entry));
      int numPrinted = 1;
      while (iter.hasNext()) {
        if (numPrinted >= 10) {
          bld.append("...");
          break;
        }
        bld.append(", ").append(entryToString(entry));
        numPrinted++;
      }
      throw new RuntimeException("There are still unfreed buffers.  " +
          bld.toString());
    }
    LOG.debug("Closed {}.", this);
  }

  private static String entryToString(Entry<Long, byte[]> entry) {
    StringBuilder bld = new StringBuilder();
    bld.append("Entry(base=0x").append(Long.toHexString(entry.getKey())).
        append(", len=0x").append(Long.toHexString(entry.getValue().length)).
        append(")");
    return bld.toString();
  }

  @Override
  public synchronized long allocate(long size) {
    if (curAddress + size > MAX_ADDRESS) {
      throw new RuntimeException("Cannot allocate any more memory.");
    }
    if (size > 0x7fffffff) {
      throw new RuntimeException("Attempted to allocate " + size +
          " bytes, but we cannot allocate a Java byte array with " +
          "more than 2^^31 entries.");
    }
    long addr = curAddress;
    curAddress += size;
    byte val[] = new byte[(int)size];
    buffers.put(Long.valueOf(addr), val);
    LOG.trace("Allocated Entry(base=0x{}, len=0x{})",
        Long.toHexString(addr), Long.toHexString(val.length));
    return addr;
  }

  @Override
  public synchronized long allocateZeroed(long size) {
    // Java byte arrays are always zeroed on construction.
    return allocate(size);
  }

  @Override
  public synchronized void free(long addr) {
    byte val[] =  buffers.remove(Long.valueOf(addr));
    if (val == null) {
      LOG.error("Attempted to free unallocated address 0x{}",
          Long.toHexString(addr));
    } else {
      LOG.trace("Freed Entry(base=0x{}, len=0x{})",
          Long.toHexString(addr), Long.toHexString(val.length));
    }
  }

  private synchronized Entry<Long, byte[]> getEntry(long addr, String op) {
    Entry<Long, byte[]> entry = buffers.floorEntry(Long.valueOf(addr));
    if (entry == null) {
      throw new RuntimeException(op + " unallocated address 0x" +
          Long.toHexString(addr));
    }
    return entry;
  }

  @Override
  public synchronized byte getByte(long addr) {
    Entry<Long, byte[]> entry = getEntry(addr, "Accessed");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 1 > arr.length) {
      throw new RuntimeException("Attempted to read unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated area " +
          "is " + entryToString(entry));
    }
    int i = (int)off;
    return arr[i];
  }

  @Override
  public void putByte(long addr, byte val) {
    Entry<Long, byte[]> entry = getEntry(addr, "Wrote to");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 1 > arr.length) {
      throw new RuntimeException("Attempted to write to unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated area " +
          "is " + entryToString(entry));
    }
    int i = (int)off;
    arr[i] = val;
  }

  @Override
  public synchronized short getShort(long addr) {
    Entry<Long, byte[]> entry = getEntry(addr, "Accessed");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 2 > arr.length) {
      throw new RuntimeException("Attempted to read unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated " +
          "area is " + entryToString(entry));
    }
    int i = (int)off;
    return (short)((arr[i + 0] & 0xff) << 8 |
        (arr[i + 1] & 0xff));
  }

  @Override
  public void putShort(long addr, short val) {
    Entry<Long, byte[]> entry = getEntry(addr, "Wrote to");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 2 > arr.length) {
      throw new RuntimeException("Attempted to write to unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated " +
          "area is " + entryToString(entry));
    }
    int i = (int)off;
    arr[i + 0] = (byte)((val >> 8) & 0xff);
    arr[i + 1] = (byte)(val & 0xff);
  }

  @Override
  public int getInt(long addr) {
    Entry<Long, byte[]> entry = getEntry(addr, "Accessed");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 4 > arr.length) {
      throw new RuntimeException("Attempted to read unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated " +
          "area is " + entryToString(entry));
    }
    int i = (int)off;
    return arr[i + 0] << 24 | 
           (arr[i + 1] & 0xff) << 16 |
           (arr[i + 2] & 0xff) << 8 |
           (arr[i + 3] & 0xff);
  }

  @Override
  public void putInt(long addr, int val) {
    Entry<Long, byte[]> entry = getEntry(addr, "Wrote to");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 4 > arr.length) {
      throw new RuntimeException("Attempted to write to unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated " +
          "area is " + entryToString(entry));
    }
    int i = (int)off;
    arr[i + 0] = (byte)((val >> 24) & 0xff);
    arr[i + 1] = (byte)((val >> 16) & 0xff);
    arr[i + 2] = (byte)((val >> 8) & 0xff);
    arr[i + 3] = (byte)(val & 0xff);
  }

  @Override
  public long getLong(long addr) {
    Entry<Long, byte[]> entry = getEntry(addr, "Accessed");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 8 > arr.length) {
      throw new RuntimeException("Attempted to read unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated " +
          "area is " + entryToString(entry));
    }
    int i = (int)off;
    return (arr[i + 0] & 0xffL) << 56 |
           (arr[i + 1] & 0xffL) << 48 |
           (arr[i + 2] & 0xffL) << 40 |
           (arr[i + 3] & 0xffL) << 32 |
           (arr[i + 4] & 0xffL) << 24 |
           (arr[i + 5] & 0xffL) << 16 |
           (arr[i + 6] & 0xffL) << 8 |
           (arr[i + 7] & 0xffL);
  }

  @Override
  public void putLong(long addr, long val) {
    Entry<Long, byte[]> entry = getEntry(addr, "Wrote to");
    long off = addr - entry.getKey();
    byte arr[] = entry.getValue();
    if (off + 8 > arr.length) {
      throw new RuntimeException("Attempted to write to unallocated memory " +
          "at 0x" + Long.toHexString(addr) + ".  Closest lower allocated " +
          "area is " + entryToString(entry));
    }
    int i = (int)off;
    arr[i + 0] = (byte)((val >> 56) & 0xff);
    arr[i + 1] = (byte)((val >> 48) & 0xff);
    arr[i + 2] = (byte)((val >> 40) & 0xff);
    arr[i + 3] = (byte)((val >> 32) & 0xff);
    arr[i + 4] = (byte)((val >> 24) & 0xff);
    arr[i + 5] = (byte)((val >> 16) & 0xff);
    arr[i + 6] = (byte)((val >> 8) & 0xff);
    arr[i + 7] = (byte)(val & 0xff);
  }

  @Override
  public String toString() {
    return "ByteArrayMemoryManager(" + name + ")";
  }
}
