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
import java.lang.reflect.Field;
import java.lang.Throwable;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/**
 * NativeMemoryManager is a memory manager which uses sun.misc.Unsafe to
 * allocate memory off-heap.  This memory will be allocated using the current
 * platform's equivalent of malloc().
 */
@Private
@Unstable
public class NativeMemoryManager implements MemoryManager {
  static final Logger LOG =
      LoggerFactory.getLogger(NativeMemoryManager.class);

  private final static Unsafe unsafe;

  private final static String loadingFailureReason;

  static {
    Unsafe myUnsafe = null;
    String myLoadingFailureReason = null;
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      myUnsafe = (Unsafe)f.get(null);
    } catch (Throwable e) {
      myLoadingFailureReason = e.getMessage();
    } finally {
      unsafe = myUnsafe;
      loadingFailureReason = myLoadingFailureReason;
    }
  }

  private final String name;

  public static boolean isAvailable() {
    return loadingFailureReason == null;
  }

  public NativeMemoryManager(String name) {
    if (loadingFailureReason != null) {
      LOG.error("Failed to load sun.misc.Unsafe: " + loadingFailureReason);
      throw new RuntimeException("Failed to load sun.misc.Unsafe: " +
          loadingFailureReason);
    }
    this.name = name;
    LOG.debug("Created {}.", this);
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
    LOG.debug("Closed {}.", this);
  }

  @Override
  public long allocate(long size) {
    return unsafe.allocateMemory(size);
  }

  @Override
  public long allocateZeroed(long size) {
    long addr = unsafe.allocateMemory(size);
    unsafe.setMemory(addr, size, (byte)0);
    return addr;
  }

  @Override
  public void free(long addr) {
    unsafe.freeMemory(addr);
  }

  @Override
  public byte getByte(long addr) {
    return unsafe.getByte(null, addr);
  }

  @Override
  public void putByte(long addr, byte val) {
    unsafe.putByte(null, addr, val);
  }

  @Override
  public short getShort(long addr) {
    return unsafe.getShort(null, addr);
  }

  @Override
  public void putShort(long addr, short val) {
    unsafe.putShort(addr, val);
  }

    @Override
  public int getInt(long addr) {
    return unsafe.getInt(null, addr);
  }

  @Override
  public void putInt(long addr, int val) {
    unsafe.putInt(null, addr, val);
  }

  @Override
  public long getLong(long addr) {
    return unsafe.getLong(null, addr);
  }

  @Override
  public void putLong(long addr, long val) {
    unsafe.putLong(null, addr, val);
  }

  @Override
  public String toString() {
    return "NativeMemoryManager(" + name + ")";
  }
}
