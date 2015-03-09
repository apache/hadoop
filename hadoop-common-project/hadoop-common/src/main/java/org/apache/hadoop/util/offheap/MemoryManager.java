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
import java.lang.Class;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allocates memory which may be off-heap.
 *
 * MemoryManager objects are thread-safe.  They can be used by multiple threads
 * at once without additional synchronization.
 */
@Private
@Unstable
public interface MemoryManager extends Closeable {
  /**
   * Allocate a memory region.  Will never return 0.
   */
  long allocate(long size);

  /**
   * Allocate a zeroed memory region.  Will never return 0.
   */
  long allocateZeroed(long size);

  /**
   * Free memory.
   */
  void free(long addr);

  byte getByte(long addr);

  void putByte(long addr, byte val);

  short getShort(long addr);

  void putShort(long addr, short val);

  int getInt(long addr);

  void putInt(long addr, int val);

  long getLong(long addr);

  void putLong(long addr, long val);

  String toString();

  public static class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);

    /**
     * Create a MemoryManager from a Configuration.
     *
     * @param conf      The Configuration
     *
     * @return          The MemoryManager.
     */
    public static MemoryManager create(String name, Configuration conf) {
      String memoryManagerKey = conf.get(
          CommonConfigurationKeys.HADOOP_MEMORY_MANAGER_KEY,
          CommonConfigurationKeys.HADOOP_MEMORY_MANAGER_DEFAULT);
      if (memoryManagerKey == null) {
        memoryManagerKey = NativeMemoryManager.class.getCanonicalName();
      }
      Class<? extends MemoryManager> clazz =
          (Class<? extends MemoryManager>)conf.
              getClassByNameOrNull(memoryManagerKey);
      if (clazz == null) {
        LOG.error("Unable to locate {}: falling back on {}.",
            memoryManagerKey, ByteArrayMemoryManager.class.getCanonicalName());
      } else if (clazz != ByteArrayMemoryManager.class) {
        try {
          return clazz.getConstructor(String.class).newInstance(name);
        } catch (Throwable t) {
          LOG.error("Unable to create {}.  Falling back on {}", memoryManagerKey,
              ByteArrayMemoryManager.class.getCanonicalName(), t);
        }
      }
      return new ByteArrayMemoryManager(name);
    }
  }
}
