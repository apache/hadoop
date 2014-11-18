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
package org.apache.hadoop.hdfs.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/**
 * Manage byte array creation and release. 
 */
@InterfaceAudience.Private
public abstract class ByteArrayManager {
  static final Log LOG = LogFactory.getLog(ByteArrayManager.class);
  private static final ThreadLocal<StringBuilder> debugMessage = new ThreadLocal<StringBuilder>() {
    protected StringBuilder initialValue() {
      return new StringBuilder();
    }
  };

  private static void logDebugMessage() {
    final StringBuilder b = debugMessage.get();
    LOG.debug(b);
    b.setLength(0);
  }

  static final int MIN_ARRAY_LENGTH = 32;
  static final byte[] EMPTY_BYTE_ARRAY = {};

  /**
   * @return the least power of two greater than or equal to n, i.e. return
   *         the least integer x with x >= n and x a power of two.
   *
   * @throws HadoopIllegalArgumentException
   *           if n <= 0.
   */
  public static int leastPowerOfTwo(final int n) {
    if (n <= 0) {
      throw new HadoopIllegalArgumentException("n = " + n + " <= 0");
    }

    final int highestOne = Integer.highestOneBit(n);
    if (highestOne == n) {
      return n; // n is a power of two.
    }
    final int roundUp = highestOne << 1;
    if (roundUp < 0) {
      final long overflow = ((long) highestOne) << 1;
      throw new ArithmeticException(
          "Overflow: for n = " + n + ", the least power of two (the least"
          + " integer x with x >= n and x a power of two) = "
          + overflow + " > Integer.MAX_VALUE = " + Integer.MAX_VALUE);
    }
    return roundUp;
  }

  /**
   * A counter with a time stamp so that it is reset automatically
   * if there is no increment for the time period.
   */
  static class Counter {
    private final long countResetTimePeriodMs;
    private long count = 0L;
    private long timestamp = Time.monotonicNow();

    Counter(long countResetTimePeriodMs) {
      this.countResetTimePeriodMs = countResetTimePeriodMs;
    }

    synchronized long getCount() {
      return count;
    }

    /**
     * Increment the counter, and reset it if there is no increment
     * for acertain time period.
     *
     * @return the new count.
     */
    synchronized long increment() {
      final long now = Time.monotonicNow();
      if (now - timestamp > countResetTimePeriodMs) {
        count = 0; // reset the counter
      }
      timestamp = now;
      return ++count;
    }
  }

  /** A map from integers to counters. */
  static class CounterMap {
    /** @see ByteArrayManager.Conf#countResetTimePeriodMs */
    private final long countResetTimePeriodMs;
    private final Map<Integer, Counter> map = new HashMap<Integer, Counter>();

    private CounterMap(long countResetTimePeriodMs) {
      this.countResetTimePeriodMs = countResetTimePeriodMs;
    }

    /**
     * @return the counter for the given key;
     *         and create a new counter if it does not exist.
     */
    synchronized Counter get(final Integer key, final boolean createIfNotExist) {
      Counter count = map.get(key);
      if (count == null && createIfNotExist) {
        count = new Counter(countResetTimePeriodMs);
        map.put(key, count);
      }
      return count;
    }

    synchronized void clear() {
      map.clear();
    }
  }

  /** Manage byte arrays with the same fixed length. */
  static class FixedLengthManager {
    private final int byteArrayLength;
    private final int maxAllocated;
    private final Queue<byte[]> freeQueue = new LinkedList<byte[]>();

    private int numAllocated = 0;

    FixedLengthManager(int arrayLength, int maxAllocated) {
      this.byteArrayLength = arrayLength;
      this.maxAllocated = maxAllocated;
    }

    /**
     * Allocate a byte array.
     *
     * If the number of allocated arrays >= maximum, the current thread is
     * blocked until the number of allocated arrays drops to below the maximum.
     * 
     * The byte array allocated by this method must be returned for recycling
     * via the {@link FixedLengthManager#recycle(byte[])} method.
     */
    synchronized byte[] allocate() throws InterruptedException {
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append(", ").append(this);
      }
      for(; numAllocated >= maxAllocated;) {
        if (LOG.isDebugEnabled()) {
          debugMessage.get().append(": wait ...");
          logDebugMessage();
        }

        wait();

        if (LOG.isDebugEnabled()) {
          debugMessage.get().append("wake up: ").append(this);
        }
      }
      numAllocated++;

      final byte[] array = freeQueue.poll();
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append(", recycled? ").append(array != null);
      }
      return array != null? array : new byte[byteArrayLength];
    }

    /**
     * Recycle the given byte array, which must have the same length as the
     * array length managed by this object.
     *
     * The byte array may or may not be allocated
     * by the {@link FixedLengthManager#allocate()} method.
     */
    synchronized int recycle(byte[] array) {
      Preconditions.checkNotNull(array);
      Preconditions.checkArgument(array.length == byteArrayLength);
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append(", ").append(this);
      }

      notify();
      numAllocated--;
      if (numAllocated < 0) {
        // it is possible to drop below 0 since
        // some byte arrays may not be created by the allocate() method.
        numAllocated = 0;
      }

      if (freeQueue.size() < maxAllocated - numAllocated) {
        if (LOG.isDebugEnabled()) {
          debugMessage.get().append(", freeQueue.offer");
        }
        freeQueue.offer(array);
      }
      return freeQueue.size();
    }

    @Override
    public synchronized String toString() {
      return "[" + byteArrayLength + ": " + numAllocated + "/"
          + maxAllocated + ", free=" + freeQueue.size() + "]";
    }
  }

  /** A map from array lengths to byte array managers. */
  static class ManagerMap {
    private final int countLimit;
    private final Map<Integer, FixedLengthManager> map = new HashMap<Integer, FixedLengthManager>();

    ManagerMap(int countLimit) {
      this.countLimit = countLimit;
    }

    /** @return the manager for the given array length. */
    synchronized FixedLengthManager get(final Integer arrayLength,
        final boolean createIfNotExist) {
      FixedLengthManager manager = map.get(arrayLength);
      if (manager == null && createIfNotExist) {
        manager = new FixedLengthManager(arrayLength, countLimit);
        map.put(arrayLength, manager);
      }
      return manager;
    }

    synchronized void clear() {
      map.clear();
    }
  }

  public static class Conf {
    /**
     * The count threshold for each array length so that a manager is created
     * only after the allocation count exceeds the threshold.
     */
    private final int countThreshold;
    /**
     * The maximum number of arrays allowed for each array length.
     */
    private final int countLimit;
    /**
     * The time period in milliseconds that the allocation count for each array
     * length is reset to zero if there is no increment.
     */
    private final long countResetTimePeriodMs;

    public Conf(int countThreshold, int countLimit, long countResetTimePeriodMs) {
      this.countThreshold = countThreshold;
      this.countLimit = countLimit;
      this.countResetTimePeriodMs = countResetTimePeriodMs;
    }
  }

  /**
   * Create a byte array for the given length, where the length of
   * the returned array is larger than or equal to the given length.
   *
   * The current thread may be blocked if some resource is unavailable.
   * 
   * The byte array created by this method must be released
   * via the {@link ByteArrayManager#release(byte[])} method.
   *
   * @return a byte array with length larger than or equal to the given length.
   */
  public abstract byte[] newByteArray(int size) throws InterruptedException;
  
  /**
   * Release the given byte array.
   * 
   * The byte array may or may not be created
   * by the {@link ByteArrayManager#newByteArray(int)} method.
   * 
   * @return the number of free array.
   */
  public abstract int release(byte[] array);

  public static ByteArrayManager newInstance(Conf conf) {
    return conf == null? new NewByteArrayWithoutLimit(): new Impl(conf);
  }

  /**
   * A dummy implementation which simply calls new byte[].
   */
  static class NewByteArrayWithoutLimit extends ByteArrayManager {
    @Override
    public byte[] newByteArray(int size) throws InterruptedException {
      return new byte[size];
    }
    
    @Override
    public int release(byte[] array) {
      return 0;
    }
  }

  /**
   * Manage byte array allocation and provide a mechanism for recycling the byte
   * array objects.
   */
  static class Impl extends ByteArrayManager {
    private final Conf conf;
  
    private final CounterMap counters;
    private final ManagerMap managers;
  
    Impl(Conf conf) {
      this.conf = conf;
      this.counters = new CounterMap(conf.countResetTimePeriodMs);
      this.managers = new ManagerMap(conf.countLimit);
    }
  
    /**
     * Allocate a byte array, where the length of the allocated array
     * is the least power of two of the given length
     * unless the given length is less than {@link #MIN_ARRAY_LENGTH}.
     * In such case, the returned array length is equal to {@link #MIN_ARRAY_LENGTH}.
     *
     * If the number of allocated arrays exceeds the capacity,
     * the current thread is blocked until
     * the number of allocated arrays drops to below the capacity.
     * 
     * The byte array allocated by this method must be returned for recycling
     * via the {@link Impl#release(byte[])} method.
     *
     * @return a byte array with length larger than or equal to the given length.
     */
    @Override
    public byte[] newByteArray(final int arrayLength) throws InterruptedException {
      Preconditions.checkArgument(arrayLength >= 0);
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append("allocate(").append(arrayLength).append(")");
      }
  
      final byte[] array;
      if (arrayLength == 0) {
        array = EMPTY_BYTE_ARRAY;
      } else {
        final int powerOfTwo = arrayLength <= MIN_ARRAY_LENGTH?
            MIN_ARRAY_LENGTH: leastPowerOfTwo(arrayLength);
        final long count = counters.get(powerOfTwo, true).increment();
        final boolean aboveThreshold = count > conf.countThreshold;
        // create a new manager only if the count is above threshold.
        final FixedLengthManager manager = managers.get(powerOfTwo, aboveThreshold);
  
        if (LOG.isDebugEnabled()) {
          debugMessage.get().append(": count=").append(count)
              .append(aboveThreshold? ", aboveThreshold": ", belowThreshold");
        }
        array = manager != null? manager.allocate(): new byte[powerOfTwo];
      }
  
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append(", return byte[").append(array.length).append("]");
        logDebugMessage();
      }
      return array;
    }
  
    /**
     * Recycle the given byte array.
     * 
     * The byte array may or may not be allocated
     * by the {@link Impl#newByteArray(int)} method.
     * 
     * This is a non-blocking call.
     */
    @Override
    public int release(final byte[] array) {
      Preconditions.checkNotNull(array);
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append("recycle: array.length=").append(array.length);
      }
  
      final int freeQueueSize;
      if (array.length == 0) {
        freeQueueSize = -1;
      } else {
        final FixedLengthManager manager = managers.get(array.length, false);
        freeQueueSize = manager == null? -1: manager.recycle(array);
      }
  
      if (LOG.isDebugEnabled()) {
        debugMessage.get().append(", freeQueueSize=").append(freeQueueSize);
        logDebugMessage();
      }
      return freeQueueSize;
    }
  
    CounterMap getCounters() {
      return counters;
    }
  
    ManagerMap getManagers() {
      return managers;
    }
  }
}
