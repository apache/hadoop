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

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * A closeable object that maintains a reference count.
 *
 * Once the object is closed, attempting to take a new reference will throw
 * ClosedChannelException.
 */
public class CloseableReferenceCount {
  /**
   * Bit mask representing a closed domain socket.
   */
  private static final int STATUS_CLOSED_MASK = 1 << 30;

  /**
   * The status bits.
   *
   * Bit 30: 0 = open, 1 = closed.
   * Bits 29 to 0: the reference count.
   */
  private final AtomicInteger status = new AtomicInteger(0);

  public CloseableReferenceCount() { }

  /**
   * Increment the reference count.
   *
   * @throws ClosedChannelException      If the status is closed.
   */
  public void reference() throws ClosedChannelException {
    int curBits = status.incrementAndGet();
    if ((curBits & STATUS_CLOSED_MASK) != 0) {
      status.decrementAndGet();
      throw new ClosedChannelException();
    }
  }

  /**
   * Decrement the reference count.
   *
   * @return          True if the object is closed and has no outstanding
   *                  references.
   */
  public boolean unreference() {
    int newVal = status.decrementAndGet();
    Preconditions.checkState(newVal != 0xffffffff,
        "called unreference when the reference count was already at 0.");
    return newVal == STATUS_CLOSED_MASK;
  }

  /**
   * Decrement the reference count, checking to make sure that the
   * CloseableReferenceCount is not closed.
   *
   * @throws AsynchronousCloseException  If the status is closed.
   */
  public void unreferenceCheckClosed() throws ClosedChannelException {
    int newVal = status.decrementAndGet();
    if ((newVal & STATUS_CLOSED_MASK) != 0) {
      throw new AsynchronousCloseException();
    }
  }

  /**
   * Return true if the status is currently open.
   *
   * @return                 True if the status is currently open.
   */
  public boolean isOpen() {
    return ((status.get() & STATUS_CLOSED_MASK) == 0);
  }

  /**
   * Mark the status as closed.
   *
   * Once the status is closed, it cannot be reopened.
   *
   * @return                         The current reference count.
   * @throws ClosedChannelException  If someone else closes the object
   *                                 before we do.
   */
  public int setClosed() throws ClosedChannelException {
    while (true) {
      int curBits = status.get();
      if ((curBits & STATUS_CLOSED_MASK) != 0) {
        throw new ClosedChannelException();
      }
      if (status.compareAndSet(curBits, curBits | STATUS_CLOSED_MASK)) {
        return curBits & (~STATUS_CLOSED_MASK);
      }
    }
  }

  /**
   * Get the current reference count.
   *
   * @return                 The current reference count.
   */
  public int getReferenceCount() {
    return status.get() & (~STATUS_CLOSED_MASK);
  }
}
