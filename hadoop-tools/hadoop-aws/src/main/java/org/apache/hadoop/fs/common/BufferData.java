/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import org.apache.hadoop.fs.common.Validate;

import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.Awaitable.CanAwait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * Holds the state of a ByteBuffer that is in use by {@code S3CachingBlockManager}.
 *
 * This class is not meant to be of general use. It exists into its own file due to its size.
 * We use the term block and buffer interchangeably in this file because one buffer
 * holds exactly one block of data.
 *
 * Holding all of the state associated with a block allows us to validate and control
 * state transitions in a synchronized fashion.
 */
public class BufferData {
  private static final Logger LOG = LoggerFactory.getLogger(BufferData.class);

  public enum State {
    // Unknown / invalid state.
    UNKNOWN,

    // Buffer has been acquired but has no data.
    BLANK,

    // This block is being prefetched.
    PREFETCHING,

    // This block is being added to the local cache.
    CACHING,

    // This block has data and is ready to be read.
    READY,

    // This block is no longer in-use and should not be used once in this state.
    DONE
  }

  // Number of the block associated with this buffer.
  public final int blockNumber;

  // The buffer associated with this block.
  private ByteBuffer buffer;

  // Current state of this block.
  private volatile State state;

  // Future of the action being performed on this block (eg, prefetching or caching).
  private Future<Void> actionFuture;

  // Checksum of the buffer contents once in READY state.
  private long checksum = 0;

  /**
   * Constructs an instances of this class.
   *
   * @param blockNumber Number of the block associated with this buffer.
   * @param buffer The buffer associated with this block.
   */
  public BufferData(int blockNumber, ByteBuffer buffer) {
    Validate.checkNotNegative(blockNumber, "blockNumber");
    Validate.checkNotNull(buffer, "buffer");

    this.blockNumber = blockNumber;
    this.buffer = buffer;
    this.state = State.BLANK;
  }

  public ByteBuffer getBuffer() {
    return this.buffer;
  }

  public State getState() {
    return this.state;
  }

  public long getChecksum() {
    return this.checksum;
  }

  /**
   * Computes CRC32 checksum of the given buffer's contents.
   */
  public static long getChecksum(ByteBuffer buffer) {
    ByteBuffer tempBuffer = buffer.duplicate();
    tempBuffer.rewind();
    CRC32 crc32 = new CRC32();
    crc32.update(tempBuffer);
    return crc32.getValue();
  }

  public synchronized Future<Void> getActionFuture() {
    return this.actionFuture;
  }

  /**
   * Indicates that a prefetch operation is in progress.
   */
  public synchronized void setPrefetch(Future<Void> actionFuture) {
    Validate.checkNotNull(actionFuture, "actionFuture");

    this.updateState(State.PREFETCHING, State.BLANK);
    this.actionFuture = actionFuture;
  }

  /**
   * Indicates that a caching operation is in progress.
   */
  public synchronized void setCaching(Future<Void> actionFuture) {
    Validate.checkNotNull(actionFuture, "actionFuture");

    this.throwIfStateIncorrect(State.PREFETCHING, State.READY);
    this.state = State.CACHING;
    this.actionFuture = actionFuture;
  }

  /**
   * Marks the completion of reading data into the buffer.
   * The buffer cannot be modified once in this state.
   */
  public synchronized void setReady(State... expectedCurrentState) {
    if (this.checksum != 0) {
      throw new IllegalStateException("Checksum cannot be changed once set");
    }

    this.buffer = this.buffer.asReadOnlyBuffer();
    this.checksum = getChecksum(this.buffer);
    this.buffer.rewind();
    this.updateState(State.READY, expectedCurrentState);
  }

  /**
   * Indicates that this block is no longer of use and can be reclaimed.
   */
  public synchronized void setDone() {
    if (this.checksum != 0) {
      long checksum = getChecksum(this.buffer);
      if (checksum != this.checksum) {
        throw new IllegalStateException("checksum changed after setReady()");
      }
    }
    this.state = State.DONE;
    this.actionFuture = null;
  }

  /**
   * Updates the current state to the specified value.
   * Asserts that the current state is as expected.
   */
  public synchronized void updateState(State newState, State... expectedCurrentState) {
    Validate.checkNotNull(newState, "newState");
    Validate.checkNotNull(expectedCurrentState, "expectedCurrentState");

    this.throwIfStateIncorrect(expectedCurrentState);
    this.state = newState;
  }

  /**
   * Helper that asserts the current state is one of the expected values.
   */
  public void throwIfStateIncorrect(State... states) {
    Validate.checkNotNull(states, "states");

    if (this.stateEqualsOneOf(states)) {
      return;
    }

    List<String> statesStr = new ArrayList();
    for (State s : states) {
      statesStr.add(s.toString());
    }

    String message = String.format(
        "Expected buffer state to be '%s' but found: %s", String.join(" or ", statesStr), this);
    throw new IllegalStateException(message);
  }

  public boolean stateEqualsOneOf(State... states) {
    State currentState = this.state;

    for (State s : states) {
      if (currentState == s) {
        return true;
      }
    }

    return false;
  }

  private static CanAwait CAN_AWAIT = () -> false;

  public String toString() {

    return String.format(
        "[%03d] id: %03d, %s: buf: %s, checksum: %d, future: %s",
        this.blockNumber,
        System.identityHashCode(this),
        this.state,
        this.getBufferStr(this.buffer),
        this.checksum,
        this.getFutureStr(this.actionFuture));
  }

  private String getFutureStr(Future<Void> f) {
    if (f == null) {
      return "--";
    } else {
      return this.actionFuture.isReady(CAN_AWAIT) ? "done" : "not done";
    }
  }

  private String getBufferStr(ByteBuffer buffer) {
    if (buffer == null) {
      return "--";
    } else {
      return String.format(
          "(id = %d, pos = %d, lim = %d)",
          System.identityHashCode(buffer),
          buffer.position(), buffer.limit());
    }
  }
}
