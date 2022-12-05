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

package org.apache.hadoop.fs.impl.prefetch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the state of a ByteBuffer that is in use by {@code CachingBlockManager}.
 *
 * This class is not meant to be of general use. It exists into its own file due to its size.
 * We use the term block and buffer interchangeably in this file because one buffer
 * holds exactly one block of data.
 *
 * Holding all of the state associated with a block allows us to validate and control
 * state transitions in a synchronized fashion.
 */
public final class BufferData {

  private static final Logger LOG = LoggerFactory.getLogger(BufferData.class);

  public enum State {
    /**
     * Unknown / invalid state.
     */
    UNKNOWN,

    /**
     * Buffer has been acquired but has no data.
     */
    BLANK,

    /**
     * This block is being prefetched.
     */
    PREFETCHING,

    /**
     * This block is being added to the local cache.
     */
    CACHING,

    /**
     * This block has data and is ready to be read.
     */
    READY,

    /**
     * This block is no longer in-use and should not be used once in this state.
     */
    DONE
  }

  /**
   * Number of the block associated with this buffer.
   */
  private final int blockNumber;

  /**
   * The buffer associated with this block.
   */
  private ByteBuffer buffer;

  /**
   * Current state of this block.
   */
  private volatile State state;

  /**
   * Future of the action being performed on this block (eg, prefetching or caching).
   */
  private Future<Void> action;

  /**
   * Checksum of the buffer contents once in READY state.
   */
  private long checksum = 0;

  /**
   * Constructs an instances of this class.
   *
   * @param blockNumber Number of the block associated with this buffer.
   * @param buffer The buffer associated with this block.
   *
   * @throws IllegalArgumentException if blockNumber is negative.
   * @throws IllegalArgumentException if buffer is null.
   */
  public BufferData(int blockNumber, ByteBuffer buffer) {
    Validate.checkNotNegative(blockNumber, "blockNumber");
    Validate.checkNotNull(buffer, "buffer");

    this.blockNumber = blockNumber;
    this.buffer = buffer;
    this.state = State.BLANK;
  }

  /**
   * Gets the id of this block.
   *
   * @return the id of this block.
   */
  public int getBlockNumber() {
    return this.blockNumber;
  }

  /**
   * Gets the buffer associated with this block.
   *
   * @return the buffer associated with this block.
   */
  public ByteBuffer getBuffer() {
    return this.buffer;
  }

  /**
   * Gets the state of this block.
   *
   * @return the state of this block.
   */
  public State getState() {
    return this.state;
  }

  /**
   * Gets the checksum of data in this block.
   *
   * @return the checksum of data in this block.
   */
  public long getChecksum() {
    return this.checksum;
  }

  /**
   * Computes CRC32 checksum of the given buffer's contents.
   *
   * @param buffer the buffer whose content's checksum is to be computed.
   * @return the computed checksum.
   */
  public static long getChecksum(ByteBuffer buffer) {
    ByteBuffer tempBuffer = buffer.duplicate();
    tempBuffer.rewind();
    CRC32 crc32 = new CRC32();
    crc32.update(tempBuffer);
    return crc32.getValue();
  }

  public synchronized Future<Void> getActionFuture() {
    return this.action;
  }

  /**
   * Indicates that a prefetch operation is in progress.
   *
   * @param actionFuture the {@code Future} of a prefetch action.
   *
   * @throws IllegalArgumentException if actionFuture is null.
   */
  public synchronized void setPrefetch(Future<Void> actionFuture) {
    Validate.checkNotNull(actionFuture, "actionFuture");

    this.updateState(State.PREFETCHING, State.BLANK);
    this.action = actionFuture;
  }

  /**
   * Indicates that a caching operation is in progress.
   *
   * @param actionFuture the {@code Future} of a caching action.
   *
   * @throws IllegalArgumentException if actionFuture is null.
   */
  public synchronized void setCaching(Future<Void> actionFuture) {
    Validate.checkNotNull(actionFuture, "actionFuture");

    this.throwIfStateIncorrect(State.PREFETCHING, State.READY);
    this.state = State.CACHING;
    this.action = actionFuture;
  }

  /**
   * Marks the completion of reading data into the buffer.
   * The buffer cannot be modified once in this state.
   *
   * @param expectedCurrentState the collection of states from which transition to READY is allowed.
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
      if (getChecksum(this.buffer) != this.checksum) {
        throw new IllegalStateException("checksum changed after setReady()");
      }
    }
    this.state = State.DONE;
    this.action = null;
  }

  /**
   * Updates the current state to the specified value.
   * Asserts that the current state is as expected.
   * @param newState the state to transition to.
   * @param expectedCurrentState the collection of states from which
   *        transition to {@code newState} is allowed.
   *
   * @throws IllegalArgumentException if newState is null.
   * @throws IllegalArgumentException if expectedCurrentState is null.
   */
  public synchronized void updateState(State newState,
      State... expectedCurrentState) {
    Validate.checkNotNull(newState, "newState");
    Validate.checkNotNull(expectedCurrentState, "expectedCurrentState");

    this.throwIfStateIncorrect(expectedCurrentState);
    this.state = newState;
  }

  /**
   * Helper that asserts the current state is one of the expected values.
   *
   * @param states the collection of allowed states.
   *
   * @throws IllegalArgumentException if states is null.
   */
  public void throwIfStateIncorrect(State... states) {
    Validate.checkNotNull(states, "states");

    if (this.stateEqualsOneOf(states)) {
      return;
    }

    List<String> statesStr = new ArrayList<String>();
    for (State s : states) {
      statesStr.add(s.toString());
    }

    String message = String.format(
        "Expected buffer state to be '%s' but found: %s",
        String.join(" or ", statesStr), this);
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

  public String toString() {

    return String.format(
        "[%03d] id: %03d, %s: buf: %s, checksum: %d, future: %s",
        this.blockNumber,
        System.identityHashCode(this),
        this.state,
        this.getBufferStr(this.buffer),
        this.checksum,
        this.getFutureStr(this.action));
  }

  private String getFutureStr(Future<Void> f) {
    if (f == null) {
      return "--";
    } else {
      return this.action.isDone() ? "done" : "not done";
    }
  }

  private String getBufferStr(ByteBuffer buf) {
    if (buf == null) {
      return "--";
    } else {
      return String.format(
          "(id = %d, pos = %d, lim = %d)",
          System.identityHashCode(buf),
          buf.position(), buf.limit());
    }
  }
}
