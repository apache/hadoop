/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * HMsg is used to send messages between master and regionservers.  Messages are
 * sent as payload on the regionserver-to-master heartbeats.  Region assignment
 * does not use this mechanism.  It goes via zookeeper.
 *
 * <p>Most of the time the messages are simple but some messages are accompanied
 * by the region affected.  HMsg may also carry an optional message.
 * 
 * <p>TODO: Clean out all messages that go from master to regionserver; by
 * design, these are to go via zk from here on out.
 */
public class HMsg implements Writable {
  public static final HMsg [] EMPTY_HMSG_ARRAY = new HMsg[0];

  public static enum Type {
    /**
     * When RegionServer receives this message, it goes into a sleep that only
     * an exit will cure.  This message is sent by unit tests simulating
     * pathological states.
     */
    TESTING_BLOCK_REGIONSERVER,
  }

  private Type type = null;
  private HRegionInfo info = null;
  private byte[] message = null;
  private HRegionInfo daughterA = null;
  private HRegionInfo daughterB = null;

  /** Default constructor. Used during deserialization */
  public HMsg() {
    this(null);
  }

  /**
   * Construct a message with the specified message and empty HRegionInfo
   * @param type Message type
   */
  public HMsg(final HMsg.Type type) {
    this(type, new HRegionInfo(), null);
  }

  /**
   * Construct a message with the specified message and HRegionInfo
   * @param type Message type
   * @param hri Region to which message <code>type</code> applies
   */
  public HMsg(final HMsg.Type type, final HRegionInfo hri) {
    this(type, hri, null);
  }

  /**
   * Construct a message with the specified message and HRegionInfo
   *
   * @param type Message type
   * @param hri Region to which message <code>type</code> applies.  Cannot be
   * null.  If no info associated, used other Constructor.
   * @param msg Optional message (Stringified exception, etc.)
   */
  public HMsg(final HMsg.Type type, final HRegionInfo hri, final byte[] msg) {
    this(type, hri, null, null, msg);
  }

  /**
   * Construct a message with the specified message and HRegionInfo
   *
   * @param type Message type
   * @param hri Region to which message <code>type</code> applies.  Cannot be
   * null.  If no info associated, used other Constructor.
   * @param daughterA
   * @param daughterB
   * @param msg Optional message (Stringified exception, etc.)
   */
  public HMsg(final HMsg.Type type, final HRegionInfo hri,
      final HRegionInfo daughterA, final HRegionInfo daughterB, final byte[] msg) {
    this.type = type;
    if (hri == null) {
      throw new NullPointerException("Region cannot be null");
    }
    this.info = hri;
    this.message = msg;
    this.daughterA = daughterA;
    this.daughterB = daughterB;
  }

  /**
   * @return Region info or null if none associated with this message type.
   */
  public HRegionInfo getRegionInfo() {
    return this.info;
  }

  /** @return the type of message */
  public Type getType() {
    return this.type;
  }

  /**
   * @param other Message type to compare to
   * @return True if we are of same message type as <code>other</code>
   */
  public boolean isType(final HMsg.Type other) {
    return this.type.equals(other);
  }

  /** @return the message type */
  public byte[] getMessage() {
    return this.message;
  }

  /**
   * @return First daughter if Type is MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS else
   * null
   */
  public HRegionInfo getDaughterA() {
    return this.daughterA;
  }

  /**
   * @return Second daughter if Type is MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS else
   * null
   */
  public HRegionInfo getDaughterB() {
    return this.daughterB;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.type.toString());
    // If null or empty region, don't bother printing it out.
    if (this.info != null && this.info.getRegionName().length > 0) {
      sb.append(": ");
      sb.append(this.info.getRegionNameAsString());
    }
    if (this.message != null && this.message.length > 0) {
      sb.append(": " + Bytes.toString(this.message));
    }
    return sb.toString();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HMsg that = (HMsg)obj;
    return this.type.equals(that.type) &&
      (this.info != null)? this.info.equals(that.info):
        that.info == null;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = this.type.hashCode();
    if (this.info != null) {
      result ^= this.info.hashCode();
    }
    return result;
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
     out.writeInt(this.type.ordinal());
     this.info.write(out);
     if (this.message == null || this.message.length == 0) {
       out.writeBoolean(false);
     } else {
       out.writeBoolean(true);
       Bytes.writeByteArray(out, this.message);
     }
   }

  /**
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
     int ordinal = in.readInt();
     this.type = HMsg.Type.values()[ordinal];
     this.info.readFields(in);
     boolean hasMessage = in.readBoolean();
     if (hasMessage) {
       this.message = Bytes.readByteArray(in);
     }
   }
}