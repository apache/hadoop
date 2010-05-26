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
 * HMsg is for communicating instructions between the HMaster and the
 * HRegionServers.
 *
 * Most of the time the messages are simple but some messages are accompanied
 * by the region affected.  HMsg may also carry optional message.
 */
public class HMsg implements Writable {
  public static final HMsg REGIONSERVER_QUIESCE =
    new HMsg(Type.MSG_REGIONSERVER_QUIESCE);
  public static final HMsg REGIONSERVER_STOP =
    new HMsg(Type.MSG_REGIONSERVER_STOP);
  public static final HMsg [] EMPTY_HMSG_ARRAY = new HMsg[0];

  /**
   * Message types sent between master and regionservers
   */
  public static enum Type {
    /** null message */
    MSG_NONE,

    // Message types sent from master to region server
    /** Start serving the specified region */
    MSG_REGION_OPEN,

    /** Stop serving the specified region */
    MSG_REGION_CLOSE,

    /** Split the specified region */
    MSG_REGION_SPLIT,

    /** Compact the specified region */
    MSG_REGION_COMPACT,

    /** Master tells region server to stop */
    MSG_REGIONSERVER_STOP,

    /** Stop serving the specified region and don't report back that it's
     * closed
     */
    MSG_REGION_CLOSE_WITHOUT_REPORT,

    /** Stop serving user regions */
    MSG_REGIONSERVER_QUIESCE,

    // Message types sent from the region server to the master
    /** region server is now serving the specified region */
    MSG_REPORT_OPEN,

    /** region server is no longer serving the specified region */
    MSG_REPORT_CLOSE,

    /** region server is processing open request */
    MSG_REPORT_PROCESS_OPEN,

    /**
     * Region server split the region associated with this message.
     *
     * Note that this message is immediately followed by two MSG_REPORT_OPEN
     * messages, one for each of the new regions resulting from the split
     * @deprecated See MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS
     */
    MSG_REPORT_SPLIT,

    /**
     * Region server is shutting down
     *
     * Note that this message is followed by MSG_REPORT_CLOSE messages for each
     * region the region server was serving, unless it was told to quiesce.
     */
    MSG_REPORT_EXITING,

    /** Region server has closed all user regions but is still serving meta
     * regions
     */
    MSG_REPORT_QUIESCED,

    /**
     * Flush
     */
    MSG_REGION_FLUSH,

    /**
     * Run Major Compaction
     */
    MSG_REGION_MAJOR_COMPACT,

    /**
     * Region server split the region associated with this message.
     *
     * Its like MSG_REPORT_SPLIT only it carries the daughters in the message
     * rather than send them individually in MSG_REPORT_OPEN messages.
     */
    MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS,

    /**
     * When RegionServer receives this message, it goes into a sleep that only
     * an exit will cure.  This message is sent by unit tests simulating
     * pathological states.
     */
    TESTING_MSG_BLOCK_RS,
  }

  private Type type = null;
  private HRegionInfo info = null;
  private byte[] message = null;
  private HRegionInfo daughterA = null;
  private HRegionInfo daughterB = null;

  /** Default constructor. Used during deserialization */
  public HMsg() {
    this(Type.MSG_NONE);
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
    if (type == null) {
      throw new NullPointerException("Message type cannot be null");
    }
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
     if (this.type.equals(Type.MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS)) {
       this.daughterA.write(out);
       this.daughterB.write(out);
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
     if (this.type.equals(Type.MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS)) {
       this.daughterA = new HRegionInfo();
       this.daughterB = new HRegionInfo();
       this.daughterA.readFields(in);
       this.daughterB.readFields(in);
     }
   }
}