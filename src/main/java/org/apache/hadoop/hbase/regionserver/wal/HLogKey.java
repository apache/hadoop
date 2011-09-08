/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A Key for an entry in the change log.
 *
 * The log intermingles edits to many tables and rows, so each log entry
 * identifies the appropriate table and row.  Within a table and row, they're
 * also sorted.
 *
 * <p>Some Transactional edits (START, COMMIT, ABORT) will not have an
 * associated row.
 */
public class HLogKey implements WritableComparable<HLogKey> {
  // should be < 0 (@see #readFields(DataInput))
  private static final int VERSION = -1;

  //  The encoded region name.
  private byte [] encodedRegionName;
  private byte [] tablename;
  private long logSeqNum;
  // Time at which this edit was written.
  private long writeTime;

  private UUID clusterId;

  /** Writable Consructor -- Do not use. */
  public HLogKey() {
    this(null, null, 0L, HConstants.LATEST_TIMESTAMP,
        HConstants.DEFAULT_CLUSTER_ID);
  }

  /**
   * Create the log key!
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   * <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename   - name of table
   * @param logSeqNum   - log sequence number
   * @param now Time at which this edit was written.
   * @param UUID of the cluster (used in Replication)
   */
  public HLogKey(final byte [] encodedRegionName, final byte [] tablename,
      long logSeqNum, final long now, UUID clusterId) {
    this.encodedRegionName = encodedRegionName;
    this.tablename = tablename;
    this.logSeqNum = logSeqNum;
    this.writeTime = now;
    this.clusterId = clusterId;
  }

  /** @return encoded region name */
  public byte [] getEncodedRegionName() {
    return encodedRegionName;
  }

  /** @return table name */
  public byte [] getTablename() {
    return tablename;
  }

  /** @return log sequence number */
  public long getLogSeqNum() {
    return logSeqNum;
  }

  void setLogSeqNum(long logSeqNum) {
    this.logSeqNum = logSeqNum;
  }

  /**
   * @return the write time
   */
  public long getWriteTime() {
    return this.writeTime;
  }

  /**
   * Get the id of the original cluster
   * @return Cluster id.
   */
  public UUID getClusterId() {
    return clusterId;
  }

  /**
   * Set the cluster id of this key
   * @param clusterId
   */
  public void setClusterId(UUID clusterId) {
    this.clusterId = clusterId;
  }

  @Override
  public String toString() {
    return Bytes.toString(tablename) + "/" + Bytes.toString(encodedRegionName) + "/" +
      logSeqNum;
  }

  /**
   * Produces a string map for this key. Useful for programmatic use and
   * manipulation of the data stored in an HLogKey, for example, printing 
   * as JSON.
   * 
   * @return a Map containing data from this key
   */
  public Map<String, Object> toStringMap() {
    Map<String, Object> stringMap = new HashMap<String, Object>();
    stringMap.put("table", Bytes.toStringBinary(tablename));
    stringMap.put("region", Bytes.toStringBinary(encodedRegionName));
    stringMap.put("sequence", logSeqNum);
    return stringMap;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return compareTo((HLogKey)obj) == 0;
  }

  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.encodedRegionName);
    result ^= this.logSeqNum;
    result ^= this.writeTime;
    result ^= this.clusterId.hashCode();
    return result;
  }

  public int compareTo(HLogKey o) {
    int result = Bytes.compareTo(this.encodedRegionName, o.encodedRegionName);
    if (result == 0) {
      if (this.logSeqNum < o.logSeqNum) {
        result = -1;
      } else if (this.logSeqNum > o.logSeqNum) {
        result = 1;
      }
      if (result == 0) {
        if (this.writeTime < o.writeTime) {
          result = -1;
        } else if (this.writeTime > o.writeTime) {
          return 1;
        }
      }
    }
    // why isn't cluster id accounted for?
    return result;
  }

  /**
   * Drop this instance's tablename byte array and instead
   * hold a reference to the provided tablename. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internTableName(byte []tablename) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(tablename, this.tablename);
    this.tablename = tablename;
  }

  /**
   * Drop this instance's region name byte array and instead
   * hold a reference to the provided region name. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internEncodedRegionName(byte []encodedRegionName) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(this.encodedRegionName, encodedRegionName);
    this.encodedRegionName = encodedRegionName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, VERSION);
    Bytes.writeByteArray(out, this.encodedRegionName);
    Bytes.writeByteArray(out, this.tablename);
    out.writeLong(this.logSeqNum);
    out.writeLong(this.writeTime);
    // avoid storing 16 bytes when replication is not enabled
    if (this.clusterId == HConstants.DEFAULT_CLUSTER_ID) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeLong(this.clusterId.getMostSignificantBits());
      out.writeLong(this.clusterId.getLeastSignificantBits());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int version = 0;
    // HLogKey was not versioned in the beginning.
    // In order to introduce it now, we make use of the fact
    // that encodedRegionName was written with Bytes.writeByteArray,
    // which encodes the array length as a vint which is >= 0.
    // Hence if the vint is >= 0 we have an old version and the vint
    // encodes the length of encodedRegionName.
    // If < 0 we just read the version and the next vint is the length.
    // @see Bytes#readByteArray(DataInput)
    int len = WritableUtils.readVInt(in);
    if (len < 0) {
      // what we just read was the version
      version = len;
      len = WritableUtils.readVInt(in);
    }
    this.encodedRegionName = new byte[len];
    in.readFully(this.encodedRegionName);
    this.tablename = Bytes.readByteArray(in);
    this.logSeqNum = in.readLong();
    this.writeTime = in.readLong();
    this.clusterId = HConstants.DEFAULT_CLUSTER_ID;
    if (version < 0) {
      if (in.readBoolean()) {
        this.clusterId = new UUID(in.readLong(), in.readLong());
      }
    } else {
      try {
        // dummy read (former byte cluster id)
        in.readByte();
      } catch(EOFException e) {
        // Means it's a very old key, just continue
      }
    }
  }
}
