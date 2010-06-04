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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

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
  private byte [] regionName;
  private byte [] tablename;
  private long logSeqNum;
  // Time at which this edit was written.
  private long writeTime;

  private byte clusterId;
  private int scope;

  /** Writable Consructor -- Do not use. */
  public HLogKey() {
    this(null, null, 0L, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Create the log key!
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param regionName  - name of region
   * @param tablename   - name of table
   * @param logSeqNum   - log sequence number
   * @param now Time at which this edit was written.
   */
  public HLogKey(final byte [] regionName, final byte [] tablename,
      long logSeqNum, final long now) {
    this.regionName = regionName;
    this.tablename = tablename;
    this.logSeqNum = logSeqNum;
    this.writeTime = now;
    this.clusterId = HConstants.DEFAULT_CLUSTER_ID;
    this.scope = HConstants.REPLICATION_SCOPE_LOCAL;
  }

  //////////////////////////////////////////////////////////////////////////////
  // A bunch of accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return region name */
  public byte [] getRegionName() {
    return regionName;
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
   * @return
   */
  public byte getClusterId() {
    return clusterId;
  }

  /**
   * Set the cluster id of this key
   * @param clusterId
   */
  public void setClusterId(byte clusterId) {
    this.clusterId = clusterId;
  }

  /**
   * Get the replication scope of this key
   * @return replication scope
   */
  public int getScope() {
    return this.scope;
  }

  /**
   * Set the replication scope of this key
   * @param scope The new scope
   */
  public void setScope(int scope) {
    this.scope = scope;
  }

  @Override
  public String toString() {
    return Bytes.toString(tablename) + "/" + Bytes.toString(regionName) + "/" +
      logSeqNum;
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
    int result = Bytes.hashCode(this.regionName);
    result ^= this.logSeqNum;
    result ^= this.writeTime;
    result ^= this.clusterId;
    result ^= this.scope;
    return result;
  }

  public int compareTo(HLogKey o) {
    int result = Bytes.compareTo(this.regionName, o.regionName);
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
    return result;
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.regionName);
    Bytes.writeByteArray(out, this.tablename);
    out.writeLong(this.logSeqNum);
    out.writeLong(this.writeTime);
    out.writeByte(this.clusterId);
    out.writeInt(this.scope);
  }

  public void readFields(DataInput in) throws IOException {
    this.regionName = Bytes.readByteArray(in);
    this.tablename = Bytes.readByteArray(in);
    this.logSeqNum = in.readLong();
    this.writeTime = in.readLong();
    try {
      this.clusterId = in.readByte();
      this.scope = in.readInt();
    } catch(EOFException e) {
      // Means it's an old key, just continue
    }
  }

}
