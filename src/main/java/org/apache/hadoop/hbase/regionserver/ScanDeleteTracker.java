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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is responsible for the tracking and enforcement of Deletes
 * during the course of a Scan operation.
 *
 * It only has to enforce Delete and DeleteColumn, since the
 * DeleteFamily is handled at a higher level.
 *
 * <p>
 * This class is utilized through three methods:
 * <ul><li>{@link #add} when encountering a Delete or DeleteColumn
 * <li>{@link #isDeleted} when checking if a Put KeyValue has been deleted
 * <li>{@link #update} when reaching the end of a StoreFile or row for scans
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
public class ScanDeleteTracker implements DeleteTracker {

  private long familyStamp = -1L;
  private byte [] deleteBuffer = null;
  private int deleteOffset = 0;
  private int deleteLength = 0;
  private byte deleteType = 0;
  private long deleteTimestamp = 0L;

  /**
   * Constructor for ScanDeleteTracker
   */
  public ScanDeleteTracker() {
    super();
  }

  /**
   * Add the specified KeyValue to the list of deletes to check against for
   * this row operation.
   * <p>
   * This is called when a Delete is encountered in a StoreFile.
   * @param buffer KeyValue buffer
   * @param qualifierOffset column qualifier offset
   * @param qualifierLength column qualifier length
   * @param timestamp timestamp
   * @param type delete type as byte
   */
  @Override
  public void add(byte[] buffer, int qualifierOffset, int qualifierLength,
      long timestamp, byte type) {
    if (timestamp > familyStamp) {
      if (type == KeyValue.Type.DeleteFamily.getCode()) {
        familyStamp = timestamp;
        return;
      }

      if (deleteBuffer != null && type < deleteType) {
        // same column, so ignore less specific delete
        if (Bytes.equals(deleteBuffer, deleteOffset, deleteLength,
            buffer, qualifierOffset, qualifierLength)){
          return;
        }
      }
      // new column, or more general delete type
      deleteBuffer = buffer;
      deleteOffset = qualifierOffset;
      deleteLength = qualifierLength;
      deleteType = type;
      deleteTimestamp = timestamp;
    }
    // missing else is never called.
  }

  /**
   * Check if the specified KeyValue buffer has been deleted by a previously
   * seen delete.
   *
   * @param buffer KeyValue buffer
   * @param qualifierOffset column qualifier offset
   * @param qualifierLength column qualifier length
   * @param timestamp timestamp
   * @return true is the specified KeyValue is deleted, false if not
   */
  @Override
  public boolean isDeleted(byte [] buffer, int qualifierOffset,
      int qualifierLength, long timestamp) {
    if (timestamp <= familyStamp) {
      return true;
    }

    if (deleteBuffer != null) {
      int ret = Bytes.compareTo(deleteBuffer, deleteOffset, deleteLength,
          buffer, qualifierOffset, qualifierLength);

      if (ret == 0) {
        if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
          return true;
        }
        // Delete (aka DeleteVersion)
        // If the timestamp is the same, keep this one
        if (timestamp == deleteTimestamp) {
          return true;
        }
        // use assert or not?
        assert timestamp < deleteTimestamp;

        // different timestamp, let's clear the buffer.
        deleteBuffer = null;
      } else if(ret < 0){
        // Next column case.
        deleteBuffer = null;
      } else {
        throw new IllegalStateException("isDelete failed: deleteBuffer="
            + Bytes.toStringBinary(deleteBuffer, deleteOffset, deleteLength)
            + ", qualifier="
            + Bytes.toStringBinary(buffer, qualifierOffset, qualifierLength)
            + ", timestamp=" + timestamp + ", comparison result: " + ret);
      }
    }

    return false;
  }

  @Override
  public boolean isEmpty() {
    return deleteBuffer == null && familyStamp == 0;
  }

  @Override
  // called between every row.
  public void reset() {
    familyStamp = 0L;
    deleteBuffer = null;
  }

  @Override
  // should not be called at all even (!)
  public void update() {
    this.reset();
  }
}