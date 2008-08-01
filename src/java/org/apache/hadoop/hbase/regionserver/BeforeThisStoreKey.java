/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.HStoreKey;

/**
 * Pass this class into {@link org.apache.hadoop.io.MapFile}.getClosest when
 * searching for the key that comes BEFORE this one but NOT this one.  THis
 * class will return > 0 when asked to compare against itself rather than 0.
 * This is a hack for case where getClosest returns a deleted key and we want
 * to get the previous.  Can't unless use use this class; it'll just keep
 * returning us the deleted key (getClosest gets exact or nearest before when
 * you pass true argument).  TODO: Throw this class away when MapFile has
 * a real 'previous' method.  See HBASE-751.
 */
public class BeforeThisStoreKey extends HStoreKey {
  private final HStoreKey beforeThisKey;

  /**
   * @param beforeThisKey 
   */
  public BeforeThisStoreKey(final HStoreKey beforeThisKey) {
    super();
    this.beforeThisKey = beforeThisKey;
  }
  
  @Override
  public int compareTo(Object o) {
    int result = this.beforeThisKey.compareTo(o);
    return result == 0? -1: result;
  }
  
  @Override
  public boolean equals(@SuppressWarnings("unused") Object obj) {
    return false;
  }

  public byte[] getColumn() {
    return this.beforeThisKey.getColumn();
  }

  public byte[] getRow() {
    return this.beforeThisKey.getRow();
  }

  public long getSize() {
    return this.beforeThisKey.getSize();
  }

  public long getTimestamp() {
    return this.beforeThisKey.getTimestamp();
  }

  public int hashCode() {
    return this.beforeThisKey.hashCode();
  }

  public boolean matchesRowCol(HStoreKey other) {
    return this.beforeThisKey.matchesRowCol(other);
  }

  public boolean matchesRowFamily(HStoreKey that) {
    return this.beforeThisKey.matchesRowFamily(that);
  }

  public boolean matchesWithoutColumn(HStoreKey other) {
    return this.beforeThisKey.matchesWithoutColumn(other);
  }

  public void readFields(DataInput in) throws IOException {
    this.beforeThisKey.readFields(in);
  }

  public void set(HStoreKey k) {
    this.beforeThisKey.set(k);
  }

  public void setColumn(byte[] c) {
    this.beforeThisKey.setColumn(c);
  }

  public void setRow(byte[] newrow) {
    this.beforeThisKey.setRow(newrow);
  }

  public void setVersion(long timestamp) {
    this.beforeThisKey.setVersion(timestamp);
  }

  public String toString() {
    return this.beforeThisKey.toString();
  }

  public void write(DataOutput out) throws IOException {
    this.beforeThisKey.write(out);
  }
}