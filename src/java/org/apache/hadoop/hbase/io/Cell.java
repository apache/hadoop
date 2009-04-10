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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import agilejson.TOJSON;

/**
 * Cell - Used to transport a cell value (byte[]) and the timestamp it was
 * stored with together as a result for get and getRow methods. This promotes
 * the timestamp of a cell to a first-class value, making it easy to take note
 * of temporal data. Cell is used all the way from HStore up to HTable.
 */
public class Cell implements Writable, Iterable<Map.Entry<Long, byte[]>>,
    ISerializable {
  protected final SortedMap<Long, byte[]> valueMap = new TreeMap<Long, byte[]>(
      new Comparator<Long>() {
        public int compare(Long l1, Long l2) {
          return l2.compareTo(l1);
        }
      });

  /** For Writable compatibility */
  public Cell() {
    super();
  }

  /**
   * Create a new Cell with a given value and timestamp. Used by HStore.
   * 
   * @param value
   * @param timestamp
   */
  public Cell(String value, long timestamp) {
    this(Bytes.toBytes(value), timestamp);
  }

  /**
   * Create a new Cell with a given value and timestamp. Used by HStore.
   * 
   * @param value
   * @param timestamp
   */
  public Cell(byte[] value, long timestamp) {
    valueMap.put(timestamp, value);
  }

  /**
   * Create a new Cell with a given value and timestamp. Used by HStore.
   * 
   * @param bb
   * @param timestamp
   */
  public Cell(final ByteBuffer bb, long timestamp) {
    this.valueMap.put(timestamp, Bytes.toBytes(bb));
  }

  /**
   * @param vals
   *          array of values
   * @param ts
   *          array of timestamps
   */
  public Cell(String [] vals, long[] ts) {
    this(Bytes.toByteArrays(vals), ts);
  }

  /**
   * @param vals
   *          array of values
   * @param ts
   *          array of timestamps
   */
  public Cell(byte[][] vals, long[] ts) {
    if (vals.length != ts.length) {
      throw new IllegalArgumentException(
          "number of values must be the same as the number of timestamps");
    }
    for (int i = 0; i < vals.length; i++) {
      valueMap.put(ts[i], vals[i]);
    }
  }

  /** @return the current cell's value */
  @TOJSON(base64=true)
  public byte[] getValue() {
    return valueMap.get(valueMap.firstKey());
  }

  /** @return the current cell's timestamp */
  @TOJSON
  public long getTimestamp() {
    return valueMap.firstKey();
  }

  /** @return the number of values this cell holds */
  public int getNumValues() {
    return valueMap.size();
  }

  /**
   * Add a new timestamp and value to this cell provided timestamp does not
   * already exist
   * 
   * @param val
   * @param ts
   */
  public void add(byte[] val, long ts) {
    if (!valueMap.containsKey(ts)) {
      valueMap.put(ts, val);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if (valueMap.size() == 1) {
      return "timestamp=" + getTimestamp() + ", value="
          + Bytes.toString(getValue());
    }
    StringBuilder s = new StringBuilder("{ ");
    int i = 0;
    for (Map.Entry<Long, byte[]> entry : valueMap.entrySet()) {
      if (i > 0) {
        s.append(", ");
      }
      s.append("[timestamp=");
      s.append(entry.getKey());
      s.append(", value=");
      s.append(Bytes.toString(entry.getValue()));
      s.append("]");
      i++;
    }
    s.append(" }");
    return s.toString();
  }

  //
  // Writable
  //

  public void readFields(final DataInput in) throws IOException {
    int nvalues = in.readInt();
    for (int i = 0; i < nvalues; i++) {
      long timestamp = in.readLong();
      byte[] value = Bytes.readByteArray(in);
      valueMap.put(timestamp, value);
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeInt(valueMap.size());
    for (Map.Entry<Long, byte[]> entry : valueMap.entrySet()) {
      out.writeLong(entry.getKey());
      Bytes.writeByteArray(out, entry.getValue());
    }
  }

  //
  // Iterable
  //

  public Iterator<Entry<Long, byte[]>> iterator() {
    return new CellIterator();
  }

  private class CellIterator implements Iterator<Entry<Long, byte[]>> {
    private Iterator<Entry<Long, byte[]>> it;

    CellIterator() {
      it = valueMap.entrySet().iterator();
    }

    public boolean hasNext() {
      return it.hasNext();
    }

    public Entry<Long, byte[]> next() {
      return it.next();
    }

    public void remove() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("remove is not supported");
    }
  }

  /**
   * @see
   * org.apache.hadoop.hbase.rest.serializer.ISerializable#restSerialize(org
   * .apache.hadoop.hbase.rest.serializer.IRestSerializer)
   */
  public void restSerialize(IRestSerializer serializer)
      throws HBaseRestException {
    serializer.serializeCell(this);
  }
}