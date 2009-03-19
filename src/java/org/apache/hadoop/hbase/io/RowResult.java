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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.rest.descriptors.RestCell;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

import agilejson.TOJSON;

/**
 * Holds row name and then a map of columns to cells.
 */
public class RowResult implements Writable, SortedMap<byte [], Cell>,
  Comparable<RowResult>, ISerializable {
  private byte [] row = null;
  private final HbaseMapWritable<byte [], Cell> cells;

  /** default constructor for writable */
  public RowResult() {
    this(null, new HbaseMapWritable<byte [], Cell>());
  }

  /**
   * Create a RowResult from a row and Cell map
   * @param row
   * @param m
   */
  public RowResult (final byte [] row,
      final HbaseMapWritable<byte [], Cell> m) {
    this.row = row;
    this.cells = m;
  }
  
  /**
   * Get the row for this RowResult
   * @return the row
   */
  @TOJSON(base64=true)
  public byte [] getRow() {
    return row;
  }

  // 
  // Map interface
  // 
  
  public Cell put(byte [] key, Cell value) {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  @SuppressWarnings("unchecked")
  public void putAll(Map map) {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  public Cell get(Object key) {
    return this.cells.get(key);
  }

  public Cell remove(Object key) {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  public boolean containsKey(Object key) {
    return cells.containsKey(key);
  }
  
  public boolean containsKey(String key) {
    return cells.containsKey(Bytes.toBytes(key));
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public boolean isEmpty() {
    return cells.isEmpty();
  }

  public int size() {
    return cells.size();
  }

  public void clear() {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  public Set<byte []> keySet() {
    Set<byte []> result = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    for (byte [] w : cells.keySet()) {
      result.add(w);
    }
    return result;
  }

  public Set<Map.Entry<byte [], Cell>> entrySet() {
    return Collections.unmodifiableSet(this.cells.entrySet());
  }
  
  /**
   * This method used solely for the REST serialization
   * 
   * @return Cells
   */
  @TOJSON
  public RestCell[] getCells() {
    RestCell[] restCells = new RestCell[this.cells.size()];
    int i = 0;
    for (Map.Entry<byte[], Cell> entry : this.cells.entrySet()) {
      restCells[i] = new RestCell(entry.getKey(), entry.getValue());
      i++;
    }
    return restCells;
  }

  public Collection<Cell> values() {
    ArrayList<Cell> result = new ArrayList<Cell>();
    for (Writable w : cells.values()) {
      result.add((Cell)w);
    }
    return result;
  }
  
  /**
   * Get the Cell that corresponds to column
   * @param column
   * @return the Cell
   */
  public Cell get(byte [] column) {
    return this.cells.get(column);
  }
  
  /**
   * Get the Cell that corresponds to column, using a String key
   * @param key
   * @return the Cell
   */
  public Cell get(String key) {
    return get(Bytes.toBytes(key));
  }
  

  public Comparator<? super byte[]> comparator() {
    return this.cells.comparator();
  }

  public byte[] firstKey() {
    return this.cells.firstKey();
  }

  public SortedMap<byte[], Cell> headMap(byte[] toKey) {
    return this.cells.headMap(toKey);
  }

  public byte[] lastKey() {
    return this.cells.lastKey();
  }

  public SortedMap<byte[], Cell> subMap(byte[] fromKey, byte[] toKey) {
    return this.cells.subMap(fromKey, toKey);
  }

  public SortedMap<byte[], Cell> tailMap(byte[] fromKey) {
    return this.cells.tailMap(fromKey);
  }

  /**
   * Row entry.
   */
  public class Entry implements Map.Entry<byte [], Cell> {
    private final byte [] column;
    private final Cell cell;
    
    Entry(byte [] row, Cell cell) {
      this.column = row;
      this.cell = cell;
    }
    
    public Cell setValue(Cell c) {
      throw new UnsupportedOperationException("RowResult is read-only!");
    }
    
    public byte [] getKey() {
      return column;
    }
    
    public Cell getValue() {
      return cell;
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("row=");
    sb.append(Bytes.toString(this.row));
    sb.append(", cells={");
    boolean moreThanOne = false;
    for (Map.Entry<byte [], Cell> e: this.cells.entrySet()) {
      if (moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append("(column=");
      sb.append(Bytes.toString(e.getKey()));
      sb.append(", timestamp=");
      sb.append(Long.toString(e.getValue().getTimestamp()));
      sb.append(", value=");
      byte [] v = e.getValue().getValue();
      if (Bytes.equals(e.getKey(), HConstants.COL_REGIONINFO)) {
        try {
          sb.append(Writables.getHRegionInfo(v).toString());
        } catch (IOException ioe) {
          sb.append(ioe.toString());
        }
      } else {
        sb.append(v); 
      }
      sb.append(")");
    }
    sb.append("}");
    return sb.toString();
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.rest.xml.IOutputXML#toXML()
   */
  public void restSerialize(IRestSerializer serializer) throws HBaseRestException {
    serializer.serializeRowResult(this);
  }  
  
  //
  // Writable
  //

  public void readFields(final DataInput in) throws IOException {
    this.row = Bytes.readByteArray(in);
    this.cells.readFields(in);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    this.cells.write(out);
  }
  
  //
  // Comparable
  //
  /**
   *  Comparing this RowResult with another one by
   *  comparing the row in it.
   *  @param o the RowResult Object to compare to
   *  @return the compare number
   */
  public int compareTo(RowResult o){
    return Bytes.compareTo(this.row, o.getRow());
  }
}