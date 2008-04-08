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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RowResult implements Writable, Map<Text, Cell> {
  protected Text row;
  protected HbaseMapWritable cells;
   
  /**
   * Used by Writable
   */
  public RowResult () {
    row = new Text();
    cells = new HbaseMapWritable();
  }
  
  /**
   * Create a RowResult from a row and Cell map
   */
  public RowResult (final Text row, final HbaseMapWritable hbw) {
    this.row = row;
    this.cells = hbw;
  }
  
  /**
   * Get the row for this RowResult
   */
  public Text getRow() {
    return row;
  }

  // 
  // Map interface
  // 
  
  public Cell put(Text key, Cell value) {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  public void putAll(Map map) {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  public Cell get(Object key) {
    return (Cell)cells.get(key);
  }

  public Cell remove(Object key) {
    throw new UnsupportedOperationException("RowResult is read-only!");
  }

  public boolean containsKey(Object key) {
    return cells.containsKey(key);
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

  public Set<Text> keySet() {
    Set<Text> result = new HashSet<Text>();
    for (Writable w : cells.keySet()) {
      result.add((Text)w);
    }
    return result;
  }

  public Set<Map.Entry<Text, Cell>> entrySet() {
    Set<Map.Entry<Text, Cell>> result = new HashSet<Map.Entry<Text, Cell>>();
    for (Map.Entry<Writable, Writable> e : cells.entrySet()) {
      result.add(new Entry((Text)e.getKey(), (Cell)e.getValue()));
    }
    return result;
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
   */
  public Cell get(Text column) {
    return (Cell)cells.get(column);
  }
  
  public class Entry implements Map.Entry<Text, Cell> {
    private Text row;
    private Cell cell;
    
    Entry(Text row, Cell cell) {
      this.row = row;
      this.cell = cell;
    }
    
    public Cell setValue(Cell c) {
      throw new UnsupportedOperationException("RowResult is read-only!");
    }
    
    public Text getKey() {
      return row;
    }
    
    public Cell getValue() {
      return cell;
    }
  }
  
  //
  // Writable
  //

  public void readFields(final DataInput in) throws IOException {
    row.readFields(in);
    cells.readFields(in);
  }

  public void write(final DataOutput out) throws IOException {
    row.write(out);
    cells.write(out);
  }  
}
