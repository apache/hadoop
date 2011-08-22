/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

/**
 * BlockCacheColumnFamilySummary represents a summary of the blockCache usage 
 * at Table/ColumnFamily granularity.
 * <br><br>
 * As ColumnFamilies are owned by Tables, a summary by ColumnFamily implies that
 * the owning Table is included in the summarization.
 *
 */
public class BlockCacheColumnFamilySummary implements Writable, Comparable<BlockCacheColumnFamilySummary> {

  private String table = "";
  private String columnFamily = "";
  private int blocks;
  private long heapSize;

  /**
   * Default constructor for Writable
   */
  public BlockCacheColumnFamilySummary() {
    
  }
  
  /**
   * 
   * @param table table
   * @param columnFamily columnFamily
   */
  public BlockCacheColumnFamilySummary(String table, String columnFamily) {
    this.table = table;
    this.columnFamily = columnFamily;
  }
  
  /**
   * 
   * @return table
   */
  public String getTable() {
    return table;
  }
  /**
   * 
   * @param table (table that owns the cached block)
   */
  public void setTable(String table) {
    this.table = table;
  }
  /**
   * 
   * @return columnFamily
   */
  public String getColumnFamily() {
    return columnFamily;
  }
  /**
   * 
   * @param columnFamily (columnFamily that owns the cached block)
   */
  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }
  
  /**
   * 
   * @return blocks in the cache
   */
  public int getBlocks() {
    return blocks;
  }
  /**
   * 
   * @param blocks in the cache
   */
  public void setBlocks(int blocks) {
    this.blocks = blocks;
  }
  
  /**
   * 
   * @return heapSize in the cache
   */
  public long getHeapSize() {
    return heapSize;
  }
  
  /**
   * Increments the number of blocks in the cache for this entry
   */
  public void incrementBlocks() {
    this.blocks++;
  }

  /**
   * 
   * @param heapSize to increment
   */
  public void incrementHeapSize(long heapSize) {
    this.heapSize = this.heapSize + heapSize;
  }

  /**
   * 
   * @param heapSize (total heapSize for the table/CF)
   */
  public void setHeapSize(long heapSize) {
    this.heapSize = heapSize;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    table = in.readUTF();
    columnFamily = in.readUTF();
    blocks = in.readInt();
    heapSize = in.readLong();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(table);
    out.writeUTF(columnFamily);
    out.writeInt(blocks);
    out.writeLong(heapSize);
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((columnFamily == null) ? 0 : columnFamily.hashCode());
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BlockCacheColumnFamilySummary other = (BlockCacheColumnFamilySummary) obj;
    if (columnFamily == null) {
      if (other.columnFamily != null)
        return false;
    } else if (!columnFamily.equals(other.columnFamily))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    return true;
  }
  
  
  
  @Override
  public String toString() {
    return "BlockCacheSummaryEntry [table=" + table + ", columnFamily="
        + columnFamily + ", blocks=" + blocks + ", heapSize=" + heapSize + "]";
  }
  
  /**
   * Construct a BlockCacheSummaryEntry from a full StoreFile Path
   * <br><br>
   * The path is expected to be in the format of...
   * <pre>
   * hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   * </pre>
   * ... where: <br>
   *  '-ROOT-' = Table <br>
   * '70236052' = Region <br>
   * 'info' = ColumnFamily <br>
   * '3944417774205889744' = StoreFile
   * 
   * @param path (full StoreFile Path)
   * @return BlockCacheSummaryEntry
   */
  public static BlockCacheColumnFamilySummary createFromStoreFilePath(Path path) {
       
    // The full path will look something like this...
    // hdfs://localhost:51169/user/doug.meil/-ROOT-/70236052/info/3944417774205889744
    //                                        tbl    region   cf   sf
    String sp = path.toString();
    String s[] = sp.split("\\/");

    BlockCacheColumnFamilySummary bcse = null;
    if (s.length >= 4) {
      // why 4?   StoreFile, CF, Region, Table
      String table = s[s.length - 4];  // 4th from the end
      String cf = s[s.length - 2];     // 2nd from the end
      bcse = new BlockCacheColumnFamilySummary(table, cf);
    } 
    return bcse;
  }

  @Override
  public int compareTo(BlockCacheColumnFamilySummary o) {
    int i = table.compareTo(o.getTable());
    if (i != 0) {
      return i;
    } 
    return columnFamily.compareTo(o.getColumnFamily());
  }

  /**
   * Creates a new BlockCacheSummaryEntry
   * 
   * @param e BlockCacheSummaryEntry
   * @return new BlockCacheSummaryEntry
   */
  public static BlockCacheColumnFamilySummary create(BlockCacheColumnFamilySummary e) {
    BlockCacheColumnFamilySummary e2 = new BlockCacheColumnFamilySummary();
    e2.setTable(e.getTable());
    e2.setColumnFamily(e.getColumnFamily());
    return e2;
  }
}
