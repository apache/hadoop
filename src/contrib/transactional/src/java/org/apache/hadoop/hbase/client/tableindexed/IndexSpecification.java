/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.tableindexed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** Holds the specification for a single secondary index. */
public class IndexSpecification implements Writable {

  // Columns that are indexed (part of the indexRowKey)
  private byte[][] indexedColumns;

  // Constructs the
  private IndexKeyGenerator keyGenerator;

  // Additional columns mapped into the indexed row. These will be available for
  // filters when scanning the index.
  private byte[][] additionalColumns;

  private byte[][] allColumns;

  // Id of this index, unique within a table.
  private String indexId;

  /** Construct an "simple" index spec for a single column. 
   * @param indexId 
   * @param indexedColumn
   */
  public IndexSpecification(String indexId, byte[] indexedColumn) {
    this(indexId, new byte[][] { indexedColumn }, null,
        new SimpleIndexKeyGenerator(indexedColumn));
  }

  /**Construct an index spec for a single column that has only unique values.
   * @param indexId the name of the index
   * @param indexedColumn the column to index
   * @return the IndexSpecification
   */
  public static IndexSpecification forUniqueIndex(String indexId, byte[] indexedColumn) {
    return new IndexSpecification(indexId, new byte[][] { indexedColumn },
        null, new UniqueIndexKeyGenerator(indexedColumn));
  }

  /**
   * Construct an index spec by specifying everything.
   * 
   * @param indexId
   * @param indexedColumns
   * @param additionalColumns
   * @param keyGenerator
   */
  public IndexSpecification(String indexId, byte[][] indexedColumns,
      byte[][] additionalColumns, IndexKeyGenerator keyGenerator) {
    this.indexId = indexId;
    this.indexedColumns = indexedColumns;
    this.additionalColumns = additionalColumns;
    this.keyGenerator = keyGenerator;
    this.makeAllColumns();
  }

  public IndexSpecification() {
    // For writable
  }

  private void makeAllColumns() {
    this.allColumns = new byte[indexedColumns.length
        + (additionalColumns == null ? 0 : additionalColumns.length)][];
    System.arraycopy(indexedColumns, 0, allColumns, 0, indexedColumns.length);
    if (additionalColumns != null) {
      System.arraycopy(additionalColumns, 0, allColumns, indexedColumns.length,
          additionalColumns.length);
    }
  }
  
  /**
   * Get the indexedColumns.
   * 
   * @return Return the indexedColumns.
   */
  public byte[][] getIndexedColumns() {
    return indexedColumns;
  }

  /**
   * Get the keyGenerator.
   * 
   * @return Return the keyGenerator.
   */
  public IndexKeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  /**
   * Get the additionalColumns.
   * 
   * @return Return the additionalColumns.
   */
  public byte[][] getAdditionalColumns() {
    return additionalColumns;
  }

  /**
   * Get the indexId.
   * 
   * @return Return the indexId.
   */
  public String getIndexId() {
    return indexId;
  }

  public byte[][] getAllColumns() {
    return allColumns;
  }

  public boolean containsColumn(byte[] column) {
    for (byte[] col : allColumns) {
      if (Bytes.equals(column, col)) {
        return true;
      }
    }
    return false;
  }

  public byte[] getIndexedTableName(byte[] baseTableName) {
    return Bytes.add(baseTableName, Bytes.toBytes("-" + indexId));
  }

  private static final HBaseConfiguration CONF = new HBaseConfiguration();
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    indexId = in.readUTF();
    int numIndexedCols = in.readInt();
    indexedColumns = new byte[numIndexedCols][];
    for (int i = 0; i < numIndexedCols; i++) {
      indexedColumns[i] = Bytes.readByteArray(in);
    }
    int numAdditionalCols = in.readInt();
    additionalColumns = new byte[numAdditionalCols][];
    for (int i = 0; i < numAdditionalCols; i++) {
      additionalColumns[i] = Bytes.readByteArray(in);
    }
    makeAllColumns();
    keyGenerator = (IndexKeyGenerator) ObjectWritable.readObject(in, CONF);
    
    // FIXME this is to read the deprecated comparator, in existing data
    ObjectWritable.readObject(in, CONF);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(indexId);
    out.writeInt(indexedColumns.length);
    for (byte[] col : indexedColumns) {
      Bytes.writeByteArray(out, col);
    }
    if (additionalColumns != null) {
      out.writeInt(additionalColumns.length);
      for (byte[] col : additionalColumns) {
        Bytes.writeByteArray(out, col);
      }
    } else {
      out.writeInt(0);
    }
    ObjectWritable
        .writeObject(out, keyGenerator, IndexKeyGenerator.class, CONF);
    
    // FIXME need to maintain this for exisitng data
    ObjectWritable.writeObject(out, null, WritableComparable.class,
        CONF);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ID => ");
    sb.append(indexId);
    return sb.toString();
  }
  
  
}
