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
package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A table split corresponds to a key range (low, high). All references to row
 * below refer to the key of the row.
 */
public class TableSplit extends InputSplit
implements Writable, Comparable<TableSplit> {

  private byte [] tableName;
  private byte [] startRow;
  private byte [] endRow;
  private String regionLocation;

  /** Default constructor. */
  public TableSplit() {
    this(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.EMPTY_BYTE_ARRAY, "");
  }

  /**
   * Creates a new instance while assigning all variables.
   *
   * @param tableName  The name of the current table.
   * @param startRow  The start row of the split.
   * @param endRow  The end row of the split.
   * @param location  The location of the region.
   */
  public TableSplit(byte [] tableName, byte [] startRow, byte [] endRow,
      final String location) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.endRow = endRow;
    this.regionLocation = location;
  }

  /**
   * Returns the table name.
   *
   * @return The table name.
   */
  public byte [] getTableName() {
    return tableName;
  }

  /**
   * Returns the start row.
   *
   * @return The start row.
   */
  public byte [] getStartRow() {
    return startRow;
  }

  /**
   * Returns the end row.
   *
   * @return The end row.
   */
  public byte [] getEndRow() {
    return endRow;
  }

  /**
   * Returns the region location.
   *
   * @return The region's location.
   */
  public String getRegionLocation() {
    return regionLocation;
  }

  /**
   * Returns the region's location as an array.
   *
   * @return The array containing the region location.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  @Override
  public String[] getLocations() {
    return new String[] {regionLocation};
  }

  /**
   * Returns the length of the split.
   *
   * @return The length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  @Override
  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }

  /**
   * Reads the values of each field.
   *
   * @param in  The input to read from.
   * @throws IOException When reading the input fails.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    tableName = Bytes.readByteArray(in);
    startRow = Bytes.readByteArray(in);
    endRow = Bytes.readByteArray(in);
    regionLocation = Bytes.toString(Bytes.readByteArray(in));
  }

  /**
   * Writes the field values to the output.
   *
   * @param out  The output to write to.
   * @throws IOException When writing the values to the output fails.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, tableName);
    Bytes.writeByteArray(out, startRow);
    Bytes.writeByteArray(out, endRow);
    Bytes.writeByteArray(out, Bytes.toBytes(regionLocation));
  }

  /**
   * Returns the details about this instance as a string.
   *
   * @return The values of this instance as a string.
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return regionLocation + ":" +
      Bytes.toStringBinary(startRow) + "," + Bytes.toStringBinary(endRow);
  }

  /**
   * Compares this split against the given one.
   *
   * @param split  The split to compare to.
   * @return The result of the comparison.
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(TableSplit split) {
    return Bytes.compareTo(getStartRow(), split.getStartRow());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof TableSplit)) {
      return false;
    }
    return Bytes.equals(tableName, ((TableSplit)o).tableName) &&
      Bytes.equals(startRow, ((TableSplit)o).startRow) &&
      Bytes.equals(endRow, ((TableSplit)o).endRow) &&
      regionLocation.equals(((TableSplit)o).regionLocation);
  }

    @Override
    public int hashCode() {
        int result = tableName != null ? Arrays.hashCode(tableName) : 0;
        result = 31 * result + (startRow != null ? Arrays.hashCode(startRow) : 0);
        result = 31 * result + (endRow != null ? Arrays.hashCode(endRow) : 0);
        result = 31 * result + (regionLocation != null ? regionLocation.hashCode() : 0);
        return result;
    }
}
