/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdfs.DFSUtil;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;

/**
 * Class that maintains Table Configuration.
 */
public class TableConfig {
  private final String name;
  private final ColumnFamilyOptions columnFamilyOptions;


  /**
   * Constructs a Table Config.
   * @param name - Name of the Table.
   * @param columnFamilyOptions - Column Family options.
   */
  public TableConfig(String name, ColumnFamilyOptions columnFamilyOptions) {
    this.name = name;
    this.columnFamilyOptions = columnFamilyOptions;
  }

  /**
   * Returns the Name for this Table.
   * @return - Name String
   */
  public String getName() {
    return name;
  }

  /**
   * Returns a ColumnFamilyDescriptor for this table.
   * @return ColumnFamilyDescriptor
   */
  public ColumnFamilyDescriptor getDescriptor() {
    return  new ColumnFamilyDescriptor(DFSUtil.string2Bytes(name),
        columnFamilyOptions);
  }

  /**
   * Returns Column family options for this Table.
   * @return  ColumnFamilyOptions used for the Table.
   */
  public ColumnFamilyOptions getColumnFamilyOptions() {
    return columnFamilyOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableConfig that = (TableConfig) o;
    return new EqualsBuilder()
        .append(getName(), that.getName())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getName())
        .toHashCode();
  }
}
