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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.ObjectWritable;

/**
 * Implementation of RowFilterInterface that represents a set of RowFilters
 * which will be evaluated with a specified boolean operator MUST_PASS_ALL 
 * (!AND) or MUST_PASS_ONE (!OR).  Since you can use RowFilterSets as children 
 * of RowFilterSet, you can create a hierarchy of filters to be evaluated.
 */
public class RowFilterSet implements RowFilterInterface {

  /** set operator */
  public static enum Operator {
    /** !AND */
    MUST_PASS_ALL,
    /** !OR */
    MUST_PASS_ONE
  }

  private Operator operator = Operator.MUST_PASS_ALL;
  private Set<RowFilterInterface> filters = new HashSet<RowFilterInterface>();

  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public RowFilterSet() {
    super();
  }

  /**
   * Constructor that takes a set of RowFilters. The default operator 
   * MUST_PASS_ALL is assumed.
   * 
   * @param rowFilters
   */
  public RowFilterSet(final Set<RowFilterInterface> rowFilters) {
    this.filters = rowFilters;
  }

  /**
   * Constructor that takes a set of RowFilters and an operator.
   * 
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public RowFilterSet(final Operator operator,
      final Set<RowFilterInterface> rowFilters) {
    this.filters = rowFilters;
    this.operator = operator;
  }

  /** Get the operator.
   * 
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }
  
  /** Get the filters.
   * 
   * @return filters
   */
  public Set<RowFilterInterface> getFilters() {
    return filters;
  }
  
  /** Add a filter.
   * 
   * @param filter
   */
  public void addFilter(RowFilterInterface filter) {
    this.filters.add(filter);
  }
  
  public void validate(final byte [][] columns) {
    for (RowFilterInterface filter : filters) {
      filter.validate(columns);
    }
  }

  public void reset() {
    for (RowFilterInterface filter : filters) {
      filter.reset();
    }
  }

  public void rowProcessed(boolean filtered, byte [] rowKey) {
    rowProcessed(filtered, rowKey, 0, rowKey.length);
  }

  public void rowProcessed(boolean filtered, byte[] key, int offset, int length) {
    for (RowFilterInterface filter : filters) {
      filter.rowProcessed(filtered, key, offset, length);
    }
  }

  public boolean processAlways() {
    for (RowFilterInterface filter : filters) {
      if (filter.processAlways()) {
        return true;
      }
    }
    return false;
  }
  
  public boolean filterAllRemaining() {
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining()) {
          return true;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining()) {
          return false;
        }
      }
    }
    return result;
  }

  public boolean filterRowKey(final byte [] rowKey) {
    return filterRowKey(rowKey, 0, rowKey.length);
  }


  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() ||
              filter.filterRowKey(rowKey, offset, length)) {
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() &&
              !filter.filterRowKey(rowKey, offset, length)) {
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filterRowKey(rowKey, offset, length);
      }
    }
    return result;
  }

  public boolean filterColumn(final byte [] rowKey, final byte [] colKey, 
    final byte[] data) {
    return filterColumn(rowKey, 0, rowKey.length, colKey, 0, colKey.length,
        data, 0, data.length);
  }

  public boolean filterColumn(byte[] rowKey, int roffset, int rlength,
      byte[] columnName, int coffset, int clength, byte[] columnValue,
      int voffset, int vlength) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() || 
            filter.filterColumn(rowKey, roffset, rlength, columnName, coffset,
                clength, columnValue, voffset, vlength)) {
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() && 
            !filter.filterColumn(rowKey, roffset, rlength, columnName, coffset,
                clength, columnValue, voffset, vlength)) {
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filterColumn(rowKey, roffset, rlength, columnName, coffset,
            clength, columnValue, voffset, vlength);
      }
    }
    return result;
  }

  public boolean filterRow(final SortedMap<byte [], Cell> columns) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() || filter.filterRow(columns)) {
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() && !filter.filterRow(columns)) {
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filterRow(columns);
      }
    }
    return result;
  }

  public boolean filterRow(List<KeyValue> results) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() || filter.filterRow(results)) {
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() && !filter.filterRow(results)) {
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filterRow(results);
      }
    }
    return result;
  }

  public void readFields(final DataInput in) throws IOException {
    Configuration conf = new HBaseConfiguration();
    byte opByte = in.readByte();
    operator = Operator.values()[opByte];
    int size = in.readInt();
    if (size > 0) {
      filters = new HashSet<RowFilterInterface>();
      for (int i = 0; i < size; i++) {
	RowFilterInterface filter = (RowFilterInterface) ObjectWritable
	    .readObject(in, conf);
	filters.add(filter);
      }
    }
  }

  public void write(final DataOutput out) throws IOException {
    Configuration conf = new HBaseConfiguration();
    out.writeByte(operator.ordinal());
    out.writeInt(filters.size());
    for (RowFilterInterface filter : filters) {
      ObjectWritable.writeObject(out, filter, RowFilterInterface.class, conf);
    }
  }
}