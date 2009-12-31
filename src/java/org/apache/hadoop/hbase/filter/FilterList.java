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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of {@link Filter} that represents an ordered List of Filters
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL} 
 * (<code>!AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>!OR</code>).
 * Since you can use Filter Lists as children of Filter Lists, you can create a
 * hierarchy of filters to be evaluated.
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 * <p>TODO: Fix creation of Configuration on serialization and deserialization. 
 */
public class FilterList implements Filter {
  /** set operator */
  public static enum Operator {
    /** !AND */
    MUST_PASS_ALL,
    /** !OR */
    MUST_PASS_ONE
  }

  private Operator operator = Operator.MUST_PASS_ALL;
  private List<Filter> filters = new ArrayList<Filter>();

  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public FilterList() {
    super();
  }

  /**
   * Constructor that takes a set of {@link Filter}s. The default operator
   * MUST_PASS_ALL is assumed.
   * 
   * @param rowFilters
   */
  public FilterList(final List<Filter> rowFilters) {
    this.filters = rowFilters;
  }

  /**
   * Constructor that takes an operator.
   * 
   * @param operator Operator to process filter set with.
   */
  public FilterList(final Operator operator) {
    this.operator = operator;
  }

  /**
   * Constructor that takes a set of {@link Filter}s and an operator.
   * 
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public FilterList(final Operator operator, final List<Filter> rowFilters) {
    this.filters = rowFilters;
    this.operator = operator;
  }

  /**
   * Get the operator.
   * 
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Get the filters.
   * 
   * @return filters
   */
  public List<Filter> getFilters() {
    return filters;
  }

  /**
   * Add a filter.
   * 
   * @param filter
   */
  public void addFilter(Filter filter) {
    this.filters.add(filter);
  }

  public void reset() {
    for (Filter filter : filters) {
      filter.reset();
    }
  }

  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    for (Filter filter : filters) {
      if (this.operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() ||
            filter.filterRowKey(rowKey, offset, length)) {
          return true;
        }
      } else if (this.operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining() &&
            !filter.filterRowKey(rowKey, offset, length)) {
          return false;
        }
      }
    }
    return this.operator == Operator.MUST_PASS_ONE;
  }

  public boolean filterAllRemaining() {
    for (Filter filter : filters) {
      if (filter.filterAllRemaining()) {
        if (operator == Operator.MUST_PASS_ALL) {
          return true;
        }
      } else {
        if (operator == Operator.MUST_PASS_ONE) {
          return false;
        }
      }
    }
    return operator == Operator.MUST_PASS_ONE;
  }

  public ReturnCode filterKeyValue(KeyValue v) {
    for (Filter filter : filters) {
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining()) {
          return ReturnCode.NEXT_ROW;
        }
        switch (filter.filterKeyValue(v)) {
        case INCLUDE:
          continue;
        case NEXT_ROW:
        case SKIP:
          return ReturnCode.SKIP;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (filter.filterAllRemaining()) {
          continue;
        }

        switch (filter.filterKeyValue(v)) {
        case INCLUDE:
          return ReturnCode.INCLUDE;
        case NEXT_ROW:
        case SKIP:
          continue;
        }
      }
    }
    return operator == Operator.MUST_PASS_ONE?
      ReturnCode.SKIP: ReturnCode.INCLUDE;
  }

  public boolean filterRow() {
    for (Filter filter : filters) {
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() || filter.filterRow()) {
          return true;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining()
            && !filter.filterRow()) {
          return false;
        }
      }
    }
    return  operator == Operator.MUST_PASS_ONE;
  }

  public void readFields(final DataInput in) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    byte opByte = in.readByte();
    operator = Operator.values()[opByte];
    int size = in.readInt();
    if (size > 0) {
      filters = new ArrayList<Filter>(size);
      for (int i = 0; i < size; i++) {
        Filter filter = (Filter)HbaseObjectWritable.readObject(in, conf);
        filters.add(filter);
      }
    }
  }

  public void write(final DataOutput out) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    out.writeByte(operator.ordinal());
    out.writeInt(filters.size());
    for (Filter filter : filters) {
      HbaseObjectWritable.writeObject(out, filter, Writable.class, conf);
    }
  }
}