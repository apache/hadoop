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
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

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

  static final Log LOG = LogFactory.getLog(RowFilterSet.class);
  
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

  /** {@inheritDoc} */
  public void validate(final Text[] columns) {
    for (RowFilterInterface filter : filters) {
      filter.validate(columns);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Validated subfilter of type " + 
          filter.getClass().getSimpleName());
      }
    }
  }

  /** {@inheritDoc} */
  public void reset() {
    for (RowFilterInterface filter : filters) {
      filter.reset();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reset subfilter of type " + 
          filter.getClass().getSimpleName());
      }
    }
  }

  /** {@inheritDoc} */
  public void rowProcessed(boolean filtered, Text rowKey) {
    for (RowFilterInterface filter : filters) {
      filter.rowProcessed(filtered, rowKey);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Called rowProcessed on subfilter of type " + 
          filter.getClass().getSimpleName());
      }
    }
  }

  /** {@inheritDoc} */
  public boolean processAlways() {
    for (RowFilterInterface filter : filters) {
      if (filter.processAlways()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("processAlways() is true due to subfilter of type " + 
            filter.getClass().getSimpleName());
        }
        return true;
      }
    }
    return false;
  }
  
  /** {@inheritDoc} */
  public boolean filterAllRemaining() {
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("op.MPALL filterAllRemaining returning true due" + 
              " to subfilter of type " + filter.getClass().getSimpleName());
          }
          return true;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("op.MPONE filterAllRemaining returning false due" + 
              " to subfilter of type " + filter.getClass().getSimpleName());
          }
          return false;
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("filterAllRemaining default returning " + result);
    }
    return result;
  }

  /** {@inheritDoc} */
  public boolean filter(final Text rowKey) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() || filter.filter(rowKey)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("op.MPALL filter(Text) will return true due" + 
                " to subfilter of type " + filter.getClass().getSimpleName());
            }
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() && !filter.filter(rowKey)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("op.MPONE filter(Text) will return false due" + 
                " to subfilter of type " + filter.getClass().getSimpleName());
            }
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filter(rowKey);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("filter(Text) returning " + result);
    }
    return result;
  }

  /** {@inheritDoc} */
  public boolean filter(final Text rowKey, final Text colKey, 
    final byte[] data) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() || 
            filter.filter(rowKey, colKey, data)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("op.MPALL filter(Text, Text, byte[]) will" + 
                " return true due to subfilter of type " + 
                filter.getClass().getSimpleName());
            }
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() && 
            !filter.filter(rowKey, colKey, data)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("op.MPONE filter(Text, Text, byte[]) will" + 
                " return false due to subfilter of type " + 
                filter.getClass().getSimpleName());
            }
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filter(rowKey, colKey, data);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("filter(Text, Text, byte[]) returning " + result);
    }
    return result;
  }

  /** {@inheritDoc} */
  public boolean filterNotNull(final SortedMap<Text, byte[]> columns) {
    boolean resultFound = false;
    boolean result = operator == Operator.MUST_PASS_ONE;
    for (RowFilterInterface filter : filters) {
      if (!resultFound) {
        if (operator == Operator.MUST_PASS_ALL) {
          if (filter.filterAllRemaining() || filter.filterNotNull(columns)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("op.MPALL filterNotNull will return true due" + 
                " to subfilter of type " + filter.getClass().getSimpleName());
            }
            result = true;
            resultFound = true;
          }
        } else if (operator == Operator.MUST_PASS_ONE) {
          if (!filter.filterAllRemaining() && !filter.filterNotNull(columns)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("op.MPONE filterNotNull will return false due" + 
                " to subfilter of type " + filter.getClass().getSimpleName());
            }
            result = false;
            resultFound = true;
          }
        }
      } else if (filter.processAlways()) {
        filter.filterNotNull(columns);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("filterNotNull returning " + result);
    }
    return result;
  }

  /** {@inheritDoc} */
  public void readFields(final DataInput in) throws IOException {
    byte opByte = in.readByte();
    operator = Operator.values()[opByte];
    int size = in.readInt();
    if (size > 0) {
      filters = new HashSet<RowFilterInterface>();
      try {
        for (int i = 0; i < size; i++) {
          String className = in.readUTF();
          Class<?> clazz = Class.forName(className);
          RowFilterInterface filter;
          filter = (RowFilterInterface) clazz.newInstance();
          filter.readFields(in);
          filters.add(filter);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully read in subfilter of type " + 
              filter.getClass().getSimpleName());
          }
        }
      } catch (InstantiationException e) {
        throw new RuntimeException("Failed to deserialize RowFilterInterface.",
            e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to deserialize RowFilterInterface.",
            e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Failed to deserialize RowFilterInterface.",
            e);
      }
    }

  }

  /** {@inheritDoc} */
  public void write(final DataOutput out) throws IOException {
    out.writeByte(operator.ordinal());
    out.writeInt(filters.size());
    for (RowFilterInterface filter : filters) {
      out.writeUTF(filter.getClass().getName());
      filter.write(out);
    }
  }

}
