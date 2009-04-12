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

import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.Writable;

/**
 * 
 * Interface used for row-level filters applied to HRegion.HScanner scan
 * results during calls to next().
 * TODO: Make Filters use proper comparator comparing rows.
 */
public interface RowFilterInterface extends Writable {
  /**
   * Resets the state of the filter. Used prior to the start of a Region scan.
   * 
   */
  void reset();

  /**
   * Called to let filter know the final decision (to pass or filter) on a 
   * given row.  With out HScanner calling this, the filter does not know if a 
   * row passed filtering even if it passed the row itself because other 
   * filters may have failed the row. E.g. when this filter is a member of a 
   * RowFilterSet with an OR operator.
   * 
   * @see RowFilterSet
   * @param filtered
   * @param key
   * @deprecated Use {@link #rowProcessed(boolean, byte[], int, int)} instead.
   */
  void rowProcessed(boolean filtered, byte [] key);

  /**
   * Called to let filter know the final decision (to pass or filter) on a 
   * given row.  With out HScanner calling this, the filter does not know if a 
   * row passed filtering even if it passed the row itself because other 
   * filters may have failed the row. E.g. when this filter is a member of a 
   * RowFilterSet with an OR operator.
   * 
   * @see RowFilterSet
   * @param filtered
   * @param key
   * @param offset
   * @param length
   */
  void rowProcessed(boolean filtered, byte [] key, int offset, int length);

  /**
   * Returns whether or not the filter should always be processed in any 
   * filtering call.  This precaution is necessary for filters that maintain 
   * state and need to be updated according to their response to filtering 
   * calls (see WhileMatchRowFilter for an example).  At times, filters nested 
   * in RowFilterSets may or may not be called because the RowFilterSet 
   * determines a result as fast as possible.  Returning true for 
   * processAlways() ensures that the filter will always be called.
   * 
   * @return whether or not to always process the filter
   */
  boolean processAlways();
  
  /**
   * Determines if the filter has decided that all remaining results should be
   * filtered (skipped). This is used to prevent the scanner from scanning a
   * the rest of the HRegion when for sure the filter will exclude all
   * remaining rows.
   * 
   * @return true if the filter intends to filter all remaining rows.
   */
  boolean filterAllRemaining();

  /**
   * Filters on just a row key. This is the first chance to stop a row.
   * 
   * @param rowKey
   * @return true if given row key is filtered and row should not be processed.
   * @deprecated Use {@link #filterRowKey(byte[], int, int)} instead.
   */
  boolean filterRowKey(final byte [] rowKey);

  /**
   * Filters on just a row key. This is the first chance to stop a row.
   * 
   * @param rowKey
   * @param offset
   * @param length
   * @return true if given row key is filtered and row should not be processed.
   */
  boolean filterRowKey(final byte [] rowKey, final int offset, final int length);

  /**
   * Filters on row key, column name, and column value. This will take individual columns out of a row, 
   * but the rest of the row will still get through.
   * 
   * @param rowKey row key to filter on.
   * @param colunmName column name to filter on
   * @param columnValue column value to filter on
   * @return true if row filtered and should not be processed.
   * @deprecated Use {@link #filterColumn(byte[], int, int, byte[], int, int, byte[], int, int)}
   * instead.
   */
  boolean filterColumn(final byte [] rowKey, final byte [] columnName,
      final byte [] columnValue);

  /**
   * Filters on row key, column name, and column value. This will take individual columns out of a row, 
   * but the rest of the row will still get through.
   * 
   * @param rowKey row key to filter on.
   * @param colunmName column name to filter on
   * @param columnValue column value to filter on
   * @return true if row filtered and should not be processed.
   */
  boolean filterColumn(final byte [] rowKey, final int roffset,
      final int rlength, final byte [] colunmName, final int coffset,
      final int clength, final byte [] columnValue, final int voffset,
      final int vlength);

  /**
   * Filter on the fully assembled row. This is the last chance to stop a row. 
   * 
   * @param columns
   * @return true if row filtered and should not be processed.
   */
  boolean filterRow(final SortedMap<byte [], Cell> columns);

  /**
   * Filter on the fully assembled row. This is the last chance to stop a row. 
   * 
   * @param results
   * @return true if row filtered and should not be processed.
   */
  boolean filterRow(final List<KeyValue> results);

  /**
   * Validates that this filter applies only to a subset of the given columns.
   * This check is done prior to opening of scanner due to the limitation that
   * filtering of columns is dependent on the retrieval of those columns within
   * the HRegion. Criteria on columns that are not part of a scanner's column
   * list will be ignored. In the case of null value filters, all rows will pass
   * the filter. This behavior should be 'undefined' for the user and therefore
   * not permitted.
   * 
   * @param columns
   */
  void validate(final byte [][] columns);
}