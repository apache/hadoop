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

import java.util.TreeMap;

import org.apache.hadoop.hbase.HRegion;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * Interface used for row-level filters applied to HRegion.HScanner scan
 * results during calls to next().
 */
public interface RowFilterInterface extends Writable {

  /**
   * Resets the state of the filter. Used prior to the start of a Region scan.
   * 
   */
  void reset();

  /**
   * Called to let filter know that the specified row has been included in the
   * results (passed all filtering). With out HScanner calling this, the filter
   * does not know if a row passed filtering even if it passed the row itself
   * because other filters may have failed the row. E.g. when this filter is a
   * member of a RowFilterSet with an OR operator.
   * 
   * @see RowFilterSet
   * @param key
   */
  void acceptedRow(final Text key);

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
   * Filters on just a row key.
   * 
   * @param rowKey
   * @return true if given row key is filtered and row should not be processed.
   */
  boolean filter(final Text rowKey);

  /**
   * Filters on row key and/or a column key.
   * 
   * @param rowKey
   *          row key to filter on. May be null for no filtering of row key.
   * @param colKey
   *          column whose data will be filtered
   * @param data
   *          column value
   * @return true if row filtered and should not be processed.
   */
  boolean filter(final Text rowKey, final Text colKey, final byte[] data);

  /**
   * Filters row if given columns are non-null and have null criteria or if
   * there exists criteria on columns not included in the column set. A column
   * is considered null if it:
   * <ul>
   * <li>Is not included in the given columns.</li>
   * <li>Has a value of HConstants.DELETE_BYTES</li>
   * </ul>
   * 
   * @param columns
   * @return true if null/non-null criteria not met.
   */
  boolean filterNotNull(final TreeMap<Text, byte[]> columns);

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
  void validate(final Text[] columns);
}