/*
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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Interface for row and column filters directly applied within the regionserver.
 * A filter can expect the following call sequence:
 *<ul>
 * <li>{@link #reset()}</li>
 * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
 * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row, 
 * if false, we will also call</li>
 * <li>{@link #filterKeyValue(KeyValue)} -> true to drop this key/value</li>
 * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
 * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
 * </li>
 * </ul>
 *
 * Filter instances are created one per region/scan.  This interface replaces
 * the old RowFilterInterface.
 */
public interface Filter extends Writable {
  /**
   * Reset the state of the filter between rows.
   */
  public void reset();

  /**
   * Filters a row based on the row key. If this returns true, the entire
   * row will be excluded.  If false, each KeyValue in the row will be
   * passed to {@link #filterKeyValue(KeyValue)} below.
   *
   * @param buffer buffer containing row key
   * @param offset offset into buffer where row key starts
   * @param length length of the row key
   * @return true, remove entire row, false, include the row (maybe).
   */
  public boolean filterRowKey(byte [] buffer, int offset, int length);

  /**
   * If this returns true, the scan will terminate.
   *
   * @return true to end scan, false to continue.
   */
  public boolean filterAllRemaining();

  /**
   * A way to filter based on the column family, column qualifier and/or the
   * column value. Return code is described below.  This allows filters to
   * filter only certain number of columns, then terminate without matching ever
   * column.
   *
   * If your filter returns <code>ReturnCode.NEXT_ROW</code>, it should return
   * <code>ReturnCode.NEXT_ROW</code> until {@link #reset()} is called
   * just in case the caller calls for the next row.
   *
   * @param v the KeyValue in question
   * @return code as described below
   * @see Filter.ReturnCode
   */
  public ReturnCode filterKeyValue(KeyValue v);

  /**
   * Return codes for filterValue().
   */
  public enum ReturnCode {
    /**
     * Include the KeyValue
     */
    INCLUDE,
    /**
     * Skip this KeyValue
     */
    SKIP,
    /**
     * Done with columns, skip to next row. Note that filterRow() will
     * still be called.
     */
    NEXT_ROW,
  };

  /**
   * Last chance to veto row based on previous {@link #filterKeyValue(KeyValue)}
   * calls. The filter needs to retain state then return a particular value for
   * this call if they wish to exclude a row if a certain column is missing
   * (for example).
   * @return true to exclude row, false to include row.
   */
  public boolean filterRow();
}