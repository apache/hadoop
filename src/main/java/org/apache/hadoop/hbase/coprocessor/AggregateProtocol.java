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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Defines the aggregation functions that are to be supported in this
 * Coprocessor. For each method, it takes a Scan object and a columnInterpreter.
 * The scan object should have a column family (else an exception will be
 * thrown), and an optional column qualifier. In the current implementation
 * {@link AggregateImplementation}, only one column family and column qualifier
 * combination is served. In case there are more than one, only first one will
 * be picked. Refer to {@link AggregationClient} for some general conditions on
 * input parameters.
 */
public interface AggregateProtocol extends CoprocessorProtocol {

  /**
   * Gives the maximum for a given combination of column qualifier and column
   * family, in the given row range as defined in the Scan object. In its
   * current implementation, it takes one column family and one column qualifier
   * (if provided). In case of null column qualifier, maximum value for the
   * entire column family will be returned.
   * @param ci
   * @param scan
   * @return max value as mentioned above
   * @throws IOException
   */
  <T, S> T getMax(ColumnInterpreter<T, S> ci, Scan scan) throws IOException;

  /**
   * Gives the minimum for a given combination of column qualifier and column
   * family, in the given row range as defined in the Scan object. In its
   * current implementation, it takes one column family and one column qualifier
   * (if provided). In case of null column qualifier, minimum value for the
   * entire column family will be returned.
   * @param ci
   * @param scan
   * @return min as mentioned above
   * @throws IOException
   */
  <T, S> T getMin(ColumnInterpreter<T, S> ci, Scan scan) throws IOException;

  /**
   * Gives the sum for a given combination of column qualifier and column
   * family, in the given row range as defined in the Scan object. In its
   * current implementation, it takes one column family and one column qualifier
   * (if provided). In case of null column qualifier, sum for the entire column
   * family will be returned.
   * @param ci
   * @param scan
   * @return sum of values as defined by the column interpreter
   * @throws IOException
   */
  <T, S> S getSum(ColumnInterpreter<T, S> ci, Scan scan) throws IOException;

  /**
   * @param ci
   * @param scan
   * @return Row count for the given column family and column qualifier, in
   * the given row range as defined in the Scan object.
   * @throws IOException
   */
  <T, S> long getRowNum(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException;

  /**
   * Gives a Pair with first object as Sum and second object as row count,
   * computed for a given combination of column qualifier and column family in
   * the given row range as defined in the Scan object. In its current
   * implementation, it takes one column family and one column qualifier (if
   * provided). In case of null column qualifier, an aggregate sum over all the
   * entire column family will be returned.
   * <p>
   * The average is computed in
   * {@link AggregationClient#avg(byte[], ColumnInterpreter, Scan)} by
   * processing results from all regions, so its "ok" to pass sum and a Long
   * type.
   * @param ci
   * @param scan
   * @return Average
   * @throws IOException
   */
  <T, S> Pair<S, Long> getAvg(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException;

  /**
   * Gives a Pair with first object a List containing Sum and sum of squares,
   * and the second object as row count. It is computed for a given combination of
   * column qualifier and column family in the given row range as defined in the
   * Scan object. In its current implementation, it takes one column family and
   * one column qualifier (if provided). The idea is get the value of variance first:
   * the average of the squares less the square of the average a standard
   * deviation is square root of variance.
   * @param ci
   * @param scan
   * @return STD
   * @throws IOException
   */
  <T, S> Pair<List<S>, Long> getStd(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException;

}