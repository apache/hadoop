/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.io.Writable;

/**
 * Defines how value for specific column is interpreted and provides utility
 * methods like compare, add, multiply etc for them. Takes column family, column
 * qualifier and return the cell value. Its concrete implementation should
 * handle null case gracefully. Refer to {@link LongColumnInterpreter} for an
 * example.
 * <p>
 * Takes two generic parameters. The cell value type of the interpreter is <T>.
 * During some computations like sum, average, the return type can be different
 * than the cell value data type, for eg, sum of int cell values might overflow
 * in case of a int result, we should use Long for its result. Therefore, this
 * class mandates to use a different (promoted) data type for result of these
 * computations <S>. All computations are performed on the promoted data type
 * <S>. There is a conversion method
 * {@link ColumnInterpreter#castToReturnType(Object)} which takes a <T> type and
 * returns a <S> type.
 * @param <T, S>: T - cell value data type, S - promoted data type
 */
public interface ColumnInterpreter<T, S> extends Writable {

  /**
   * @param colFamily
   * @param colQualifier
   * @param value
   * @return value of type T
   * @throws IOException
   */
  T getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException;

  /**
   * returns sum or non null value among (if either of them is null); otherwise
   * returns a null.
   * @param l1
   * @param l2
   * @return
   */
  public S add(S l1, S l2);

  /**
   * returns the maximum value for this type T
   * @return
   */

  T getMaxValue();

  /**
   * @return
   */

  T getMinValue();

  /**
   * @param o1
   * @param o2
   * @return
   */
  S multiply(S o1, S o2);

  /**
   * @param o
   * @return
   */
  S increment(S o);

  /**
   * provides casting opportunity between the data types.
   * @param o
   * @return
   */
  S castToReturnType(T o);

  /**
   * This takes care if either of arguments are null. returns 0 if they are
   * equal or both are null;
   * <ul>
   * <li>>0 if l1 > l2 or l1 is not null and l2 is null.
   * <li>< 0 if l1 < l2 or l1 is null and l2 is not null.
   */
  int compare(final T l1, final T l2);

  /**
   * used for computing average of <S> data values. Not providing the divide
   * method that takes two <S> values as it si not needed as of now.
   * @param o
   * @param l
   * @return
   */
  double divideForAvg(S o, Long l);
}