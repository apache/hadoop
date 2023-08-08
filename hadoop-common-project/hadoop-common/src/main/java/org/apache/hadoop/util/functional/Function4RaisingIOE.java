/*
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

package org.apache.hadoop.util.functional;

import java.io.IOException;

/**
 * Function of arity 4 which may raise an IOException.
 * @param <I1> type of arg1.
 * @param <I2> type of arg2.
 * @param <I3> type of arg3.
 * @param <I4> type of arg4.
 * @param <R> return type.
 */
public interface Function4RaisingIOE<I1, I2, I3, I4, R> {

  /**
   * Apply the function.
   * @param i1 argument 1.
   * @param i2 argument 2.
   * @param i3 argument 3.
   * @param i4 argument 4.
   * @return return value.
   * @throws IOException any IOE.
   */
  R apply(I1 i1, I2 i2, I3 i3, I4 i4) throws IOException;
}
