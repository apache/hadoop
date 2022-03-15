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

package org.apache.hadoop.fs.impl;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Support for functional programming/lambda-expressions.
 * @deprecated use {@code org.apache.hadoop.util.functional}
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class FunctionsRaisingIOE {

  private FunctionsRaisingIOE() {
  }

  /**
   * Function of arity 1 which may raise an IOException.
   * @param <T> type of arg1
   * @param <R> type of return value.
   * @deprecated use {@link org.apache.hadoop.util.functional.FunctionRaisingIOE}
   */
  @FunctionalInterface
  public interface FunctionRaisingIOE<T, R> {

    R apply(T t) throws IOException;
  }

  /**
   * Function of arity 2 which may raise an IOException.
   * @param <T> type of arg1
   * @param <U> type of arg2
   * @param <R> type of return value.
   * @deprecated use {@link org.apache.hadoop.util.functional.BiFunctionRaisingIOE}
   */
  @FunctionalInterface
  public interface BiFunctionRaisingIOE<T, U, R> {

    R apply(T t, U u) throws IOException;
  }

  /**
   * This is a callable which only raises an IOException.
   * @param <R> return type
   * @deprecated use {@link org.apache.hadoop.util.functional.CallableRaisingIOE}
   */
  @FunctionalInterface
  public interface CallableRaisingIOE<R> {

    R apply() throws IOException;
  }

}
