/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router.async;

import java.io.IOException;

/**
 * The {@code BiFunction} interface is a functional interface that represents a function
 * that accepts two arguments and produces a result. This interface is primarily used
 * in methods that expect a function that performs some operation on two inputs and returns
 * an output.
 *
 * <p>A {@code BiFunction} should be used where a function with two arguments is required.
 * It can be implemented explicitly or a lambda expression can be provided to create an
 * instance of {@code BiFunction}.</p>
 *
 * <p>For example, the following code demonstrates the use of a lambda expression
 * to create a {@code BiFunction} that adds two integers:</p>
 *
 * <pre>
 * {@code BiFunction<Integer, Integer, Integer> sum = (a, b) -> a + b;}
 * </pre>
 *
 * <p>Or, it can be used in a method that expects a {@code BiFunction}:</p>
 *
 * <pre>
 * {@code someMethod.acceptBiFunction((a, b) -> a * b);}
 * </pre>
 *
 * @param <T> the type of the first argument to the function
 * @param <P> the type of the second argument to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface BiFunction<T, P, R> {

  /**
   * Applies this function to the given arguments.
   *
   * @param t the first function argument
   * @param p the second function argument
   * @return the result of applying the function
   * @throws IOException if an I/O error occurs
   */
  R apply(T t, P p) throws IOException;
}
