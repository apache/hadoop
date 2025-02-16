/**
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

package org.apache.hadoop.fs.azurebfs;

import org.mockito.invocation.InvocationOnMock;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;

/**
 * Interface used to intercept and customize the behavior of mocked
 * `AbfsRestOperation` objects. The implementing class should define
 * how to handle the mock operation when it is invoked.
 *
 * @param <T> the type of the mocked object, typically an `AbfsRestOperation`
 */
public interface MockIntercept<T> {

  /**
   * Defines custom behavior for handling the mocked object during its execution.
   *
   * @param mockedObj the mocked `AbfsRestOperation` object
   * @param answer the invocation details for the mock method
   * @throws AbfsRestOperationException if an error occurs during the
   * mock operation handling
   */
  void answer(T mockedObj, InvocationOnMock answer) throws AbfsRestOperationException;
}
