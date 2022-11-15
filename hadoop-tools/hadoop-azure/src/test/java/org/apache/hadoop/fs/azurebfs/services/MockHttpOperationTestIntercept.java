/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

/**
 * Interface to be instantiated with different behaviours by the test-classes
 * when using following spy of:
 * <ol>
 *   <li>{@link org.apache.hadoop.fs.azurebfs.services.AbfsClient}</li>
 *   <li>{@link org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation}</li>
 *   <li>{@link org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation}</li>
 * </ol>
 * */
public interface MockHttpOperationTestIntercept {

  /**
   * Called by spy of {@link org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation#processResponse(byte[], int, int)}
   * Implementation can define how the mocking has to be done for communication
   * between the client and the backend.
   * @return {@link org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult}
   * */
  MockHttpOperationTestInterceptResult intercept(AbfsHttpOperation abfsHttpOperation,
      byte[] buffer,
      int offset,
      int length) throws IOException;

  /**
   * @return times the server-client mock-communication was invoked.
   * */
  int getCallCount();
}
