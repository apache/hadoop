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

import org.mockito.invocation.InvocationOnMock;

/**
 * Implementation of this class would define how the mockito's doAnswer() function
 * should work on the mocked/spied object.
 * <p>
 *   Mockito.doAnswer(answer -> {<br>
 *      &emsp;mockClassInterceptorImpl.intercept(answer, additionalAttributesToBeSentForProcessing)<br>
 *   }).when(mockedObj).methodToBeMocked(args...)
 * </p>
 * */
public interface MockClassInterceptor {
  public Object intercept(InvocationOnMock invocationOnMock, Object... objects)
      throws IOException;
}
