/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Captures operations from mock {@link PrivilegedOperation} instances.
 */
public final class MockPrivilegedOperationCaptor {

  private MockPrivilegedOperationCaptor() {}

  /**
   * Capture the operation that should be performed by the
   * PrivilegedOperationExecutor.
   *
   * @param mockExecutor    mock PrivilegedOperationExecutor.
   * @param invocationCount number of invocations expected.
   * @return a list of operations that were invoked.
   * @throws PrivilegedOperationException when the operation fails to execute.
   */
  @SuppressWarnings("unchecked")
  public static List<PrivilegedOperation> capturePrivilegedOperations(
      PrivilegedOperationExecutor mockExecutor, int invocationCount,
      boolean grabOutput) throws PrivilegedOperationException {
    ArgumentCaptor<PrivilegedOperation> opCaptor =
        ArgumentCaptor.forClass(PrivilegedOperation.class);

    //one or more invocations expected
    //due to type erasure + mocking, this verification requires a suppress
    // warning annotation on the entire method
    verify(mockExecutor, times(invocationCount))
        .executePrivilegedOperation(any(), opCaptor.capture(),
            any(), any(Map.class), eq(grabOutput), eq(false));

    //verification completed. we need to isolate specific invications.
    // hence, reset mock here
    Mockito.reset(mockExecutor);

    return opCaptor.getAllValues();
  }
}
