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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator.AllocatorRunnable;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRMCommunicator {

  class MockRMCommunicator extends RMCommunicator {
    public MockRMCommunicator(ClientService clientService, AppContext context) {
      super(clientService, context);
    }

    @Override
    protected void heartbeat() throws Exception {
    }
  }

  @Test(timeout = 2000)
  public void testRMContainerAllocatorExceptionIsHandled() throws Exception {
    ClientService mockClientService = mock(ClientService.class);
    AppContext mockContext = mock(AppContext.class);
    MockRMCommunicator mockRMCommunicator =
        new MockRMCommunicator(mockClientService, mockContext);
    RMCommunicator communicator = spy(mockRMCommunicator);
    Clock mockClock = mock(Clock.class);
    when(mockContext.getClock()).thenReturn(mockClock);

    doThrow(new RMContainerAllocationException("Test")).doNothing()
        .when(communicator).heartbeat();

    when(mockClock.getTime()).thenReturn(1L).thenThrow(new AssertionError(
        "GetClock called second time, when it should not have since the " +
        "thread should have quit"));

    AllocatorRunnable testRunnable = communicator.new AllocatorRunnable();
    testRunnable.run();
  }

  @Test(timeout = 2000)
  public void testRMContainerAllocatorYarnRuntimeExceptionIsHandled()
      throws Exception {
    ClientService mockClientService = mock(ClientService.class);
    AppContext mockContext = mock(AppContext.class);
    MockRMCommunicator mockRMCommunicator =
        new MockRMCommunicator(mockClientService, mockContext);
    final RMCommunicator communicator = spy(mockRMCommunicator);
    Clock mockClock = mock(Clock.class);
    when(mockContext.getClock()).thenReturn(mockClock);

    doThrow(new YarnRuntimeException("Test")).doNothing()
        .when(communicator).heartbeat();

    when(mockClock.getTime()).thenReturn(1L).thenAnswer(
        (Answer<Long>) invocation -> {
        communicator.stop();
        return 2L;
      }).thenThrow(new AssertionError(
          "GetClock called second time, when it should not " +
              "have since the thread should have quit"));

    AllocatorRunnable testRunnable = communicator.new AllocatorRunnable();
    testRunnable.run();

    verify(mockClock, times(2)).getTime();
  }
}
