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
package org.apache.hadoop.test;

import java.io.Closeable;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

public abstract class MockitoUtil {

  /**
   * Return a mock object for an IPC protocol. This special
   * method is necessary, since the IPC proxies have to implement
   * Closeable in addition to their protocol interface.
   * @param clazz the protocol class
   */
  public static <T> T mockProtocol(Class<T> clazz) {
    return Mockito.mock(clazz,
        Mockito.withSettings().extraInterfaces(Closeable.class));
  }

  /**
   * Throw an exception from the mock/spy only in the case that the
   * call stack at the time the method has a line which matches the given
   * pattern.
   *
   * @param t the Throwable to throw
   * @param pattern the pattern against which to match the call stack trace
   * @return the stub in progress
   */
  public static Stubber doThrowWhenCallStackMatches(
      final Throwable t, final String pattern) {
    return Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        t.setStackTrace(Thread.currentThread().getStackTrace());
        for (StackTraceElement elem : t.getStackTrace()) {
          if (elem.toString().matches(pattern)) {
            throw t;
          }
        }
        return invocation.callRealMethod();
      }
    });
  }
}
