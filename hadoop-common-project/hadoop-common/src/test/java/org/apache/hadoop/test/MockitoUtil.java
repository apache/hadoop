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
}
