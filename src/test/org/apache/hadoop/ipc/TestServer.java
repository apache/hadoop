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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

/**
 * This is intended to be a set of unit tests for the 
 * org.apache.hadoop.ipc.Server class.
 */
public class TestServer {

  @Test
  public void testExceptionsHandler() throws IOException {
    Server.ExceptionsHandler handler = new Server.ExceptionsHandler();
    handler.addTerseExceptions(IOException.class);
    handler.addTerseExceptions(RemoteException.class);

    assertTrue(handler.isTerse(IOException.class));
    assertTrue(handler.isTerse(RemoteException.class));
  }
}
