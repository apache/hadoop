/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A call whose response can be delayed by the server.
 */
public interface Delayable {
  /**
   * Signal that the call response should be delayed, thus freeing the RPC
   * server to handle different requests.
   */
  public void startDelay();

  /**
   * @return is the call delayed?
   */
  public boolean isDelayed();

  /**
   * Signal that the response to the call is ready and the RPC server is now
   * allowed to send the response.
   * @param result The result to return to the caller.
   * @throws IOException
   */
  public void endDelay(Object result) throws IOException;
}
