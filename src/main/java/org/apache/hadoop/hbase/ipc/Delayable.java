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

/**
 * A call whose response can be delayed by the server.
 */
public interface Delayable {
  /**
   * Signal that the call response should be delayed, thus freeing the RPC
   * server to handle different requests.
   *
   * @param delayReturnValue Controls whether the return value of the call
   * should be set when ending the delay or right away.  There are cases when
   * the return value can be set right away, even if the call is delayed.
   */
  public void startDelay(boolean delayReturnValue);

  /**
   * @return is the call delayed?
   */
  public boolean isDelayed();

  /**
   * @return is the return value delayed?
   */
  public boolean isReturnValueDelayed();

  /**
   * Signal that the  RPC server is now allowed to send the response.
   * @param result The value to return to the caller.  If the corresponding
   * {@link #delayResponse(boolean)} specified that the return value should
   * not be delayed, this parameter must be null.
   * @throws IOException
   */
  public void endDelay(Object result) throws IOException;

  /**
   * Signal the end of a delayed RPC, without specifying the return value.  Use
   * this only if the return value was not delayed
   * @throws IOException
   */
  public void endDelay() throws IOException;

  /**
   * End the call, throwing and exception to the caller.  This works regardless
   * of the return value being delayed.
   * @param t Object to throw to the client.
   * @throws IOException
   */
  public void endDelayThrowing(Throwable t) throws IOException;
}
