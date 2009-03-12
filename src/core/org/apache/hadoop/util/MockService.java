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
package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * A mock service that can be set to fail in different parts of its lifecycle,
 * and which counts the number of times its inner classes changed state.
 */

public class MockService extends Service {

  /**
   * Build from an empty configuration
   */
  public MockService() {
    super(new Configuration());
  }

  /**
   * Build from a configuration file
   * @param conf
   */
  public MockService(Configuration conf) {
    super(conf);
  }

  private boolean failOnStart, failOnPing, failOnClose;
  private boolean goLiveInStart = true;
  private boolean closed = true;
  private volatile int stateChangeCount = 0;
  private volatile int pingCount = 0;

  public void setFailOnStart(boolean failOnStart) {
    this.failOnStart = failOnStart;
  }

  public void setFailOnPing(boolean failOnPing) {
    this.failOnPing = failOnPing;
  }

  public void setGoLiveInStart(boolean goLiveInStart) {
    this.goLiveInStart = goLiveInStart;
  }

  public void setFailOnClose(boolean failOnClose) {
    this.failOnClose = failOnClose;
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * Go live
   *
   * @throws ServiceStateException if we were not in a state to do so
   */
  public void goLive() throws ServiceStateException {
    enterLiveState();
  }

  /**
   * {@inheritDoc}
   * @throws IOException  if {@link #failOnStart is set}
   */
  @Override
  protected void innerStart() throws IOException {
    if (failOnStart) {
      throw new MockServiceException("failOnStart");
    }
    if (goLiveInStart) {
      goLive();
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException if {@link #failOnPing is set} @param status
   */
  @Override
  protected void innerPing(ServiceStatus status) throws IOException {
    pingCount++;
    if (failOnPing) {
      throw new MockServiceException("failOnPing");
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IOException if {@link #failOnClose} is true
   */
  protected void innerClose() throws IOException {
    closed = true;
    if (failOnClose) {
      throw new MockServiceException("failOnClose");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void onStateChange(ServiceState oldState,
                               ServiceState newState) {
    super.onStateChange(oldState, newState);
    stateChangeCount++;
  }

  /**
   * {@inheritDoc}
   *
   * A public method do change state
   */
  public void changeState(ServiceState state)
          throws ServiceStateException {
    setServiceState(state);
  }

  public int getStateChangeCount() {
    return stateChangeCount;
  }

  public int getPingCount() {
    return pingCount;
  }

  /**
   * An exception to indicate we have triggered a mock event
   */
  static class MockServiceException extends IOException {

    private MockServiceException(String message) {
      super(message);
    }
  }
}
