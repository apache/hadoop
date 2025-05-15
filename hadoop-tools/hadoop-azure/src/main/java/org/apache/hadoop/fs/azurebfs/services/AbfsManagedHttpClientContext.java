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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.http.HttpClientConnection;
import org.apache.http.client.protocol.HttpClientContext;

/**
 * Registers the latency of different phases of a network call.
 */
public class AbfsManagedHttpClientContext extends HttpClientContext {

  /**Connection establishment time*/
  private long connectTime = 0L;

  /**Time taken to receive and read response*/
  private long readTime = 0L;

 /***Time taken to send request*/
  private long sendTime = 0L;

  public AbfsManagedHttpClientContext() {
  }

  /**
   * This to be used only in tests to get connection level activity.
   * @param httpClientConnection HttpClientConnection which has to be intercepted
   * by test-implementation.
   * @return HttpClientConnection which is intercepted by test-implementation. For production
   * implementation, it will return the same httpClientConnection.
   */
  protected HttpClientConnection interceptConnectionActivity(
      HttpClientConnection httpClientConnection) {
    return httpClientConnection;
  }

  public long getConnectTime() {
    return connectTime;
  }

  public void setConnectTime(long connectTime) {
    this.connectTime = connectTime;
  }

  public long getReadTime() {
    return readTime;
  }

  public long getSendTime() {
    return sendTime;
  }

  public void addSendTime(long sendTime) {
    this.sendTime += sendTime;
  }

  public void addReadTime(long readTime) {
    this.readTime += readTime;
  }
}
