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
package org.apache.hadoop.hdfs.web.http2;

import io.netty.handler.codec.http.LastHttpContent;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Used to tell an inbound handler that the remote side of an HTTP/2 stream is
 * closed, or used by an outbound handler to tell the HTTP/2 stream to close
 * local side.
 * @see LastHttpContent#EMPTY_LAST_CONTENT
 */
@InterfaceAudience.Private
public final class LastHttp2Message {

  private static final LastHttp2Message INSTANCE = new LastHttp2Message();

  private LastHttp2Message() {
  }

  /**
   * Get the singleton <tt>LastHttp2Message</tt> instance.
   */
  public static LastHttp2Message get() {
    return INSTANCE;
  }
}
