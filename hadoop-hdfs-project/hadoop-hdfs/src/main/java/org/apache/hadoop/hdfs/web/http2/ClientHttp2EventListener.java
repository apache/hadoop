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

import io.netty.channel.Channel;
import io.netty.handler.codec.http2.Http2Connection;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * An HTTP/2 FrameListener and EventListener to manage
 * {@link Http2StreamChannel}s for client.
 */
@InterfaceAudience.Private
public class ClientHttp2EventListener extends AbstractHttp2EventListener {

  public ClientHttp2EventListener(Channel parentChannel, Http2Connection conn) {
    super(parentChannel, conn);
  }

  @Override
  protected void initChannelOnStreamActive(Http2StreamChannel subChannel) {
    // disable read until pipeline initialized
    subChannel.config().setAutoRead(false);
  }

}
