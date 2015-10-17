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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.concurrent.Promise;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Used by {@link Http2StreamBootstrap} to establish an
 * {@link Http2StreamChannel}.
 */
@InterfaceAudience.Private
class StartHttp2StreamRequest {

  final Http2Headers headers;

  final ByteBuf data;

  final boolean endStream;

  final Promise<Http2StreamChannel> promise;

  StartHttp2StreamRequest(Http2Headers headers, ByteBuf data,
      boolean endStream, Promise<Http2StreamChannel> promise) {
    this.headers = headers;
    this.data = data;
    this.endStream = endStream;
    this.promise = promise;
  }
}
