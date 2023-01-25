/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class LoggingHttpResponseEncoder extends HttpResponseEncoder {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingHttpResponseEncoder.class);
  private final boolean logStacktraceOfEncodingMethods;

  LoggingHttpResponseEncoder(boolean logStacktraceOfEncodingMethods) {
    this.logStacktraceOfEncodingMethods = logStacktraceOfEncodingMethods;
  }

  @Override
  public boolean acceptOutboundMessage(Object msg) throws Exception {
    printExecutingMethod();
    LOG.info("OUTBOUND MESSAGE: " + msg);
    return super.acceptOutboundMessage(msg);
  }

  @Override
  protected void encodeInitialLine(ByteBuf buf, HttpResponse response) throws Exception {
    LOG.debug("Executing method: {}, response: {}",
        getExecutingMethodName(), response);
    logStacktraceIfRequired();
    super.encodeInitialLine(buf, response);
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg,
      List<Object> out) throws Exception {
    LOG.debug("Encoding to channel {}: {}", ctx.channel(), msg);
    printExecutingMethod();
    logStacktraceIfRequired();
    super.encode(ctx, msg, out);
  }

  @Override
  protected void encodeHeaders(HttpHeaders headers, ByteBuf buf) {
    printExecutingMethod();
    super.encodeHeaders(headers, buf);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise
      promise) throws Exception {
    LOG.debug("Writing to channel {}: {}", ctx.channel(), msg);
    printExecutingMethod();
    super.write(ctx, msg, promise);
  }

  private void logStacktraceIfRequired() {
    if (logStacktraceOfEncodingMethods) {
      LOG.debug("Stacktrace: ", new Throwable());
    }
  }

  private void printExecutingMethod() {
    String methodName = getExecutingMethodName(1);
    LOG.debug("Executing method: {}", methodName);
  }

  private String getExecutingMethodName() {
    return getExecutingMethodName(0);
  }

  private String getExecutingMethodName(int additionalSkipFrames) {
    try {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      // Array items (indices):
      // 0: java.lang.Thread.getStackTrace(...)
      // 1: TestShuffleHandler$LoggingHttpResponseEncoder.getExecutingMethodName(...)
      int skipFrames = 2 + additionalSkipFrames;
      String methodName = stackTrace[skipFrames].getMethodName();
      String className = this.getClass().getSimpleName();
      return className + "#" + methodName;
    } catch (Throwable t) {
      LOG.error("Error while getting execution method name", t);
      return "unknown";
    }
  }
}
