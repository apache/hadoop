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
package org.apache.hadoop.tracing;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for SpanContext to avoid using OpenTracing/OpenTelemetry
 * SpanContext class directly for better separation.
 */
public class SpanContext implements Closeable  {
  public static final Logger LOG = LoggerFactory.getLogger(SpanContext.class.getName());
  private static final String TRACE_ID = "TRACE_ID";
  private static final String SPAN_ID = "SPAN_ID";
  private static final String TRACE_FLAGS = "TRACE_FLAGS";


  private io.opentelemetry.api.trace.SpanContext spanContext = null;
  public SpanContext(io.opentelemetry.api.trace.SpanContext spanContext) {
    this.spanContext = spanContext;
  }

  public void close() {

  }

  public Map<String, String> getKVSpanContext(){
    if(spanContext != null){
      //TODO: may we should move this to Proto
      Map<String, String> kvMap = new HashMap<>();
      kvMap.put(TRACE_ID, spanContext.getTraceId());
      kvMap.put(SPAN_ID, spanContext.getSpanId());
      kvMap.put(TRACE_FLAGS, spanContext.getTraceFlags().asHex());
      kvMap.putAll(spanContext.getTraceState().asMap());
      return kvMap;
    }
    return null;
  }

  static SpanContext buildFromKVMap(Map<String, String> kvMap){
    String traceId = kvMap.get(TRACE_ID);
    kvMap.remove(TRACE_ID);
    String spanId = kvMap.get(SPAN_ID);
    kvMap.remove(SPAN_ID);
    String traceFlagsHex = kvMap.get(TRACE_FLAGS);
    kvMap.remove(TRACE_FLAGS);
    TraceFlags traceFlags = TraceFlags.fromHex(traceFlagsHex, 0);
    TraceStateBuilder traceStateBuilder = TraceState.builder();
    for(Map.Entry<String, String> keyValue: kvMap.entrySet()){
      traceStateBuilder.put(keyValue.getKey(), keyValue.getValue());
    }
    TraceState traceState = traceStateBuilder.build();
    io.opentelemetry.api.trace.SpanContext spanContext = io.opentelemetry.api.trace.SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, traceState );

    return new SpanContext(spanContext);
  }

  public io.opentelemetry.api.trace.SpanContext getSpanContext() {
      return spanContext;
  }
}
