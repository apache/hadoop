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

import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
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


  private io.opentelemetry.api.trace.SpanContext openSpanContext;
  public SpanContext(io.opentelemetry.api.trace.SpanContext openSpanContext) {
    this.openSpanContext = openSpanContext;
  }

  public void close() {

  }

  public Map<String, String> getKVSpanContext(){
    if(openSpanContext != null){
      //TODO: may we should move this to Proto
      Map<String, String> kvMap = new HashMap<>();
      kvMap.put(TRACE_ID, openSpanContext.getTraceId());
      kvMap.put(SPAN_ID, openSpanContext.getSpanId());
      kvMap.put(TRACE_FLAGS, openSpanContext.getTraceFlags().asHex());
      kvMap.putAll(openSpanContext.getTraceState().asMap());
      return kvMap;
    }
    return null;
  }

  static SpanContext buildFromKVMap(Map<String, String> kvMap){
    try{
      String traceId = kvMap.get(TRACE_ID);
      String spanId = kvMap.get(SPAN_ID);
      String traceFlagsHex = kvMap.get(TRACE_FLAGS);
      if(traceId == null || spanId == null || traceFlagsHex == null){
        return null;
      }
      TraceFlags traceFlags = TraceFlags.fromHex(traceFlagsHex, 0);
      TraceStateBuilder traceStateBuilder = TraceState.builder();
      for(Map.Entry<String, String> keyValue: kvMap.entrySet()){
        if(keyValue.getKey().equals(TRACE_ID) || keyValue.getKey().equals(SPAN_ID) || keyValue.getKey().equals(TRACE_FLAGS)){
          continue;
        }
        traceStateBuilder.put(keyValue.getKey(), keyValue.getValue());
      }
      TraceState traceState = traceStateBuilder.build();
      io.opentelemetry.api.trace.SpanContext spanContext = io.opentelemetry.api.trace.SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, traceState );
      return new SpanContext(spanContext);
    } catch (Exception e){
      LOG.error("Error in processing remote context :", e);
      return null;
    }


  }

  public io.opentelemetry.api.trace.SpanContext getOpenSpanContext() {
      return openSpanContext;
  }
}
