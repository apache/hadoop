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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * No-Op Tracer (for now) to remove HTrace without changing too many files.
 */
public class Tracer {
  public static final Logger LOG = LoggerFactory.getLogger(Tracer.class.getName());
  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.contrib.hadoop";
  // Singleton
  private static Tracer globalTracer = null;
  io.opentelemetry.api.trace.Tracer OTelTracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);

  public final static String SPAN_RECEIVER_CLASSES_KEY =
      "span.receiver.classes";

  private Tracer() {}

  // Keeping this function at the moment for HTrace compatiblity,
  // in fact all threads share a single global tracer for OpenTracing.
  public static Tracer curThreadTracer() {
    return globalTracer;
  }

  /***
   * Return active span.
   * @return org.apache.hadoop.tracing.Span
   */
  public static Span getCurrentSpan() {
    io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.current();
    return span.getSpanContext().isValid()? new Span(span): null;
  }

  public TraceScope newScope(String description) {
    Span span = new Span(OTelTracer.spanBuilder(description).startSpan());
    return new TraceScope(span);
  }

  public Span newSpan(String description, SpanContext spanCtx) {
    io.opentelemetry.api.trace.Span parentSpan = io.opentelemetry.api.trace.Span.wrap(spanCtx.getOpenSpanContext());
    io.opentelemetry.api.trace.Span span = OTelTracer.spanBuilder(description).setParent(Context.current().with(parentSpan)).startSpan();
    return new Span(span);
  }

  public TraceScope newScope(String description, SpanContext spanCtx) {
    if(spanCtx == null){
      return new TraceScope(new Span(io.opentelemetry.api.trace.Span.getInvalid()));
    }
    return new TraceScope(newSpan(description, spanCtx));
  }

  public TraceScope newScope(String description, SpanContext spanCtx,
      boolean finishSpanOnClose) {
    return new TraceScope(newSpan(description, spanCtx));
  }

  public TraceScope activateSpan(Span span) {
    return new TraceScope(span);
  }

  public void close() {
  }

  
  public static class Builder {
    static Tracer globalTracer = new Tracer();

    public Builder() {
    }

    public Builder conf(TraceConfiguration conf) {
      return this;
    }

    public synchronized Tracer build() {
      return globalTracer;
    }
  }
}
