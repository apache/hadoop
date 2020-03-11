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

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.util.GlobalTracer;

public class Tracer {
  // Avoid creating new objects every time it is called
  private static Tracer globalTracer;
  public io.opentracing.Tracer tracer;

  public Tracer(io.opentracing.Tracer tracer) {
    this.tracer = tracer;
  }

  public static io.opentracing.Tracer get() {
    return GlobalTracer.get();
  }

  // Keeping this function at the moment for HTrace compatiblity,
  // in fact all threads share a single global tracer for OpenTracing.
  public static Tracer curThreadTracer() {
    if (globalTracer == null) {
      globalTracer = new Tracer(GlobalTracer.get());
    }
    return globalTracer;
  }

  /***
   * Return active span.
   * @return org.apache.hadoop.tracing.Span
   */
  public static Span getCurrentSpan() {
    io.opentracing.Span span = GlobalTracer.get().activeSpan();
    if (span != null) {
      // Only wrap the OpenTracing span when it isn't null
      return new Span(span);
    } else {
      return null;
    }
  }

  public TraceScope newScope(String description) {
    Scope scope = tracer.buildSpan(description).startActive(true);
    return new TraceScope(scope);
  }

  public Span newSpan(String description, SpanContext spanCtx) {
    io.opentracing.Span otspan = tracer.buildSpan(description)
        .asChildOf(spanCtx).start();
    return new Span(otspan);
  }

  public TraceScope newScope(String description, SpanContext spanCtx) {
    io.opentracing.Scope otscope = tracer.buildSpan(description)
        .asChildOf(spanCtx).startActive(true);
    return new TraceScope(otscope);
  }

  public TraceScope newScope(String description, SpanContext spanCtx,
      boolean finishSpanOnClose) {
    io.opentracing.Scope otscope = tracer.buildSpan(description)
        .asChildOf(spanCtx).startActive(finishSpanOnClose);
    return new TraceScope(otscope);
  }

  public TraceScope activateSpan(Span span) {
    return new TraceScope(tracer.scopeManager().activate(span.otSpan, true));
  }

  public void close() {
  }

  public static class Builder {
    static Tracer globalTracer;

    private String name;

    public Builder(final String name) {
      this.name = name;
    }

    public Builder conf(TraceConfiguration conf) {
      return this;
    }

    public Tracer build() {
      if (globalTracer == null) {
        io.opentracing.Tracer oTracer = TraceUtils.createAndRegisterTracer(name);
        globalTracer = new Tracer(oTracer);
      }
      return globalTracer;
    }
  }
}
