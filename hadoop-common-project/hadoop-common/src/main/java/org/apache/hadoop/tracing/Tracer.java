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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * No-Op Tracer (for now) to remove HTrace without changing too many files.
 */
public class Tracer {
  public static final Logger LOG = LoggerFactory.getLogger(Tracer.class.getName());
  // Singleton
  private static Tracer globalTracer = null;
  io.opentelemetry.api.trace.Tracer OTelTracer;
  private final NullTraceScope nullTraceScope;
  private final String name;

  public final static String SPAN_RECEIVER_CLASSES_KEY =
      "span.receiver.classes";

  private Tracer(String name, io.opentelemetry.api.trace.Tracer tracer) {
    this.name = name;
    OTelTracer = tracer;
    nullTraceScope = NullTraceScope.INSTANCE;
  }

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
    io.opentelemetry.api.trace.Span parentSpan = io.opentelemetry.api.trace.Span.wrap(spanCtx.getSpanContext());
    io.opentelemetry.api.trace.Span span = OTelTracer.spanBuilder(description).setParent(Context.current().with(parentSpan)).startSpan();
    return new Span(span);
  }

  public TraceScope newScope(String description, SpanContext spanCtx) {
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

  public String getName() {
    return name;
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

    OpenTelemetry initialiseJaegerExporter(String jaegerHost, int jaegerPort, String name) {
      ManagedChannel jaegerChannel =
          ManagedChannelBuilder.forAddress(jaegerHost, jaegerPort).usePlaintext().build();
      // Export traces to Jaeger
      JaegerGrpcSpanExporter jaegerExporter =
          JaegerGrpcSpanExporter.builder()
              .setChannel(jaegerChannel)
              .setTimeout(30, TimeUnit.SECONDS)
              .build();
      Resource serviceNameResource =
          Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), name));
      // Set to process the spans by the Jaeger Exporter
      SdkTracerProvider tracerProvider =
          SdkTracerProvider.builder()
              .addSpanProcessor(SimpleSpanProcessor.create(jaegerExporter))
              .setResource(Resource.getDefault().merge(serviceNameResource))
              .build();
      OpenTelemetrySdk openTelemetry =
          OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

      // it's always a good idea to shut down the SDK cleanly at JVM exit.
      Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));

      return openTelemetry;
    }

    OpenTelemetry noOpTracer(){
      return OpenTelemetry.noop();
    }

    public Tracer build() {
      if (globalTracer == null) {
        //jaeger tracing changes
        OpenTelemetry openTelemetry = initialiseJaegerExporter("localhost", 14250, name);
        io.opentelemetry.api.trace.Tracer tracer = openTelemetry.getTracer(name);
        globalTracer = new Tracer(name, tracer);

        //globalTracer = new Tracer(name, noOpTracer().getTracer(name));
        Tracer.globalTracer = globalTracer;
      }
      return globalTracer;
    }
  }
}
