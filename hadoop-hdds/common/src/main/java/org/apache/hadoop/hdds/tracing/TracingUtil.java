/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.tracing;

import java.lang.reflect.Proxy;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

import org.apache.hadoop.hdds.scm.ScmConfigKeys;

/**
 * Utility class to collect all the tracing helper methods.
 */
public final class TracingUtil {

  private static final String NULL_SPAN_AS_STRING = "";

  private TracingUtil() {
  }

  /**
   * Initialize the tracing with the given service name.
   *
   * @param serviceName
   */
  public static void initTracing(String serviceName) {
    if (!GlobalTracer.isRegistered()) {
      Configuration config = Configuration.fromEnv(serviceName);
      JaegerTracer tracer = config.getTracerBuilder()
          .registerExtractor(StringCodec.FORMAT, new StringCodec())
          .registerInjector(StringCodec.FORMAT, new StringCodec())
          .build();
      GlobalTracer.register(tracer);
    }
  }

  /**
   * Export the active tracing span as a string.
   *
   * @return encoded tracing context.
   */
  public static String exportCurrentSpan() {
    if (GlobalTracer.get().activeSpan() != null) {
      StringBuilder builder = new StringBuilder();
      GlobalTracer.get().inject(GlobalTracer.get().activeSpan().context(),
          StringCodec.FORMAT, builder);
      return builder.toString();
    }
    return NULL_SPAN_AS_STRING;
  }

  /**
   * Export the specific span as a string.
   *
   * @return encoded tracing context.
   */
  public static String exportSpan(Span span) {
    if (span != null) {
      StringBuilder builder = new StringBuilder();
      GlobalTracer.get().inject(span.context(), StringCodec.FORMAT, builder);
      return builder.toString();
    }
    return NULL_SPAN_AS_STRING;
  }

  /**
   * Create a new scope and use the imported span as the parent.
   *
   * @param name          name of the newly created scope
   * @param encodedParent Encoded parent span (could be null or empty)
   *
   * @return OpenTracing scope.
   */
  public static Scope importAndCreateScope(String name, String encodedParent) {
    Tracer.SpanBuilder spanBuilder;
    Tracer tracer = GlobalTracer.get();
    SpanContext parentSpan = null;
    if (encodedParent != null && encodedParent.length() > 0) {
      StringBuilder builder = new StringBuilder();
      builder.append(encodedParent);
      parentSpan = tracer.extract(StringCodec.FORMAT, builder);

    }

    if (parentSpan == null) {
      spanBuilder = tracer.buildSpan(name);
    } else {
      spanBuilder =
          tracer.buildSpan(name).asChildOf(parentSpan);
    }
    return spanBuilder.startActive(true);
  }

  /**
   * Creates a proxy of the implementation and trace all the method calls.
   *
   * @param delegate the original class instance
   * @param interfce the interface which should be implemented by the proxy
   * @param <T> the type of the interface
   * @param conf configuration
   *
   * @return A new interface which implements interfce but delegate all the
   * calls to the delegate and also enables tracing.
   */
  public static <T> T createProxy(T delegate, Class<T> interfce,
                                  org.apache.hadoop.conf.Configuration conf) {
    boolean isTracingEnabled = conf.getBoolean(
        ScmConfigKeys.HDDS_TRACING_ENABLED,
        ScmConfigKeys.HDDS_TRACING_ENABLED_DEFAULT);
    if (!isTracingEnabled) {
      return delegate;
    }
    Class<?> aClass = delegate.getClass();
    return  (T) Proxy.newProxyInstance(aClass.getClassLoader(),
        new Class<?>[] {interfce},
        new TraceAllMethod<T>(delegate, interfce.getSimpleName()));
  }

}
