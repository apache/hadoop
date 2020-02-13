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

public class TraceScope implements Closeable {
  private io.opentracing.Scope otScope;

  public TraceScope(io.opentracing.Scope scope) {
    this.otScope = scope;
  }

  // Add tag to the span
  public Span addKVAnnotation(String key, String value) {
    // TODO: Try to reduce overhead from "new" object by returning void?
    return new Span(this.otScope.span().setTag(key, value));
  }

  public Span addKVAnnotation(String key, Number value) {
    return new Span(this.otScope.span().setTag(key, value));
  }

  public Span addTimelineAnnotation(String msg) {
    return new Span(this.otScope.span().log(msg));
  }

  public Span span() {
    return new Span(this.otScope.span());
  }

  public Span getSpan() {
    /* e.g.
      TraceScope scope = tracer.newScope(instance.getCommandName());
      if (scope.getSpan() != null) {
    */
    return new Span(this.otScope.span());
  }

  public void reattach() {
    // TODO: Server.java:2820
    // scope = GlobalTracer.get().scopeManager().activate(call.span, true);
  }

  public void detach() {
  }

  public void close() {
    otScope.close();
  }
}
