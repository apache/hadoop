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

import io.opentelemetry.context.Scope;

import java.io.Closeable;

public class Span implements Closeable {
  private io.opentelemetry.api.trace.Span span = null;
  public Span() {
  }

  public Span(io.opentelemetry.api.trace.Span span){
    this.span = span;
  }

  public Span addKVAnnotation(String key, String value) {
    if(span != null){
      span.setAttribute(key, value);
    }
    return this;
  }

  public Span addTimelineAnnotation(String msg) {
    if(span != null){
      span.addEvent(msg);
    }
    return this;
  }

  public SpanContext getContext() {
    if(span != null){
      return  new SpanContext(span.getSpanContext());
    }
    return null;
  }

  public void finish() {
    close();
  }

  public void close() {
    if(span != null){
      span.end();
    }
  }

  public Scope makeCurrent() {
    if(span != null){
      return span.makeCurrent();
    }
    return null;
  }
}
