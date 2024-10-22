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

public class TraceScope implements Closeable {
  Span span;
  Scope openScope;
  public TraceScope(Span span) {
    this.span = span;
    if(span != null){
      this.openScope = span.makeCurrent();
    }
  }

  // Add tag to the span
  public void addKVAnnotation(String key, String value) {
    span.addKVAnnotation(key, value);
  }

  public void addKVAnnotation(String key, Number value) {
    span.addKVAnnotation(key, value.toString());
  }


  public void addTimelineAnnotation(String msg) {
    span.addTimelineAnnotation(msg);
  }

  public Span span() {
    return span;
  }

  public Span getSpan() {
    return span;
  }

  public void reattach() {
  }

  public void detach() {
  }

  public void close() {
    if(openScope != null){
      openScope.close();
    }
    if (span != null) {
      span.close();
    }
  }
}
