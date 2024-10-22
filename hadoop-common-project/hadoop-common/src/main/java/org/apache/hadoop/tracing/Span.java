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

/***
 * This class is a wrapper class on top of opentelemetry Span class
 * avoiding direct dependency on opentelemetry API.
 */
public class Span implements Closeable {
  private io.opentelemetry.api.trace.Span openSpan;
  public Span() {
  }

  public Span(io.opentelemetry.api.trace.Span openSpan){
    this.openSpan = openSpan;
  }

  public Span addKVAnnotation(String key, String value) {
    if(openSpan != null){
      openSpan.setAttribute(key, value);
    }
    return this;
  }

  public Span addTimelineAnnotation(String msg) {
    if(openSpan != null){
      openSpan.addEvent(msg);
    }
    return this;
  }

  public SpanContext getContext() {
    if(openSpan != null){
      return  new SpanContext(openSpan.getSpanContext());
    }
    return null;
  }

  public void finish() {
    close();
  }

  public void close() {
    if(openSpan != null){
      openSpan.end();
    }
  }

  /***
   * This method activates the current span on the current thread
   * @return the scope for the current span
   */
  public Scope makeCurrent() {
    if(openSpan != null){
      return openSpan.makeCurrent();
    }
    return null;
  }
}
