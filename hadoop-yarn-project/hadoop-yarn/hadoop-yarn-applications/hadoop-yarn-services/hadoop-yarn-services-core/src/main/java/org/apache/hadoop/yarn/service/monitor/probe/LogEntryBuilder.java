/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.monitor.probe;

/**
 * Build up log entries for ease of splunk
 */
public class LogEntryBuilder {

  private final StringBuilder builder = new StringBuilder();

  public LogEntryBuilder() {
  }

  public LogEntryBuilder(String text) {
    elt(text);
  }


  public LogEntryBuilder(String name, Object value) {
    entry(name, value);
  }

  public LogEntryBuilder elt(String text) {
    addComma();
    builder.append(text);
    return this;
  }

  public LogEntryBuilder elt(String name, Object value) {
    addComma();
    entry(name, value);
    return this;
  }

  private void addComma() {
    if (!isEmpty()) {
      builder.append(", ");
    }
  }

  private void entry(String name, Object value) {
    builder.append(name).append('=');
    if (value != null) {
      builder.append('"').append(value.toString()).append('"');
    } else {
      builder.append("null");
    }
  }

  @Override
  public String toString() {
    return builder.toString();
  }

  private boolean isEmpty() {
    return builder.length() == 0;
  }


}
