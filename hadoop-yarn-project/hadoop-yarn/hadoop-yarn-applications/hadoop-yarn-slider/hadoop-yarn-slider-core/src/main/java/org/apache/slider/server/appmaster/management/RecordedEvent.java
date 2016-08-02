/*
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

package org.apache.slider.server.appmaster.management;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.text.DateFormat;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class RecordedEvent {
  private static final DateFormat dateFormat = DateFormat.getDateInstance();
  public long id;
  public String name;
  public long timestamp;
  public String time;
  public String category;
  public String host;
  public int role;
  public String text;

  public RecordedEvent() {
  }

  /**
   * Create an event. The timestamp is also converted to a time string
   * @param id id counter
   * @param name event name
   * @param timestamp timestamp. If non-zero, is used to build the {@code time} text field.
   * @param category even category
   * @param text arbitrary text
   */
  public RecordedEvent(long id, String name, long timestamp, String category, String text) {
    this.id = id;
    this.name = name;
    this.timestamp = timestamp;
    this.time = timestamp > 0 ? dateFormat.format(timestamp) : "";
    this.category = category;
    this.text = text;
  }
}
