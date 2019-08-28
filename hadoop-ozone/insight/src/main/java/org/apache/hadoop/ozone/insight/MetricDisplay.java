/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import java.util.HashMap;
import java.util.Map;

/**
 * Definition of one displayable hadoop metrics.
 */
public class MetricDisplay {

  /**
   * Prometheus metrics name.
   */
  private String id;

  /**
   * Human readable definition of the metrhics.
   */
  private String description;

  /**
   * Prometheus metrics tag to filter out the right metrics.
   */
  private Map<String, String> filter;

  public MetricDisplay(String description, String id) {
    this(description, id, new HashMap<>());
  }

  public MetricDisplay(String description, String id,
      Map<String, String> filter) {
    this.id = id;
    this.description = description;
    this.filter = filter;
  }

  public String getId() {
    return id;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, String> getFilter() {
    return filter;
  }

  public boolean checkLine(String line) {
    return false;
  }
}
