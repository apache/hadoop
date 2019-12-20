/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.common.api;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents the type of Runtime.
 */
public enum Runtime {
  TONY(Constants.TONY), YARN_SERVICE(Constants.YARN_SERVICE);

  private String value;

  Runtime(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static Runtime parseByValue(String value) {
    for (Runtime rt : Runtime.values()) {
      if (rt.value.equalsIgnoreCase(value)) {
        return rt;
      }
    }
    return null;
  }

  public static String getValues() {
    List<String> values = Lists.newArrayList(Runtime.values()).stream()
        .map(rt -> rt.value).collect(Collectors.toList());
    return String.join(",", values);
  }

  public static class Constants {
    public static final String TONY = "tony";
    public static final String YARN_SERVICE = "yarnservice";
  }
}