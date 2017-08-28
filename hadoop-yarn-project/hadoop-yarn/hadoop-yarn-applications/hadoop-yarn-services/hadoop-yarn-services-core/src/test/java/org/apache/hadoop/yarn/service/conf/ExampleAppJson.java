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

package org.apache.hadoop.yarn.service.conf;


import org.apache.hadoop.yarn.service.api.records.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.service.ServiceTestUtils.JSON_SER_DESER;

/**
 * Names of the example configs.
 */
public final class ExampleAppJson {

  public static final String APP_JSON = "app.json";
  public static final String OVERRIDE_JSON = "app-override.json";
  public static final String DEFAULT_JSON = "default.json";
  public static final String EXTERNAL_JSON_0 = "external0.json";
  public static final String EXTERNAL_JSON_1 = "external1.json";
  public static final String EXTERNAL_JSON_2 = "external2.json";

  public static final String PACKAGE = "/org/apache/hadoop/yarn/service/conf/examples/";


  private static final String[] ALL_EXAMPLES = {APP_JSON, OVERRIDE_JSON,
      DEFAULT_JSON};

  public static final List<String> ALL_EXAMPLE_RESOURCES = new ArrayList<>();
  static {
    for (String example : ALL_EXAMPLES) {
      ALL_EXAMPLE_RESOURCES.add(PACKAGE + example);
    }
  }

  private ExampleAppJson() {
  }

  public static Service loadResource(String name) throws IOException {
    return JSON_SER_DESER.fromResource(PACKAGE + name);
  }

  public static String resourceName(String name) {
    return "target/test-classes" + PACKAGE + name;
  }
}
