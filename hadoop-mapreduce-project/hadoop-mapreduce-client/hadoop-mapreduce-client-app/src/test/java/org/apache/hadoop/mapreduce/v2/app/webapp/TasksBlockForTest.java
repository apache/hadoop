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
package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.util.HashMap;
import java.util.Map;

/**
 *    Class TasksBlockForTest overrides some methods for test
 */
public class TasksBlockForTest extends TasksBlock{
  private final Map<String, String> params = new HashMap<String, String>();


  public TasksBlockForTest(App app) {
     super(app);
   }

  public void addParameter(String name, String value) {
    params.put(name, value);
  }
  @Override
  public String $(String key, String defaultValue) {
    String value = params.get(key);
    return value == null ? defaultValue : value;
  }
  public String url(String... parts) {
    String result = "url://";
    for (String string : parts) {
      result += string +":";
   }
    return result;
  }
}
