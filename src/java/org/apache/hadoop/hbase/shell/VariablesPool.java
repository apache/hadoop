/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.shell;

import java.util.HashMap;

/**
 * Variable pool is a collection of substitution variables.
 */
public class VariablesPool {
  static HashMap<String, HashMap<String, VariableRef>> variables = new HashMap<String, HashMap<String, VariableRef>>();

  /**
   * puts the date in the substitution variable.
   * 
   * @param key
   * @param parentKey
   * @param statement
   */
  public static void put(String key, String parentKey, VariableRef statement) {
    HashMap<String, VariableRef> value = new HashMap<String, VariableRef>();
    value.put(parentKey, statement);
    variables.put(key, value);
  }

  /**
   * returns the substitution variable's value.
   * 
   * @param key
   * @return HashMap<String, VariableRef>
   */
  public static HashMap<String, VariableRef> get(String key) {
    return variables.get(key);
  }
}
