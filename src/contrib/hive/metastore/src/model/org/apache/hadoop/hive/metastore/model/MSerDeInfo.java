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

package org.apache.hadoop.hive.metastore.model;

import java.util.Map;

public class MSerDeInfo {
  private String name;
  private String serializationLib;
  private Map<String, String> parameters;

  /**
   * @param name
   * @param serializationLib
   * @param parameters
   */
  public MSerDeInfo(String name, String serializationLib, Map<String, String> parameters) {
    this.name = name;
    this.serializationLib = serializationLib;
    this.parameters = parameters;
  }

  /**
   * @return the serializationLib
   */
  public String getSerializationLib() {
    return serializationLib;
  }

  /**
   * @param serializationLib the serializationLib to set
   */
  public void setSerializationLib(String serializationLib) {
    this.serializationLib = serializationLib;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the parameters
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

}
