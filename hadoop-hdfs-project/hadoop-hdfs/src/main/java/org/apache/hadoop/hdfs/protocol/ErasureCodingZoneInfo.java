/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.io.erasurecode.ECSchema;

/**
 * Information about the EC Zone at the specified path.
 */
public class ErasureCodingZoneInfo {

  private String dir;
  private ECSchema schema;

  public ErasureCodingZoneInfo(String dir, ECSchema schema) {
    this.dir = dir;
    this.schema = schema;
  }

  /**
   * Get directory of the EC zone.
   * 
   * @return
   */
  public String getDir() {
    return dir;
  }

  /**
   * Get the schema for the EC Zone
   * 
   * @return
   */
  public ECSchema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "Dir: " + getDir() + ", Schema: " + schema;
  }
}
