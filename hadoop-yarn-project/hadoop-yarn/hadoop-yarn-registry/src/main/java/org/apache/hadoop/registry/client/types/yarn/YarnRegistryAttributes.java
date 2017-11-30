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

package org.apache.hadoop.registry.client.types.yarn;

/**
 * YARN specific attributes in the registry.
 */
public final class YarnRegistryAttributes {

  /**
   * Hidden constructor.
   */
  private YarnRegistryAttributes() {
  }

  /**
   * ID. For containers: container ID. For application instances,
   * application ID.
   */
  public static final String YARN_ID = "yarn:id";
  public static final String YARN_PERSISTENCE = "yarn:persistence";
  public static final String YARN_PATH = "yarn:path";
  public static final String YARN_HOSTNAME = "yarn:hostname";
  public static final String YARN_IP = "yarn:ip";
}
