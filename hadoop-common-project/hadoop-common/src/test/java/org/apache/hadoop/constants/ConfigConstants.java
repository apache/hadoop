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

package org.apache.hadoop.constants;

/**
 * Evolving config constants class used in various hadoop tests.
 */
public final class ConfigConstants {

  private ConfigConstants() {}

 /**
  * System property name for the avro dependency.
  * This property is used to configure trusted packages,
  * which the avro dependency can use for serialization.
  */
  public static final String CONFIG_AVRO_SERIALIZABLE_PACKAGES =
      "org.apache.avro.SERIALIZABLE_PACKAGES";
}
