/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Mark field to be configurable from ozone-site.xml.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Config {

  /**
   * Configuration fragment relative to the prefix defined with @ConfigGroup.
   */
  String key();

  /**
   * Default value to use if not set.
   */
  String defaultValue();

  /**
   * Custom description as a help.
   */
  String description();

  /**
   * Type of configuration. Use AUTO to decide it based on the java type.
   */
  ConfigType type() default ConfigType.AUTO;

  /**
   * If type == TIME the unit should be defined with this attribute.
   */
  TimeUnit timeUnit() default TimeUnit.MILLISECONDS;

  ConfigTag[] tags();
}
