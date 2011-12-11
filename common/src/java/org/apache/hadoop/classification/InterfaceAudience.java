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
package org.apache.hadoop.classification;

import java.lang.annotation.Documented;

/**
 * Annotation to inform users of a package, class or method's intended audience.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceAudience {
  /**
   * Intended for use by any project or application.
   */
  @Documented public @interface Public {};
  
  /**
   * Intended only for the project(s) specified in the annotation.
   * For example, "Common", "HDFS", "MapReduce", "ZooKeeper", "HBase".
   */
  @Documented public @interface LimitedPrivate {
    String[] value();
  };
  
  /**
   * Intended for use only within Hadoop itself.
   */
  @Documented public @interface Private {};

  private InterfaceAudience() {} // Audience can't exist on its own
}
