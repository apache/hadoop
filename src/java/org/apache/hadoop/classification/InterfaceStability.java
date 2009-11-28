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
 * Annotation to inform users of how much to rely on a particular package,
 * class or method not changing over time.
 */
public class InterfaceStability {
  /**
   * Can evolve while retaining compatibility for minor release boundaries.; 
   * can break compatibility only at major release (ie. at m.0).
   */
  @Documented
  public @interface Stable {};
  
  /**
   * Evolving, but can break compatibility at minor release (i.e. m.x)
   */
  @Documented
  public @interface Evolving {};
  
  /**
   * No guarantee is provided as to reliability or stability across any
   * level of release granularity.
   */
  @Documented
  public @interface Unstable {};
}
