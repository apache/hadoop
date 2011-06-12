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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsSource;

/**
 * Metrics annotation helpers.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetricsAnnotations {
  /**
   * Make an metrics source from an annotated object.
   * @param source  the annotated object.
   * @return a metrics source
   */
  public static MetricsSource makeSource(Object source) {
    return new MetricsSourceBuilder(source,
        DefaultMetricsFactory.getAnnotatedMetricsFactory()).build();
  }

  public static MetricsSourceBuilder newSourceBuilder(Object source) {
    return new MetricsSourceBuilder(source,
        DefaultMetricsFactory.getAnnotatedMetricsFactory());
  }
}
