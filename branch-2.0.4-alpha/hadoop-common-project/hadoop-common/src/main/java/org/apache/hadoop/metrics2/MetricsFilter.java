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

package org.apache.hadoop.metrics2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The metrics filter interface
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class MetricsFilter implements MetricsPlugin {

  /**
   * Whether to accept the name
   * @param name  to filter on
   * @return  true to accept; false otherwise.
   */
  public abstract boolean accepts(String name);

  /**
   * Whether to accept the tag
   * @param tag to filter on
   * @return  true to accept; false otherwise
   */
  public abstract boolean accepts(MetricsTag tag);

  /**
   * Whether to accept the tags
   * @param tags to filter on
   * @return  true to accept; false otherwise
   */
  public abstract boolean accepts(Iterable<MetricsTag> tags);

  /**
   * Whether to accept the record
   * @param record  to filter on
   * @return  true to accept; false otherwise.
   */
  public boolean accepts(MetricsRecord record) {
    return accepts(record.name()) && accepts(record.tags());
  }

}
