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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Map;

/**
 * This is the JMX management interface for information related to
 * PipelineManager.
 */
@InterfaceAudience.Private
public interface PipelineManagerMXBean {

  /**
   * Returns the number of pipelines in different state.
   * @return state to number of pipeline map
   */
  Map<String, Integer> getPipelineInfo();

}
