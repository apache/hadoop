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


/**
 * Utilities for accessing and filtering configuration properties
 */

/**
 * Get all properties for a specific queue from the configuration map
 * @param configData The configuration data map
 * @param queuePath The queue path (e.g., "root.default")
 * @returns Record of property names to values for the queue
 */
export function getQueueProperties(
  configData: Map<string, string>,
  queuePath: string,
): Record<string, string> {
  const prefix = `yarn.scheduler.capacity.${queuePath}.`;
  const properties: Record<string, string> = {};

  configData.forEach((value, key) => {
    if (key.startsWith(prefix)) {
      const propName = key.substring(prefix.length);
      // Skip nested properties (e.g., accessible-node-labels.gpu.capacity)
      if (!propName.includes('.')) {
        properties[propName] = value;
      }
    }
  });

  return properties;
}
