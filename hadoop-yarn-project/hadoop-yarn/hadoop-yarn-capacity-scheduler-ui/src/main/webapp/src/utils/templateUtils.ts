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
 * Utilities for working with auto-created queue templates
 */

/**
 * Template suffix constants for auto-created queues
 */
export const TEMPLATE_SUFFIXES = {
  /** Legacy auto-created leaf queues */
  LEGACY: 'leaf-queue-template',
  /** Flexible auto-created queues (shared template) */
  FLEXIBLE_TEMPLATE: 'auto-queue-creation-v2.template',
  /** Flexible auto-created leaf queues */
  FLEXIBLE_LEAF: 'auto-queue-creation-v2.leaf-template',
  /** Flexible auto-created parent queues */
  FLEXIBLE_PARENT: 'auto-queue-creation-v2.parent-template',
} as const;

/**
 * Check if a queue path represents a template queue
 * @param queuePath Queue path to check
 * @returns True if the path includes a template marker
 */
export function isTemplateQueuePath(queuePath: string): boolean {
  return [TEMPLATE_SUFFIXES.LEGACY, 'auto-queue-creation-v2.'].some((marker) =>
    queuePath.includes(marker),
  );
}
