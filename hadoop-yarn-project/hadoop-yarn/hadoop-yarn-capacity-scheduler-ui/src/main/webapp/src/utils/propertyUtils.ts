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
 * Property utilities for YARN Capacity Scheduler configuration
 *
 * These utilities handle building and parsing property keys for:
 * - Queue properties: yarn.scheduler.capacity.<queue-path>.<property>
 * - Global properties: yarn.scheduler.capacity.<property>
 * - Node label properties: yarn.scheduler.capacity.<queue-path>.accessible-node-labels.<label>.<property>
 */

import { SPECIAL_VALUES, CONFIG_PREFIXES, getQueueNameValidationError } from '~/types';

const YARN_SCHEDULER_PREFIX = CONFIG_PREFIXES.BASE;
const NODE_LABELS_SEGMENT = 'accessible-node-labels';

/**
 * Builds a property key for a queue property
 * @param queuePath The full queue path (e.g. 'root.production.team1')
 * @param property The property name (e.g. 'capacity')
 * @returns The full property key (e.g. 'yarn.scheduler.capacity.root.production.team1.capacity')
 */
export function buildPropertyKey(queuePath: string, property: string): string {
  return `${YARN_SCHEDULER_PREFIX}.${queuePath}.${property}`;
}

/**
 * Builds a property key for a global scheduler property
 * @param property The property name (e.g. 'maximum-applications')
 * @returns The full property key (e.g. 'yarn.scheduler.capacity.maximum-applications')
 */
export function buildGlobalPropertyKey(property: string): string {
  const prefix = `${YARN_SCHEDULER_PREFIX}.`;
  // Avoid double-prefixing when callers already pass the fully-qualified key
  if (property.startsWith(prefix) || property === YARN_SCHEDULER_PREFIX) {
    return property;
  }
  return `${prefix}${property}`;
}

/**
 * Builds a property key for a node label property
 * @param queuePath The full queue path (e.g. 'root.production')
 * @param label The node label name (e.g. 'gpu')
 * @param property The property name (e.g. 'capacity')
 * @returns The full property key (e.g. 'yarn.scheduler.capacity.root.production.accessible-node-labels.gpu.capacity')
 */
export function buildNodeLabelPropertyKey(
  queuePath: string,
  label: string,
  property: string,
): string {
  return `${YARN_SCHEDULER_PREFIX}.${queuePath}.${NODE_LABELS_SEGMENT}.${label}.${property}`;
}

/**
 * Result of queue name validation
 */
export type ValidationResult = {
  valid: boolean;
  message?: string;
};

/**
 * Validates a queue name according to YARN rules.
 * Uses getQueueNameValidationError() as the single source of truth for validation logic.
 * @param queueName The queue name to validate
 * @returns Validation result with error message if invalid
 */
export function validateQueueName(queueName: string): ValidationResult {
  const error = getQueueNameValidationError(queueName);
  if (error) {
    return { valid: false, message: error };
  }
  return { valid: true };
}

/**
 * Splits a queue path into segments
 * @param queuePath The queue path (e.g. 'root.production.team1')
 * @returns Array of path segments (e.g. ['root', 'production', 'team1'])
 */
export function splitQueuePath(queuePath: string): string[] {
  if (!queuePath) {
    return [];
  }
  return queuePath.split('.');
}

/**
 * Joins queue path segments into a path
 * @param segments Array of path segments (e.g. ['root', 'production', 'team1'])
 * @returns The joined queue path (e.g. 'root.production.team1')
 */
export function joinQueuePath(segments: string[]): string {
  return segments.join('.');
}

/**
 * Gets the queue name from a queue path
 * @param queuePath The queue path (e.g. 'root.production.team1')
 * @returns The queue name (e.g. 'team1')
 */
export function getQueueNameFromPath(queuePath: string): string {
  if (!queuePath) {
    return '';
  }
  const segments = splitQueuePath(queuePath);
  return segments[segments.length - 1] || '';
}

/**
 * Gets the parent queue path from a queue path
 * @param queuePath The queue path (e.g. 'root.production.team1')
 * @returns The parent queue path (e.g. 'root.production') or null if no parent
 */
export function getParentQueuePath(queuePath: string): string | null {
  if (!queuePath) {
    return null;
  }
  const segments = splitQueuePath(queuePath);
  if (segments.length <= 1) {
    return null;
  }
  return joinQueuePath(segments.slice(0, -1));
}

/**
 * Checks if a queue path represents the root queue
 * @param queuePath The queue path to check
 * @returns True if this is the root queue
 */
export function isRootQueue(queuePath: string): boolean {
  return queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME;
}

/**
 * Checks if a property key is a global property (not queue-specific)
 * @param propertyKey The full property key
 * @returns True if this is a global property
 */
export function isGlobalPropertyKey(propertyKey: string): boolean {
  if (!propertyKey.startsWith(YARN_SCHEDULER_PREFIX + '.')) {
    return false;
  }

  const suffix = propertyKey.substring(YARN_SCHEDULER_PREFIX.length + 1);

  return !suffix.startsWith(SPECIAL_VALUES.ROOT_QUEUE_NAME + '.');
}
