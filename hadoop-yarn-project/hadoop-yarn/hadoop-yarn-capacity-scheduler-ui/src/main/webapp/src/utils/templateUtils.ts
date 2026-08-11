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


import { getParentQueuePath } from '~/utils/propertyUtils';
import type { QueueCreationMethod } from '~/types/queue';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';

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

export type QueuePropertyLookup = (
  queuePath: string,
  property: string,
) => {
  value: string;
  isStaged: boolean;
};

export type ResolvedQueueCapacityConfig = {
  capacityConfig: string;
  maxCapacityConfig: string;
};

const isAutoCreationEnabled = (
  getQueuePropertyValue: QueuePropertyLookup,
  queuePath: string,
  property: string,
): boolean => getQueuePropertyValue(queuePath, property).value.trim() === 'true';

/**
 * Resolves the template queue path for an auto-created child queue based on the
 * parent's auto-creation settings:
 * - flexible v2 (`auto-queue-creation-v2.enabled`) → leaf-template
 * - legacy (`auto-create-child-queue.enabled`) → leaf-queue-template
 */
export function getAutoCreatedQueueTemplatePath(
  queuePath: string,
  getQueuePropertyValue: QueuePropertyLookup,
): string | null {
  const parentPath = getParentQueuePath(queuePath);
  if (!parentPath) {
    return null;
  }

  if (isAutoCreationEnabled(getQueuePropertyValue, parentPath, AUTO_CREATION_PROPS.FLEXIBLE_ENABLED)) {
    return `${parentPath}.${TEMPLATE_SUFFIXES.FLEXIBLE_LEAF}`;
  }

  if (isAutoCreationEnabled(getQueuePropertyValue, parentPath, AUTO_CREATION_PROPS.LEGACY_ENABLED)) {
    return `${parentPath}.${TEMPLATE_SUFFIXES.LEGACY}`;
  }

  return null;
}

export function resolveAutoCreatedQueueCapacityConfigs(
  queuePath: string,
  creationMethod: QueueCreationMethod | undefined,
  getQueuePropertyValue: QueuePropertyLookup,
  defaults: { capacity: string; maxCapacity: string } = { capacity: '0', maxCapacity: '100' },
): ResolvedQueueCapacityConfig {
  const capacityDisplay = getQueuePropertyValue(queuePath, 'capacity');
  const maxCapacityDisplay = getQueuePropertyValue(queuePath, 'maximum-capacity');

  let capacityConfig = capacityDisplay.value || defaults.capacity;
  let maxCapacityConfig = maxCapacityDisplay.value || defaults.maxCapacity;

  const isAutoCreatedQueue =
    creationMethod === 'dynamicLegacy' || creationMethod === 'dynamicFlexible';

  if (!isAutoCreatedQueue) {
    return { capacityConfig, maxCapacityConfig };
  }

  const templatePath = getAutoCreatedQueueTemplatePath(queuePath, getQueuePropertyValue);
  if (!templatePath) {
    return { capacityConfig, maxCapacityConfig };
  }

  const templateCapacity = getQueuePropertyValue(templatePath, 'capacity');
  if (templateCapacity.value.trim()) {
    capacityConfig = templateCapacity.value.trim();
  }

  const templateMaxCapacity = getQueuePropertyValue(templatePath, 'maximum-capacity');
  if (templateMaxCapacity.value.trim()) {
    maxCapacityConfig = templateMaxCapacity.value.trim();
  }

  return { capacityConfig, maxCapacityConfig };
}
