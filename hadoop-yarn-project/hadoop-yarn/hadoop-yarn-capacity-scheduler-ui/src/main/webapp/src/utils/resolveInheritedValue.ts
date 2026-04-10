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

import type { InheritanceResolver, InheritedValueInfo } from '~/types/property-descriptor';
import type { StagedChange } from '~/types';
import { SPECIAL_VALUES } from '~/types';
import { buildPropertyKey, buildGlobalPropertyKey, getParentQueuePath } from './propertyUtils';

export type { InheritedValueInfo } from '~/types/property-descriptor';

interface ResolveOptions {
  queuePath: string;
  propertyName: string;
  configData: Map<string, string>;
  stagedChanges?: StagedChange[];
  inheritanceResolver?: InheritanceResolver;
}

export function getQueueValue(
  queuePath: string,
  propertyName: string,
  configData: Map<string, string>,
  stagedChanges?: StagedChange[],
): string | undefined {
  const staged = stagedChanges?.find(
    (c) => c.queuePath === queuePath && c.property === propertyName && c.newValue !== undefined,
  );
  if (staged) {
    return staged.newValue;
  }

  const key = buildPropertyKey(queuePath, propertyName);
  const value = configData.get(key);
  return value || undefined;
}

export function getGlobalValue(
  propertyName: string,
  configData: Map<string, string>,
  stagedChanges?: StagedChange[],
): string | undefined {
  const staged = stagedChanges?.find(
    (c) =>
      c.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH &&
      c.property === propertyName &&
      c.newValue !== undefined,
  );
  if (staged) {
    return staged.newValue;
  }

  const key = buildGlobalPropertyKey(propertyName);
  const value = configData.get(key);
  return value || undefined;
}

/**
 * Walks parent queues to find the nearest ancestor with the property set.
 * No global fallback — the global key is often a different property entirely (e.g., preemption).
 */
export const parentChainResolver: InheritanceResolver = ({
  queuePath,
  propertyName,
  configData,
  stagedChanges,
}) => {
  let currentPath = getParentQueuePath(queuePath);
  while (currentPath) {
    const parentValue = getQueueValue(currentPath, propertyName, configData, stagedChanges);
    if (parentValue !== undefined) {
      return {
        value: parentValue,
        source: 'queue',
        sourcePath: currentPath,
      };
    }
    currentPath = getParentQueuePath(currentPath);
  }
  return null;
};

/**
 * Checks the global yarn.scheduler.capacity.<same-suffix> property. Skips parent queues.
 */
export const globalOnlyResolver: InheritanceResolver = ({
  propertyName,
  configData,
  stagedChanges,
}) => {
  const value = getGlobalValue(propertyName, configData, stagedChanges);
  if (value !== undefined) {
    return { value, source: 'global' };
  }
  return null;
};

/**
 * Resolves the inherited value source for a queue property by delegating
 * to the property's inheritanceResolver. Returns null if no resolver is set
 * or the resolver finds no inherited value.
 */
export function resolveInheritedValue(options: ResolveOptions): InheritedValueInfo | null {
  const { inheritanceResolver, ...context } = options;
  if (!inheritanceResolver) {
    return null;
  }
  return inheritanceResolver(context);
}
