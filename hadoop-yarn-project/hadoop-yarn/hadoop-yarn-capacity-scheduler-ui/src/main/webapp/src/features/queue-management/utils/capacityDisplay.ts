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
 * Capacity display utilities
 *
 * Functions for parsing and formatting capacity values for display in queue cards.
 */

import {
  parseCapacityValue,
  parseResourceVector,
  type ResourceVectorEntry,
} from '~/utils/capacityUtils';

export type CapacityDisplay =
  | { type: 'vector'; entries: ResourceVectorEntry[]; raw: string }
  | { type: 'percentage'; formatted: string; raw: string }
  | { type: 'weight'; formatted: string; raw: string }
  | { type: 'unknown'; raw?: string };

export const PRIORITY_RESOURCES = ['memory', 'vcores'];
export const INLINE_RESOURCE_LIMIT = 2;

export const normalizeResourceKey = (resource: string): string => resource.toLowerCase();

export const createEntryMap = (
  entries: ResourceVectorEntry[],
): Map<string, ResourceVectorEntry> => {
  const map = new Map<string, ResourceVectorEntry>();
  entries.forEach((entry) => {
    map.set(normalizeResourceKey(entry.resource), entry);
  });
  return map;
};

export const getResourceOrder = (
  capacityEntries: ResourceVectorEntry[],
  maxEntries: ResourceVectorEntry[],
): string[] => {
  const ordered: string[] = [];
  const seen = new Set<string>();

  const register = (entry?: ResourceVectorEntry) => {
    if (!entry) {
      return;
    }
    const key = normalizeResourceKey(entry.resource);
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    ordered.push(entry.resource);
  };

  PRIORITY_RESOURCES.forEach((priority) => {
    const match =
      capacityEntries.find((entry) => normalizeResourceKey(entry.resource) === priority) ??
      maxEntries.find((entry) => normalizeResourceKey(entry.resource) === priority);
    register(match);
  });

  capacityEntries.forEach((entry) => register(entry));
  maxEntries.forEach((entry) => register(entry));

  return ordered;
};

export const getCapacityDisplay = (input?: string): CapacityDisplay => {
  if (!input) {
    return { type: 'unknown', raw: input };
  }

  const trimmed = input.trim();
  if (!trimmed) {
    return { type: 'unknown', raw: trimmed };
  }

  const parsed = parseCapacityValue(trimmed);

  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    return {
      type: 'vector',
      entries: parseResourceVector(trimmed),
      raw: trimmed,
    };
  }

  if (!parsed) {
    return { type: 'unknown', raw: trimmed };
  }

  switch (parsed.type) {
    case 'percentage': {
      const formatted = trimmed.endsWith('%') ? trimmed : `${parsed.value}%`;
      return { type: 'percentage', formatted, raw: trimmed };
    }
    case 'weight': {
      const formatted = trimmed.endsWith('w') ? trimmed : `${parsed.value}w`;
      return { type: 'weight', formatted, raw: trimmed };
    }
    case 'absolute': {
      return {
        type: 'vector',
        entries: parseResourceVector(trimmed),
        raw: trimmed,
      };
    }
    default:
      return { type: 'unknown', raw: trimmed };
  }
};
