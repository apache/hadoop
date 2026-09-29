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
 * Mutation builder for YARN Capacity Scheduler configuration updates
 *
 * This module provides utilities for building mutation requests from staged changes
 * to be sent to the YARN REST API for applying configuration updates.
 */

import type { StagedChange, SchedConfUpdateInfo } from '~/types';
import { SPECIAL_VALUES, MUTATION_OPERATIONS } from '~/types';
import { buildGlobalPropertyKey } from '~/utils/propertyUtils';
import { groupBy } from 'es-toolkit';

/**
 * Builds a complete mutation request from staged changes
 *
 * Groups changes by type (update, add, remove) and queue, then formats them
 * according to the YARN scheduler configuration API requirements.
 *
 * @param stagedChanges Array of staged changes to build into a request
 * @returns SchedConfUpdateInfo object ready to be sent to the API
 */
export function buildMutationRequest(stagedChanges: StagedChange[]): SchedConfUpdateInfo {
  const request: SchedConfUpdateInfo = {};

  // Separate global changes from queue-specific changes
  const globalChanges = stagedChanges.filter(
    (change) => change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
  );
  const queueChanges = stagedChanges.filter(
    (change) => change.queuePath !== SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
  );

  // Group queue changes by path for efficient processing
  const changesByQueue = groupBy(queueChanges, (change: StagedChange) => change.queuePath);

  const updatesByQueue = new Map<string, Record<string, string>>();
  const addsByQueue = new Map<string, Record<string, string>>();
  const removals: string[] = [];
  const globalUpdates: Record<string, string> = {};

  // Process global changes
  for (const change of globalChanges) {
    if (change.property && change.newValue !== undefined) {
      globalUpdates[change.property] = change.newValue;
    }
  }

  // Process queue changes efficiently using grouped data
  for (const [queuePath, changes] of Object.entries(changesByQueue)) {
    for (const change of changes as StagedChange[]) {
      switch (change.type) {
        case 'update': {
          if (!change.property || change.newValue === undefined) continue;

          const updates = updatesByQueue.get(queuePath) || {};
          updates[change.property] = change.newValue;
          updatesByQueue.set(queuePath, updates);
          break;
        }
        case 'add': {
          if (!change.property || change.newValue === undefined) continue;

          const adds = addsByQueue.get(queuePath) || {};
          adds[change.property] = change.newValue;
          addsByQueue.set(queuePath, adds);
          break;
        }
        case 'remove': {
          if (change.property === SPECIAL_VALUES.QUEUE_MARKER) {
            removals.push(queuePath);
          }
          break;
        }
      }
    }
  }

  if (updatesByQueue.size > 0) {
    request[MUTATION_OPERATIONS.UPDATE_QUEUE] = Array.from(updatesByQueue.entries()).map(
      ([queuePath, params]) => ({
        'queue-name': queuePath,
        params: {
          entry: Object.entries(params).map(([key, value]) => ({ key, value })),
        },
      }),
    );
  }

  if (addsByQueue.size > 0) {
    request[MUTATION_OPERATIONS.ADD_QUEUE] = Array.from(addsByQueue.entries()).map(
      ([queuePath, params]) => ({
        'queue-name': queuePath,
        params: {
          entry: Object.entries(params).map(([key, value]) => ({ key, value })),
        },
      }),
    );
  }

  if (removals.length === 1) {
    request[MUTATION_OPERATIONS.REMOVE_QUEUE] = removals[0];
  } else if (removals.length > 1) {
    request[MUTATION_OPERATIONS.REMOVE_QUEUE] = removals;
  }

  const globalUpdateEntries = Object.entries(globalUpdates).map(([key, value]) => ({
    key: buildGlobalPropertyKey(key),
    value,
  }));

  if (globalUpdateEntries.length > 0) {
    request[MUTATION_OPERATIONS.GLOBAL_UPDATES] = [
      {
        entry: globalUpdateEntries,
      },
    ];
  }

  return request;
}

/**
 * Groups staged changes by queue path
 *
 * @param changes Array of staged changes
 * @returns Map of queue paths to their changes
 */
export function groupChangesByQueue(changes: StagedChange[]): Map<string, StagedChange[]> {
  const grouped = groupBy(changes, (change: StagedChange) => change.queuePath);
  return new Map(Object.entries(grouped));
}
