/**
 * Queue state management utilities for mutation operations
 *
 * These functions help determine which queues need to be stopped/started
 * during mutation operations and prepare mutation requests for submission.
 */

import { AUTO_CREATION_PROPS, MUTATION_OPERATIONS, SPECIAL_VALUES } from '~/types';
import type { SchedConfUpdateInfo, StagedChange } from '~/types';
import { buildGlobalPropertyKey } from '~/utils/propertyUtils';

const MUTATION_VERSION_PROPERTY_KEY = 'yarn.webservice.mutation-api.version';

/**
 * Get parent queues that need to be stopped when adding child queues.
 * Parent queues must be stopped before children can be added.
 */
export function getParentQueuesForAdditions(
  addQueueMutations: SchedConfUpdateInfo[typeof MUTATION_OPERATIONS.ADD_QUEUE],
): string[] {
  const parents = new Set<string>();

  for (const mutation of addQueueMutations ?? []) {
    const queueName = mutation['queue-name'];
    const lastDotIndex = queueName.lastIndexOf('.');
    if (lastDotIndex <= 0) {
      continue;
    }

    const parentQueue = queueName.slice(0, lastDotIndex);
    if (parentQueue === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      continue;
    }

    parents.add(parentQueue);
  }

  return Array.from(parents);
}

/**
 * Get queues that need to be stopped for removal.
 */
export function getQueuesForRemoval(
  removeQueueMutations: SchedConfUpdateInfo[typeof MUTATION_OPERATIONS.REMOVE_QUEUE],
): string[] {
  if (!removeQueueMutations) {
    return [];
  }

  if (Array.isArray(removeQueueMutations)) {
    return removeQueueMutations.filter((queue): queue is string => typeof queue === 'string');
  }

  if (typeof removeQueueMutations === 'string') {
    return [removeQueueMutations];
  }

  return [];
}

/**
 * Get queues that need to be stopped when enabling auto-creation.
 * Queues must be stopped before auto-creation can be enabled.
 */
export function getQueuesForAutoCreationEnable(changes: StagedChange[]): string[] {
  if (!changes.length) {
    return [];
  }

  const queues = new Set<string>();
  const normalize = (value?: string) => value?.trim().toLowerCase() ?? '';

  for (const change of changes) {
    if (
      change.type !== 'update' ||
      !change.queuePath ||
      change.queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME ||
      change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH
    ) {
      continue;
    }

    if (
      change.property === AUTO_CREATION_PROPS.LEGACY_ENABLED ||
      change.property === AUTO_CREATION_PROPS.FLEXIBLE_ENABLED
    ) {
      const newValue = normalize(change.newValue);
      const oldValue = normalize(change.oldValue);

      if (newValue === 'true' && oldValue !== 'true') {
        queues.add(change.queuePath);
      }
    }
  }

  return Array.from(queues);
}

/**
 * Prepare mutation request for submission.
 * - Converts queue states to STOPPED for new queues (they'll be started after)
 * - Normalizes global property keys
 */
export function prepareMutationRequestForSubmission(request: SchedConfUpdateInfo): {
  request: SchedConfUpdateInfo;
  childQueuesToStart: string[];
} {
  const clonedRequest = JSON.parse(JSON.stringify(request)) as SchedConfUpdateInfo;
  const childQueuesToStart: string[] = [];

  const addQueueMutations = clonedRequest[MUTATION_OPERATIONS.ADD_QUEUE] ?? [];
  for (const mutation of addQueueMutations) {
    const stateEntry = mutation.params.entry.find((entry) => entry.key === 'state');
    if (!stateEntry) continue;

    const desiredState = stateEntry.value?.toUpperCase?.();
    if (desiredState === 'RUNNING') {
      childQueuesToStart.push(mutation['queue-name']);
      stateEntry.value = 'STOPPED';
    }
  }

  const globalUpdateBlocks = clonedRequest[MUTATION_OPERATIONS.GLOBAL_UPDATES];
  if (globalUpdateBlocks) {
    for (const block of globalUpdateBlocks) {
      block.entry = block.entry.map(({ key, value }) => ({
        key: buildGlobalPropertyKey(key),
        value,
      }));
    }
  }

  return { request: clonedRequest, childQueuesToStart };
}

/**
 * Add version information to a mutation request.
 */
export function prepareMutationRequestWithVersion(
  request: SchedConfUpdateInfo,
  version?: string | number,
): SchedConfUpdateInfo {
  const clonedRequest = JSON.parse(JSON.stringify(request)) as SchedConfUpdateInfo;

  const existingGlobalUpdates =
    clonedRequest[MUTATION_OPERATIONS.GLOBAL_UPDATES]?.filter((block) => block.entry.length > 0) ??
    [];

  for (const block of existingGlobalUpdates) {
    block.entry = block.entry.map(({ key, value }) => ({
      key: buildGlobalPropertyKey(key),
      value,
    }));
  }

  if (version !== undefined) {
    const versionValue = String(version);
    let versionEntryUpdated = false;

    for (const block of existingGlobalUpdates) {
      const entry = block.entry.find((item) => item.key === MUTATION_VERSION_PROPERTY_KEY);
      if (entry) {
        entry.value = versionValue;
        versionEntryUpdated = true;
        break;
      }
    }

    if (!versionEntryUpdated) {
      existingGlobalUpdates.unshift({
        entry: [{ key: MUTATION_VERSION_PROPERTY_KEY, value: versionValue }],
      });
    }
  }

  if (existingGlobalUpdates.length > 0) {
    clonedRequest[MUTATION_OPERATIONS.GLOBAL_UPDATES] = existingGlobalUpdates;
  } else {
    delete clonedRequest[MUTATION_OPERATIONS.GLOBAL_UPDATES];
  }

  return clonedRequest;
}

/**
 * Apply queue states (STOPPED or RUNNING) via the API client.
 * Filters out root queue and empty queue names.
 */
export async function applyQueueStates(
  queueNames: Iterable<string>,
  state: 'STOPPED' | 'RUNNING',
  apiClient: { updateSchedulerConf: (mutation: SchedConfUpdateInfo) => Promise<void> },
): Promise<void> {
  const uniqueQueueNames = Array.from(new Set(queueNames)).filter(
    (queueName) => queueName && queueName !== SPECIAL_VALUES.ROOT_QUEUE_NAME,
  );
  if (uniqueQueueNames.length === 0) return;

  const stateMutation: SchedConfUpdateInfo = {
    [MUTATION_OPERATIONS.UPDATE_QUEUE]: [
      ...uniqueQueueNames.map((queueName) => ({
        'queue-name': queueName,
        params: {
          entry: [{ key: 'state', value: state }],
        },
      })),
    ],
  };

  await apiClient.updateSchedulerConf(stateMutation);
}

/**
 * Collect all queues in a hierarchy starting from a given path.
 * Uses a callback to get child queues to avoid store dependency.
 */
export function collectQueueHierarchy(
  queuePath: string,
  getChildQueues: (path: string) => Array<{ queuePath?: string; queueName: string }>,
): string[] {
  const visited = new Set<string>();

  const traverse = (path: string) => {
    if (!path || visited.has(path)) {
      return;
    }

    visited.add(path);

    const childQueues = getChildQueues(path)
      .map((child) => child.queuePath || `${path}.${child.queueName}`)
      .filter(
        (childPath): childPath is string => typeof childPath === 'string' && childPath.length > 0,
      );

    for (const childQueue of childQueues) {
      traverse(childQueue);
    }
  };

  traverse(queuePath);
  return Array.from(visited);
}

/**
 * Add all queues in a hierarchy to a tracking set.
 * Excludes root queue.
 */
export function addQueueHierarchyToSet(
  queuePath: string,
  trackingSet: Set<string>,
  getChildQueues: (path: string) => Array<{ queuePath?: string; queueName: string }>,
): void {
  for (const path of collectQueueHierarchy(queuePath, getChildQueues)) {
    if (path !== SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      trackingSet.add(path);
    }
  }
}

/**
 * Restart queues by setting their state to RUNNING.
 * Clears the tracking set after completion.
 */
export async function restartQueues(
  queueSet: Set<string>,
  apiClient: { updateSchedulerConf: (mutation: SchedConfUpdateInfo) => Promise<void> },
): Promise<void> {
  if (queueSet.size === 0) return;

  try {
    await applyQueueStates(queueSet, 'RUNNING', apiClient);
  } catch (startError) {
    console.error(`Failed to restart queues:`, startError);
  } finally {
    queueSet.clear();
  }
}
