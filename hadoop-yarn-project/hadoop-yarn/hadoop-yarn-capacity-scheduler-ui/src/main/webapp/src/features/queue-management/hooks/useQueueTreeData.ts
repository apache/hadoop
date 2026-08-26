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


import type { Node, Edge } from '@xyflow/react';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type {
  QueueInfo,
  QueueType,
  StagedChange,
  SchedulerInfo,
  CapacitySchedulerInfo,
  QueueCapacitiesByPartition,
} from '~/types';
import type { QueueStateValue } from '~/types/constants/queue';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import { DagreLayout } from '~/features/queue-management/utils/DagreLayout';
import {
  QUEUE_CARD_CORNER_RADIUS,
  QUEUE_CARD_FLOW_MARGIN,
  QUEUE_CARD_HEIGHT,
  QUEUE_CARD_WIDTH,
} from '~/features/queue-management/constants';
import type { ValidationIssue } from '~/types';

export type QueueCardData = QueueInfo & {
  stagedStatus?: 'new' | 'modified' | 'deleted';
  isLeaf: boolean;
  capacityConfig: string;
  maxCapacityConfig: string;
  stagedState?: string;
  autoCreationEligibility?: string;
  autoCreationStatus?: { status: 'off' | 'legacy' | 'flexible'; isStaged: boolean };
  validationErrors?: ValidationIssue[];
  isAffectedByErrors?: boolean;
  errorSource?: string;
  isAutoCreatedQueue: boolean;
};

export type UseQueueTreeDataResult = {
  nodes: Node<QueueCardData>[];
  edges: Edge[];
  isLoading: boolean;
  loadError: string | null;
  applyError: string | null;
};

const layoutEngine = new DagreLayout({
  nodeWidth: QUEUE_CARD_WIDTH,
  nodeHeight: QUEUE_CARD_HEIGHT,
  horizontalSpacing: 120,
  verticalSpacing: 80,
  orientation: 'horizontal',
});

type QueueWithPartitions = QueueInfo & {
  capacities?: {
    queueCapacitiesByPartition?: QueueCapacitiesByPartition[] | QueueCapacitiesByPartition;
  };
};

type PartitionSource = CapacitySchedulerInfo | QueueWithPartitions;

const DEFAULT_PARTITION = '';

function toArray<T>(value: T | T[] | undefined): T[] {
  if (!value) {
    return [];
  }

  return Array.isArray(value) ? value : [value];
}

function normalizePartitionName(name?: string | null): string {
  return name ?? DEFAULT_PARTITION;
}

function getPartitionEntry(
  source: PartitionSource,
  partitionName: string,
): QueueCapacitiesByPartition | undefined {
  const partitions = source.capacities?.queueCapacitiesByPartition;
  if (!partitions) {
    return undefined;
  }

  const partitionArray = toArray(partitions);
  const normalizedTarget = normalizePartitionName(partitionName);

  const exactMatch = partitionArray.find(
    (entry) => normalizePartitionName(entry.partitionName) === normalizedTarget,
  );

  if (exactMatch) {
    return exactMatch;
  }

  return partitionArray.find(
    (entry) => normalizePartitionName(entry.partitionName) === DEFAULT_PARTITION,
  );
}

function applyPartitionCapacities(queueInfo: QueueInfo, partitionName: string): QueueInfo {
  const queueWithPartitions = { ...queueInfo } as QueueWithPartitions;
  const partitionEntry = getPartitionEntry(queueWithPartitions, partitionName);

  if (partitionEntry) {
    queueWithPartitions.capacity = partitionEntry.capacity ?? queueWithPartitions.capacity;
    queueWithPartitions.maxCapacity = partitionEntry.maxCapacity ?? queueWithPartitions.maxCapacity;
    queueWithPartitions.usedCapacity =
      partitionEntry.usedCapacity ?? queueWithPartitions.usedCapacity;
    queueWithPartitions.absoluteCapacity =
      partitionEntry.absoluteCapacity ?? queueWithPartitions.absoluteCapacity;
    queueWithPartitions.absoluteMaxCapacity =
      partitionEntry.absoluteMaxCapacity ?? queueWithPartitions.absoluteMaxCapacity;
    queueWithPartitions.absoluteUsedCapacity =
      partitionEntry.absoluteUsedCapacity ?? queueWithPartitions.absoluteUsedCapacity;

    if (partitionEntry.usedResource) {
      queueWithPartitions.resourcesUsed = partitionEntry.usedResource;
    }
  }

  if (queueInfo.queues?.queue) {
    const children = toArray(queueInfo.queues.queue);
    queueWithPartitions.queues = {
      queue: children.map((child) => applyPartitionCapacities(child, partitionName)),
    };
  }

  return queueWithPartitions;
}

function createRootQueueInfo(schedulerInfo: SchedulerInfo, partitionName: string): QueueInfo {
  const capacitySchedulerInfo = schedulerInfo as CapacitySchedulerInfo;
  const partitionEntry = getPartitionEntry(capacitySchedulerInfo, partitionName);

  const resolvedCapacity = partitionEntry?.capacity ?? schedulerInfo.capacity;
  const resolvedUsedCapacity = partitionEntry?.usedCapacity ?? schedulerInfo.usedCapacity;
  const resolvedMaxCapacity = partitionEntry?.maxCapacity ?? schedulerInfo.maxCapacity;
  const resolvedAbsoluteCapacity = partitionEntry?.absoluteCapacity ?? schedulerInfo.capacity;
  const resolvedAbsoluteMaxCapacity =
    partitionEntry?.absoluteMaxCapacity ?? schedulerInfo.maxCapacity;
  const resolvedAbsoluteUsedCapacity =
    partitionEntry?.absoluteUsedCapacity ?? schedulerInfo.usedCapacity;

  const resourcesUsed = partitionEntry?.usedResource ??
    capacitySchedulerInfo.usedResources ?? { memory: 0, vCores: 0 };

  const childQueues = toArray(schedulerInfo.queues?.queue).map((queue) =>
    applyPartitionCapacities(queue, partitionName),
  );

  // Create a synthetic root queue that contains the scheduler's queues
  return {
    queueType: 'parent' as QueueType,
    capacity: resolvedCapacity,
    usedCapacity: resolvedUsedCapacity,
    maxCapacity: resolvedMaxCapacity,
    absoluteCapacity: resolvedAbsoluteCapacity,
    absoluteMaxCapacity: resolvedAbsoluteMaxCapacity,
    absoluteUsedCapacity: resolvedAbsoluteUsedCapacity,
    numApplications: 0,
    numActiveApplications: 0,
    numPendingApplications: 0,
    resourcesUsed,
    queueName: schedulerInfo.queueName,
    queuePath: 'root',
    state: 'RUNNING' as QueueStateValue,
    queues: { queue: childQueues },
    autoCreationEligibility: capacitySchedulerInfo.autoCreationEligibility,
    creationMethod: 'static',
  };
}

function getStagedStatus(
  queuePath: string,
  stagedChanges: StagedChange[],
): 'new' | 'modified' | 'deleted' | undefined {
  const changes = stagedChanges.filter((change) => change.queuePath === queuePath);

  if (changes.some((c) => c.type === 'remove')) {
    return 'deleted';
  }
  // Check if any change is type 'add' (indicating new queue)
  if (changes.some((c) => c.type === 'add')) {
    return 'new';
  }
  if (changes.length > 0) {
    return 'modified';
  }

  return undefined;
}

function getAutoCreationStatus(
  queuePath: string,
  liveAutoCreationEligibility: string | undefined,
  stagedChanges: StagedChange[],
): { status: 'off' | 'legacy' | 'flexible'; isStaged: boolean } {
  const legacyChange = stagedChanges.find(
    (c) => c.queuePath === queuePath && c.property === AUTO_CREATION_PROPS.LEGACY_ENABLED,
  );
  const flexibleChange = stagedChanges.find(
    (c) => c.queuePath === queuePath && c.property === AUTO_CREATION_PROPS.FLEXIBLE_ENABLED,
  );

  if (legacyChange || flexibleChange) {
    const isLegacyEnabled = legacyChange?.newValue === 'true';
    const isFlexibleEnabled = flexibleChange?.newValue === 'true';

    if (isFlexibleEnabled) {
      return { status: 'flexible' as const, isStaged: true };
    } else if (isLegacyEnabled) {
      return { status: 'legacy' as const, isStaged: true };
    } else {
      return { status: 'off' as const, isStaged: true };
    }
  }

  const status =
    liveAutoCreationEligibility === AUTO_CREATION_PROPS.ELIGIBILITY_FLEXIBLE
      ? ('flexible' as const)
      : liveAutoCreationEligibility === AUTO_CREATION_PROPS.ELIGIBILITY_LEGACY
        ? ('legacy' as const)
        : ('off' as const);

  return {
    status,
    isStaged: false,
  };
}

function transformToCardData(queueInfo: QueueInfo, stagedChanges: StagedChange[]): QueueCardData {
  const getQueuePropertyValue = useSchedulerStore.getState().getQueuePropertyValue;

  const capacityDisplay = getQueuePropertyValue(queueInfo.queuePath, 'capacity');
  const maxCapacityDisplay = getQueuePropertyValue(queueInfo.queuePath, 'maximum-capacity');

  const stateDisplay = getQueuePropertyValue(queueInfo.queuePath, 'state');

  // Collect validation errors for this queue
  const directErrors: ValidationIssue[] = [];
  let isAffectedByErrors = false;
  let errorSource: string | undefined;

  stagedChanges.forEach((change) => {
    if (change.validationErrors && change.validationErrors.length > 0) {
      // Check if this queue has direct errors
      if (change.queuePath === queueInfo.queuePath) {
        directErrors.push(...change.validationErrors);
      } else {
        // Check if this queue is affected by errors from other queues
        // For capacity sum errors, the parent queue is affected by child changes
        change.validationErrors.forEach((error) => {
          if (error.rule === 'child-capacity-sum' || error.rule === 'capacity-type-consistency') {
            // Get parent path of the changed queue
            const changedQueueParts = change.queuePath.split('.');
            if (changedQueueParts.length > 1) {
              const parentPath = changedQueueParts.slice(0, -1).join('.');
              if (parentPath === queueInfo.queuePath) {
                isAffectedByErrors = true;
                errorSource = change.queuePath;
              }
            }
          }
        });
      }
    }
  });

  return {
    ...queueInfo,

    stagedStatus: getStagedStatus(queueInfo.queuePath, stagedChanges),
    isLeaf:
      !queueInfo.queues?.queue ||
      (Array.isArray(queueInfo.queues.queue) ? queueInfo.queues.queue.length === 0 : false),

    capacityConfig: capacityDisplay.value || '0',
    maxCapacityConfig: maxCapacityDisplay.value || '100',

    stagedState: stateDisplay.isStaged ? stateDisplay.value : undefined,

    autoCreationEligibility: queueInfo.autoCreationEligibility,

    autoCreationStatus: getAutoCreationStatus(
      queueInfo.queuePath,
      queueInfo.autoCreationEligibility,
      stagedChanges,
    ),
    isAutoCreatedQueue:
      queueInfo.creationMethod === 'dynamicLegacy' ||
      queueInfo.creationMethod === 'dynamicFlexible',

    validationErrors: directErrors.length > 0 ? directErrors : undefined,
    isAffectedByErrors,
    errorSource,
  };
}

function flattenQueueTree(queueInfo: QueueInfo, stagedChanges: StagedChange[]): QueueCardData[] {
  const result: QueueCardData[] = [];

  result.push(transformToCardData(queueInfo, stagedChanges));

  if (queueInfo.queues?.queue) {
    const children = toArray(queueInfo.queues.queue);

    for (const child of children) {
      result.push(...flattenQueueTree(child, stagedChanges));
    }
  }

  return result;
}

function createNodes(
  queues: QueueCardData[],
  positions: Map<string, { x: number; y: number; width: number; height: number }>,
  stagedChanges: StagedChange[],
): Node<QueueCardData>[] {
  const nodes: Node<QueueCardData>[] = [];

  for (const queue of queues) {
    const position = positions.get(queue.queuePath);

    if (position) {
      nodes.push({
        id: queue.queuePath,
        type: 'queueCard',
        position: { x: position.x, y: position.y },
        data: queue,
        width: position.width,
        height: position.height,
      });
    }
  }

  // Get all unique queue paths from 'add' type changes
  const newQueuePaths = new Set(
    stagedChanges.filter((c) => c.type === 'add').map((c) => c.queuePath),
  );

  // For each new queue path, create a node if it doesn't already exist
  newQueuePaths.forEach((queuePath) => {
    if (!nodes.find((n) => n.id === queuePath)) {
      const position = positions.get(queuePath);
      const queueName = queuePath.split('.').pop() || '';

      // Get the staged values for this new queue
      const getQueuePropertyValue = useSchedulerStore.getState().getQueuePropertyValue;
      const capacityDisplay = getQueuePropertyValue(queuePath, 'capacity');
      const maxCapacityDisplay = getQueuePropertyValue(queuePath, 'maximum-capacity');
      const stateDisplay = getQueuePropertyValue(queuePath, 'state');

      // Collect validation errors from all staged changes for this queue
      const queueErrors = stagedChanges
        .filter((c) => c.type === 'add' && c.queuePath === queuePath && c.validationErrors)
        .flatMap((c) => c.validationErrors || []);

      const nodeData: QueueCardData = {
        queueType: 'leaf' as const,
        capacity: 0,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 0,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        queueName,
        queuePath,
        state: (stateDisplay.value || 'RUNNING') as QueueStateValue,
        creationMethod: 'static',

        stagedStatus: 'new' as const,
        isLeaf: true,
        capacityConfig: capacityDisplay.value || '0',
        maxCapacityConfig: maxCapacityDisplay.value || '100',
        stagedState: stateDisplay.value,
        validationErrors: queueErrors.length > 0 ? queueErrors : undefined,
        isAutoCreatedQueue: false,
      };

      if (position) {
        nodes.push({
          id: queuePath,
          type: 'queueCard',
          position: { x: position.x, y: position.y },
          data: nodeData,
          width: position.width,
          height: position.height,
        });
      } else {
        // Fallback position if not in layout
        const depth = queuePath.split('.').length - 1;
        nodes.push({
          id: queuePath,
          type: 'queueCard',
          position: { x: depth * 520, y: 0 },
          data: nodeData,
          width: QUEUE_CARD_WIDTH,
          height: QUEUE_CARD_HEIGHT,
        });
      }
    }
  });

  return nodes;
}

function createEdges(
  queueInfo: QueueInfo,
  positions: Map<string, { x: number; y: number; width: number; height: number }>,
  stagedChanges?: StagedChange[],
): Edge[] {
  const edges: Edge[] = [];
  const MIN_SEGMENT_HEIGHT = 4; // Minimum visible height for very small capacity percentages
  const DEFAULT_CARD_HEIGHT = QUEUE_CARD_HEIGHT;

  if (queueInfo.queues?.queue) {
    const children = toArray(queueInfo.queues.queue);

    const rawCapacities = children.map((child) => Math.max(child.capacity || 0, 0));
    const nonZeroSum = rawCapacities.reduce((sum, cap) => (cap > 0 ? sum + cap : sum), 0);
    const zeroIndices = rawCapacities
      .map((cap, index) => (cap <= 0 ? index : null))
      .filter((index): index is number => index !== null);

    let adjustedCapacities = [...rawCapacities];

    if (children.length > 0 && nonZeroSum === 0) {
      // If all capacities are zero, distribute equally
      adjustedCapacities = adjustedCapacities.map(() => 1);
    } else if (zeroIndices.length > 0) {
      const candidateTotals = [
        typeof queueInfo.capacity === 'number' && queueInfo.capacity > nonZeroSum
          ? queueInfo.capacity
          : 0,
        typeof queueInfo.maxCapacity === 'number' && queueInfo.maxCapacity > nonZeroSum
          ? queueInfo.maxCapacity
          : 0,
        nonZeroSum > 0 && nonZeroSum <= 100 ? 100 : 0,
        nonZeroSum + zeroIndices.length,
      ];

      const assumedTotal =
        candidateTotals.find((total) => total > nonZeroSum) ?? nonZeroSum + zeroIndices.length;

      let leftover = Math.max(assumedTotal - nonZeroSum, 0);
      if (leftover === 0) {
        leftover = nonZeroSum / zeroIndices.length || 1;
      }

      const sharePerZero = leftover / zeroIndices.length || 1;
      adjustedCapacities = adjustedCapacities.map((cap) => (cap > 0 ? cap : sharePerZero));
    }

    const totalChildCapacity = adjustedCapacities.reduce((sum, cap) => sum + cap, 0);

    // Get source position for the parent queue
    const sourcePos = positions.get(queueInfo.queuePath);

    if (sourcePos && totalChildCapacity > 0) {
      let cumulativeCapacity = 0;

      children.forEach((child, index) => {
        const targetPos = positions.get(child.queuePath);

        if (targetPos) {
          // Calculate proportional segment for this child on parent's side
          const childAdjustedCapacity = adjustedCapacities[index];
          const childPercentage = childAdjustedCapacity / totalChildCapacity;

          // Calculate segment boundaries (0.0 to 1.0 scale)
          const segmentStart = cumulativeCapacity / totalChildCapacity;
          const segmentEnd = (cumulativeCapacity + childAdjustedCapacity) / totalChildCapacity;

          const parentHeight = sourcePos.height ?? DEFAULT_CARD_HEIGHT;
          const parentFlowTop = sourcePos.y + QUEUE_CARD_FLOW_MARGIN;
          const parentFlowHeight = Math.max(
            parentHeight - 2 * QUEUE_CARD_FLOW_MARGIN,
            MIN_SEGMENT_HEIGHT,
          );
          const parentFlowBottom = parentFlowTop + parentFlowHeight;

          const childHeight = targetPos.height ?? DEFAULT_CARD_HEIGHT;
          const targetFlowTop = targetPos.y + QUEUE_CARD_FLOW_MARGIN;
          const targetFlowHeight = Math.max(
            childHeight - 2 * QUEUE_CARD_FLOW_MARGIN,
            MIN_SEGMENT_HEIGHT,
          );
          const targetFlowBottom = targetFlowTop + targetFlowHeight;

          // Ensure minimum segment height for visibility
          const segmentHeight = Math.max(
            (segmentEnd - segmentStart) * parentFlowHeight,
            MIN_SEGMENT_HEIGHT,
          );
          const adjustedSegmentEnd = Math.min(segmentStart + segmentHeight / parentFlowHeight, 1);

          // Parent side: proportional segment based on capacity
          const parentTop = sourcePos.y;
          const sourceStartY = parentFlowTop + segmentStart * parentFlowHeight;
          const sourceEndY = Math.min(
            parentFlowBottom,
            parentFlowTop + adjustedSegmentEnd * parentFlowHeight,
          );

          // Target side: full height (child receives the full connector)
          const targetTop = targetPos.y;
          const targetStartY = targetFlowTop;
          const targetEndY = targetFlowBottom;

          const edge: Edge = {
            id: `${queueInfo.queuePath}-${child.queuePath}`,
            source: queueInfo.queuePath,
            target: child.queuePath,
            type: 'sankeyFlow',
            data: {
              capacity: child.capacity,
              targetState: child.state,
              // Proportional positioning data for true Sankey visualization
              sourceStartY,
              sourceEndY,
              targetStartY,
              targetEndY,
              sourceTop: parentTop,
              sourceBottom: parentTop + parentHeight,
              targetTop,
              targetBottom: targetTop + childHeight,
              cornerRadius: QUEUE_CARD_CORNER_RADIUS,
              // Additional metadata for debugging/visualization
              childPercentage,
              segmentStart,
              segmentEnd: adjustedSegmentEnd,
            },
          };
          edges.push(edge);

          cumulativeCapacity += childAdjustedCapacity;
        }
      });
    } else {
      // Fallback for cases without proper capacity data or positioning
      children.forEach((child) => {
        const targetPos = positions.get(child.queuePath);

        if (sourcePos && targetPos) {
          const parentHeight = sourcePos.height ?? DEFAULT_CARD_HEIGHT;
          const parentFlowTop = sourcePos.y + QUEUE_CARD_FLOW_MARGIN;
          const parentFlowHeight = Math.max(
            parentHeight - 2 * QUEUE_CARD_FLOW_MARGIN,
            MIN_SEGMENT_HEIGHT,
          );
          const parentFlowBottom = parentFlowTop + parentFlowHeight;

          const childHeight = targetPos.height ?? DEFAULT_CARD_HEIGHT;
          const targetFlowTop = targetPos.y + QUEUE_CARD_FLOW_MARGIN;
          const targetFlowHeight = Math.max(
            childHeight - 2 * QUEUE_CARD_FLOW_MARGIN,
            MIN_SEGMENT_HEIGHT,
          );
          const targetFlowBottom = targetFlowTop + targetFlowHeight;

          const edge: Edge = {
            id: `${queueInfo.queuePath}-${child.queuePath}`,
            source: queueInfo.queuePath,
            target: child.queuePath,
            type: 'sankeyFlow',
            data: {
              capacity: child.capacity,
              targetState: child.state,
              // Fallback to full height
              sourceStartY: parentFlowTop,
              sourceEndY: parentFlowBottom,
              targetStartY: targetFlowTop,
              targetEndY: targetFlowBottom,
              sourceTop: sourcePos.y,
              sourceBottom: sourcePos.y + parentHeight,
              targetTop: targetPos.y,
              targetBottom: targetPos.y + childHeight,
              cornerRadius: QUEUE_CARD_CORNER_RADIUS,
            },
          };
          edges.push(edge);
        }
      });
    }

    // Recursively create edges for child queues
    children.forEach((child) => {
      edges.push(...createEdges(child, positions, stagedChanges));
    });
  }

  return edges;
}

// Function to augment the queue tree with staged new queues
function augmentQueueTreeWithStagedQueues(
  rootQueue: QueueInfo,
  stagedChanges: StagedChange[],
): QueueInfo {
  // Deep clone the root queue to avoid mutations
  const augmentedRoot = JSON.parse(JSON.stringify(rootQueue)) as QueueInfo;

  // Get all unique new queue paths
  const newQueuePaths = new Set(
    stagedChanges.filter((c) => c.type === 'add').map((c) => c.queuePath),
  );

  // For each new queue, add it to its parent in the tree
  newQueuePaths.forEach((queuePath) => {
    const pathParts = queuePath.split('.');
    const queueName = pathParts[pathParts.length - 1];
    const parentPath = pathParts.slice(0, -1).join('.');

    // Find the parent queue in the tree
    const findAndAddQueue = (queue: QueueInfo): boolean => {
      if (queue.queuePath === parentPath) {
        // Create the new queue info
        const newQueue: QueueInfo = {
          queueType: 'leaf' as const,
          capacity: 0,
          usedCapacity: 0,
          maxCapacity: 100,
          absoluteCapacity: 0,
          absoluteMaxCapacity: 100,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          queueName,
          queuePath,
          state: 'RUNNING' as QueueStateValue,
        };

        // Add to parent's queues
        if (!queue.queues) {
          queue.queues = { queue: [] };
        } else if (!queue.queues.queue) {
          queue.queues.queue = [];
        } else if (!Array.isArray(queue.queues.queue)) {
          queue.queues.queue = [queue.queues.queue];
        }

        if (Array.isArray(queue.queues.queue)) {
          queue.queues.queue.push(newQueue);
        }

        return true;
      }

      // Recursively search in children
      if (queue.queues?.queue) {
        const children = toArray(queue.queues.queue);

        for (const child of children) {
          if (findAndAddQueue(child)) {
            return true;
          }
        }
      }

      return false;
    };

    findAndAddQueue(augmentedRoot);
  });

  return augmentedRoot;
}

export function useQueueTreeData(): UseQueueTreeDataResult {
  const schedulerData = useSchedulerStore((state) => state.schedulerData);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const isLoading = useSchedulerStore((state) => state.isLoading);
  const loadError = useSchedulerStore((state) =>
    state.errorContext === 'load' ? state.error : null,
  );
  const applyError = useSchedulerStore((state) => state.applyError);
  const searchQuery = useSchedulerStore((state) => state.searchQuery);
  const getFilteredQueues = useSchedulerStore((state) => state.getFilteredQueues);
  const selectedNodeLabelFilter = useSchedulerStore((state) => state.selectedNodeLabelFilter);

  const { nodes, edges } = (() => {
    if (!schedulerData || isLoading) {
      return { nodes: [], edges: [] };
    }

    try {
      const partitionName = normalizePartitionName(selectedNodeLabelFilter);

      // Use filtered data if search is active
      const dataToUse = searchQuery ? getFilteredQueues() : schedulerData;

      if (!dataToUse) {
        return { nodes: [], edges: [] };
      }

      // Create a root queue wrapper for visualization purposes
      const rootQueue = createRootQueueInfo(dataToUse, partitionName);

      // Augment the tree with staged new queues
      const augmentedRootQueue = augmentQueueTreeWithStagedQueues(rootQueue, stagedChanges);

      const flatQueues = flattenQueueTree(augmentedRootQueue, stagedChanges);

      const positions = layoutEngine.calculatePositions(augmentedRootQueue);

      const flowNodes = createNodes(flatQueues, positions, stagedChanges);

      const flowEdges = createEdges(augmentedRootQueue, positions, stagedChanges);

      return { nodes: flowNodes, edges: flowEdges };
    } catch (err) {
      console.error('Error processing queue tree data:', err);
      return { nodes: [], edges: [] };
    }
  })();

  return {
    nodes,
    edges,
    isLoading,
    loadError,
    applyError,
  };
}
