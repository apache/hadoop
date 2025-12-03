/**
 * Capacity remaining helper utilities
 *
 * Computes remaining capacity information to help users balance allocations.
 */

import { parseCapacityValue } from '~/utils/capacityUtils';
import { SPECIAL_VALUES } from '~/types';
import type { QueueCapacitiesByPartition } from '~/types';
import type { CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';

const SUPPORTED_ABSOLUTE_RESOURCES = ['memory', 'vcores'] as const;
export type SupportedAbsoluteResource = (typeof SUPPORTED_ABSOLUTE_RESOURCES)[number];

export type RemainingHelper =
  | {
      kind: 'percentage-legacy';
      remaining: number;
      target: number;
      isOverOrUnder: boolean;
    }
  | {
      kind: 'weight-legacy';
      sum: number;
    }
  | {
      kind: 'absolute-legacy';
      resources: Array<{
        resource: SupportedAbsoluteResource;
        allocated: number;
        remaining: number;
        total: number;
      }>;
    };

export const formatNumber = (value: number): string => {
  if (!Number.isFinite(value)) {
    return '0';
  }
  const rounded = Math.round((value + Number.EPSILON) * 100) / 100;
  const formatted = rounded.toString();
  if (formatted.includes('.')) {
    return formatted.replace(/\.0+$/, '').replace(/(\.\d*[1-9])0+$/, '$1');
  }
  return formatted;
};

export interface ComputeRemainingHelperParams {
  rows: CapacityRowDraft[];
  parentCapacityValue: string;
  isLegacyMode: boolean;
  parentQueuePath: string | null;
  selectedNodeLabel: string | null;
  getQueuePartitionCapacities: (
    path: string,
    partition: string,
  ) => QueueCapacitiesByPartition | null;
}

export const computeRemainingHelper = ({
  rows,
  isLegacyMode,
  parentQueuePath,
  selectedNodeLabel,
  getQueuePartitionCapacities,
}: ComputeRemainingHelperParams): RemainingHelper | null => {
  if (rows.length === 0) {
    return null;
  }

  const allSimple = rows.every((row) => row.mode === 'simple');
  const allVector = rows.every((row) => row.mode === 'vector');

  // Legacy Mode
  if (isLegacyMode) {
    if (allSimple) {
      let determinedType: 'percentage' | 'weight' | null = null;
      let currentTotal = 0;

      for (const row of rows) {
        const currentParsed = parseCapacityValue(row.capacityValue);
        const baseParsed = parseCapacityValue(row.baseCapacityValue);

        const candidateType =
          currentParsed && (currentParsed.type === 'percentage' || currentParsed.type === 'weight')
            ? currentParsed.type
            : baseParsed && (baseParsed.type === 'percentage' || baseParsed.type === 'weight')
              ? baseParsed.type
              : null;

        if (!candidateType) {
          return null;
        }

        if (!determinedType) {
          determinedType = candidateType;
        } else if (determinedType !== candidateType) {
          return null;
        }

        if (currentParsed?.type === determinedType) {
          currentTotal += currentParsed.value;
        }
      }

      if (!determinedType) {
        return null;
      }

      if (determinedType === 'percentage') {
        const target = 100;
        const remaining = target - currentTotal;
        return {
          kind: 'percentage-legacy',
          remaining,
          target,
          isOverOrUnder: remaining !== 0,
        };
      }

      if (determinedType === 'weight') {
        return {
          kind: 'weight-legacy',
          sum: currentTotal,
        };
      }
    }

    if (allVector) {
      // Legacy mode with absolute resources: show remaining capacity
      if (!parentQueuePath) {
        return null;
      }

      const partitionName = selectedNodeLabel || '';
      const partition = getQueuePartitionCapacities(parentQueuePath, partitionName);

      if (!partition) {
        return null;
      }

      // Determine if parent is root to choose the appropriate total
      const isParentRoot = parentQueuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME;

      // Calculate allocated resources from all rows
      const allocatedResources = new Map<string, number>();
      rows.forEach((row) => {
        row.vectorCapacity.forEach(({ key, value }) => {
          if (key.trim().length === 0) {
            return;
          }
          const numeric = Number.parseFloat(value);
          if (!Number.isNaN(numeric)) {
            const current = allocatedResources.get(key) ?? 0;
            allocatedResources.set(key, current + numeric);
          }
        });
      });

      const resources: Array<{
        resource: SupportedAbsoluteResource;
        allocated: number;
        remaining: number;
        total: number;
      }> = [];

      SUPPORTED_ABSOLUTE_RESOURCES.forEach((resource) => {
        const resourceKey = resource === 'vcores' ? 'vCores' : resource;

        // For root, use effectiveMaxResource; for non-root, use configuredMinResource
        const total = isParentRoot
          ? (partition.effectiveMaxResource?.[resourceKey] ?? 0)
          : (partition.configuredMinResource?.[resourceKey] ?? 0);

        const allocated = allocatedResources.get(resource) ?? 0;
        const remaining = total - allocated;

        // Only include resources that have a total or allocation
        if (total > 0 || allocated > 0) {
          resources.push({
            resource,
            allocated,
            remaining,
            total,
          });
        }
      });

      if (resources.length === 0) {
        return null;
      }

      return {
        kind: 'absolute-legacy',
        resources,
      };
    }

    return null;
  }

  // Non-Legacy Mode: no strict validation rules, don't show helper
  return null;
};
