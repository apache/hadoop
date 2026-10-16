/**
 * Node data normalizers - shared utilities for normalizing node label API responses
 *
 * These functions handle the various formats that YARN APIs return for node labels
 * and node-to-label mappings, providing consistent normalized output.
 */

import type {
  NodeLabel,
  NodeLabelInfoItem,
  NodeLabelsResponse,
  NodeToLabelMapping,
  NodeToLabelsMapEntry,
  NodeToLabelsResponse,
} from '~/types';

/**
 * Type representing the various formats a nodeLabelInfo can appear in API responses
 */
type NodeLabelInfoLike =
  | NodeLabelInfoItem
  | NodeLabelInfoItem[]
  | {
      nodeLabelInfo?: NodeLabelInfoItem | NodeLabelInfoItem[];
    };

/**
 * Normalize node labels from API response.
 * Ensures exclusivity defaults to true if not specified (YARN default).
 */
export function normalizeNodeLabels(response?: NodeLabelsResponse): NodeLabel[] {
  const rawNodeLabelInfo = response?.nodeLabelInfo;
  const nodeLabelInfo = Array.isArray(rawNodeLabelInfo)
    ? rawNodeLabelInfo
    : rawNodeLabelInfo
      ? [rawNodeLabelInfo]
      : [];

  return nodeLabelInfo.map((label) => ({
    ...label,
    exclusivity: parseExclusivity(label.exclusivity),
  }));
}

/**
 * Parse exclusivity value from API response.
 * YARN can return boolean or string 'true'/'false'.
 * Defaults to true if not specified (YARN's default behavior).
 */
export function parseExclusivity(value?: boolean | 'true' | 'false'): boolean {
  if (typeof value === 'string') {
    return value.toLowerCase() !== 'false';
  }

  return value ?? true;
}

/**
 * Normalize node-to-labels mappings from API response.
 * Handles the complex nested structure that YARN returns.
 */
export function normalizeNodeToLabels(response?: NodeToLabelsResponse): NodeToLabelMapping[] {
  const entries = extractEntries(response?.nodeToLabels);

  return entries.map((entry) => ({
    nodeId: entry.key,
    nodeLabels: extractLabelNames(entry.value?.nodeLabelInfo),
  }));
}

/**
 * Extract entries from the nodeToLabels response.
 * Handles both single entry and array formats.
 */
function extractEntries(data?: NodeToLabelsResponse['nodeToLabels']): NodeToLabelsMapEntry[] {
  if (!data || !data.entry) {
    return [];
  }

  const { entry } = data;
  return Array.isArray(entry) ? entry : [entry];
}

/**
 * Extract label names from various nodeLabelInfo formats.
 * YARN API can return labels in multiple nested formats.
 */
function extractLabelNames(info?: NodeLabelInfoLike): string[] {
  if (!info) {
    return [];
  }

  if (Array.isArray(info)) {
    return info.map((item) => item?.name).filter((name): name is string => Boolean(name));
  }

  if ('nodeLabelInfo' in info && info.nodeLabelInfo) {
    // Some responses nest nodeLabelInfo inside another object
    return extractLabelNames(info.nodeLabelInfo as NodeLabelInfoItem | NodeLabelInfoItem[]);
  }

  if (typeof info === 'object' && info !== null && 'name' in info) {
    const name = (info as NodeLabelInfoItem).name;
    return name ? [name] : [];
  }

  return [];
}
