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


import type { NodeToLabelMapping } from '~/types';

/**
 * Get the host wildcard to remove from the node-to-labels mapping when unassigning a node.
 *
 * - Returns the host wildcard to clear when unassigning nodeId, or null when host:0 should be preserved.
 */

/** YARN host-wide wildcard port in CommonNodeLabelsManager. */
const HOST_WILDCARD_PORT = '0';

function getHostFromNodeId(nodeId: string): string | null {
  const lastColonIndex = nodeId.lastIndexOf(':');
  if (lastColonIndex <= 0) {
    return null;
  }
  return nodeId.slice(0, lastColonIndex);
}

function getPortFromNodeId(nodeId: string): string | null {
  const lastColonIndex = nodeId.lastIndexOf(':');
  if (lastColonIndex <= 0) {
    return null;
  }
  return nodeId.slice(lastColonIndex + 1);
}

function getHostWildcard(nodeId: string): string | null {
  const host = getHostFromNodeId(nodeId);
  const port = getPortFromNodeId(nodeId);

  if (!host || !port || port === HOST_WILDCARD_PORT) {
    return null;
  }
  return `${host}:${HOST_WILDCARD_PORT}`;
}

/**
 * Returns the host:0 to clear when unassigning nodeId, or null when host:0 should be preserved.
 */
export function getHostWildcardToClearOnUnassign(
  nodeId: string,
  nodeToLabels: ReadonlyArray<NodeToLabelMapping>,
): string | null {
  const host = getHostFromNodeId(nodeId);
  const hostWildcard = getHostWildcard(nodeId);
  if (!host || !hostWildcard) {
    return null;
  }

  const nmLabel = nodeToLabels.find((mapping) => mapping.nodeId === nodeId)?.nodeLabels[0];
  const wildcardLabel = nodeToLabels.find((mapping) => mapping.nodeId === hostWildcard)
    ?.nodeLabels[0];

  if (!wildcardLabel || nmLabel !== wildcardLabel) { // label doesn't match
    return null;
  }

  const isOtherLabeledNMOnSameHost = nodeToLabels.some((mapping) => {
    if (mapping.nodeId === nodeId || mapping.nodeId === hostWildcard) {
      return false;
    }
    if (mapping.nodeLabels.length === 0) {
      return false;
    }
    return getHostFromNodeId(mapping.nodeId) === host;
  });

  return isOtherLabeledNMOnSameHost ? null : hostWildcard;
}
