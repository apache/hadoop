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
 * Host-level node label mapping helpers.
 *
 * YARN represents a host-wide label assignment as a synthetic node id with
 * wildcard port 0 (for example, `worker1.example.com:0`) via
 * CommonNodeLabelsManager.WILDCARD_PORT. That entry applies the label to every
 * NodeManager on the host and is exposed alongside per-NM mappings in
 * node-to-labels API responses.
 *
 * Host-wide assignments (for example via `yarn rmadmin -replaceLabelsOnNode
 * "hostname,label"`) create host:0 plus per-NM entries. When unassigning from
 * the UI, matching host-level mappings and sibling NMs with the same label must
 * be cleared together so the label can be removed from the cluster.
 */

import type { NodeToLabelMapping } from '~/types';

/** Wildcard port used for host-level node label mappings in YARN. */
const HOST_LEVEL_WILDCARD_PORT = '0';

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

function findHostLevelNodeId(
  nodeId: string,
  nodeToLabels: ReadonlyArray<NodeToLabelMapping>,
): string | null {
  const host = getHostFromNodeId(nodeId);
  const port = getPortFromNodeId(nodeId);
  if (!host || !port || port === HOST_LEVEL_WILDCARD_PORT) {
    return null;
  }

  const computedHostLevelNodeId = `${host}:${HOST_LEVEL_WILDCARD_PORT}`;
  if (nodeToLabels.some((mapping) => mapping.nodeId === computedHostLevelNodeId)) {
    return computedHostLevelNodeId;
  }

  return (
    nodeToLabels.find((mapping) => {
      return (
        getPortFromNodeId(mapping.nodeId) === HOST_LEVEL_WILDCARD_PORT &&
        getHostFromNodeId(mapping.nodeId) === host
      );
    })?.nodeId ?? null
  );
}

/**
 * Returns every node id that should be cleared when unassigning a label from
 * nodeId in the UI.
 *
 * When the NM label matches a host-level mapping on the same host, also clear
 * host:0 and any other NMs on that host carrying the same label. This mirrors
 * host-wide rmadmin assignments and avoids leaving orphan mappings (for
 * example host:8042) that block label removal.
 *
 * @param nodeId NodeManager node id being unassigned
 * @param nodeToLabels Current node-to-label mappings from the cluster
 * @returns Node ids to replace with an empty label list
 */
export function getNodeIdsToClearOnUnassign(
  nodeId: string,
  nodeToLabels: ReadonlyArray<NodeToLabelMapping>,
): string[] {
  const nodeIdsToClear = new Set<string>([nodeId]);
  const host = getHostFromNodeId(nodeId);
  const hostLevelNodeId = findHostLevelNodeId(nodeId, nodeToLabels);

  if (!host || !hostLevelNodeId) {
    return [...nodeIdsToClear];
  }

  const nmLabel = nodeToLabels.find((mapping) => mapping.nodeId === nodeId)?.nodeLabels[0];
  const hostLevelLabel = nodeToLabels.find((mapping) => mapping.nodeId === hostLevelNodeId)
    ?.nodeLabels[0];

  if (!hostLevelLabel || nmLabel !== hostLevelLabel) {
    return [...nodeIdsToClear];
  }

  nodeIdsToClear.add(hostLevelNodeId);

  for (const mapping of nodeToLabels) {
    if (mapping.nodeId === nodeId || mapping.nodeId === hostLevelNodeId) {
      continue;
    }
    if (mapping.nodeLabels.length === 0 || mapping.nodeLabels[0] !== hostLevelLabel) {
      continue;
    }

    const port = getPortFromNodeId(mapping.nodeId);
    if (!port || port === HOST_LEVEL_WILDCARD_PORT) {
      continue;
    }
    if (getHostFromNodeId(mapping.nodeId) !== host) {
      continue;
    }

    nodeIdsToClear.add(mapping.nodeId);
  }

  return [...nodeIdsToClear];
}
