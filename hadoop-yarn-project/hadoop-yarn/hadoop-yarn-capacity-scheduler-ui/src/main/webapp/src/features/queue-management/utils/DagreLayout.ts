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


import dagre from 'dagre';
import type { QueueInfo } from '~/types';

export type LayoutPosition = {
  x: number;
  y: number;
  width: number;
  height: number;
};

export type DagreLayoutOptions = {
  nodeWidth?: number;
  nodeHeight?: number;
  horizontalSpacing?: number;
  verticalSpacing?: number;
  orientation?: 'horizontal' | 'vertical';
};

export class DagreLayout {
  private options: Required<DagreLayoutOptions>;

  constructor(options: DagreLayoutOptions = {}) {
    this.options = {
      nodeWidth: 280,
      nodeHeight: 220,
      horizontalSpacing: 80,
      verticalSpacing: 150,
      orientation: 'horizontal',
      ...options,
    };
  }

  /**
   * Calculate positions for all nodes in the queue tree using Dagre
   */
  calculatePositions(root: QueueInfo): Map<string, LayoutPosition> {
    const positions = new Map<string, LayoutPosition>();

    const g = new dagre.graphlib.Graph();

    g.setGraph({
      rankdir: this.options.orientation === 'horizontal' ? 'LR' : 'TB',
      nodesep: this.options.horizontalSpacing,
      ranksep: this.options.verticalSpacing,
      marginx: 0,
      marginy: 0,
    });

    g.setDefaultEdgeLabel(() => ({}));

    this.addNodesRecursively(g, root);

    dagre.layout(g);

    g.nodes().forEach((nodeId) => {
      const node = g.node(nodeId);
      if (node) {
        positions.set(nodeId, {
          x: node.x - this.options.nodeWidth / 2,
          y: node.y - this.options.nodeHeight / 2,
          width: this.options.nodeWidth,
          height: this.options.nodeHeight,
        });
      }
    });

    return positions;
  }

  /**
   * Get the bounds of the layout
   */
  getBounds(positions: Map<string, LayoutPosition>): {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
  } {
    if (positions.size === 0) {
      return { minX: 0, minY: 0, maxX: 0, maxY: 0 };
    }

    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;

    positions.forEach((pos) => {
      minX = Math.min(minX, pos.x);
      minY = Math.min(minY, pos.y);
      maxX = Math.max(maxX, pos.x + pos.width);
      maxY = Math.max(maxY, pos.y + pos.height);
    });

    return { minX, minY, maxX, maxY };
  }

  /**
   * Recursively add nodes and edges to the Dagre graph
   */
  private addNodesRecursively(g: dagre.graphlib.Graph, node: QueueInfo, parent?: string): void {
    g.setNode(node.queuePath, {
      label: node.queueName,
      width: this.options.nodeWidth,
      height: this.options.nodeHeight,
    });

    if (parent) {
      g.setEdge(parent, node.queuePath);
    }

    if (node.queues?.queue) {
      const children = Array.isArray(node.queues.queue) ? node.queues.queue : [node.queues.queue];

      children.forEach((child) => {
        this.addNodesRecursively(g, child, node.queuePath);
      });
    }
  }
}
