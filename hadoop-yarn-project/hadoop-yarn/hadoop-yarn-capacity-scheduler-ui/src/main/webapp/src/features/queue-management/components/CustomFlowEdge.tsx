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


import { type EdgeProps } from '@xyflow/react';
import { QUEUE_CARD_FLOW_MARGIN, QUEUE_CARD_HEIGHT } from '~/features/queue-management/constants';

/**
 * Custom Sankey-like edge component that creates flowing connections between queue nodes.
 *
 * This component creates thick, flowing connections where the width represents the capacity
 * flow between parent and child queues, similar to a Sankey diagram. The connections
 * use gradients and proper curve handling for a professional appearance.
 *
 */
function CustomFlowEdge({ id, sourceX, sourceY, targetX, targetY, data }: EdgeProps) {
  // Generate unique gradient ID for this edge
  const gradientId = `gradient-${id}`;

  // Determine flow colors based on target queue state using CSS variables
  const getFlowColors = () => {
    const rootStyles = getComputedStyle(document.documentElement);

    if (data?.targetState === 'RUNNING') {
      const color = rootStyles.getPropertyValue('--color-queue-running').trim();
      return {
        startColor: color,
        endColor: color,
        opacity: 0.8,
      };
    } else if (data?.targetState === 'STOPPED') {
      const color = rootStyles.getPropertyValue('--color-queue-stopped').trim();
      return {
        startColor: color,
        endColor: color,
        opacity: 0.8,
      };
    } else if (data?.targetState === 'DRAINING') {
      const color = rootStyles.getPropertyValue('--color-queue-draining').trim();
      return {
        startColor: color,
        endColor: color,
        opacity: 0.8,
      };
    } else {
      // Default to a neutral color for other states
      const color = rootStyles.getPropertyValue('--color-muted').trim();
      return {
        startColor: color,
        endColor: color,
        opacity: 0.5,
      };
    }
  };

  const { startColor, endColor, opacity } = getFlowColors();

  const createSankeyPath = () => {
    const controlPointDistance = Math.abs(targetX - sourceX) * 0.5;

    const fallbackHeight = Math.max(QUEUE_CARD_HEIGHT - 2 * QUEUE_CARD_FLOW_MARGIN, 0);
    const halfFallbackHeight = fallbackHeight / 2;
    const sourceStartY = data?.sourceStartY ?? sourceY - halfFallbackHeight;
    const sourceEndY = data?.sourceEndY ?? sourceY + halfFallbackHeight;
    const targetStartY = data?.targetStartY ?? targetY - halfFallbackHeight;
    const targetEndY = data?.targetEndY ?? targetY + halfFallbackHeight;

    return [
      // Start at source (proportional segment start)
      `M ${sourceX} ${sourceStartY}`,

      // Top curve to target
      `C ${sourceX + controlPointDistance} ${sourceStartY}`,
      `${targetX - controlPointDistance} ${targetStartY}`,
      `${targetX} ${targetStartY}`,

      // Line to bottom of target segment
      `L ${targetX} ${targetEndY}`,

      // Bottom curve back to source
      `C ${targetX - controlPointDistance} ${targetEndY}`,
      `${sourceX + controlPointDistance} ${sourceEndY}`,
      `${sourceX} ${sourceEndY}`,

      // Close the path
      'Z',
    ].join(' ');
  };

  const sankeyPath = createSankeyPath();

  return (
    <g>
      <defs>
        <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" style={{ stopColor: startColor, stopOpacity: opacity }} />
          <stop offset="100%" style={{ stopColor: endColor, stopOpacity: opacity }} />
        </linearGradient>

        {/* Add a subtle shadow for depth */}
        <filter id={`shadow-${id}`} x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="2" stdDeviation="2" floodOpacity="0.1" />
        </filter>
      </defs>

      {/* Shadow path */}
      <path d={sankeyPath} fill="rgba(0, 0, 0, 0.1)" transform="translate(2, 2)" />

      {/* Main Sankey flow path */}
      <path
        d={sankeyPath}
        fill={`url(#${gradientId})`}
        filter={`url(#shadow-${id})`}
        style={{
          transition: 'all 0.2s ease-in-out',
        }}
      />

      {data?.targetState === 'RUNNING' && <path d={sankeyPath} fill="none" />}

      <style>
        {`
                    @keyframes flow {
                        0% { stroke-dashoffset: 0; }
                        100% { stroke-dashoffset: 20; }
                    }
                `}
      </style>
    </g>
  );
}

export default CustomFlowEdge;
