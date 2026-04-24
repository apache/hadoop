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


import React from 'react';
import { Monitor, HardDrive, Cpu, X } from 'lucide-react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { Progress } from '~/components/ui/progress';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '~/components/ui/table';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import type { NodeInfo, NodeToLabelMapping } from '~/types';
import { formatMemory } from '~/utils/formatUtils';
import { HighlightedText } from '~/components/search/HighlightedText';

interface NodesPanelProps {
  selectedLabel: string | null;
}

export const NodesPanel: React.FC<NodesPanelProps> = ({ selectedLabel }) => {
  // State values (trigger re-renders only when these specific values change)
  const { nodes, nodeToLabels, nodeLabels, isLoading, searchQuery } = useSchedulerStore(
    useShallow((s) => ({
      nodes: s.nodes,
      nodeToLabels: s.nodeToLabels,
      nodeLabels: s.nodeLabels,
      isLoading: s.isLoading,
      searchQuery: s.searchQuery,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const assignNodeToLabel = useSchedulerStore((s) => s.assignNodeToLabel);
  const getFilteredNodes = useSchedulerStore((s) => s.getFilteredNodes);

  // Create a map of nodeId -> labels for quick lookup
  const nodeLabelsMap = new Map<string, string[]>();
  nodeToLabels.forEach((mapping: NodeToLabelMapping) => {
    nodeLabelsMap.set(mapping.nodeId, mapping.nodeLabels);
  });

  // Get nodes filtered by search query
  const searchFilteredNodes = searchQuery ? getFilteredNodes() : nodes;

  // Filter nodes based on selected label
  const filteredNodes = !selectedLabel
    ? searchFilteredNodes
    : searchFilteredNodes.filter((node: NodeInfo) => {
        const assignedLabels = nodeLabelsMap.get(node.id) || [];
        return assignedLabels.includes(selectedLabel);
      });

  const handleLabelChange = async (nodeId: string, newLabel: string | null) => {
    try {
      await assignNodeToLabel(nodeId, newLabel);
    } catch {
      // Error is already set in the store, will be displayed by parent component
    }
  };

  const getUtilizationColor = (used: number, total: number): string => {
    const percentage = (used / total) * 100;
    if (percentage < 70) return '';
    if (percentage < 90) return 'text-warning';
    return 'text-destructive';
  };

  const getNodeStateVariant = (
    state: string,
  ): 'default' | 'secondary' | 'destructive' | 'outline' => {
    switch (state) {
      case 'RUNNING':
        return 'default';
      case 'UNHEALTHY':
        return 'destructive';
      case 'SHUTDOWN':
        return 'secondary';
      default:
        return 'outline';
    }
  };

  if (nodes.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16">
        <Monitor className="mb-2 h-12 w-12 text-muted-foreground" />
        <p className="text-sm text-muted-foreground">No cluster nodes found</p>
        <p className="text-xs text-muted-foreground">
          Node information will appear here when available
        </p>
      </div>
    );
  }

  return (
    <TooltipProvider>
      <div>
        <div className="mb-4 flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            {selectedLabel ? (
              <>
                Nodes with label: <strong>{selectedLabel}</strong> ({filteredNodes.length})
              </>
            ) : (
              <>All cluster nodes ({nodes.length}) - showing default partition and labeled nodes</>
            )}
          </p>
        </div>

        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Node</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Label</TableHead>
                <TableHead>Memory</TableHead>
                <TableHead>Cores</TableHead>
                <TableHead>Containers</TableHead>
                <TableHead>Action</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredNodes.map((node: NodeInfo) => {
                const assignedLabels = nodeLabelsMap.get(node.id) || [];
                const primaryLabel = assignedLabels[0] || null;
                const totalMemory = node.usedMemoryMB + node.availMemoryMB;
                const totalCores = node.usedVirtualCores + node.availableVirtualCores;
                const memoryUsedPercent = (node.usedMemoryMB / totalMemory) * 100;
                const coresUsedPercent = (node.usedVirtualCores / totalCores) * 100;

                return (
                  <TableRow key={node.id}>
                    <TableCell>
                      <div>
                        <p className="font-medium">
                          <HighlightedText text={node.nodeHostName} highlight={searchQuery} />
                        </p>
                        <p className="text-xs text-muted-foreground">
                          <HighlightedText text={node.rack || ''} highlight={searchQuery} />
                        </p>
                      </div>
                    </TableCell>

                    <TableCell>
                      <Badge variant={getNodeStateVariant(node.state)}>{node.state}</Badge>
                    </TableCell>

                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {primaryLabel ? (
                          <Badge variant={primaryLabel === selectedLabel ? 'default' : 'outline'}>
                            <HighlightedText text={primaryLabel} highlight={searchQuery} />
                          </Badge>
                        ) : (
                          <Badge variant={selectedLabel === null ? 'default' : 'outline'}>
                            Default
                          </Badge>
                        )}
                      </div>
                    </TableCell>

                    <TableCell>
                      <div className="w-32 space-y-1">
                        <div className="flex items-center gap-2">
                          <HardDrive className="h-3 w-3 text-muted-foreground" />
                          <span className="text-xs">
                            {formatMemory(node.usedMemoryMB)} / {formatMemory(totalMemory)}
                          </span>
                        </div>
                        <Progress
                          value={memoryUsedPercent}
                          className={`h-1 ${getUtilizationColor(node.usedMemoryMB, totalMemory)}`}
                        />
                      </div>
                    </TableCell>

                    <TableCell>
                      <div className="w-28 space-y-1">
                        <div className="flex items-center gap-2">
                          <Cpu className="h-3 w-3 text-muted-foreground" />
                          <span className="text-xs">
                            {node.usedVirtualCores} / {totalCores}
                          </span>
                        </div>
                        <Progress
                          value={coresUsedPercent}
                          className={`h-1 ${getUtilizationColor(node.usedVirtualCores, totalCores)}`}
                        />
                      </div>
                    </TableCell>

                    <TableCell>
                      <span className="text-sm">{node.numContainers}</span>
                    </TableCell>

                    <TableCell>
                      <div className="flex items-center gap-2">
                        <Select
                          value={primaryLabel || 'default'}
                          onValueChange={(value) =>
                            handleLabelChange(node.id, value === 'default' ? null : value)
                          }
                          disabled={isLoading}
                        >
                          <SelectTrigger className="h-8 w-32">
                            <SelectValue placeholder="Select label" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="default">
                              <em>Default (no label)</em>
                            </SelectItem>
                            {nodeLabels.map((label) => (
                              <SelectItem key={label.name} value={label.name}>
                                <div className="flex items-center gap-2">
                                  {label.name}
                                  {label.exclusivity && (
                                    <Badge
                                      variant="outline"
                                      className="ml-2 h-5 border-warning text-warning"
                                    >
                                      Exclusive
                                    </Badge>
                                  )}
                                </div>
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>

                        {primaryLabel && (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={() => handleLabelChange(node.id, null)}
                                disabled={isLoading}
                              >
                                <X className="h-4 w-4 text-destructive" />
                              </Button>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>Remove label</p>
                            </TooltipContent>
                          </Tooltip>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>

        {filteredNodes.length === 0 && selectedLabel && (
          <div className="py-8 text-center">
            <p className="text-sm text-muted-foreground">
              No nodes assigned to label "{selectedLabel}"
            </p>
            <p className="text-xs text-muted-foreground">
              Nodes without this label operate on the default partition where regular capacity
              values apply
            </p>
          </div>
        )}
      </div>
    </TooltipProvider>
  );
};
