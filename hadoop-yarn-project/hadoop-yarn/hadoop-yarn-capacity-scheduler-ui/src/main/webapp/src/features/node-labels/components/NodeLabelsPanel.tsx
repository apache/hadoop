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


import React, { useState } from 'react';
import { Tag, Plus, Trash2, Shield, Tags } from 'lucide-react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { AddLabelDialog } from '~/features/node-labels';

export const NodeLabelsPanel: React.FC = () => {
  // State values (trigger re-renders only when these specific values change)
  const { nodeLabels, selectedNodeLabel, isLoading } = useSchedulerStore(
    useShallow((s) => ({
      nodeLabels: s.nodeLabels,
      selectedNodeLabel: s.selectedNodeLabel,
      isLoading: s.isLoading,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const selectNodeLabel = useSchedulerStore((s) => s.selectNodeLabel);
  const addNodeLabel = useSchedulerStore((s) => s.addNodeLabel);
  const removeNodeLabel = useSchedulerStore((s) => s.removeNodeLabel);

  const [addDialogOpen, setAddDialogOpen] = useState(false);

  const handleAddLabel = async (name: string, exclusivity: boolean) => {
    try {
      await addNodeLabel(name, exclusivity);
    } catch {
      // Error is already set in the store, will be displayed by parent component
    }
  };

  const handleRemoveLabel = async (labelName: string) => {
    try {
      await removeNodeLabel(labelName);
    } catch {
      // Error is already set in the store, will be displayed by parent component
    }
  };

  const handleLabelSelect = (labelName: string) => {
    selectNodeLabel(labelName === selectedNodeLabel ? null : labelName);
  };

  const existingLabelNames = nodeLabels.map((label) => label.name);

  return (
    <TooltipProvider>
      <div>
        {/* Add Label Button */}
        <div className="mb-4 flex items-center justify-between">
          <span className="text-sm text-muted-foreground">
            {nodeLabels.length} label{nodeLabels.length !== 1 ? 's' : ''} available
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setAddDialogOpen(true)}
            disabled={isLoading}
            className="gap-2"
          >
            <Plus className="h-4 w-4" />
            Add
          </Button>
        </div>

        {/* Labels List */}
        <ul className="space-y-1">
          {nodeLabels.map((label) => {
            const isSelected = selectedNodeLabel === label.name;

            return (
              <li key={label.name}>
                <div
                  className={`
                                        group flex cursor-pointer items-center justify-between rounded-md px-3 py-2
                                        transition-colors hover:bg-accent
                                        ${isSelected ? 'bg-accent' : ''}
                                    `}
                  onClick={() => handleLabelSelect(label.name)}
                >
                  <div className="flex items-center gap-3">
                    {label.exclusivity ? (
                      <Shield className="h-4 w-4 text-warning" />
                    ) : (
                      <Tag className="h-4 w-4 text-primary" />
                    )}

                    <div className="flex items-center gap-2">
                      <span className={`text-sm ${isSelected ? 'font-semibold' : ''}`}>
                        {label.name}
                      </span>

                      {label.exclusivity && (
                        <Badge variant="outline" className="border-warning text-warning">
                          Exclusive
                        </Badge>
                      )}
                    </div>
                  </div>

                  {label.name.toUpperCase() !== 'DEFAULT' && (
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8 opacity-0 transition-opacity group-hover:opacity-100"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleRemoveLabel(label.name);
                          }}
                          disabled={isLoading}
                        >
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Remove label</p>
                      </TooltipContent>
                    </Tooltip>
                  )}
                </div>
              </li>
            );
          })}

          {nodeLabels.length === 0 && (
            <li>
              <div className="flex flex-col items-center py-10 text-center">
                <div className="mb-3 rounded-full bg-muted p-3">
                  <Tags className="h-6 w-6 text-muted-foreground" />
                </div>
                <p className="mb-1 text-sm font-medium">No node labels found</p>
                <p className="mb-4 max-w-[220px] text-xs text-muted-foreground">
                  Node labels let you partition cluster nodes for dedicated resource allocation.
                </p>
                <Button
                  variant="default"
                  size="sm"
                  onClick={() => setAddDialogOpen(true)}
                  disabled={isLoading}
                  className="gap-2"
                >
                  <Plus className="h-4 w-4" />
                  Create First Label
                </Button>
              </div>
            </li>
          )}
        </ul>

        <AddLabelDialog
          open={addDialogOpen}
          onClose={() => setAddDialogOpen(false)}
          onConfirm={handleAddLabel}
          existingLabels={existingLabelNames}
          isLoading={isLoading}
        />
      </div>
    </TooltipProvider>
  );
};
