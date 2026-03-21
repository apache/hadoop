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
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  type OnNodesChange,
  type OnEdgesChange,
  type NodeMouseHandler,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert';
import { AlertCircle, Tag, Search, X, Info } from 'lucide-react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import {
  useQueueTreeData,
  type QueueCardData,
} from '~/features/queue-management/hooks/useQueueTreeData';
import { QueueCardNode } from './QueueCardNode';
import CustomFlowEdge from './CustomFlowEdge';
import { useTheme } from '~/components/providers/use-theme';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import { CompareButton } from '~/features/queue-comparison/components/CompareButton';
import { NodeLabelSelector } from '~/components/search/NodeLabelSelector';
import { CapacityEditorDialog } from './CapacityEditorDialog';
import { LegacyModeDocumentation } from './LegacyModeDocumentation';
import { mergeStagedConfig } from '~/utils/configUtils';
import { SPECIAL_VALUES } from '~/types';

export interface QueueVisualizationContainerProps {
  className?: string;
}

const nodeTypes = {
  queueCard: QueueCardNode,
};

const edgeTypes = {
  sankeyFlow: CustomFlowEdge,
};

const FlowInner: React.FC = () => {
  // State values (trigger re-renders only when these specific values change)
  const {
    stagedChanges,
    searchQuery,
    selectedNodeLabelFilter,
    configData,
    isComparisonModeActive,
  } = useSchedulerStore(
    useShallow((s) => ({
      stagedChanges: s.stagedChanges,
      searchQuery: s.searchQuery,
      selectedNodeLabelFilter: s.selectedNodeLabelFilter,
      configData: s.configData,
      isComparisonModeActive: s.isComparisonModeActive,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const selectQueue = useSchedulerStore((s) => s.selectQueue);
  const getSearchResults = useSchedulerStore((s) => s.getSearchResults);
  const { theme } = useTheme();

  // Get legacy mode status considering staged changes
  const mergedData = mergeStagedConfig(configData, stagedChanges);
  const legacyModeEnabled = mergedData.get(SPECIAL_VALUES.LEGACY_MODE_PROPERTY) !== 'false';

  const { nodes, edges, isLoading, loadError, applyError } = useQueueTreeData();

  let errorCount = 0;
  let warningCount = 0;
  const affectedQueues = new Set<string>();

  stagedChanges.forEach((change) => {
    if (change.validationErrors) {
      change.validationErrors.forEach((error) => {
        if (error.severity === 'error') {
          errorCount++;
        } else {
          warningCount++;
        }
      });
      affectedQueues.add(change.queuePath);
    }
  });

  const validationSummary = { errorCount, warningCount, affectedQueueCount: affectedQueues.size };

  const searchResults = getSearchResults();
  const hasSearchFilter = Boolean(searchQuery && searchResults.hasResults && nodes.length > 0);

  const colorMode =
    theme === 'system'
      ? window.matchMedia('(prefers-color-scheme: dark)').matches
        ? 'dark'
        : 'light'
      : theme;

  const onNodesChange: OnNodesChange = () => {
    // don't allow node position changes
  };

  const onEdgesChange: OnEdgesChange = () => {
    // don't allow edge changes
  };

  const onNodeClick: NodeMouseHandler = (_, node) => {
    // Don't open property panel in comparison mode - let card click handler manage it
    if (!isComparisonModeActive) {
      selectQueue?.(node.id);
    }
  };

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
          <p className="text-sm text-muted-foreground">Loading queue hierarchy...</p>
        </div>
      </div>
    );
  }

  if (loadError && nodes.length === 0) {
    return (
      <div className="flex h-full items-center justify-center p-4">
        <Alert variant="destructive" className="max-w-md">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error Loading Queue Data</AlertTitle>
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      </div>
    );
  }

  if (!nodes.length) {
    return (
      <div className="flex h-full items-center justify-center">
        <Alert className="max-w-md">
          <AlertTitle>{searchQuery ? 'No Matching Queues' : 'No Queue Data'}</AlertTitle>
          <AlertDescription>
            {searchQuery
              ? `No queues match your search for "${searchQuery}". Try a different search term.`
              : 'No queue hierarchy data is available. Please check your scheduler configuration.'}
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  const hasAlert = Boolean(applyError);

  return (
    <div className="relative h-full w-full flex flex-col">
      {hasAlert && (
        <div className="absolute top-4 left-1/2 z-20 flex w-full max-w-xl -translate-x-1/2 justify-center px-4">
          <div className="space-y-2 w-full">
            {applyError && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertTitle>Failed to Apply Changes</AlertTitle>
                <AlertDescription className="break-words">{applyError}</AlertDescription>
              </Alert>
            )}
          </div>
        </div>
      )}
      {/* Header with controls */}
      <div className="pointer-events-none absolute inset-x-0 top-4 z-30 flex justify-end items-center gap-3 px-4">
        <div className="pointer-events-auto">
          <LegacyModeDocumentation legacyModeEnabled={legacyModeEnabled}>
            <Badge
              variant="outline"
              className={`gap-1.5 text-xs cursor-pointer hover:bg-accent transition-colors ${
                legacyModeEnabled
                  ? 'border-amber-500/60 bg-amber-50/60 text-amber-900 hover:bg-amber-100/80 dark:border-amber-500/60 dark:bg-amber-900/20 dark:text-amber-400 dark:hover:bg-amber-900/30'
                  : 'border-primary/60 bg-primary/5 text-primary hover:bg-primary/10 dark:border-primary/60 dark:bg-primary/10 dark:text-primary dark:hover:bg-primary/20'
              }`}
            >
              <Info className="h-3 w-3" />
              {legacyModeEnabled ? 'Legacy Mode' : 'Flexible Mode'}
            </Badge>
          </LegacyModeDocumentation>
        </div>
        <div className="pointer-events-auto">
          <NodeLabelSelector />
        </div>
      </div>

      {/* Label filter information */}
      {selectedNodeLabelFilter && (
        <div className={`absolute left-4 z-10 ${hasAlert ? 'top-24' : 'top-4'}`}>
          <Alert className="py-2 px-4">
            <Tag className="h-4 w-4" />
            <AlertDescription>
              <span>
                Filtering by partition: <strong>{selectedNodeLabelFilter}</strong>. Queues without
                configured capacity are shown in gray.
              </span>
            </AlertDescription>
          </Alert>
        </div>
      )}

      {/* Search filter information */}
      {hasSearchFilter && (
        <div
          className={`absolute left-4 z-10 ${
            hasAlert
              ? selectedNodeLabelFilter
                ? 'top-40'
                : 'top-24'
              : selectedNodeLabelFilter
                ? 'top-20'
                : 'top-4'
          }`}
        >
          <Alert className="py-2 px-4 pr-2">
            <Search className="h-4 w-4" />
            <AlertDescription className="flex items-center gap-2">
              <span>
                Filtered view - showing <strong>{searchResults.count}</strong> match
                {searchResults.count !== 1 ? 'es' : ''} for{' '}
                <strong>&quot;{searchQuery}&quot;</strong>
              </span>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 w-6 p-0 ml-auto"
                onClick={() => {
                  useSchedulerStore.getState().setSearchQuery('');
                }}
                aria-label="Clear search filter"
              >
                <X className="h-4 w-4" />
              </Button>
            </AlertDescription>
          </Alert>
        </div>
      )}

      {/* Validation summary banner */}
      {validationSummary.errorCount > 0 && (
        <div
          className={`absolute left-1/2 transform -translate-x-1/2 z-10 ${hasAlert ? 'top-32' : 'top-16'}`}
        >
          <Alert className="flex items-center gap-3 py-2 px-4 shadow-lg border-destructive">
            <AlertCircle className="h-4 w-4 text-destructive" />
            <div className="flex items-center gap-3">
              <span className="text-sm font-medium">
                {validationSummary.errorCount} validation error
                {validationSummary.errorCount !== 1 ? 's' : ''} in{' '}
                {validationSummary.affectedQueueCount} queue
                {validationSummary.affectedQueueCount !== 1 ? 's' : ''}
              </span>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  // Find and open staged changes panel
                  const openButton = document.querySelector('[data-staged-changes-trigger]');
                  if (openButton instanceof HTMLElement) {
                    openButton.click();
                  }
                }}
              >
                View Details
              </Button>
            </div>
          </Alert>
        </div>
      )}

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{
          padding: 0.2,
          includeHiddenNodes: false,
        }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        minZoom={0.1}
        maxZoom={2}
        defaultViewport={{ x: 0, y: 0, zoom: 1 }}
        colorMode={colorMode}
      >
        <Background gap={16} />
        <Controls showInteractive={false} />
        <MiniMap
          nodeColor={(node) => {
            const data = node.data as QueueCardData;

            // Use default colors during SSR
            if (typeof window === 'undefined') {
              if (data.stagedStatus === 'new') return '#22c55e';
              if (data.stagedStatus === 'deleted') return '#ef4444';
              if (data.stagedStatus === 'modified') return '#f59e0b';
              return '#94a3b8';
            }

            const rootStyles = getComputedStyle(document.documentElement);

            if (data.stagedStatus === 'new') {
              return rootStyles.getPropertyValue('--color-queue-new').trim();
            }
            if (data.stagedStatus === 'deleted') {
              return rootStyles.getPropertyValue('--color-queue-deleted').trim();
            }
            if (data.stagedStatus === 'modified') {
              return rootStyles.getPropertyValue('--color-queue-modified').trim();
            }
            return rootStyles.getPropertyValue('--color-muted-foreground').trim();
          }}
          pannable
          zoomable
        />
      </ReactFlow>
    </div>
  );
};

export const QueueVisualizationContainer: React.FC<QueueVisualizationContainerProps> = ({
  className,
}) => {
  return (
    <div className={`h-full w-full ${className || ''}`}>
      <ReactFlowProvider>
        <FlowInner />
        <CompareButton />
        <CapacityEditorDialog />
      </ReactFlowProvider>
    </div>
  );
};
