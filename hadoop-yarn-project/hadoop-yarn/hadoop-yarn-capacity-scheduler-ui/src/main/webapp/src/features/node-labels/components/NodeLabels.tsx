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
import { AlertCircle } from 'lucide-react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert';
import { Card, CardContent, CardHeader, CardTitle } from '~/components/ui/card';
import { Skeleton } from '~/components/ui/skeleton';
import { NodeLabelsPanel } from './NodeLabelsPanel';
import { NodesPanel } from './NodesPanel';

export const NodeLabels: React.FC = () => {
  // State values (trigger re-renders only when these specific values change)
  const { isLoading, error, errorContext, applyError, nodeLabels, selectedNodeLabel } =
    useSchedulerStore(
      useShallow((s) => ({
        isLoading: s.isLoading,
        error: s.error,
        errorContext: s.errorContext,
        applyError: s.applyError,
        nodeLabels: s.nodeLabels,
        selectedNodeLabel: s.selectedNodeLabel,
      })),
    );

  if (isLoading && nodeLabels.length === 0) {
    return (
      <div className="flex h-96 items-center justify-center">
        <div className="flex items-center space-x-2">
          <Skeleton className="h-8 w-8 rounded-full" />
          <span className="text-muted-foreground">Loading node labels...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      {/* Error Display */}
      {applyError && (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Failed to Apply Changes</AlertTitle>
          <AlertDescription>{applyError}</AlertDescription>
        </Alert>
      )}

      {error && errorContext === 'nodeLabels' && (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Node Label Operation Failed</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Main Content */}
      <div className="min-h-0 flex-grow">
        <div className="grid h-full gap-6 md:grid-cols-[400px_1fr]">
          {/* Labels Panel */}
          <Card className="h-full overflow-hidden">
            <CardHeader>
              <CardTitle>Available Labels</CardTitle>
              <p className="text-sm text-muted-foreground">
                Select labels to configure queue capacity for each label
              </p>
            </CardHeader>
            <CardContent className="h-[calc(100%-5rem)] overflow-auto">
              <NodeLabelsPanel />
            </CardContent>
          </Card>

          {/* Nodes Panel */}
          <Card className="h-full overflow-hidden">
            <CardHeader>
              <div className="flex items-center">
                <CardTitle>Node Label Configuration</CardTitle>
                {selectedNodeLabel && (
                  <span className="ml-2 inline-flex items-center rounded-md bg-primary/10 px-2 py-1 text-xs font-medium text-primary">
                    {selectedNodeLabel}
                  </span>
                )}
              </div>
              <p className="text-sm text-muted-foreground">
                Assign nodes to labels for resource allocation
              </p>
            </CardHeader>
            <CardContent className="h-[calc(100%-5rem)] overflow-auto">
              <NodesPanel selectedLabel={selectedNodeLabel} />
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};
