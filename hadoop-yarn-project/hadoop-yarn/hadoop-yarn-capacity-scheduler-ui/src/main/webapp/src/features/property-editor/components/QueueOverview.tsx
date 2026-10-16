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
import { Sparkles, RefreshCw, Plus, Trash2, Pause, Play } from 'lucide-react';
import { cn } from '~/utils/cn';
import { Card, CardContent } from '~/components/ui/card';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { QueueCapacityProgress } from '~/features/queue-management/components/QueueCapacityProgress';
import { AddQueueDialog } from '~/features/queue-management/components/dialogs/AddQueueDialog';
import { DeleteQueueDialog } from '~/features/queue-management/components/dialogs/DeleteQueueDialog';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { QUEUE_STATES, SPECIAL_VALUES } from '~/types';
import type { QueueInfo } from '~/types';

interface QueueOverviewProps {
  queue: QueueInfo;
}

function getStagedStatus(
  queuePath: string,
  stagedChanges: Array<{ queuePath: string; type: string }>,
): 'new' | 'modified' | 'deleted' | undefined {
  const changes = stagedChanges.filter((change) => change.queuePath === queuePath);

  if (changes.some((c) => c.type === 'remove')) {
    return 'deleted';
  }
  if (changes.some((c) => c.type === 'add')) {
    return 'new';
  }
  if (changes.length > 0) {
    return 'modified';
  }
  return undefined;
}

export const QueueOverview: React.FC<QueueOverviewProps> = ({ queue }) => {
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

  const selectedNodeLabelFilter = useSchedulerStore((state) => state.selectedNodeLabelFilter);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const { canAddChildQueue, canDeleteQueue, updateQueueProperty } = useQueueActions();

  const isParentQueue = !!queue.queues?.queue && queue.queues.queue.length > 0;
  const queuePath = queue.queuePath;
  const stagedStatus = getStagedStatus(queuePath, stagedChanges);

  const canAdd = canAddChildQueue(queuePath);
  const canDelete = canDeleteQueue(queuePath);
  const isRootQueue = queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME;
  const isRunning = queue.state === QUEUE_STATES.RUNNING;

  const partitionLabel =
    selectedNodeLabelFilter === '' ? 'DEFAULT_PARTITION' : selectedNodeLabelFilter;

  // Get partition-specific capacity if a partition is selected
  const getPartitionCapacity = () => {
    if (selectedNodeLabelFilter === '' || !queue.capacities?.queueCapacitiesByPartition) {
      // Use default capacity values
      return {
        capacity: queue.capacity || 0,
        maxCapacity: queue.maxCapacity || 100,
        usedCapacity: queue.usedCapacity || 0,
        absoluteCapacity: queue.absoluteCapacity || 0,
        absoluteMaxCapacity: queue.absoluteMaxCapacity || 100,
        absoluteUsedCapacity: queue.absoluteUsedCapacity || 0,
      };
    }

    // Find the partition entry for the selected label
    const normalizedLabel = selectedNodeLabelFilter.toLowerCase();
    const partitionEntry = queue.capacities.queueCapacitiesByPartition.find(
      (p) => (p.partitionName || '').toLowerCase() === normalizedLabel,
    );

    if (!partitionEntry) {
      // If partition not found, fall back to default values
      return {
        capacity: queue.capacity || 0,
        maxCapacity: queue.maxCapacity || 100,
        usedCapacity: queue.usedCapacity || 0,
        absoluteCapacity: queue.absoluteCapacity || 0,
        absoluteMaxCapacity: queue.absoluteMaxCapacity || 100,
        absoluteUsedCapacity: queue.absoluteUsedCapacity || 0,
      };
    }

    // Use partition-specific values
    return {
      capacity: partitionEntry.capacity ?? queue.capacity ?? 0,
      maxCapacity: partitionEntry.maxCapacity ?? queue.maxCapacity ?? 100,
      usedCapacity: partitionEntry.usedCapacity ?? queue.usedCapacity ?? 0,
      absoluteCapacity: partitionEntry.absoluteCapacity ?? queue.absoluteCapacity ?? 0,
      absoluteMaxCapacity: partitionEntry.absoluteMaxCapacity ?? queue.absoluteMaxCapacity ?? 100,
      absoluteUsedCapacity: partitionEntry.absoluteUsedCapacity ?? queue.absoluteUsedCapacity ?? 0,
    };
  };

  const capacityData = getPartitionCapacity();
  const capacityPercent = capacityData.capacity;

  const getStateVariant = (state: string): 'default' | 'success' | 'destructive' => {
    switch (state) {
      case 'RUNNING':
        return 'success';
      case 'STOPPED':
        return 'destructive';
      default:
        return 'default';
    }
  };

  const handleToggleState = () => {
    const newState = isRunning ? QUEUE_STATES.STOPPED : QUEUE_STATES.RUNNING;
    updateQueueProperty(queuePath, 'state', newState);
  };

  const resourceStats = [
    {
      label: 'Applications',
      value: queue.numApplications || 0,
      icon: <Sparkles className="h-3 w-3" />,
      color: 'text-blue-500',
    },
    {
      label: 'Memory',
      value: queue.resourcesUsed?.memory ? `${queue.resourcesUsed.memory} MB` : '0 MB',
      icon: <RefreshCw className="h-3 w-3" />,
      color: 'text-green-500',
    },
    {
      label: 'vCores',
      value: queue.resourcesUsed?.vCores || 0,
      icon: <RefreshCw className="h-3 w-3" />,
      color: 'text-purple-500',
    },
  ];

  const creationMethodLabel = queue.creationMethod
    ? queue.creationMethod === 'static'
      ? 'Static'
      : queue.creationMethod === 'dynamicLegacy'
        ? 'Dynamic (Legacy)'
        : 'Dynamic (Flexible)'
    : 'Static';

  return (
    <>
      <div className="p-4 space-y-4">
        {/* Status Card - Full Width */}
        <Card className="bg-muted/50">
          <CardContent className="p-4 space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">Status</span>
              <div className="flex items-center gap-2">
                <Badge variant={getStateVariant(queue.state)}>{queue.state}</Badge>
                <Badge variant="secondary">{partitionLabel}</Badge>
              </div>
            </div>

            {/* Capacity Visualization */}
            <div>
              <div className="flex justify-between text-sm mb-1">
                <span>Capacity</span>
                <span className="font-medium">{capacityPercent.toFixed(1)}%</span>
              </div>

              <QueueCapacityProgress
                capacity={capacityData.capacity}
                maxCapacity={capacityData.maxCapacity}
                usedCapacity={capacityData.usedCapacity}
                showHeader={false}
              />

              <div className="flex justify-between text-xs text-muted-foreground">
                <span>{capacityData.usedCapacity.toFixed(1)}% used</span>
                <span>Max: {capacityData.maxCapacity.toFixed(1)}%</span>
              </div>
            </div>

            {/* Queue Details */}
            <div className="space-y-2 text-sm pt-2 border-t">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Queue Path</span>
                <span className="font-mono text-xs">{queue.queuePath}</span>
              </div>
              {isParentQueue && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Child Queues</span>
                  <span>{queue.queues?.queue?.length || 0}</span>
                </div>
              )}
              <div className="flex justify-between">
                <span className="text-muted-foreground">Absolute Capacity</span>
                <span>{capacityData.absoluteCapacity.toFixed(2)}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Absolute Max Capacity</span>
                <span>{capacityData.absoluteMaxCapacity.toFixed(2)}%</span>
              </div>
            </div>

            {/* Node Labels */}
            {(queue.nodeLabels && queue.nodeLabels.length > 0) ||
            queue.defaultNodeLabelExpression ? (
              <div className="space-y-2 text-sm pt-2 border-t">
                {queue.nodeLabels && queue.nodeLabels.length > 0 && (
                  <div className="flex justify-between items-center">
                    <span className="text-muted-foreground">Accessible Node Labels</span>
                    <div className="flex flex-wrap gap-1">
                      {queue.nodeLabels.map((label) => (
                        <Badge key={label} variant="outline" className="text-xs">
                          {label === '*' ? 'all' : label}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
                {queue.defaultNodeLabelExpression && (
                  <div className="flex justify-between items-center">
                    <span className="text-muted-foreground">Default Expression</span>
                    <Badge variant="secondary" className="text-xs">
                      {queue.defaultNodeLabelExpression}
                    </Badge>
                  </div>
                )}
              </div>
            ) : null}
          </CardContent>
        </Card>

        {/* Resource Stats - 3 Column Grid */}
        <div className="grid grid-cols-3 gap-2">
          {resourceStats.map((stat) => (
            <Card key={stat.label} className="bg-muted/30">
              <CardContent className="p-3">
                <div className="flex items-center gap-1 mb-1">
                  <span className={cn('opacity-70', stat.color)}>{stat.icon}</span>
                  <span className="text-xs text-muted-foreground">{stat.label}</span>
                </div>
                <div className="text-sm font-medium">{stat.value}</div>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Queue Type Card */}
        <Card className="bg-muted/30">
          <CardContent className="p-3 space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Type</span>
              <Badge variant="outline">{isParentQueue ? 'Parent Queue' : 'Leaf Queue'}</Badge>
            </div>

            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Creation Method</span>
              <Badge variant="outline">{creationMethodLabel}</Badge>
            </div>

            {queue.autoCreationEligibility && (
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Auto-Creation</span>
                <Badge variant="outline">{queue.autoCreationEligibility}</Badge>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Actions Card */}
        <Card className="bg-muted/30">
          <CardContent className="p-3">
            <div className="flex flex-col gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setAddDialogOpen(true)}
                disabled={!canAdd || stagedStatus === 'new'}
                className="w-full"
              >
                <Plus className="mr-2 h-4 w-4" />
                Add Child Queue
              </Button>

              <Button
                variant="outline"
                size="sm"
                onClick={handleToggleState}
                disabled={isRootQueue}
                className="w-full"
              >
                {isRunning ? (
                  <>
                    <Pause className="mr-2 h-4 w-4" />
                    Stop Queue
                  </>
                ) : (
                  <>
                    <Play className="mr-2 h-4 w-4" />
                    Start Queue
                  </>
                )}
              </Button>

              {canDelete && stagedStatus !== 'new' && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setDeleteDialogOpen(true)}
                  className="w-full text-destructive hover:text-destructive"
                >
                  <Trash2 className="mr-2 h-4 w-4" />
                  Delete Queue
                </Button>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Dialogs */}
      <AddQueueDialog
        open={addDialogOpen}
        parentQueuePath={queuePath}
        onClose={() => setAddDialogOpen(false)}
      />

      <DeleteQueueDialog
        open={deleteDialogOpen}
        queuePath={queuePath}
        onClose={() => setDeleteDialogOpen(false)}
      />
    </>
  );
};
