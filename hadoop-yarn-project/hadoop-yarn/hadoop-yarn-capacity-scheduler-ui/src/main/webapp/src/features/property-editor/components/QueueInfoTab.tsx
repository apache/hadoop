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
  HardDrive,
  Cpu,
  AppWindow,
  Shield,
  Box,
  Shuffle,
  Tag,
  LockKeyhole,
  Clock,
  Zap,
} from 'lucide-react';
import { Badge } from '~/components/ui/badge';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '~/components/ui/accordion';
import { MetricRow } from './MetricRow';
import { ResourceDisplay } from './ResourceDisplay';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { formatPercentage, formatCount } from '~/utils/formatUtils';
import type { QueueInfo } from '~/types';

interface QueueInfoTabProps {
  queue: QueueInfo;
}

export const QueueInfoTab: React.FC<QueueInfoTabProps> = ({ queue }) => {
  const selectedNodeLabelFilter = useSchedulerStore((state) => state.selectedNodeLabelFilter);
  const partitionLabel =
    selectedNodeLabelFilter === '' ? 'DEFAULT_PARTITION' : selectedNodeLabelFilter;

  const maxCapacityDisplay =
    queue.maxCapacity >= 100 ? 'unlimited' : formatPercentage(queue.maxCapacity);

  return (
    <div className="p-4 space-y-2">
      <Accordion type="multiple" defaultValue={['resources']}>
        {/* 1. Resource Utilization Details */}
        <AccordionItem value="resources" className="border rounded-lg">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <HardDrive className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Resource Utilization Details</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            <MetricRow
              label="Partition"
              value={<Badge variant="secondary">{partitionLabel}</Badge>}
            />
            <MetricRow label="Queue State" value={queue.state} />
            <MetricRow
              label="Used Capacity"
              value={<ResourceDisplay resources={queue.resourcesUsed} />}
            />
            <MetricRow label="Used Capacity %" value={formatPercentage(queue.usedCapacity)} />
            {queue.effectiveMinResource && (
              <MetricRow
                label="Configured Capacity"
                value={<ResourceDisplay resources={queue.effectiveMinResource} />}
              />
            )}
            {queue.effectiveMaxResource && (
              <MetricRow
                label="Configured Max Capacity"
                value={<ResourceDisplay resources={queue.effectiveMaxResource} />}
              />
            )}
            {queue.effectiveMinResource && (
              <MetricRow
                label="Effective Capacity"
                value={<ResourceDisplay resources={queue.effectiveMinResource} />}
              />
            )}
            {queue.effectiveMaxResource && (
              <MetricRow
                label="Effective Max Capacity"
                value={<ResourceDisplay resources={queue.effectiveMaxResource} />}
              />
            )}
            <MetricRow label="Max Capacity Display" value={maxCapacityDisplay} />
          </AccordionContent>
        </AccordionItem>

        {/* 2. Absolute Capacity Metrics */}
        <AccordionItem value="absolute-capacity" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <Cpu className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Absolute Capacity Metrics</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            <MetricRow
              label="Absolute Used Capacity"
              value={formatPercentage(queue.absoluteUsedCapacity)}
            />
            <MetricRow
              label="Absolute Configured Capacity"
              value={formatPercentage(queue.absoluteCapacity)}
            />
            <MetricRow
              label="Absolute Configured Max Capacity"
              value={formatPercentage(queue.absoluteMaxCapacity)}
            />
          </AccordionContent>
        </AccordionItem>

        {/* 3. Application Master Limits */}
        <AccordionItem value="am-limits" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <Zap className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Application Master Limits</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.configuredMaxAMResourceLimit !== undefined && (
              <MetricRow
                label="Configured Max Application Master Limit"
                value={formatPercentage(queue.configuredMaxAMResourceLimit)}
              />
            )}
            {queue.AMResourceLimit && (
              <MetricRow
                label="Max Application Master Resources"
                value={<ResourceDisplay resources={queue.AMResourceLimit} />}
              />
            )}
            {queue.amUsedResource && (
              <MetricRow
                label="Used Application Master Resources"
                value={<ResourceDisplay resources={queue.amUsedResource} />}
              />
            )}
            {queue.userAMResourceLimit && (
              <MetricRow
                label="Max Application Master Resources Per User"
                value={<ResourceDisplay resources={queue.userAMResourceLimit} />}
              />
            )}
            {!queue.AMResourceLimit &&
              !queue.amUsedResource &&
              !queue.userAMResourceLimit &&
              queue.configuredMaxAMResourceLimit === undefined && (
                <div className="text-xs text-muted-foreground italic">
                  No AM limit data available
                </div>
              )}
          </AccordionContent>
        </AccordionItem>

        {/* 4. Application Limits & Policies */}
        <AccordionItem value="app-limits" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <AppWindow className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Application Limits & Policies</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.maxApplications !== undefined && (
              <MetricRow label="Max Applications" value={formatCount(queue.maxApplications)} />
            )}
            {queue.maxApplicationsPerUser !== undefined && (
              <MetricRow
                label="Max Applications Per User"
                value={formatCount(queue.maxApplicationsPerUser)}
              />
            )}
            {queue.maxParallelApps !== undefined && (
              <MetricRow label="Max Parallel Apps" value={formatCount(queue.maxParallelApps)} />
            )}
            {queue.numSchedulableApplications !== undefined && (
              <MetricRow
                label="Num Schedulable Applications"
                value={formatCount(queue.numSchedulableApplications)}
              />
            )}
            {queue.numNonSchedulableApplications !== undefined && (
              <MetricRow
                label="Num Non-Schedulable Applications"
                value={formatCount(queue.numNonSchedulableApplications)}
              />
            )}
            <MetricRow label="Total Applications" value={formatCount(queue.numApplications)} />
            <MetricRow
              label="Active Applications"
              value={formatCount(queue.numActiveApplications || 0)}
            />
            <MetricRow
              label="Pending Applications"
              value={formatCount(queue.numPendingApplications || 0)}
            />
          </AccordionContent>
        </AccordionItem>

        {/* 5. Container Information */}
        <AccordionItem value="containers" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <Box className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Container Information</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.numContainers !== undefined && (
              <MetricRow label="Num Containers" value={formatCount(queue.numContainers)} />
            )}
            {queue.allocatedContainers !== undefined && (
              <MetricRow
                label="Allocated Containers"
                value={formatCount(queue.allocatedContainers)}
              />
            )}
            {queue.pendingContainers !== undefined && (
              <MetricRow label="Pending Containers" value={formatCount(queue.pendingContainers)} />
            )}
            {queue.reservedContainers !== undefined && (
              <MetricRow
                label="Reserved Containers"
                value={formatCount(queue.reservedContainers)}
              />
            )}
            {queue.numContainers === undefined &&
              queue.allocatedContainers === undefined &&
              queue.pendingContainers === undefined &&
              queue.reservedContainers === undefined && (
                <div className="text-xs text-muted-foreground italic">
                  No container data available
                </div>
              )}
          </AccordionContent>
        </AccordionItem>

        {/* 6. Ordering & Preemption */}
        <AccordionItem value="ordering" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <Shuffle className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Ordering & Preemption</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.orderingPolicy && (
              <MetricRow label="Ordering Policy" value={queue.orderingPolicy} />
            )}
            {queue.preemptionDisabled !== undefined && (
              <MetricRow
                label="Preemption"
                value={
                  <Badge variant={queue.preemptionDisabled ? 'destructive' : 'success'}>
                    {queue.preemptionDisabled ? 'disabled' : 'enabled'}
                  </Badge>
                }
              />
            )}
            {queue.intraQueuePreemptionDisabled !== undefined && (
              <MetricRow
                label="Intra-queue Preemption"
                value={
                  <Badge variant={queue.intraQueuePreemptionDisabled ? 'destructive' : 'success'}>
                    {queue.intraQueuePreemptionDisabled ? 'disabled' : 'enabled'}
                  </Badge>
                }
              />
            )}
            {!queue.orderingPolicy &&
              queue.preemptionDisabled === undefined &&
              queue.intraQueuePreemptionDisabled === undefined && (
                <div className="text-xs text-muted-foreground italic">
                  No ordering/preemption data available
                </div>
              )}
          </AccordionContent>
        </AccordionItem>

        {/* 7. Node Labels & Partitions */}
        <AccordionItem value="node-labels" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <Tag className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Node Labels & Partitions</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.nodeLabels && queue.nodeLabels.length > 0 && (
              <MetricRow
                label="Accessible Node Labels"
                value={
                  <div className="flex flex-wrap gap-1">
                    {queue.nodeLabels.map((label) => (
                      <Badge key={label} variant="outline" className="text-xs">
                        {label === '*' ? 'all' : label}
                      </Badge>
                    ))}
                  </div>
                }
              />
            )}
            {queue.defaultNodeLabelExpression && (
              <MetricRow
                label="Default Node Label Expression"
                value={<Badge variant="secondary">{queue.defaultNodeLabelExpression}</Badge>}
              />
            )}
            {(!queue.nodeLabels || queue.nodeLabels.length === 0) &&
              !queue.defaultNodeLabelExpression && (
                <div className="text-xs text-muted-foreground italic">
                  No node label data available
                </div>
              )}
          </AccordionContent>
        </AccordionItem>

        {/* 8. Access & Priority */}
        <AccordionItem value="access" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <LockKeyhole className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Access & Priority</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.queueAcls && queue.queueAcls.queueAcl && queue.queueAcls.queueAcl.length > 0 && (
              <>
                {queue.queueAcls.queueAcl.map((acl) => (
                  <MetricRow
                    key={acl.accessType}
                    label={acl.accessType}
                    value={
                      <span className="font-mono text-xs">{acl.accessControlList || 'N/A'}</span>
                    }
                  />
                ))}
              </>
            )}
            {queue.defaultPriority !== undefined && (
              <MetricRow label="Default Application Priority" value={queue.defaultPriority} />
            )}
            {queue.queuePriority !== undefined && (
              <MetricRow label="Queue Priority" value={queue.queuePriority} />
            )}
            {(!queue.queueAcls ||
              !queue.queueAcls.queueAcl ||
              queue.queueAcls.queueAcl.length === 0) &&
              queue.defaultPriority === undefined &&
              queue.queuePriority === undefined && (
                <div className="text-xs text-muted-foreground italic">
                  No access/priority data available
                </div>
              )}
          </AccordionContent>
        </AccordionItem>

        {/* 9. Lifecycle Settings */}
        <AccordionItem value="lifecycle" className="border rounded-lg mt-2">
          <AccordionTrigger className="px-4 py-3 hover:no-underline">
            <div className="flex items-center gap-2">
              <Clock className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">Lifecycle Settings</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="p-4 space-y-1">
            {queue.defaultApplicationLifetime !== undefined && (
              <MetricRow
                label="Default Application Lifetime"
                value={`${queue.defaultApplicationLifetime} seconds`}
              />
            )}
            {queue.maxApplicationLifetime !== undefined && (
              <MetricRow
                label="Max Application Lifetime"
                value={`${queue.maxApplicationLifetime} seconds`}
              />
            )}
            {queue.defaultApplicationLifetime === undefined &&
              queue.maxApplicationLifetime === undefined && (
                <div className="text-xs text-muted-foreground italic">
                  No lifecycle settings configured
                </div>
              )}
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </div>
  );
};
