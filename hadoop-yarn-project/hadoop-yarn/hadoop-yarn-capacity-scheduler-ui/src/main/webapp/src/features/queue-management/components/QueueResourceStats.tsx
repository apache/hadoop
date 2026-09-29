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
import { formatMemory } from '~/utils/formatUtils';
import { cn } from '~/utils/cn';

interface QueueResourceStatsProps {
  numApplications: number;
  resourcesUsed?: {
    memory?: number;
    vCores?: number;
  };
  className?: string;
}

export const QueueResourceStats: React.FC<QueueResourceStatsProps> = ({
  numApplications,
  resourcesUsed,
  className,
}) => {
  return (
    <div className={cn('text-xs text-muted-foreground text-center', className)}>
      <span>Apps: {numApplications}</span>
      <span className="mx-2">•</span>
      <span>Memory: {resourcesUsed?.memory ? formatMemory(resourcesUsed.memory) : '0 MB'}</span>
      <span className="mx-2">•</span>
      <span>vCores: {resourcesUsed?.vCores || 0}</span>
    </div>
  );
};
