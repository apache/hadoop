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
import { Badge } from '~/components/ui/badge';
import {
  Sparkles,
  RefreshCw,
  Percent,
  Weight,
  Box,
  Play,
  Square,
  ArrowDownToLine,
  ArrowRight,
  PlusCircle,
  Edit,
  MinusCircle,
  Tag,
} from 'lucide-react';
import { cn } from '~/utils/cn';
import { Tooltip, TooltipContent, TooltipTrigger } from '~/components/ui/tooltip';
import { QUEUE_STATES, type QueueCreationMethod } from '~/types';

interface QueueStatusBadgesProps {
  capacityMode: 'percentage' | 'weight' | 'absolute';
  state: string;
  stagedState?: string;
  stagedStatus?: 'new' | 'modified' | 'deleted';
  autoCreationStatus?: {
    status: 'off' | 'legacy' | 'flexible';
    isStaged?: boolean;
  };
  labelInfo?: {
    isLabelSpecific: boolean;
    label: string;
  };
  creationMethod?: QueueCreationMethod;
}

export const QueueStatusBadges: React.FC<QueueStatusBadgesProps> = ({
  capacityMode,
  state,
  stagedState,
  stagedStatus,
  autoCreationStatus,
  labelInfo,
  creationMethod,
}) => {
  const baseIconBadgeClasses = 'h-7 px-2 py-1 gap-1.5 [&>svg]:h-[16px] [&>svg]:w-[16px]';

  const getCapacityModeBadgeClass = () => {
    switch (capacityMode) {
      case 'weight':
        return 'bg-purple-500 hover:bg-purple-600 text-white';
      case 'absolute':
        return 'bg-orange-500 hover:bg-orange-600 text-white';
      default:
        return 'bg-blue-500 hover:bg-blue-600 text-white';
    }
  };

  const getStateVariant = ():
    | 'default'
    | 'secondary'
    | 'destructive'
    | 'outline'
    | 'success'
    | 'warning' => {
    if (state === QUEUE_STATES.RUNNING) return 'success';
    if (state === QUEUE_STATES.STOPPED) return 'destructive';
    if (state === QUEUE_STATES.DRAINING) return 'warning';
    return 'secondary';
  };

  const getCapacityModeIcon = () => {
    switch (capacityMode) {
      case 'weight':
        return <Weight className="w-4 h-4" />;
      case 'absolute':
        return <Box className="w-4 h-4" />;
      default:
        return <Percent className="w-4 h-4" />;
    }
  };

  const getStateIcon = (currentState: string) => {
    if (currentState === QUEUE_STATES.RUNNING) return <Play className="w-4 h-4" />;
    if (currentState === QUEUE_STATES.STOPPED) return <Square className="w-4 h-4" />;
    if (currentState === QUEUE_STATES.DRAINING) return <ArrowDownToLine className="w-4 h-4" />;
    return null;
  };

  const getModificationIcon = () => {
    if (stagedStatus === 'new') return <PlusCircle className="w-4 h-4" />;
    if (stagedStatus === 'modified') return <Edit className="w-4 h-4" />;
    if (stagedStatus === 'deleted') return <MinusCircle className="w-4 h-4" />;
    return null;
  };

  return (
    <div className="flex items-center gap-1.5 mb-3">
      {/* Capacity Mode */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge
            variant="secondary"
            className={cn(
              baseIconBadgeClasses,
              'min-w-[28px] justify-center',
              getCapacityModeBadgeClass(),
            )}
            aria-label={
              capacityMode === 'weight'
                ? 'Weight-based capacity'
                : capacityMode === 'absolute'
                  ? 'Absolute capacity'
                  : 'Percentage capacity'
            }
          >
            {getCapacityModeIcon()}
          </Badge>
        </TooltipTrigger>
        <TooltipContent>
          {capacityMode === 'weight'
            ? 'Weight-based capacity'
            : capacityMode === 'absolute'
              ? 'Absolute capacity'
              : 'Percentage capacity'}
        </TooltipContent>
      </Tooltip>

      {/* Queue State */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge
            variant={getStateVariant()}
            className={cn(baseIconBadgeClasses, 'min-w-[28px] justify-center')}
            aria-label={`Queue is ${state.toLowerCase()}`}
          >
            {getStateIcon(state)}
          </Badge>
        </TooltipTrigger>
        <TooltipContent>Queue is {state.toLowerCase()}</TooltipContent>
      </Tooltip>

      {/* Staged State (if different) */}
      {stagedState && stagedState !== state && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge
              variant="outline"
              className={cn(
                baseIconBadgeClasses,
                'flex items-center gap-1 text-queue-modified border-queue-modified/30',
              )}
              aria-label={`Will change to ${stagedState.toLowerCase()}`}
            >
              <ArrowRight className="w-4 h-4" />
              {getStateIcon(stagedState)}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>Will change to {stagedState.toLowerCase()}</TooltipContent>
        </Tooltip>
      )}

      {/* Auto-Creation Status */}
      {autoCreationStatus && autoCreationStatus.status !== 'off' && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge
              variant="outline"
              className={cn(
                baseIconBadgeClasses,
                'flex items-center gap-1',
                autoCreationStatus.status === 'flexible'
                  ? 'text-queue-running border-queue-running/30'
                  : 'text-queue-modified border-queue-modified/30',
              )}
              aria-label={`${autoCreationStatus.status === 'flexible' ? 'Flexible auto-queue creation' : 'Legacy auto-queue creation'}${autoCreationStatus.isStaged ? ' (staged)' : ''}`}
            >
              {autoCreationStatus.status === 'flexible' ? (
                <Sparkles className="w-4 h-4" />
              ) : (
                <RefreshCw className="w-4 h-4" />
              )}
              {autoCreationStatus.isStaged && <ArrowRight className="w-4 h-4" />}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>
            {autoCreationStatus.status === 'flexible'
              ? 'Flexible auto-queue creation'
              : 'Legacy auto-queue creation'}
            {autoCreationStatus.isStaged && ' (staged)'}
          </TooltipContent>
        </Tooltip>
      )}

      {/* Queue Creation Method */}
      {creationMethod && creationMethod !== 'static' && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge
              variant="outline"
              className={cn(
                baseIconBadgeClasses,
                'flex items-center gap-1 text-sm border-amber-400 text-amber-600 dark:text-amber-400',
              )}
            >
              {creationMethod === 'dynamicFlexible' ? (
                <Sparkles className="w-4 h-4" />
              ) : (
                <RefreshCw className="w-4 h-4" />
              )}
              <span className="text-xs font-medium">
                {creationMethod === 'dynamicFlexible'
                  ? 'Flexible auto-created'
                  : 'Legacy auto-created'}
              </span>
            </Badge>
          </TooltipTrigger>
          <TooltipContent className="max-w-xs">
            This queue was created automatically by the scheduler and may be removed when its
            applications finish.
          </TooltipContent>
        </Tooltip>
      )}

      {/* Modification Status */}
      {stagedStatus && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge
              variant="outline"
              className={cn(
                baseIconBadgeClasses,
                'min-w-[28px] justify-center',
                stagedStatus === 'new'
                  ? 'text-queue-new border-queue-new/30'
                  : stagedStatus === 'modified'
                    ? 'text-queue-modified border-queue-modified/30'
                    : 'text-queue-deleted border-queue-deleted/30',
              )}
              aria-label={`Queue ${stagedStatus === 'new' ? 'will be created' : stagedStatus === 'modified' ? 'has modifications' : 'will be deleted'}`}
            >
              {getModificationIcon()}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>
            Queue{' '}
            {stagedStatus === 'new'
              ? 'will be created'
              : stagedStatus === 'modified'
                ? 'has modifications'
                : 'will be deleted'}
          </TooltipContent>
        </Tooltip>
      )}

      {/* Label indicator when showing label-specific capacity */}
      {labelInfo?.isLabelSpecific && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge
              variant="outline"
              className={cn(baseIconBadgeClasses, 'flex items-center gap-1 text-sm')}
              aria-label={`Showing capacity for partition: ${labelInfo.label}`}
            >
              <Tag className="w-4 h-4" />
              <span className="text-xs">{labelInfo.label}</span>
            </Badge>
          </TooltipTrigger>
          <TooltipContent>Showing capacity for partition: {labelInfo.label}</TooltipContent>
        </Tooltip>
      )}
    </div>
  );
};
