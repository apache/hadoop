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


/**
 * Queue vector capacity display
 *
 * Displays capacity values in vector format with overflow popover for additional resources.
 */

import React from 'react';
import { Badge } from '~/components/ui/badge';
import { Popover, PopoverContent, PopoverTrigger } from '~/components/ui/popover';
import { type ResourceVectorEntry } from '~/utils/capacityUtils';
import {
  type CapacityDisplay,
  INLINE_RESOURCE_LIMIT,
  normalizeResourceKey,
  createEntryMap,
  getResourceOrder,
} from '../utils/capacityDisplay';

interface QueueVectorCapacityDisplayProps {
  capacityDisplay: CapacityDisplay;
  maxCapacityDisplay: CapacityDisplay;
}

const getInlineBadges = (
    entryMap: Map<string, ResourceVectorEntry>,
    inlineResourceNames: string[],
): React.ReactNode[] => {
  const badges: React.ReactNode[] = [];
  inlineResourceNames.forEach((resourceName) => {
    const entry = entryMap.get(normalizeResourceKey(resourceName));
    if (!entry) {
      return;
    }
    badges.push(
        <Badge
            key={`inline-${entry.resource}-${entry.value}`}
            variant="outline"
            className="px-1.5 py-0.5 text-[11px] leading-tight font-medium whitespace-normal break-all"
        >
          {entry.resource}: {entry.value}
        </Badge>,
    );
  });
  return badges;
};

export const QueueVectorCapacityDisplay: React.FC<QueueVectorCapacityDisplayProps> = ({
                                                                                        capacityDisplay,
                                                                                        maxCapacityDisplay,
                                                                                      }) => {
  const capacityEntries: ResourceVectorEntry[] =
      capacityDisplay.type === 'vector' ? capacityDisplay.entries : [];
  const maxCapacityEntries: ResourceVectorEntry[] =
      maxCapacityDisplay.type === 'vector' ? maxCapacityDisplay.entries : [];

  const capacityEntryMap = createEntryMap(capacityEntries);
  const maxCapacityEntryMap = createEntryMap(maxCapacityEntries);
  const resourceOrder = getResourceOrder(capacityEntries, maxCapacityEntries);

  const inlineResourceNames = resourceOrder.slice(0, INLINE_RESOURCE_LIMIT);
  const overflowResourceNames = resourceOrder.slice(INLINE_RESOURCE_LIMIT);
  const hasOverflowResources = overflowResourceNames.length > 0;

  const capacityInlineBadges = getInlineBadges(capacityEntryMap, inlineResourceNames);
  const maxCapacityInlineBadges = getInlineBadges(maxCapacityEntryMap, inlineResourceNames);

  const overflowSummaryBadge = hasOverflowResources ? (
      <Popover>
        <PopoverTrigger asChild>
          <Badge
              asChild
              variant="outline"
              className="px-1.5 py-0.5 text-[11px] leading-tight font-medium cursor-pointer"
          >
            <button
                type="button"
                onClick={(event) => event.stopPropagation()}
                onMouseDown={(event) => event.stopPropagation()}
                onPointerDown={(event) => event.stopPropagation()}
                aria-label={`Show ${overflowResourceNames.length} additional resource${
                    overflowResourceNames.length === 1 ? '' : 's'
                }`}
            >
              +{overflowResourceNames.length} more
            </button>
          </Badge>
        </PopoverTrigger>
        <PopoverContent align="end" className="w-80">
          <div className="space-y-3">
            <div>
              <p className="text-sm font-medium">Resource capacity details</p>
              <p className="text-xs text-muted-foreground">
                Review the full capacity and maximum capacity values.
              </p>
            </div>
            <div className="grid grid-cols-[1.2fr_1fr_1fr] gap-x-3 gap-y-2 text-sm">
              <span className="text-xs uppercase tracking-wide text-muted-foreground">resource</span>
              <span className="text-xs uppercase tracking-wide text-muted-foreground text-right">
              capacity
            </span>
              <span className="text-xs uppercase tracking-wide text-muted-foreground text-right">
              max
            </span>
              {resourceOrder.map((resourceName) => {
                const key = normalizeResourceKey(resourceName);
                const capacityEntry = capacityEntryMap.get(key);
                const maxEntry = maxCapacityEntryMap.get(key);
                const displayName = capacityEntry?.resource ?? maxEntry?.resource ?? resourceName;
                return (
                    <React.Fragment key={`resource-${key}`}>
                      <span className="font-medium text-foreground">{displayName}</span>
                      <span className="text-right tabular-nums">{capacityEntry?.value ?? '—'}</span>
                      <span className="text-right tabular-nums">{maxEntry?.value ?? '—'}</span>
                    </React.Fragment>
                );
              })}
            </div>
          </div>
        </PopoverContent>
      </Popover>
  ) : null;

  return (
      <div className="flex flex-col gap-1">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs uppercase tracking-wide text-muted-foreground">capacity</span>
          <div className="flex-1 min-w-[120px]">
            {capacityDisplay.type === 'vector' ? (
                capacityInlineBadges.length > 0 ? (
                    <div className="flex flex-wrap gap-1">{capacityInlineBadges}</div>
                ) : (
                    <span className="text-xs text-muted-foreground">N/A</span>
                )
            ) : (
                <span className="text-sm font-medium">
              {capacityDisplay.type === 'percentage' || capacityDisplay.type === 'weight'
                  ? capacityDisplay.formatted
                  : 'N/A'}
            </span>
            )}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs uppercase tracking-wide text-muted-foreground">max capacity</span>
          <div className="flex-1 min-w-[120px]">
            {maxCapacityDisplay.type === 'vector' ? (
                maxCapacityInlineBadges.length > 0 ? (
                    <div className="flex flex-wrap gap-1">{maxCapacityInlineBadges}</div>
                ) : (
                    <span className="text-xs text-muted-foreground">N/A</span>
                )
            ) : (
                <span className="text-sm font-medium text-muted-foreground">
              {maxCapacityDisplay.type === 'percentage' || maxCapacityDisplay.type === 'weight'
                  ? maxCapacityDisplay.formatted
                  : 'N/A'}
            </span>
            )}
          </div>
        </div>
        {overflowSummaryBadge && (
            <div className="flex justify-end pt-0.5">{overflowSummaryBadge}</div>
        )}
      </div>
  );
};