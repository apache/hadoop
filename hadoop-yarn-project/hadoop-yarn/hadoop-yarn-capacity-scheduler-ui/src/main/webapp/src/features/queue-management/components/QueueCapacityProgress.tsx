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
import { cn } from '~/utils/cn';

interface QueueCapacityProgressProps {
  capacity: number;
  maxCapacity: number;
  usedCapacity: number;
  className?: string;
  title?: string;
  showHeader?: boolean;
  usedLabelFormatter?: (usedCapacity: number) => string;
}

export const QueueCapacityProgress: React.FC<QueueCapacityProgressProps> = ({
  capacity,
  maxCapacity,
  usedCapacity,
  className,
  title = 'Live Resource Usage',
  showHeader = true,
  usedLabelFormatter = (used) => `${used.toFixed(1)}% used`,
}) => {
  const getUsageColor = (used: number): string => {
    if (capacity === 0) return 'bg-muted-foreground/30';
    if (used >= 90) return 'bg-destructive';
    if (used >= 75) return 'bg-orange-500';
    if (used >= 50) return 'bg-yellow-500';
    if (used > 0) return 'bg-green-500';
    return 'bg-green-600';
  };

  const showCapacityMarker = capacity > 5 && capacity < 95;
  const showMaxCapacityMarker = maxCapacity < 95 && maxCapacity !== capacity;
  const capacityMaxOverlap = Math.abs(capacity - maxCapacity) < 10;

  return (
    <div className={cn('mb-3 relative', className)}>
      {showHeader && (
        <div className="flex justify-between text-xs text-muted-foreground mb-1">
          <span>{title}</span>
          <span>{usedLabelFormatter(usedCapacity)}</span>
        </div>
      )}

      <div className="relative h-4 bg-secondary rounded-full overflow-visible mt-1">
        {/* Capacity bar (semi-transparent) */}
        <div
          className="absolute h-full bg-primary/30 rounded-full transition-all duration-300"
          style={{ width: `${capacity}%` }}
        />

        {/* Usage bar (solid color based on usage level) */}
        <div
          className={cn(
            'absolute h-full rounded-full transition-all duration-300',
            getUsageColor(usedCapacity),
          )}
          style={{ width: `${usedCapacity}%` }}
        />

        {/* Max capacity marker line */}
        {maxCapacity < 100 && (
          <div
            className="absolute top-0 bottom-0 w-0.5 bg-destructive"
            style={{ left: `${maxCapacity}%` }}
          />
        )}
      </div>

      {/* Scale indicators */}
      <div className="relative flex justify-between text-[10px] text-muted-foreground mt-1">
        <span>0%</span>
        {/* Check if we need to show capacity/max capacity markers */}
        {(() => {
          if (capacityMaxOverlap && showCapacityMarker && showMaxCapacityMarker) {
            // Show combined label if they overlap
            return (
              <span
                className="absolute"
                style={{
                  left: `${(capacity + maxCapacity) / 2}%`,
                  transform: 'translateX(-50%)',
                }}
              >
                {capacity}%/{maxCapacity}%
              </span>
            );
          }

          return (
            <>
              {showCapacityMarker && (
                <span
                  className="absolute"
                  style={{
                    left: `${capacity}%`,
                    transform: 'translateX(-50%)',
                  }}
                >
                  {capacity}%
                </span>
              )}
              {showMaxCapacityMarker && (
                <span
                  className="absolute text-destructive"
                  style={{
                    left: `${maxCapacity}%`,
                    transform: 'translateX(-50%)',
                  }}
                >
                  {maxCapacity}%
                </span>
              )}
            </>
          );
        })()}
        {/* Only show 100% marker if maxCapacity is 95% or above */}
        {maxCapacity >= 95 && <span>100%</span>}
      </div>
    </div>
  );
};
