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
 * Remaining helper display
 *
 * Shows remaining capacity information to help users balance allocations.
 */

import React from 'react';
import { cn } from '~/utils/cn';
import { type RemainingHelper, formatNumber } from '../utils/capacityRemainingHelper';

interface RemainingHelperDisplayProps {
  helper: RemainingHelper;
}

export const RemainingHelperDisplay: React.FC<RemainingHelperDisplayProps> = ({ helper }) => {
  const isOverAllocated =
    (helper.kind === 'percentage-legacy' && helper.isOverOrUnder) ||
    (helper.kind === 'absolute-legacy' && helper.resources.some((r) => r.remaining < 0));

  return (
    <div
      className={cn(
        'mt-3 rounded-md border border-dashed px-3 py-2 text-xs text-left space-y-1',
        isOverAllocated
          ? 'bg-amber-50/60 border-amber-500/60 text-amber-900'
          : 'bg-muted/40 text-muted-foreground',
      )}
    >
      {helper.kind === 'percentage-legacy' && (
        <p>
          {helper.remaining >= 0
            ? `${formatNumber(helper.remaining)}% capacity remaining`
            : `${formatNumber(Math.abs(helper.remaining))}% over target`}{' '}
          (target {formatNumber(helper.target)}%)
        </p>
      )}
      {helper.kind === 'weight-legacy' && <p>Sum of weights: {formatNumber(helper.sum)}</p>}
      {helper.kind === 'absolute-legacy' &&
        helper.resources.map((resource) => (
          <p key={resource.resource}>
            {resource.resource}:{' '}
            {resource.remaining >= 0
              ? `${formatNumber(resource.remaining)} remaining`
              : `${formatNumber(Math.abs(resource.remaining))} over target`}{' '}
            (total {formatNumber(resource.total)})
          </p>
        ))}
    </div>
  );
};
