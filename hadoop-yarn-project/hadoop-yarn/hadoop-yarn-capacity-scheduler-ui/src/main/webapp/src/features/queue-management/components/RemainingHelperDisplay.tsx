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
