/**
 * Validation issues popover
 *
 * Displays validation errors and warnings with navigation to affected fields.
 */

import React from 'react';
import { AlertCircle, AlertTriangle } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '~/components/ui/popover';
import type { ValidationIssue } from '~/types';

interface ValidationIssueWithKey extends ValidationIssue {
  field: string;
  key: string;
}

interface ValidationIssuesPopoverProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  issues: ValidationIssueWithKey[];
  onIssueSelect: (field: string) => void;
}

export const ValidationIssuesPopover: React.FC<ValidationIssuesPopoverProps> = ({
  isOpen,
  onOpenChange,
  issues,
  onIssueSelect,
}) => {
  if (issues.length === 0) {
    return null;
  }

  const errorIssues = issues.filter((issue) => issue.severity === 'error');
  const warningIssues = issues.filter((issue) => issue.severity === 'warning');

  const summaryLabel = (() => {
    const parts: string[] = [];
    if (errorIssues.length) {
      parts.push(`${errorIssues.length} error${errorIssues.length === 1 ? '' : 's'}`);
    }
    if (warningIssues.length) {
      parts.push(`${warningIssues.length} warning${warningIssues.length === 1 ? '' : 's'}`);
    }
    return parts.length > 0 ? parts.join(', ') : 'Validation issues';
  })();

  return (
    <Popover open={isOpen} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        <Button
          size="sm"
          variant={errorIssues.length ? 'destructive' : 'secondary'}
          className="h-6 px-2 gap-1"
        >
          {errorIssues.length > 0 ? (
            <AlertCircle className="h-3.5 w-3.5" />
          ) : (
            <AlertTriangle className="h-3.5 w-3.5" />
          )}
          <span className="text-xs font-medium">{summaryLabel}</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-80">
        <div className="space-y-3">
          <div className="text-sm font-semibold">Validation issues</div>
          {errorIssues.length > 0 && (
            <div className="space-y-1">
              <div className="text-xs font-semibold text-destructive uppercase">Errors</div>
              <div className="space-y-1">
                {errorIssues.map((issue) => (
                  <button
                    key={issue.key}
                    className="w-full text-left text-xs px-2 py-1 rounded-md hover:bg-muted flex items-start gap-2"
                    onClick={() => onIssueSelect(issue.field)}
                  >
                    <span className="mt-1 h-2 w-2 rounded-full bg-destructive flex-shrink-0" />
                    <span>
                      <span className="font-medium">{issue.field}</span>
                      <span className="block text-muted-foreground">{issue.message}</span>
                    </span>
                  </button>
                ))}
              </div>
            </div>
          )}
          {warningIssues.length > 0 && (
            <div className="space-y-1">
              <div className="text-xs font-semibold text-amber-600 uppercase">Warnings</div>
              <div className="space-y-1">
                {warningIssues.map((issue) => (
                  <button
                    key={issue.key}
                    className="w-full text-left text-xs px-2 py-1 rounded-md hover:bg-muted flex items-start gap-2"
                    onClick={() => onIssueSelect(issue.field)}
                  >
                    <span className="mt-1 h-2 w-2 rounded-full bg-amber-500 flex-shrink-0" />
                    <span>
                      <span className="font-medium">{issue.field}</span>
                      <span className="block text-muted-foreground">{issue.message}</span>
                    </span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
};
