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
import { Trash2, Check, Gauge, AlertTriangle, AlertCircle, Info } from 'lucide-react';
import {
  Drawer,
  DrawerContent,
  DrawerHeader,
  DrawerTitle,
  DrawerFooter,
} from '~/components/ui/drawer';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { StagedChange } from '~/types';
import { QueueChangeGroup } from './QueueChangeGroup';
import { toast } from 'sonner';
import { Kbd } from '~/components/ui/kbd';
import { getModifierKey } from '~/hooks/useKeyboardShortcuts';
import { READ_ONLY_PROPERTY } from '~/config';

interface StagedChangesPanelProps {
  open: boolean;
  onClose: () => void;
  onOpen?: () => void;
}

export function StagedChangesPanel({ open, onClose, onOpen }: StagedChangesPanelProps) {
  const [isApplying, setIsApplying] = useState(false);

  // State values (trigger re-renders only when these specific values change)
  const { stagedChanges, applyError, isReadOnly } = useSchedulerStore(
    useShallow((s) => ({
      stagedChanges: s.stagedChanges,
      applyError: s.applyError,
      isReadOnly: s.isReadOnly,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const revertChange = useSchedulerStore((s) => s.revertChange);
  const clearAllChanges = useSchedulerStore((s) => s.clearAllChanges);
  const applyChanges = useSchedulerStore((s) => s.applyChanges);

  // Group changes by queue path for organized display
  const changesByQueue = stagedChanges.reduce(
    (acc, change) => {
      const queuePath = change.queuePath;
      if (!acc[queuePath]) {
        acc[queuePath] = [];
      }
      acc[queuePath].push(change);
      return acc;
    },
    {} as Record<string, StagedChange[]>,
  );

  // Sort queue paths to show 'global' first, then alphabetically
  const sortedQueuePaths = Object.keys(changesByQueue).sort((a, b) => {
    const isAGlobal = a === 'global';
    const isBGlobal = b === 'global';
    if (isAGlobal && !isBGlobal) return -1;
    if (!isAGlobal && isBGlobal) return 1;
    return a.localeCompare(b);
  });

  // Calculate validation summary
  let errorCount = 0;
  let warningCount = 0;

  stagedChanges.forEach((change) => {
    if (change.validationErrors) {
      change.validationErrors.forEach((error) => {
        if (error.severity === 'error') {
          errorCount++;
        } else {
          warningCount++;
        }
      });
    }
  });

  const validationSummary = { errorCount, warningCount };

  const handleApplyChanges = async () => {
    setIsApplying(true);
    try {
      await applyChanges();
      toast.success('All changes applied successfully');
      onClose();
    } catch (error) {
      toast.error('Failed to apply changes');
      console.error('Failed to apply changes:', error);
    } finally {
      setIsApplying(false);
    }
  };

  const handleClearAll = () => {
    clearAllChanges();
    toast.info('All staged changes cleared');
  };

  const handleRevertChange = (change: StagedChange) => {
    revertChange(change.id);
    toast.info(`Reverted change: ${change.property}`);
  };

  // Show floating button when panel is closed and there are staged changes
  if (!open && stagedChanges.length > 0) {
    return (
      <div className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50">
        <Button
          variant="default"
          size="lg"
          className="relative shadow-xl hover:shadow-2xl rounded-full px-8 py-6 backdrop-blur-sm bg-primary/95 hover:bg-primary transition-all duration-300 hover:scale-105 animate-in fade-in slide-in-from-bottom-4"
          onClick={onOpen}
          data-staged-changes-trigger
        >
          <span className="absolute inset-0 rounded-full bg-primary/20 animate-pulse" />
          <Gauge className="h-5 w-5 mr-2 relative z-10" />
          <span className="relative z-10 font-semibold">View Staged Changes</span>
          <Badge
            variant="destructive"
            className="absolute -top-2 -right-2 shadow-lg animate-bounce"
          >
            {stagedChanges.length}
          </Badge>
        </Button>
      </div>
    );
  }

  return (
    <Drawer open={open} onOpenChange={onClose}>
      <DrawerContent className="max-h-[85vh]">
        <div className="flex flex-col h-full max-h-[85vh]">
          {/* Header */}
          <DrawerHeader className="border-b pb-4 bg-gradient-to-r from-muted/30 to-transparent">
            <div className="space-y-3">
              {applyError && (
                <Alert variant="destructive">
                  <AlertCircle className="h-4 w-4" />
                  <AlertTitle>Failed to Apply Changes</AlertTitle>
                  <AlertDescription className="break-words">{applyError}</AlertDescription>
                </Alert>
              )}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <DrawerTitle className="text-xl font-bold tracking-tight">
                    Staged Changes
                  </DrawerTitle>
                  <Badge variant="secondary" className="font-semibold">
                    {stagedChanges.length} {stagedChanges.length === 1 ? 'change' : 'changes'}
                  </Badge>
                </div>
              </div>

              {/* Validation Summary */}
              {(validationSummary.errorCount > 0 || validationSummary.warningCount > 0) && (
                <div className="flex gap-2">
                  {validationSummary.errorCount > 0 && (
                    <div className="flex items-center gap-1.5 text-sm text-destructive">
                      <AlertCircle className="h-4 w-4" />
                      <span>
                        {validationSummary.errorCount} validation{' '}
                        {validationSummary.errorCount === 1 ? 'error' : 'errors'}
                      </span>
                    </div>
                  )}
                  {validationSummary.warningCount > 0 && (
                    <div className="flex items-center gap-1.5 text-sm text-amber-600 dark:text-amber-400">
                      <AlertTriangle className="h-4 w-4" />
                      <span>
                        {validationSummary.warningCount}{' '}
                        {validationSummary.warningCount === 1 ? 'warning' : 'warnings'}
                      </span>
                    </div>
                  )}
                </div>
              )}
            </div>
          </DrawerHeader>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-4">
            {stagedChanges.length === 0 ? (
              <div className="text-center text-muted-foreground py-8">No staged changes</div>
            ) : (
              <div className="space-y-4">
                {/* Read-only mode alert */}
                {isReadOnly && (
                  <Alert>
                    <Info className="h-4 w-4" />
                    <AlertTitle>Read-Only Mode</AlertTitle>
                    <AlertDescription className="w-full">
                      <div className="break-words">
                        Changes are staged but cannot be applied. Set{' '}
                        <code className="text-xs bg-muted px-1 py-0.5 rounded break-all">
                          {READ_ONLY_PROPERTY}=false
                        </code>{' '}
                        in YARN to enable editing.
                      </div>
                    </AlertDescription>
                  </Alert>
                )}

                {sortedQueuePaths.map((queuePath) => (
                  <QueueChangeGroup
                    key={queuePath}
                    queuePath={queuePath}
                    changes={changesByQueue[queuePath]}
                    onRevert={handleRevertChange}
                  />
                ))}
              </div>
            )}
          </div>

          {/* Actions */}
          {stagedChanges.length > 0 && (
            <DrawerFooter className="border-t">
              <div className="flex justify-between items-center w-full gap-2">
                <Button
                  variant="outline"
                  onClick={handleClearAll}
                  disabled={isApplying}
                  className="gap-2"
                >
                  <Trash2 className="h-4 w-4" />
                  Clear All
                  <Kbd className="ml-auto">{getModifierKey()}+K</Kbd>
                </Button>
                <Button
                  variant="default"
                  onClick={handleApplyChanges}
                  disabled={isApplying || validationSummary.errorCount > 0 || isReadOnly}
                  title={
                    isReadOnly
                      ? 'Cannot apply changes in read-only mode'
                      : validationSummary.errorCount > 0
                        ? 'Fix validation errors before applying changes'
                        : undefined
                  }
                  className="gap-2"
                >
                  {isApplying ? (
                    <>
                      <div className="h-4 w-4 animate-spin rounded-full border-2 border-background border-t-transparent" />
                      Applying...
                    </>
                  ) : (
                    <>
                      <Check className="h-4 w-4" />
                      Apply All Changes
                      <Kbd className="ml-auto">{getModifierKey()}+S</Kbd>
                    </>
                  )}
                </Button>
              </div>
            </DrawerFooter>
          )}
        </div>
      </DrawerContent>
    </Drawer>
  );
}
