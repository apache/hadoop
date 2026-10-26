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
import { GitCompareArrows, X } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { QueueComparisonDialog } from './QueueComparisonDialog';

export const CompareButton: React.FC = () => {
  const comparisonQueues = useSchedulerStore((state) => state.comparisonQueues);
  const isComparisonModeActive = useSchedulerStore((state) => state.isComparisonModeActive);
  const setComparisonMode = useSchedulerStore((state) => state.setComparisonMode);
  const [isOpen, setIsOpen] = useState(false);

  const selectedCount = comparisonQueues.length;
  const canCompare = selectedCount >= 2;

  // Only show when comparison mode is active
  if (!isComparisonModeActive) return null;

  const handleOpenDialog = () => {
    if (canCompare) {
      setIsOpen(true);
    }
  };

  const handleCloseDialog = (open: boolean) => {
    setIsOpen(open);
    // Don't exit comparison mode when dialog closes
    // Let the user explicitly exit via the X button
  };

  return (
    <>
      <div className="fixed bottom-6 right-6 z-50">
        <div className="flex items-center gap-2">
          <Button onClick={handleOpenDialog} size="lg" className="shadow-lg" disabled={!canCompare}>
            <GitCompareArrows className="mr-2 h-4 w-4" />
            Compare {selectedCount} Queue{selectedCount !== 1 ? 's' : ''}
          </Button>
          <Button
            variant="outline"
            size="icon"
            onClick={() => setComparisonMode(false)}
            aria-label="Exit comparison mode"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <QueueComparisonDialog open={isOpen} onOpenChange={handleCloseDialog} />
    </>
  );
};
