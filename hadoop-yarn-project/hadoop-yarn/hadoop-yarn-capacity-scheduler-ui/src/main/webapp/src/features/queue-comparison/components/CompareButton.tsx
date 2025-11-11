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
  const { comparisonQueues, clearComparisonQueues, canCompareQueues } = useSchedulerStore();
  const [isOpen, setIsOpen] = useState(false);

  const selectedCount = comparisonQueues.length;

  if (!canCompareQueues()) return null;

  return (
    <>
      <div className="fixed bottom-6 right-6 z-50">
        <div className="flex items-center gap-2">
          <Button onClick={() => setIsOpen(true)} size="lg" className="shadow-lg">
            <GitCompareArrows className="mr-2 h-4 w-4" />
            Compare {selectedCount} Queues
          </Button>
          <Button
            variant="outline"
            size="icon"
            onClick={clearComparisonQueues}
            aria-label="Clear selection"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <QueueComparisonDialog open={isOpen} onOpenChange={setIsOpen} />
    </>
  );
};
