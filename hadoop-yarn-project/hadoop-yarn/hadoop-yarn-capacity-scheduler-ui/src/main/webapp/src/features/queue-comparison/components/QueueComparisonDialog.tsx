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
import { Download } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { Button } from '~/components/ui/button';
import { useSchedulerStore } from '~/stores/schedulerStore';
import {
  buildComparisonData,
  exportComparison,
} from '~/features/queue-comparison/utils/comparison';
import { ComparisonTable } from './ComparisonTable';
import { getQueueProperties } from '~/utils/configPropertyUtils';

interface QueueComparisonDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export const QueueComparisonDialog: React.FC<QueueComparisonDialogProps> = ({
  open,
  onOpenChange,
}) => {
  const comparisonQueues = useSchedulerStore((state) => state.comparisonQueues);
  const configData = useSchedulerStore((state) => state.configData);

  // Build comparison data from current store state
  const configs = React.useMemo(() => {
    const result = new Map<string, Record<string, string>>();
    comparisonQueues.forEach((queuePath) => {
      const properties = getQueueProperties(configData, queuePath);
      result.set(queuePath, properties);
    });
    return result;
  }, [comparisonQueues, configData]);

  const comparisonData = buildComparisonData(configs);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="!max-w-[95vw] !w-[95vw] max-h-[95vh] h-[95vh] flex flex-col p-0 sm:!max-w-[95vw]">
        <DialogHeader className="px-6 py-4 border-b shrink-0">
          <DialogTitle>Queue Configuration Comparison</DialogTitle>
          <DialogDescription>
            Comparing {comparisonData.queues.length} queues. Differences are highlighted in blue.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 px-6 py-4">
          <ComparisonTable data={comparisonData} />
        </div>

        <DialogFooter className="px-6 py-4 border-t shrink-0">
          <Button variant="outline" onClick={() => exportComparison(comparisonData)}>
            <Download className="mr-2 h-4 w-4" />
            Export
          </Button>
          <Button onClick={() => onOpenChange(false)}>Close</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
