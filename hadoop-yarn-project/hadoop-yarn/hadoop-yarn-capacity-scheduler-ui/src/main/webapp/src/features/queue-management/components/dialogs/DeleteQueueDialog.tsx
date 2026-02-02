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


import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { Button } from '~/components/ui/button';
import { Alert, AlertDescription } from '~/components/ui/alert';
import { Trash2, AlertTriangle } from 'lucide-react';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';
import { SPECIAL_VALUES } from '~/types';

interface DeleteQueueDialogProps {
  open: boolean;
  queuePath: string;
  onClose: () => void;
}

export function DeleteQueueDialog({ open, queuePath, onClose }: DeleteQueueDialogProps) {
  const { deleteQueue, canDeleteQueue } = useQueueActions();
  const queueName = queuePath.split('.').pop() || queuePath;

  const canDelete = canDeleteQueue(queuePath);
  const isRoot = queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME;

  const handleDelete = () => {
    try {
      deleteQueue(queuePath);
      onClose();
    } catch (error) {
      console.error('Failed to delete queue:', error);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-md" onClick={(e) => e.stopPropagation()}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-red-600">
            <Trash2 className="h-5 w-5" />
            Delete Queue
          </DialogTitle>
          <DialogDescription>Remove a queue from the scheduler configuration</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {isRoot ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>The root queue cannot be deleted.</AlertDescription>
            </Alert>
          ) : !canDelete ? (
            <Alert>
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>
                This queue has child queues and cannot be deleted. Please delete all child queues
                first.
              </AlertDescription>
            </Alert>
          ) : (
            <>
              <p className="text-sm">
                Are you sure you want to delete the queue <strong>{queueName}</strong>?
              </p>
              <Alert>
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription>
                  This action cannot be undone. The queue and all its configurations will be
                  removed.
                </AlertDescription>
              </Alert>
            </>
          )}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={onClose}>
            Cancel
          </Button>
          {canDelete && !isRoot && (
            <Button onClick={handleDelete} variant="destructive">
              <Trash2 className="mr-2 h-4 w-4" />
              Delete Queue
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
