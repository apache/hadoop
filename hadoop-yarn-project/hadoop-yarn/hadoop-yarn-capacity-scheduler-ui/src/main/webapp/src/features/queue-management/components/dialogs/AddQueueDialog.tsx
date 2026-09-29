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


import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { Button } from '~/components/ui/button';
import { Input } from '~/components/ui/input';
import { Field, FieldControl, FieldLabel, FieldMessage } from '~/components/ui/field';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import { Plus, Info } from 'lucide-react';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';
import { useCapacityEditor } from '~/features/queue-management/hooks/useCapacityEditor';

const addQueueSchema = z.object({
  queueName: z
    .string()
    .min(1, 'Queue name is required')
    .max(50, 'Queue name must be 50 characters or less')
    .regex(
      /^[a-zA-Z0-9_-]+$/,
      'Queue name can only contain letters, numbers, underscores, and hyphens',
    )
    .refine((name) => !name.includes('.'), {
      message: 'Queue name cannot contain dots',
    }),
  capacity: z.string().min(1, 'Capacity is required'),
  maxCapacity: z.string().min(1, 'Max capacity is required'),
  state: z.enum(['RUNNING', 'STOPPED']),
});

type AddQueueFormData = z.infer<typeof addQueueSchema>;

interface AddQueueDialogProps {
  open: boolean;
  parentQueuePath: string;
  onClose: () => void;
}

export function AddQueueDialog({ open, parentQueuePath, onClose }: AddQueueDialogProps) {
  const { addChildQueue } = useQueueActions();
  const parentQueueName = parentQueuePath.split('.').pop() || parentQueuePath;

  const { openCapacityEditor } = useCapacityEditor();

  const {
    register,
    handleSubmit,
    watch,
    reset,
    setValue,
    formState: { errors, isValid },
  } = useForm<AddQueueFormData>({
    resolver: zodResolver(addQueueSchema),
    defaultValues: {
      queueName: '',
      capacity: '10',
      maxCapacity: '100',
      state: 'RUNNING',
    },
    mode: 'onChange',
  });

  const handleClose = () => {
    reset();
    onClose();
  };

  const onSubmit = (data: AddQueueFormData) => {
    try {
      // Add the queue using our hook
      addChildQueue(parentQueuePath, data.queueName, {
        capacity: data.capacity,
        'maximum-capacity': data.maxCapacity,
        state: data.state,
      });

      const newQueuePath =
        parentQueuePath === 'root'
          ? `root.${data.queueName}`
          : `${parentQueuePath}.${data.queueName}`;

      openCapacityEditor({
        origin: 'add-queue',
        parentQueuePath,
        originQueuePath: newQueuePath,
        originQueueName: data.queueName,
        capacityValue: data.capacity,
        maxCapacityValue: data.maxCapacity,
        markOriginAsNew: true,
        queueState: data.state,
      });

      handleClose();
    } catch (error) {
      // Error handling is done by the hook
      console.error('Failed to add queue:', error);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent onClick={(e) => e.stopPropagation()}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Plus className="h-5 w-5" />
            Add Child Queue
          </DialogTitle>
          <DialogDescription>
            Creating new queue under: <strong>{parentQueueName}</strong>
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          {/* Queue Name */}
          <Field>
            <FieldLabel htmlFor="queueName">
              Queue Name <span className="text-red-500">*</span>
            </FieldLabel>
            <FieldControl>
              <Input
                {...register('queueName')}
                id="queueName"
                placeholder="e.g., production, development"
                autoFocus
                aria-invalid={Boolean(errors.queueName)}
              />
            </FieldControl>
            {errors.queueName && <FieldMessage>{errors.queueName.message}</FieldMessage>}
          </Field>

          {/* Hidden capacity defaults until Capacity Editor handles them */}
          <input type="hidden" {...register('capacity')} />
          <input type="hidden" {...register('maxCapacity')} />

          {/* Workflow help text */}
          <div className="flex items-start gap-2 p-3 rounded-md bg-muted/50 text-sm text-muted-foreground">
            <Info className="h-4 w-4 mt-0.5 flex-shrink-0" />
            <span>
              After creating the queue, you will be able to set capacity for this queue and
              adjust sibling capacities in the capacity editor.
            </span>
          </div>

          {/* State */}
          <Field>
            <FieldLabel htmlFor="state">State</FieldLabel>
            <Select
              value={watch('state')}
              onValueChange={(value) => setValue('state', value as 'RUNNING' | 'STOPPED')}
            >
              <FieldControl>
                <SelectTrigger id="state">
                  <SelectValue />
                </SelectTrigger>
              </FieldControl>
              <SelectContent>
                <SelectItem value="RUNNING">Running</SelectItem>
                <SelectItem value="STOPPED">Stopped</SelectItem>
              </SelectContent>
            </Select>
          </Field>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={handleClose}>
              Cancel
            </Button>
            <Button type="submit" disabled={!isValid}>
              <Plus className="mr-2 h-4 w-4" />
              Create Queue
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
