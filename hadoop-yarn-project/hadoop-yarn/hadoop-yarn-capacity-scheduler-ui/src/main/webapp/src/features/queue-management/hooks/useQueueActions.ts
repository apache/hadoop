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


import { useSchedulerStore } from '~/stores/schedulerStore';
import { SPECIAL_VALUES } from '~/types';

export type UseQueueActionsResult = {
  addChildQueue: (parentPath: string, queueName: string, config: Record<string, string>) => void;
  deleteQueue: (queuePath: string) => void;
  updateQueueProperty: (queuePath: string, property: string, value: string) => void;
  canAddChildQueue: (parentPath: string) => boolean;
  canDeleteQueue: (queuePath: string) => boolean;
};

export function useQueueActions(): UseQueueActionsResult {
  const stageQueueAddition = useSchedulerStore((state) => state.stageQueueAddition);
  const stageQueueRemoval = useSchedulerStore((state) => state.stageQueueRemoval);
  const stageQueueChange = useSchedulerStore((state) => state.stageQueueChange);
  const getQueueByPath = useSchedulerStore((state) => state.getQueueByPath);

  const addChildQueue = (parentPath: string, queueName: string, config: Record<string, string>) => {
    if (queueName.includes('.')) {
      throw new Error('Queue name cannot contain dots');
    }

    const parent = getQueueByPath(parentPath);
    if (!parent) {
      throw new Error('Parent queue not found');
    }

    stageQueueAddition(parentPath, queueName, config);
  };

  const deleteQueue = (queuePath: string) => {
    if (queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      throw new Error('Cannot delete root queue');
    }

    const queue = getQueueByPath(queuePath);
    if (!queue) {
      throw new Error('Queue not found');
    }

    if (queue.queues?.queue && queue.queues.queue.length > 0) {
      throw new Error('Cannot delete queue with children');
    }

    stageQueueRemoval(queuePath);
  };

  const updateQueueProperty = (queuePath: string, property: string, value: string) => {
    stageQueueChange(queuePath, property, value);
  };

  const canAddChildQueue = (parentPath: string) => {
    const parent = getQueueByPath(parentPath);
    return parent !== null;
  };

  const canDeleteQueue = (queuePath: string) => {
    if (queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      return false;
    }

    const queue = getQueueByPath(queuePath);
    if (!queue) {
      return false;
    }

    return !queue.queues?.queue || queue.queues.queue.length === 0;
  };

  return {
    addChildQueue,
    deleteQueue,
    updateQueueProperty,
    canAddChildQueue,
    canDeleteQueue,
  };
}
