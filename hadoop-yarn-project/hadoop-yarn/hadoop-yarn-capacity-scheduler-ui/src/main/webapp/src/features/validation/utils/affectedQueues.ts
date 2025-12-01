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


import type { SchedulerInfo, StagedChange } from '~/types';
import { findQueueByPath } from '~/utils/treeUtils';
import { getParentQueuePath } from '~/utils/propertyUtils';
import { isTemplateQueuePath } from '~/utils/templateUtils';

export function getAffectedQueuesForValidation(
  propertyName: string,
  queuePath: string,
  schedulerData: SchedulerInfo | null,
  stagedChanges: StagedChange[] = [],
): string[] {
  const affectedQueues: string[] = [queuePath];

  if (isTemplateQueuePath(queuePath)) {
    return affectedQueues;
  }

  if (!schedulerData) {
    return affectedQueues;
  }

  if (propertyName === 'capacity') {
    const currentQueue = findQueueByPath(schedulerData, queuePath);
    const parentPath = getParentQueuePath(queuePath);

    if (parentPath && !affectedQueues.includes(parentPath)) {
      affectedQueues.push(parentPath);

      const parentQueue = findQueueByPath(schedulerData, parentPath);
      if (parentQueue?.queues?.queue) {
        parentQueue.queues.queue.forEach((sibling) => {
          if (sibling.queuePath !== queuePath && !affectedQueues.includes(sibling.queuePath)) {
            affectedQueues.push(sibling.queuePath);
          }
        });
      }

      stagedChanges
        .filter(
          (change) => change.type === 'add' && getParentQueuePath(change.queuePath) === parentPath,
        )
        .forEach((change) => {
          if (!affectedQueues.includes(change.queuePath)) {
            affectedQueues.push(change.queuePath);
          }
        });
    }

    if (currentQueue?.queues?.queue?.length) {
      currentQueue.queues.queue.forEach((child) => {
        if (!affectedQueues.includes(child.queuePath)) {
          affectedQueues.push(child.queuePath);
        }
      });
    }
  }

  if (propertyName === 'state') {
    const currentQueue = findQueueByPath(schedulerData, queuePath);
    if (!currentQueue) {
      return affectedQueues;
    }

    const parentPath = getParentQueuePath(queuePath);
    if (parentPath) {
      affectedQueues.push(parentPath);
    }

    if (currentQueue.queues?.queue?.length) {
      currentQueue.queues.queue.forEach((child) => {
        if (!affectedQueues.includes(child.queuePath)) {
          affectedQueues.push(child.queuePath);
        }
      });
    }
  }

  return affectedQueues;
}
