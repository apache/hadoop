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


import type { QueueInfo } from './queue';
import { QUEUE_TYPES } from './constants';

export function isLeafQueue(queue: QueueInfo): boolean {
  return queue.queueType === QUEUE_TYPES.LEAF;
}

export function isParentQueue(queue: QueueInfo): boolean {
  return queue.queueType === QUEUE_TYPES.PARENT;
}

/**
 * Returns an error message if the queue name is invalid, or null if valid.
 * This is the single source of truth for queue name validation rules.
 */
export function getQueueNameValidationError(name: string): string | null {
  if (!name || name.trim() === '') {
    return 'Queue name cannot be empty';
  }

  if (name.trim() !== name) {
    return 'Queue name cannot have leading or trailing whitespace';
  }

  // Queue names cannot contain dots (they are used as path separators)
  if (name.includes('.')) {
    return 'Queue names cannot contain dots (.)';
  }

  // Must match alphanumeric, hyphen, underscore pattern
  if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
    return 'Queue names should only contain letters, numbers, hyphens, and underscores';
  }

  return null;
}

/**
 * Returns true if the queue name is valid.
 */
export function isValidQueueName(name: string): boolean {
  return getQueueNameValidationError(name) === null;
}
