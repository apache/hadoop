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


import { cn } from '~/utils/cn';

interface QueueCardStyleParams {
  isAutoCreatedQueue: boolean;
  stagedStatus: string | undefined;
  isSelectedQueue: boolean;
  isSelectedForComparison: boolean;
  validationErrors: Array<{ severity: string }> | undefined;
  isAffectedByErrors: boolean | undefined;
  shouldGrayOut: boolean;
}

export function getQueueCardClassName(params: QueueCardStyleParams): string {
  const {
    isAutoCreatedQueue,
    stagedStatus,
    isSelectedQueue,
    isSelectedForComparison,
    validationErrors,
    isAffectedByErrors,
    shouldGrayOut,
  } = params;

  const hasErrors = validationErrors && validationErrors.some((e) => e.severity === 'error');

  return cn(
    'relative flex flex-col',
    // Smooth transitions with spring-like feel
    'transition-all duration-200 ease-out',
    // Enhanced background with subtle gradient
    'bg-gradient-to-br from-gray-50 to-white dark:from-gray-900 dark:to-gray-950',
    'border-gray-200 dark:border-gray-700/80',
    // Auto-created queue styling
    isAutoCreatedQueue &&
      'border-amber-400 dark:border-amber-500 border-2 border-dashed from-amber-50/70 to-amber-50/50 dark:from-amber-900/30 dark:to-amber-950/20',
    // Shadow for depth with hover enhancement
    'shadow-lg hover:shadow-xl hover:-translate-y-0.5',
    'dark:shadow-md dark:shadow-black/20 dark:hover:shadow-lg dark:hover:shadow-black/30',
    // Cursor styling - not clickable for new queues
    stagedStatus === 'new' ? 'opacity-75 cursor-default' : 'cursor-pointer',
    // Left border for staged status (always visible regardless of errors)
    stagedStatus === 'new' && 'border-l-4 border-l-queue-new',
    stagedStatus === 'deleted' && 'border-l-4 border-l-queue-deleted',
    stagedStatus === 'modified' && 'border-l-4 border-l-queue-modified',
    // Ring for staged status (only if no validation errors)
    stagedStatus === 'new' && !hasErrors && 'ring-2 ring-queue-new',
    stagedStatus === 'deleted' && !hasErrors && 'ring-2 ring-queue-deleted',
    stagedStatus === 'modified' && !hasErrors && 'ring-2 ring-queue-modified',
    !stagedStatus && isSelectedQueue && 'ring-2 ring-primary shadow-primary/10',
    // Ring for validation errors (can coexist with staged status border)
    hasErrors && 'ring-2 ring-destructive',
    // Left border for affected queues only if no staged status
    !stagedStatus && hasErrors && 'border-l-4 border-l-destructive',
    isAffectedByErrors &&
      !validationErrors &&
      !stagedStatus &&
      'ring-2 ring-amber-500 border-l-4 border-l-amber-500',
    isAffectedByErrors && !validationErrors && stagedStatus && 'ring-2 ring-amber-500',
    // Background styling for states
    isSelectedQueue &&
      'from-primary/10 to-primary/5 dark:from-primary/15 dark:to-primary/5 scale-[1.01]',
    isSelectedForComparison &&
      !isSelectedQueue &&
      'from-gray-100 to-gray-50 dark:from-gray-800 dark:to-gray-900',
    // Gray out inaccessible queues when filtered by label
    shouldGrayOut && 'opacity-50 grayscale',
    'gap-4 py-5',
  );
}
