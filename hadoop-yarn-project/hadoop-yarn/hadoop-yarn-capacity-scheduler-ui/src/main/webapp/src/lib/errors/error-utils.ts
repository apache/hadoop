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


import { SchedulerStoreError } from './scheduler-store-error';

export function createStoreError(
  code: string,
  message: string,
  details?: unknown,
): SchedulerStoreError {
  return new SchedulerStoreError(message, code, details);
}

/**
 * Extract a user-friendly error message from various error types
 */
export function extractErrorMessage(error: unknown): string {
  if (error instanceof SchedulerStoreError) {
    return error.message;
  }

  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  return 'An unexpected error occurred';
}

/**
 * Type guard to check if an error is a network error
 */
export function isNetworkError(error: unknown): boolean {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    return (
      message.includes('network') ||
      message.includes('fetch') ||
      message.includes('timeout') ||
      message.includes('abort')
    );
  }
  return false;
}

/**
 * Create a detailed error message with context
 */
export function createDetailedErrorMessage(
  operation: string,
  error: unknown,
  context?: Record<string, unknown>,
): string {
  const baseMessage = `Failed to ${operation}`;
  const errorMessage = extractErrorMessage(error);

  let message = `${baseMessage}: ${errorMessage}`;

  if (context && Object.keys(context).length > 0) {
    const contextStr = Object.entries(context)
      .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
      .join(', ');
    message += ` (${contextStr})`;
  }

  return message;
}
