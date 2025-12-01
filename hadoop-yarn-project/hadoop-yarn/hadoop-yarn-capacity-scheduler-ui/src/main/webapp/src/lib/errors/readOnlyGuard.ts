/**
 * Read-only guard utilities
 *
 * Helper functions for blocking mutations when the scheduler is in read-only mode.
 */

import { READ_ONLY_PROPERTY } from '~/config';
import { createStoreError, ERROR_CODES } from './index';
import type { SchedulerStoreError } from './scheduler-store-error';

/**
 * Create an error for blocked read-only operations.
 * Returns both the error message (for state) and the error object (for throwing).
 */
export function createReadOnlyBlockedError(operation: string): {
  errorMessage: string;
  error: SchedulerStoreError;
} {
  const errorMessage = `Cannot ${operation} in read-only mode. Set ${READ_ONLY_PROPERTY}=false in YARN to enable editing.`;
  return {
    errorMessage,
    error: createStoreError(ERROR_CODES.MUTATION_BLOCKED, errorMessage),
  };
}

/**
 * Check if a mutation should be blocked and throw an error if so.
 * Use this when you don't need to set error state.
 */
export function assertWritable(isReadOnly: boolean, operation: string): void {
  if (isReadOnly) {
    const { error } = createReadOnlyBlockedError(operation);
    throw error;
  }
}
