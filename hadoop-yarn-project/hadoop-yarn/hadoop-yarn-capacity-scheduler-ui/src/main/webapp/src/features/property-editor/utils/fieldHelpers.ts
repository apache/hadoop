/**
 * Property field helper utilities
 *
 * Shared utility functions for property form fields.
 */

import { cn } from '~/utils/cn';

/**
 * Get common field className based on staged status and error state
 */
export const getCommonFieldClassName = (
  stagedStatus?: 'new' | 'modified' | 'deleted',
  hasError?: boolean,
): string => {
  return cn(
    stagedStatus === 'modified' && 'ring-2 ring-primary ring-offset-1',
    hasError && 'ring-2 ring-destructive ring-offset-1',
  );
};

/**
 * Parse field errors into inline and remaining errors
 */
export const parseFieldErrors = (
  errors: string[] = [],
): { inline?: string; remaining: string[] } => {
  const fieldErrors = errors
    .map((message) => (typeof message === 'string' ? message.trim() : ''))
    .filter((message) => message.length > 0);

  return {
    inline: fieldErrors[0],
    remaining: fieldErrors.slice(1),
  };
};
