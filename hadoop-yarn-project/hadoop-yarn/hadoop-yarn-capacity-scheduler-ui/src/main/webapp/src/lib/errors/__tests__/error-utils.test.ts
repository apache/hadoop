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


import { describe, it, expect } from 'vitest';
import {
  createStoreError,
  extractErrorMessage,
  isNetworkError,
  createDetailedErrorMessage,
} from '~/lib/errors/error-utils';
import { SchedulerStoreError } from '~/lib/errors/scheduler-store-error';
import { ERROR_CODES } from '~/lib/errors/error-codes';

describe('error-utils', () => {
  describe('createStoreError', () => {
    it('should create SchedulerStoreError with message and code', () => {
      const error = createStoreError(ERROR_CODES.API_ERROR, 'Test error message');

      expect(error).toBeInstanceOf(SchedulerStoreError);
      expect(error.message).toBe('Test error message');
      expect(error.code).toBe(ERROR_CODES.API_ERROR);
      expect(error.details).toBeUndefined();
    });

    it('should create error with details', () => {
      const details = { statusCode: 500, endpoint: '/api/test' };
      const error = createStoreError(ERROR_CODES.NETWORK_ERROR, 'Network failure', details);

      expect(error).toBeInstanceOf(SchedulerStoreError);
      expect(error.message).toBe('Network failure');
      expect(error.code).toBe(ERROR_CODES.NETWORK_ERROR);
      expect(error.details).toEqual(details);
    });

    it('should handle undefined details', () => {
      const error = createStoreError(ERROR_CODES.VALIDATION_ERROR, 'Validation failed', undefined);

      expect(error.details).toBeUndefined();
    });

    it('should handle null details', () => {
      const error = createStoreError(ERROR_CODES.API_ERROR, 'Error', null);

      expect(error.details).toBeNull();
    });

    it('should handle complex details objects', () => {
      const details = {
        errors: ['error1', 'error2'],
        timestamp: new Date(),
        metadata: {
          version: '1.0.0',
          environment: 'test',
        },
      };
      const error = createStoreError(ERROR_CODES.VALIDATION_ERROR, 'Complex error', details);

      expect(error.details).toEqual(details);
    });
  });

  describe('extractErrorMessage', () => {
    it('should extract message from SchedulerStoreError', () => {
      const error = new SchedulerStoreError('Store error message', ERROR_CODES.API_ERROR);
      const message = extractErrorMessage(error);

      expect(message).toBe('Store error message');
    });

    it('should extract message from standard Error', () => {
      const error = new Error('Standard error message');
      const message = extractErrorMessage(error);

      expect(message).toBe('Standard error message');
    });

    it('should extract message from TypeError', () => {
      const error = new TypeError('Type error message');
      const message = extractErrorMessage(error);

      expect(message).toBe('Type error message');
    });

    it('should extract message from RangeError', () => {
      const error = new RangeError('Range error message');
      const message = extractErrorMessage(error);

      expect(message).toBe('Range error message');
    });

    it('should handle string errors', () => {
      const message = extractErrorMessage('Plain string error');

      expect(message).toBe('Plain string error');
    });

    it('should return default message for null', () => {
      const message = extractErrorMessage(null);

      expect(message).toBe('An unexpected error occurred');
    });

    it('should return default message for undefined', () => {
      const message = extractErrorMessage(undefined);

      expect(message).toBe('An unexpected error occurred');
    });

    it('should return default message for number', () => {
      const message = extractErrorMessage(404);

      expect(message).toBe('An unexpected error occurred');
    });

    it('should return default message for boolean', () => {
      const message = extractErrorMessage(true);

      expect(message).toBe('An unexpected error occurred');
    });

    it('should return default message for plain object', () => {
      const message = extractErrorMessage({ error: 'test' });

      expect(message).toBe('An unexpected error occurred');
    });

    it('should return default message for array', () => {
      const message = extractErrorMessage(['error1', 'error2']);

      expect(message).toBe('An unexpected error occurred');
    });

    it('should handle empty string', () => {
      const message = extractErrorMessage('');

      expect(message).toBe('');
    });

    it('should handle object without message property', () => {
      const message = extractErrorMessage({ status: 500 });

      expect(message).toBe('An unexpected error occurred');
    });

    it('should prioritize SchedulerStoreError over Error', () => {
      const error = new SchedulerStoreError('Store error', ERROR_CODES.API_ERROR);
      const message = extractErrorMessage(error);

      // Should match SchedulerStoreError first
      expect(message).toBe('Store error');
    });
  });

  describe('isNetworkError', () => {
    it('should detect network keyword in error message', () => {
      const error = new Error('network request failed');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect Network keyword (capitalized)', () => {
      const error = new Error('Network connection lost');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect NETWORK keyword (uppercase)', () => {
      const error = new Error('NETWORK ERROR');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect fetch keyword in error message', () => {
      const error = new Error('fetch operation failed');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect Fetch keyword (capitalized)', () => {
      const error = new Error('Fetch request timed out');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect timeout keyword in error message', () => {
      const error = new Error('Request timeout after 30s');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect Timeout keyword (capitalized)', () => {
      const error = new Error('Timeout exceeded');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect abort keyword in error message', () => {
      const error = new Error('Request was aborted');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should detect Abort keyword (capitalized)', () => {
      const error = new Error('Abort signal received');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should return false for non-network errors', () => {
      const error = new Error('Validation failed');
      expect(isNetworkError(error)).toBe(false);
    });

    it('should return false for non-Error objects', () => {
      expect(isNetworkError('network error')).toBe(false);
      expect(isNetworkError({ message: 'network error' })).toBe(false);
      expect(isNetworkError(null)).toBe(false);
      expect(isNetworkError(undefined)).toBe(false);
    });

    it('should return false for Error without network keywords', () => {
      const error = new Error('Something went wrong');
      expect(isNetworkError(error)).toBe(false);
    });

    it('should return false for empty error message', () => {
      const error = new Error('');
      expect(isNetworkError(error)).toBe(false);
    });

    it('should handle SchedulerStoreError with network keywords', () => {
      const error = new SchedulerStoreError('Network failure', ERROR_CODES.NETWORK_ERROR);
      expect(isNetworkError(error)).toBe(true);
    });

    it('should handle TypeError with network keywords', () => {
      const error = new TypeError('Failed to fetch resource');
      expect(isNetworkError(error)).toBe(true);
    });

    it('should handle partial keyword matches', () => {
      expect(isNetworkError(new Error('networking issue'))).toBe(true);
      expect(isNetworkError(new Error('prefetch failed'))).toBe(true);
      expect(isNetworkError(new Error('timeout-exceeded'))).toBe(true);
      expect(isNetworkError(new Error('aborted by user'))).toBe(true);
    });

    it('should be case-insensitive for all keywords', () => {
      expect(isNetworkError(new Error('NETWORK'))).toBe(true);
      expect(isNetworkError(new Error('network'))).toBe(true);
      expect(isNetworkError(new Error('Network'))).toBe(true);
      expect(isNetworkError(new Error('nEtWoRk'))).toBe(true);
    });
  });

  describe('createDetailedErrorMessage', () => {
    it('should format message with operation and error', () => {
      const error = new Error('Connection refused');
      const message = createDetailedErrorMessage('fetch data', error);

      expect(message).toBe('Failed to fetch data: Connection refused');
    });

    it('should handle SchedulerStoreError', () => {
      const error = new SchedulerStoreError('Invalid queue path', ERROR_CODES.INVALID_QUEUE_PATH);
      const message = createDetailedErrorMessage('create queue', error);

      expect(message).toBe('Failed to create queue: Invalid queue path');
    });

    it('should handle string errors', () => {
      const message = createDetailedErrorMessage('save configuration', 'Permission denied');

      expect(message).toBe('Failed to save configuration: Permission denied');
    });

    it('should handle unknown error types', () => {
      const message = createDetailedErrorMessage('process request', null);

      expect(message).toBe('Failed to process request: An unexpected error occurred');
    });

    it('should include context when provided', () => {
      const error = new Error('Not found');
      const context = { queuePath: 'root.production', statusCode: 404 };
      const message = createDetailedErrorMessage('find queue', error, context);

      expect(message).toBe(
        'Failed to find queue: Not found (queuePath="root.production", statusCode=404)',
      );
    });

    it('should handle empty context', () => {
      const error = new Error('Test error');
      const message = createDetailedErrorMessage('test operation', error, {});

      expect(message).toBe('Failed to test operation: Test error');
    });

    it('should handle undefined context', () => {
      const error = new Error('Test error');
      const message = createDetailedErrorMessage('test operation', error, undefined);

      expect(message).toBe('Failed to test operation: Test error');
    });

    it('should serialize context values as JSON', () => {
      const error = new Error('Failed');
      const context = {
        retryCount: 3,
        timeout: 5000,
        enabled: true,
      };
      const message = createDetailedErrorMessage('connect', error, context);

      expect(message).toContain('retryCount=3');
      expect(message).toContain('timeout=5000');
      expect(message).toContain('enabled=true');
    });

    it('should handle multiple context entries', () => {
      const error = new Error('Failed');
      const context = {
        endpoint: '/api/scheduler',
        method: 'GET',
        status: 500,
      };
      const message = createDetailedErrorMessage('fetch', error, context);

      expect(message).toContain('endpoint="/api/scheduler"');
      expect(message).toContain('method="GET"');
      expect(message).toContain('status=500');
      expect(message).toMatch(/\(.*,.*,.*\)/); // Has commas separating context entries
    });

    it('should handle context with special characters', () => {
      const error = new Error('Failed');
      const context = {
        path: 'root.production.queue-1',
        value: '50%',
      };
      const message = createDetailedErrorMessage('validate', error, context);

      expect(message).toContain('path="root.production.queue-1"');
      expect(message).toContain('value="50%"');
    });

    it('should handle context with nested objects', () => {
      const error = new Error('Failed');
      const context = {
        config: {
          capacity: '50',
          maxCapacity: '100',
        },
      };
      const message = createDetailedErrorMessage('apply', error, context);

      expect(message).toContain('config=');
      expect(message).toContain('capacity');
      expect(message).toContain('maxCapacity');
    });

    it('should handle context with arrays', () => {
      const error = new Error('Failed');
      const context = {
        errors: ['error1', 'error2', 'error3'],
      };
      const message = createDetailedErrorMessage('validate', error, context);

      expect(message).toContain('errors=');
      expect(message).toContain('error1');
      expect(message).toContain('error2');
    });

    it('should handle context with null values', () => {
      const error = new Error('Failed');
      const context = {
        value: null,
      };
      const message = createDetailedErrorMessage('process', error, context);

      expect(message).toContain('value=null');
    });

    it('should handle context with undefined values', () => {
      const error = new Error('Failed');
      const context = {
        optional: undefined,
      };
      const message = createDetailedErrorMessage('process', error, context);

      expect(message).toContain('optional');
    });

    it('should work with different error types', () => {
      const typeError = new TypeError('Invalid type');
      const message1 = createDetailedErrorMessage('convert', typeError);
      expect(message1).toBe('Failed to convert: Invalid type');

      const rangeError = new RangeError('Out of range');
      const message2 = createDetailedErrorMessage('calculate', rangeError);
      expect(message2).toBe('Failed to calculate: Out of range');

      const referenceError = new ReferenceError('Variable not defined');
      const message3 = createDetailedErrorMessage('reference', referenceError);
      expect(message3).toBe('Failed to reference: Variable not defined');
    });

    it('should handle very long operation names', () => {
      const error = new Error('Failed');
      const operation = 'perform a very long and complex operation with multiple steps';
      const message = createDetailedErrorMessage(operation, error);

      expect(message).toBe(`Failed to ${operation}: Failed`);
    });

    it('should handle very long error messages', () => {
      const longMessage = 'A'.repeat(500);
      const error = new Error(longMessage);
      const message = createDetailedErrorMessage('test', error);

      expect(message).toContain('Failed to test:');
      expect(message).toContain(longMessage);
    });

    it('should handle real-world network error scenario', () => {
      const error = new Error('Network request failed');
      const context = {
        url: 'http://localhost:8088/ws/v1/cluster/scheduler',
        method: 'GET',
        timeout: 30000,
      };
      const message = createDetailedErrorMessage('fetch scheduler configuration', error, context);

      expect(message).toContain('Failed to fetch scheduler configuration');
      expect(message).toContain('Network request failed');
      expect(message).toContain('url=');
      expect(message).toContain('method="GET"');
      expect(message).toContain('timeout=30000');
    });

    it('should handle real-world validation error scenario', () => {
      const error = new SchedulerStoreError(
        'Capacity exceeds parent capacity',
        ERROR_CODES.VALIDATION_ERROR,
      );
      const context = {
        queuePath: 'root.production.critical',
        field: 'capacity',
        value: '80',
        parentCapacity: '60',
      };
      const message = createDetailedErrorMessage('validate queue capacity', error, context);

      expect(message).toContain('Failed to validate queue capacity');
      expect(message).toContain('Capacity exceeds parent capacity');
      expect(message).toContain('queuePath="root.production.critical"');
      expect(message).toContain('field="capacity"');
    });
  });
});
