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
import { SchedulerStoreError } from '~/lib/errors/scheduler-store-error';
import { ERROR_CODES } from '~/lib/errors/error-codes';
import { READ_ONLY_PROPERTY } from '~/config';

describe('SchedulerStoreError', () => {
  describe('constructor', () => {
    it('should create error with message and code', () => {
      const error = new SchedulerStoreError('Test error message', ERROR_CODES.NETWORK_ERROR);

      expect(error.message).toBe('Test error message');
      expect(error.code).toBe(ERROR_CODES.NETWORK_ERROR);
      expect(error.details).toBeUndefined();
    });

    it('should create error with optional details', () => {
      const details = { statusCode: 500, response: 'Internal Server Error' };
      const error = new SchedulerStoreError('Server error', ERROR_CODES.API_ERROR, details);

      expect(error.message).toBe('Server error');
      expect(error.code).toBe(ERROR_CODES.API_ERROR);
      expect(error.details).toEqual(details);
    });

    it('should create error with null details', () => {
      const error = new SchedulerStoreError('Test error', ERROR_CODES.VALIDATION_ERROR, null);

      expect(error.details).toBeNull();
    });

    it('should create error with undefined details explicitly', () => {
      const error = new SchedulerStoreError('Test error', ERROR_CODES.VALIDATION_ERROR, undefined);

      expect(error.details).toBeUndefined();
    });

    it('should create error with empty message', () => {
      const error = new SchedulerStoreError('', ERROR_CODES.NETWORK_ERROR);

      expect(error.message).toBe('');
      expect(error.code).toBe(ERROR_CODES.NETWORK_ERROR);
    });

    it('should create error with empty code', () => {
      const error = new SchedulerStoreError('Test message', '');

      expect(error.message).toBe('Test message');
      expect(error.code).toBe('');
    });
  });

  describe('properties', () => {
    it('should set name to SchedulerStoreError', () => {
      const error = new SchedulerStoreError('Test', ERROR_CODES.API_ERROR);

      expect(error.name).toBe('SchedulerStoreError');
    });

    it('should expose code as public readonly property', () => {
      const error = new SchedulerStoreError('Test', ERROR_CODES.NETWORK_ERROR);

      expect(error.code).toBe(ERROR_CODES.NETWORK_ERROR);

      // Verify code is accessible
      expect(error).toHaveProperty('code');
      expect(typeof error.code).toBe('string');
    });

    it('should expose details as public readonly property', () => {
      const details = { test: 'value' };
      const error = new SchedulerStoreError('Test', ERROR_CODES.API_ERROR, details);

      expect(error.details).toEqual(details);

      // Verify details is accessible
      expect(error).toHaveProperty('details');
      expect(error.details).toBe(details);
    });

    it('should maintain stack trace', () => {
      const error = new SchedulerStoreError('Test', ERROR_CODES.NETWORK_ERROR);

      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('SchedulerStoreError');
    });
  });

  describe('Error inheritance', () => {
    it('should be instance of Error', () => {
      const error = new SchedulerStoreError('Test', ERROR_CODES.API_ERROR);

      expect(error).toBeInstanceOf(Error);
    });

    it('should be instance of SchedulerStoreError', () => {
      const error = new SchedulerStoreError('Test', ERROR_CODES.API_ERROR);

      expect(error).toBeInstanceOf(SchedulerStoreError);
    });

    it('should be catchable as Error', () => {
      expect(() => {
        throw new SchedulerStoreError('Test', ERROR_CODES.NETWORK_ERROR);
      }).toThrow(Error);
    });

    it('should be catchable as SchedulerStoreError', () => {
      expect(() => {
        throw new SchedulerStoreError('Test', ERROR_CODES.NETWORK_ERROR);
      }).toThrow(SchedulerStoreError);
    });

    it('should work with instanceof checks in try-catch', () => {
      try {
        throw new SchedulerStoreError('Test', ERROR_CODES.API_ERROR);
      } catch (error) {
        expect(error).toBeInstanceOf(SchedulerStoreError);
        if (error instanceof SchedulerStoreError) {
          expect(error.code).toBe(ERROR_CODES.API_ERROR);
        }
      }
    });
  });

  describe('details handling', () => {
    it('should handle object details', () => {
      const details = {
        statusCode: 404,
        endpoint: '/api/scheduler',
        method: 'GET',
      };
      const error = new SchedulerStoreError('API Error', ERROR_CODES.API_ERROR, details);

      expect(error.details).toEqual(details);
    });

    it('should handle array details', () => {
      const details = ['error1', 'error2', 'error3'];
      const error = new SchedulerStoreError(
        'Multiple errors',
        ERROR_CODES.VALIDATION_ERROR,
        details,
      );

      expect(error.details).toEqual(details);
    });

    it('should handle string details', () => {
      const details = 'Additional context information';
      const error = new SchedulerStoreError('Test', ERROR_CODES.API_ERROR, details);

      expect(error.details).toBe(details);
    });

    it('should handle number details', () => {
      const details = 500;
      const error = new SchedulerStoreError('HTTP Error', ERROR_CODES.NETWORK_ERROR, details);

      expect(error.details).toBe(details);
    });

    it('should handle boolean details', () => {
      const details = true;
      const error = new SchedulerStoreError('Test', ERROR_CODES.API_ERROR, details);

      expect(error.details).toBe(details);
    });

    it('should handle nested object details', () => {
      const details = {
        error: {
          message: 'Nested error',
          stack: 'Error stack trace',
          nested: {
            deep: 'value',
          },
        },
      };
      const error = new SchedulerStoreError('Nested', ERROR_CODES.API_ERROR, details);

      expect(error.details).toEqual(details);
    });
  });

  describe('error message handling', () => {
    it('should handle multiline error messages', () => {
      const message = 'Line 1\nLine 2\nLine 3';
      const error = new SchedulerStoreError(message, ERROR_CODES.VALIDATION_ERROR);

      expect(error.message).toBe(message);
    });

    it('should handle error messages with special characters', () => {
      const message = 'Error: "Invalid value" (code=500) at /path/to/file.ts:123';
      const error = new SchedulerStoreError(message, ERROR_CODES.API_ERROR);

      expect(error.message).toBe(message);
    });

    it('should handle very long error messages', () => {
      const message = 'A'.repeat(1000);
      const error = new SchedulerStoreError(message, ERROR_CODES.NETWORK_ERROR);

      expect(error.message).toBe(message);
      expect(error.message.length).toBe(1000);
    });
  });

  describe('serialization', () => {
    it('should serialize basic properties to JSON', () => {
      const error = new SchedulerStoreError('Test error', ERROR_CODES.API_ERROR, {
        status: 500,
      });

      const serialized = JSON.stringify({
        name: error.name,
        message: error.message,
        code: error.code,
        details: error.details,
      });

      const parsed = JSON.parse(serialized);

      expect(parsed.name).toBe('SchedulerStoreError');
      expect(parsed.message).toBe('Test error');
      expect(parsed.code).toBe(ERROR_CODES.API_ERROR);
      expect(parsed.details).toEqual({ status: 500 });
    });

    it('should handle serialization of complex details', () => {
      const complexDetails = {
        timestamp: new Date('2024-01-01T00:00:00Z'),
        errors: ['error1', 'error2'],
        metadata: {
          version: '1.0.0',
          environment: 'test',
        },
      };

      const error = new SchedulerStoreError(
        'Complex',
        ERROR_CODES.VALIDATION_ERROR,
        complexDetails,
      );

      // JSON.stringify will convert Date to string
      const serialized = JSON.stringify({
        message: error.message,
        code: error.code,
        details: error.details,
      });

      expect(serialized).toContain('Complex');
      expect(serialized).toContain('VALIDATION_ERROR');
    });
  });

  describe('edge cases', () => {
    it('should handle throwing in async context', async () => {
      await expect(async () => {
        throw new SchedulerStoreError('Async error', ERROR_CODES.NETWORK_ERROR);
      }).rejects.toThrow(SchedulerStoreError);
    });

    it('should handle rethrowing', () => {
      try {
        throw new SchedulerStoreError('Original', ERROR_CODES.API_ERROR);
      } catch (error) {
        expect(error).toBeInstanceOf(SchedulerStoreError);
        if (error instanceof SchedulerStoreError) {
          expect(error.message).toBe('Original');
        }
      }
    });

    it('should handle wrapping other errors', () => {
      const originalError = new TypeError('Type mismatch');
      const wrappedError = new SchedulerStoreError('Wrapped error', ERROR_CODES.VALIDATION_ERROR, {
        originalError,
      });

      expect(wrappedError.details).toHaveProperty('originalError');
      expect((wrappedError.details as any).originalError).toBeInstanceOf(TypeError);
    });
  });

  describe('real-world usage scenarios', () => {
    it('should handle network error scenario', () => {
      const error = new SchedulerStoreError(
        'Failed to fetch scheduler configuration',
        ERROR_CODES.NETWORK_ERROR,
        {
          endpoint: '/ws/v1/cluster/scheduler',
          method: 'GET',
          timeout: 30000,
        },
      );

      expect(error.code).toBe(ERROR_CODES.NETWORK_ERROR);
      expect(error.message).toContain('fetch scheduler configuration');
      expect((error.details as any).endpoint).toBe('/ws/v1/cluster/scheduler');
    });

    it('should handle validation error scenario', () => {
      const error = new SchedulerStoreError(
        'Queue capacity validation failed',
        ERROR_CODES.VALIDATION_ERROR,
        {
          queuePath: 'root.production',
          field: 'capacity',
          value: '150',
          rule: 'max-capacity-exceeded',
        },
      );

      expect(error.code).toBe(ERROR_CODES.VALIDATION_ERROR);
      expect((error.details as any).queuePath).toBe('root.production');
    });

    it('should handle mutation blocked scenario', () => {
      const error = new SchedulerStoreError(
        'Configuration is in read-only mode',
        ERROR_CODES.MUTATION_BLOCKED,
        {
          readOnlyProperty: READ_ONLY_PROPERTY,
          instruction: `Set ${READ_ONLY_PROPERTY}=false to enable editing`,
        },
      );

      expect(error.code).toBe(ERROR_CODES.MUTATION_BLOCKED);
      expect((error.details as any).instruction).toContain('read-only.enable=false');
    });
  });
});
